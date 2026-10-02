package com.flowforge.worker.runtime;

import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.common.messaging.TaskResult;
import com.flowforge.common.messaging.Topics;
import com.flowforge.common.tracing.KafkaTraceContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@Component
public class WorkerListener {

    private static final Logger log = LoggerFactory.getLogger(WorkerListener.class);
    private static final Duration LEASE_GRACE = Duration.ofSeconds(2);
    /**
     * Phase-1 claim lifetime. MUST be shorter than the consumer session timeout (6s): if this worker dies
     * before STARTED is acknowledged, the claim must have expired by the time Kafka redelivers the record.
     */
    private static final Duration PENDING_CLAIM_TTL = Duration.ofSeconds(3);

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final TaskRunner runner;
    private final IdempotencyStore idempotency;
    private final WorkerMetrics metrics;
    private final KafkaTraceContext trace;
    private final Tracer tracer;
    private final Clock clock = Clock.systemUTC();
    private final String workerId;
    private final Semaphore inFlight;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public WorkerListener(KafkaTemplate<String, String> kafka, JsonMapper json, TaskRunner runner,
                          IdempotencyStore idempotency, WorkerMetrics metrics,
                          KafkaTraceContext trace, Tracer tracer,
                          @Value("${HOSTNAME:local}") String host,
                          @Value("${flowforge.worker.max-in-flight:1000}") int maxInFlight) {
        this.kafka = kafka;
        this.json = json;
        this.runner = runner;
        this.idempotency = idempotency;
        this.metrics = metrics;
        this.trace = trace;
        this.tracer = tracer;
        this.workerId = host + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.inFlight = new Semaphore(maxInFlight);
        metrics.bindInFlight(inFlight, maxInFlight);
        log.info("Worker {} ready; task types {}", workerId, runner.supportedTypes());
    }

    @KafkaListener(topics = Topics.TASKS, batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        metrics.batch(records.size());
        Instant now = clock.instant();
        List<TaskCommand> commands = new ArrayList<>(records.size());
        Map<TaskCommand, Span> spans = new IdentityHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            Span span = trace.startConsumerSpan("worker task", record.headers());
            try {
                TaskCommand c = json.readValue(record.value(), TaskCommand.class);
                span.name("worker " + c.type() + " " + c.taskId())
                        .tag("workflow.id", c.workflowId()).tag("task.id", c.taskId())
                        .tag("attempt", String.valueOf(c.attempt())).tag("worker.id", workerId);
                commands.add(c);
                spans.put(c, span);
            } catch (RuntimeException e) {
                log.error("Skipping unreadable task record {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), e.getMessage());
                span.error(e);
                span.end();
            }
        }

        // Phase 1 for the whole batch in ONE Redis round trip: result-cache lookup + short claim.
        List<TaskCommand> toRun = new ArrayList<>();
        List<CompletableFuture<?>> acks = new ArrayList<>();
        for (IdempotencyStore.Prepared p : idempotency.prepare(commands, workerId, PENDING_CLAIM_TTL)) {
            TaskCommand command = p.command();
            Span span = spans.get(command);
            switch (p.outcome()) {
                case CACHED -> {
                    log.info("{} already completed; replaying cached result for attempt {}",
                            command.idempotencyKey(), command.attempt());
                    metrics.cacheReplay();
                    span.tag("outcome", "replayed-from-cache");
                    acks.add(send(TaskResult.succeeded(command, workerId, p.cachedOutput(), now), span));
                    span.end();
                }
                case TAKEN -> {
                    log.debug("{} attempt {} already claimed by another worker", command.idempotencyKey(), command.attempt());
                    metrics.claimSkipped();
                    span.tag("outcome", "claim-held-elsewhere");
                    span.end();
                }
                case CLAIMED -> {
                    acks.add(send(TaskResult.started(command, workerId, now.plus(lease(command)), now), span));
                    toRun.add(command);
                }
            }
        }

        // STARTED must be durable before we return and Kafka commits these offsets.
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).join();

        // Phase 2 for the whole batch in ONE round trip: the orchestrator's lease now covers these attempts.
        idempotency.extendClaims(toRun, workerId, WorkerListener::lease);

        for (TaskCommand command : toRun) {
            Span span = spans.get(command);
            inFlight.acquireUninterruptibly();   // backpressure: blocks the consumer when saturated
            executor.execute(() -> {
                try (Tracer.SpanInScope ws = tracer.withSpan(span)) {
                    execute(command, span);
                } finally {
                    span.end();
                    inFlight.release();
                }
            });
        }
    }

    private static Duration lease(TaskCommand command) {
        return Duration.ofMillis(command.timeoutMillis()).plus(LEASE_GRACE);
    }

    private void execute(TaskCommand command, Span span) {
        TaskResult result;
        String outcome;
        long startNanos = System.nanoTime();
        try {
            Map<String, String> output = runner.run(command);
            outcome = "success";
            idempotency.markCompleted(command.idempotencyKey(), output);   // never throws: degrades if Redis is down
            result = TaskResult.succeeded(command, workerId, output, clock.instant());
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            outcome = e instanceof TaskTimeoutException ? "timeout" : "failure";
            span.error(e);
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            result = TaskResult.failed(command, workerId, error, clock.instant());
        }
        span.tag("outcome", outcome);
        metrics.execution(command.type(), outcome, System.nanoTime() - startNanos);
        send(result, span).whenComplete((ok, err) -> {
            if (err != null) log.warn("Could not report result for {}; lease expiry will retry it",
                    command.idempotencyKey(), err);
        });
    }

    private CompletableFuture<?> send(TaskResult result, Span span) {
        return kafka.send(trace.record(Topics.TASK_RESULTS, result.workflowId(), json.writeValueAsString(result), span));
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
