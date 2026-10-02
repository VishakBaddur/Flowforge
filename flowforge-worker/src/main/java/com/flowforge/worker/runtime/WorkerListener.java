package com.flowforge.worker.runtime;

import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.common.messaging.TaskResult;
import com.flowforge.common.messaging.Topics;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final Clock clock = Clock.systemUTC();
    private final String workerId;
    private final Semaphore inFlight;
    private final WorkerMetrics metrics;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public WorkerListener(KafkaTemplate<String, String> kafka, JsonMapper json, TaskRunner runner,
                          IdempotencyStore idempotency, WorkerMetrics metrics,
                          @Value("${HOSTNAME:local}") String host,
                          @Value("${flowforge.worker.max-in-flight:1000}") int maxInFlight) {
        this.kafka = kafka;
        this.json = json;
        this.runner = runner;
        this.idempotency = idempotency;
        this.workerId = host + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.inFlight = new Semaphore(maxInFlight);
        this.metrics = metrics;
        metrics.bindInFlight(inFlight, maxInFlight);
        log.info("Worker {} ready; task types {}", workerId, runner.supportedTypes());
    }

    @KafkaListener(topics = Topics.TASKS, batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        metrics.batch(records.size());
        Instant now = clock.instant();
        List<TaskCommand> commands = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            try {
                commands.add(json.readValue(record.value(), TaskCommand.class));
            } catch (RuntimeException e) {
                log.error("Skipping unreadable task record {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), e.getMessage());
            }
        }

        // Phase 1 for the whole batch in ONE Redis round trip: result-cache lookup + short claim.
        List<TaskCommand> toRun = new ArrayList<>();
        List<CompletableFuture<?>> acks = new ArrayList<>();
        for (IdempotencyStore.Prepared p : idempotency.prepare(commands, workerId, PENDING_CLAIM_TTL)) {
            TaskCommand command = p.command();
            switch (p.outcome()) {
                case CACHED -> {
                    log.info("{} already completed; replaying cached result for attempt {}",
                            command.idempotencyKey(), command.attempt());
                    metrics.cacheReplay();
                    acks.add(send(TaskResult.succeeded(command, workerId, p.cachedOutput(), now)));
                }
                case TAKEN -> {
                    log.debug("{} attempt {} already claimed by another worker", command.idempotencyKey(), command.attempt());
                    metrics.claimSkipped();
                }
                case CLAIMED -> {
                    acks.add(send(TaskResult.started(command, workerId, now.plus(lease(command)), now)));
                    toRun.add(command);
                }
            }
        }

        // STARTED must be durable before we return and Kafka commits these offsets.
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).join();

        // Phase 2 for the whole batch in ONE round trip: the orchestrator's lease now covers these attempts.
        idempotency.extendClaims(toRun, workerId, WorkerListener::lease);

        for (TaskCommand command : toRun) {
            inFlight.acquireUninterruptibly();   // backpressure: blocks the consumer when saturated
            executor.execute(() -> {
                try {
                    execute(command);
                } finally {
                    inFlight.release();
                }
            });
        }
    }

    private static Duration lease(TaskCommand command) {
        return Duration.ofMillis(command.timeoutMillis()).plus(LEASE_GRACE);
    }

    private void execute(TaskCommand command) {
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
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            result = TaskResult.failed(command, workerId, error, clock.instant());
        }
        metrics.execution(command.type(), outcome, System.nanoTime() - startNanos);
        send(result).whenComplete((ok, err) -> {
            if (err != null) log.warn("Could not report result for {}; lease expiry will retry it",
                    command.idempotencyKey(), err);
        });
    }

    private CompletableFuture<?> send(TaskResult result) {
        return kafka.send(Topics.TASK_RESULTS, result.workflowId(), json.writeValueAsString(result));
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
