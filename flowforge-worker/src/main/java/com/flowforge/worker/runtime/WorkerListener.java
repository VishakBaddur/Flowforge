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

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final TaskRunner runner;
    private final IdempotencyStore idempotency;
    private final Clock clock = Clock.systemUTC();
    private final String workerId;
    private final Semaphore inFlight;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public WorkerListener(KafkaTemplate<String, String> kafka, JsonMapper json, TaskRunner runner,
                          IdempotencyStore idempotency,
                          @Value("${HOSTNAME:local}") String host,
                          @Value("${flowforge.worker.max-in-flight:1000}") int maxInFlight) {
        this.kafka = kafka;
        this.json = json;
        this.runner = runner;
        this.idempotency = idempotency;
        this.workerId = host + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.inFlight = new Semaphore(maxInFlight);
        log.info("Worker {} ready; task types {}", workerId, runner.supportedTypes());
    }

    @KafkaListener(topics = Topics.TASKS, batch = "true")
    public void onBatch(List<ConsumerRecord<String, String>> records) {
        Instant now = clock.instant();
        List<TaskCommand> toRun = new ArrayList<>();
        List<CompletableFuture<?>> acks = new ArrayList<>();

        for (ConsumerRecord<String, String> record : records) {
            TaskCommand command = json.readValue(record.value(), TaskCommand.class);

            Optional<Map<String, String>> done = idempotency.completedOutput(command.idempotencyKey());
            if (done.isPresent()) {
                log.info("{} already completed; replaying cached result for attempt {}",
                        command.idempotencyKey(), command.attempt());
                acks.add(send(TaskResult.succeeded(command, workerId, done.get(), now)));
                continue;
            }

            Duration lease = Duration.ofMillis(command.timeoutMillis()).plus(LEASE_GRACE);
            if (!idempotency.tryClaim(command.idempotencyKey(), command.attempt(), workerId, lease)) {
                log.debug("{} attempt {} already claimed by another worker", command.idempotencyKey(), command.attempt());
                continue;
            }
            acks.add(send(TaskResult.started(command, workerId, now.plus(lease), now)));
            toRun.add(command);
        }

        // STARTED must be durable before we return and Kafka commits these offsets.
        CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).join();

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

    private void execute(TaskCommand command) {
        TaskResult result;
        try {
            Map<String, String> output = runner.run(command);
            idempotency.markCompleted(command.idempotencyKey(), output);
            result = TaskResult.succeeded(command, workerId, output, clock.instant());
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            result = TaskResult.failed(command, workerId, error, clock.instant());
        }
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
