package com.flowforge.worker.runtime;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
public class WorkerMetrics {

    private final MeterRegistry registry;
    private final Counter cacheReplays;
    private final Counter claimsSkipped;
    private final DistributionSummary batchSize;

    public WorkerMetrics(MeterRegistry registry, IdempotencyStore idempotency) {
        this.registry = registry;
        cacheReplays = Counter.builder("flowforge.worker.cache.replays")
                .description("Tasks answered from the Redis result cache instead of re-running").register(registry);
        claimsSkipped = Counter.builder("flowforge.worker.claims.skipped")
                .description("Attempts skipped because another worker holds the claim").register(registry);
        batchSize = DistributionSummary.builder("flowforge.worker.batch.size").register(registry);
        Gauge.builder("flowforge.worker.redis.degraded", idempotency, s -> s.isDegraded() ? 1 : 0)
                .description("1 while the Redis circuit breaker is open").register(registry);
    }

    void bindInFlight(Semaphore permits, int max) {
        Gauge.builder("flowforge.worker.in.flight", permits, p -> max - p.availablePermits())
                .description("Tasks currently executing on this worker").register(registry);
    }

    void batch(int size) {
        batchSize.record(size);
    }

    void cacheReplay() {
        cacheReplays.increment();
    }

    void claimSkipped() {
        claimsSkipped.increment();
    }

    void execution(String type, String outcome, long nanos) {
        Timer.builder("flowforge.worker.task.execution").tag("type", type).tag("outcome", outcome)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(2))
                .register(registry)
                .record(nanos, TimeUnit.NANOSECONDS);
    }
}
