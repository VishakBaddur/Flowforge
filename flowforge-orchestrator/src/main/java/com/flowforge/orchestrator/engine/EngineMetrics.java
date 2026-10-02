package com.flowforge.orchestrator.engine;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.*;
import com.flowforge.common.state.WorkflowState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Workflow-level metrics, derived from the events each decision commits. */
@Component
public class EngineMetrics {

    private static final List<String> TASK_EVENTS =
            List.of("queued", "started", "succeeded", "retry_scheduled", "dead_lettered", "skipped");

    private final MeterRegistry registry;
    private final Map<String, Counter> taskEvents = new HashMap<>();
    private final Counter workflowsStarted;
    private final Counter conflicts;
    private final Timer dispatchLatency;
    private final Timer turnaround;
    private final Timer append;
    private final Timer recovery;
    private final DistributionSummary recoveredWorkflows;

    public EngineMetrics(MeterRegistry registry) {
        this.registry = registry;
        for (String e : TASK_EVENTS) {
            taskEvents.put(e, Counter.builder("flowforge.task.events").tag("event", e)
                    .description("Task lifecycle events").register(registry));
        }
        workflowsStarted = Counter.builder("flowforge.workflows.started").register(registry);
        conflicts = Counter.builder("flowforge.concurrency.conflicts")
                .description("Optimistic-concurrency conflicts on the event store").register(registry);
        dispatchLatency = histogram("flowforge.task.dispatch.latency", "TaskQueued -> TaskStarted (queue wait + Kafka hops)");
        turnaround = histogram("flowforge.task.turnaround", "TaskQueued -> TaskSucceeded for one attempt");
        append = histogram("flowforge.eventstore.append", "Postgres transaction time for one decision");
        recovery = histogram("flowforge.recovery.duration", "State rebuild time after partitions are assigned");
        recoveredWorkflows = DistributionSummary.builder("flowforge.recovery.workflows")
                .description("Workflows rebuilt per recovery").register(registry);
    }

    private Timer histogram(String name, String description) {
        return Timer.builder(name).description(description)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(2))
                .register(registry);
    }

    void bindActiveWorkflows(Map<?, ?> cache) {
        Gauge.builder("flowforge.workflows.active", cache, Map::size)
                .description("Running workflows owned by this instance").register(registry);
    }

    void timeAppend(Runnable write) {
        append.record(write);
    }

    void conflict() {
        conflicts.increment();
    }

    void recovery(long nanos, int workflows) {
        recovery.record(nanos, TimeUnit.NANOSECONDS);
        recoveredWorkflows.record(workflows);
    }

    void ignored(String reason) {
        registry.counter("flowforge.inputs.ignored", "kind", classify(reason)).increment();
    }

    static String classify(String reason) {
        if (reason == null) return "other";
        if (reason.startsWith("stale attempt")) return "stale_attempt";
        if (reason.contains("already started")) return "duplicate_start";
        if (reason.startsWith("workflow is") || reason.startsWith("workflow not")) return "workflow_not_running";
        if (reason.contains("not due") || reason.contains("valid until")) return "timer_early";
        if (reason.startsWith("task ")) return "duplicate_or_late";
        return "other";
    }

    void recordEvents(WorkflowState state, List<WorkflowEvent> events) {
        for (WorkflowEvent event : events) {
            switch (event) {
                case WorkflowStarted e -> workflowsStarted.increment();
                case TaskQueued e -> taskEvents.get("queued").increment();
                case TaskStarted e -> {
                    taskEvents.get("started").increment();
                    since(state.task(e.taskId()).queuedAt(), e.occurredAt(), dispatchLatency);
                }
                case TaskSucceeded e -> {
                    taskEvents.get("succeeded").increment();
                    since(state.task(e.taskId()).queuedAt(), e.occurredAt(), turnaround);
                }
                case TaskFailed e -> taskEvents.get("retry_scheduled").increment();
                case TaskDeadLettered e -> taskEvents.get("dead_lettered").increment();
                case TaskSkipped e -> taskEvents.get("skipped").increment();
                case WorkflowCompleted e -> finished(state, "COMPLETED", e.occurredAt());
                case WorkflowFailed e -> finished(state, "FAILED", e.occurredAt());
                case WorkflowCancelled e -> finished(state, "CANCELLED", e.occurredAt());
            }
        }
    }

    private void finished(WorkflowState state, String status, Instant at) {
        registry.counter("flowforge.workflows.finished", "status", status).increment();
        Timer.builder("flowforge.workflow.duration").tag("status", status)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(10))
                .register(registry)
                .record(Duration.between(state.startedAt(), at));
    }

    private static void since(Instant from, Instant to, Timer timer) {
        if (from != null && !to.isBefore(from)) timer.record(Duration.between(from, to));
    }
}
