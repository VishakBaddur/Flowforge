package com.flowforge.orchestrator.engine;

import com.flowforge.common.dag.InvalidWorkflowException;
import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.*;
import com.flowforge.common.messaging.DeadLetter;
import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.common.messaging.TaskResult;
import com.flowforge.common.messaging.WorkflowCommand;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.state.Decision;
import com.flowforge.common.state.TaskState;
import com.flowforge.common.state.WorkflowDecider;
import com.flowforge.common.state.WorkflowState;
import com.flowforge.orchestrator.store.ConcurrencyConflictException;
import com.flowforge.orchestrator.store.EventStore;
import com.flowforge.orchestrator.store.PendingAppend;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import com.flowforge.orchestrator.store.WorkflowRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Owns the workflows of the partitions assigned to this instance.
 * Every input (command, task result, timer) goes through execute(): lock -> decide -> persist -> side effects.
 */
@Service
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);
    private static final int LOCK_STRIPES = 1024;
    private static final int MAX_CONFLICT_RETRIES = 3;
    private static final Duration REPUBLISH_DELAY = Duration.ofSeconds(1);
    /** After a takeover, results produced while the partition had no owner are still unread in Kafka. */
    private static final Duration RECOVERY_LEASE_GRACE = Duration.ofSeconds(10);

    private final WorkflowRepository repository;
    private final EventStore store;
    private final TaskPublisher publisher;
    private final Clock clock;
    private final EngineMetrics metrics;
    private final Tracer tracer;
    private final WorkflowDecider decider = WorkflowDecider.withRandomJitter();

    private final Map<String, WorkflowState> cache = new ConcurrentHashMap<>();
    private final Set<Integer> ownedPartitions = ConcurrentHashMap.newKeySet();
    private final Object[] locks = new Object[LOCK_STRIPES];
    private final ScheduledExecutorService timers =
            Executors.newScheduledThreadPool(4, Thread.ofPlatform().name("ff-timer-", 0).daemon().factory());

    public WorkflowEngine(WorkflowRepository repository, EventStore store, TaskPublisher publisher, Clock clock,
                          EngineMetrics metrics, Tracer tracer) {
        this.tracer = tracer;
        this.repository = repository;
        this.store = store;
        this.publisher = publisher;
        this.clock = clock;
        this.metrics = metrics;
        metrics.bindActiveWorkflows(cache);
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
    }

    // ------------------------------------------------------------------ inputs

    /** One unit of work for a workflow: what to decide once its state is loaded. */
    public record Input(String workflowId, boolean createIfMissing, Function<WorkflowState, Decision> action, Span span) {
        public Input withSpan(Span s) {
            return new Input(workflowId, createIfMissing, action, s);
        }
    }

    public Input inputFor(WorkflowCommand command) {
        Instant now = clock.instant();
        return switch (command.kind()) {
            case START -> new Input(command.workflowId(), true,
                    s -> decider.start(s, command.definition(), command.owner(), now), null);
            case CANCEL -> new Input(command.workflowId(), false, s -> decider.cancel(s, command.reason(), now), null);
        };
    }

    public Input inputFor(TaskResult r) {
        Instant now = clock.instant();
        return switch (r.kind()) {
            case STARTED -> new Input(r.workflowId(), false,
                    s -> decider.taskStarted(s, r.taskId(), r.attempt(), r.workerId(), r.leaseExpiresAt(), now), null);
            case SUCCEEDED -> new Input(r.workflowId(), false,
                    s -> decider.taskSucceeded(s, r.taskId(), r.attempt(), r.output(), now), null);
            case FAILED -> new Input(r.workflowId(), false,
                    s -> decider.taskFailed(s, r.taskId(), r.attempt(), r.error(), now), null);
        };
    }

    public void handle(WorkflowCommand command) {
        run(inputFor(command));
    }

    public void handle(TaskResult result) {
        run(inputFor(result));
    }

    private void run(Input in) {
        execute(in.workflowId(), in.createIfMissing(), in.action());
    }

    /**
     * Batched path: decide every input of a Kafka poll in memory, commit ALL resulting events in ONE
     * Postgres transaction, then publish. Holds the stripe locks of every workflow involved, acquired in
     * ascending order so concurrent batches and timers cannot deadlock. On ANY failure the transaction rolls
     * back, the in-memory changes are discarded, and the batch is replayed one input at a time (the
     * per-message path proven by the failure tests).
     */
    public void handleBatch(List<Input> inputs) {
        if (inputs.isEmpty()) return;
        Map<String, List<Input>> byWorkflow = new LinkedHashMap<>();
        for (Input in : inputs) byWorkflow.computeIfAbsent(in.workflowId(), k -> new ArrayList<>()).add(in);
        List<Object> stripes = byWorkflow.keySet().stream()
                .map(this::stripeOf).distinct().sorted().map(i -> locks[i]).toList();

        Map<String, Span> spanOf = new HashMap<>();
        for (Input in : inputs) if (in.span() != null) spanOf.put(in.workflowId(), in.span());
        boolean[] fallback = {false};
        withLocks(stripes, 0, () -> {
            List<PendingAppend> pending = new ArrayList<>();
            try {
                for (Map.Entry<String, List<Input>> entry : byWorkflow.entrySet()) {
                    String id = entry.getKey();
                    boolean create = entry.getValue().stream().anyMatch(Input::createIfMissing);
                    WorkflowState state = cache.get(id);
                    if (state == null) state = repository.load(id).orElse(create ? new WorkflowState(id) : null);
                    if (state == null) {
                        log.warn("Ignoring input for unknown workflow {}", id);
                        continue;
                    }
                    List<WorkflowEvent> events = new ArrayList<>();
                    for (Input in : entry.getValue()) {
                        Decision d = in.action().apply(state);
                        if (d.isIgnored()) {
                            metrics.ignored(d.ignoredReason());
                            if (in.span() != null) in.span().tag("ignored", d.ignoredReason());
                        }
                        else events.addAll(d.events());
                    }
                    if (events.isEmpty()) cacheIfActive(state);
                    else pending.add(new PendingAppend(state, events));
                }
                metrics.timeAppend(() -> store.appendAll(pending));
            } catch (RuntimeException e) {
                byWorkflow.keySet().forEach(cache::remove);   // in-memory state may hold uncommitted events
                log.info("Batch of {} inputs not committed ({}: {}); replaying one at a time",
                        inputs.size(), e.getClass().getSimpleName(), e.getMessage());
                fallback[0] = true;
                return;
            }
            for (PendingAppend p : pending) {
                cacheIfActive(p.state());
                metrics.recordEvents(p.state(), p.events());
                // Publish follow-ups inside the span of the input that caused them, so traceparent flows on.
                try (Tracer.SpanInScope ws = tracer.withSpan(spanOf.get(p.state().workflowId()))) {
                    afterCommit(p.state(), p.events());
                }
            }
        });
        try {
            if (fallback[0]) {
                for (Input in : inputs) {
                    try (Tracer.SpanInScope ws = tracer.withSpan(in.span())) {
                        run(in);
                    }
                }
            }
        } finally {
            for (Input in : inputs) {
                if (in.span() != null) {
                    in.span().tag("batch.size", String.valueOf(inputs.size()));
                    in.span().end();
                }
            }
        }
    }

    /** Acquires monitors in list order (callers pass ascending stripe order), then runs body. */
    private static void withLocks(List<Object> locks, int i, Runnable body) {
        if (i == locks.size()) {
            body.run();
            return;
        }
        synchronized (locks.get(i)) {
            withLocks(locks, i + 1, body);
        }
    }

    // ------------------------------------------------------- ownership/recovery

    /** Called on the consumer thread before any record from these partitions is processed. */
    public void onPartitionsAssigned(Collection<Integer> partitions) {
        if (partitions.isEmpty()) return;
        long startNanos = System.nanoTime();
        Set<Integer> assigned = Set.copyOf(partitions);
        ownedPartitions.addAll(assigned);

        List<String> ids = store.findRunningWorkflowIds().stream()
                .filter(id -> assigned.contains(Partitions.of(id)))
                .toList();
        Map<String, WorkflowState> states = repository.loadAll(ids);
        states.forEach((id, state) -> {
            synchronized (lockFor(id)) {
                cache.put(id, state);
                rearm(state);
            }
        });
        metrics.recovery(System.nanoTime() - startNanos, states.size());
        log.info("Recovered {} running workflows for partitions {} in {} ms",
                states.size(), new TreeSet<>(assigned), (System.nanoTime() - startNanos) / 1_000_000);
    }

    public void onPartitionsRevoked(Collection<Integer> partitions) {
        if (partitions.isEmpty()) return;
        ownedPartitions.removeAll(partitions);
        cache.keySet().removeIf(id -> partitions.contains(Partitions.of(id)));
        log.info("Released partitions {}", new TreeSet<>(partitions));
    }

    private boolean owns(String workflowId) {
        return ownedPartitions.contains(Partitions.of(workflowId));
    }

    // -------------------------------------------------------------------- core

    private void execute(String workflowId, boolean createIfMissing, Function<WorkflowState, Decision> action) {
        synchronized (lockFor(workflowId)) {
            for (int attempt = 1; ; attempt++) {
                WorkflowState state = cache.get(workflowId);
                if (state == null) {
                    state = repository.load(workflowId).orElse(null);
                    if (state == null) {
                        if (!createIfMissing) {
                            log.warn("Ignoring input for unknown workflow {}", workflowId);
                            return;
                        }
                        state = new WorkflowState(workflowId);
                    }
                }

                Decision decision;
                try {
                    decision = action.apply(state);
                    if (decision.isIgnored()) {
                        log.debug("Ignored input for {}: {}", workflowId, decision.ignoredReason());
                        metrics.ignored(decision.ignoredReason());
                        cacheIfActive(state);
                        return;
                    }
                    WorkflowState toSave = state;
                    Decision toPersist = decision;
                    metrics.timeAppend(() -> store.append(toSave, toPersist.events()));
                } catch (ConcurrencyConflictException e) {
                    cache.remove(workflowId);
                    metrics.conflict();
                    if (attempt >= MAX_CONFLICT_RETRIES) throw e;
                    log.info("Concurrency conflict on {}; reloading (attempt {})", workflowId, attempt);
                    continue;
                } catch (InvalidWorkflowException | IllegalArgumentException e) {
                    cache.remove(workflowId);
                    log.warn("Rejected input for workflow {}: {}", workflowId, e.getMessage());
                    return;
                } catch (RuntimeException e) {
                    cache.remove(workflowId);   // in-memory state may contain unsaved changes
                    throw e;
                }

                cacheIfActive(state);
                metrics.recordEvents(state, decision.events());
                afterCommit(state, decision.events());
                return;
            }
        }
    }

    private void cacheIfActive(WorkflowState state) {
        if (state.status() != null && !state.status().isTerminal()) {
            cache.put(state.workflowId(), state);
        } else {
            cache.remove(state.workflowId());
        }
    }

    /** Side effects run only after the events are durably committed. */
    private void afterCommit(WorkflowState state, List<WorkflowEvent> events) {
        String id = state.workflowId();
        for (WorkflowEvent event : events) {
            switch (event) {
                case TaskQueued e -> publishTask(state, e.taskId(), e.attempt());
                case TaskStarted e -> scheduleLeaseCheck(id, e.taskId(), e.attempt(), e.leaseExpiresAt());
                case TaskFailed e -> scheduleRetry(id, e.taskId(), e.retryAt());
                case TaskDeadLettered e -> publishDeadLetter(state, e);
                case WorkflowCompleted e -> log.info("Workflow {} COMPLETED", id);
                case WorkflowFailed e -> log.info("Workflow {} FAILED: {}", id, e.reason());
                case WorkflowCancelled e -> log.info("Workflow {} CANCELLED: {}", id, e.reason());
                default -> { }
            }
        }
    }

    /** After recovery: re-send queued work and re-arm every pending timer. */
    private void rearm(WorkflowState state) {
        String id = state.workflowId();
        for (TaskState t : state.tasks().values()) {
            switch (t.status()) {
                case QUEUED -> publishTask(state, t.taskId(), t.attempt());
                // Don't expire leases until the result backlog has had a chance to drain.
                case RUNNING -> scheduleLeaseCheck(id, t.taskId(), t.attempt(),
                        latest(t.leaseExpiresAt(), clock.instant().plus(RECOVERY_LEASE_GRACE)));
                case RETRY_WAIT -> scheduleRetry(id, t.taskId(), t.retryAt());
                default -> { }
            }
        }
    }

    // ------------------------------------------------------------- publishing

    private void publishTask(WorkflowState state, String taskId, int attempt) {
        TaskDefinition def = state.dag().task(taskId);
        TaskCommand command = new TaskCommand(state.workflowId(), taskId, attempt, def.type(), def.input(),
                def.timeout().toMillis(), clock.instant());
        publisher.sendTask(command).whenComplete((result, error) -> {
            if (error != null) {
                log.warn("Publish failed for {}/{} attempt {}; retrying in {}",
                        state.workflowId(), taskId, attempt, REPUBLISH_DELAY, error);
                scheduleRepublish(state.workflowId(), taskId, attempt);
            }
        });
    }

    private void publishDeadLetter(WorkflowState state, TaskDeadLettered e) {
        TaskDefinition def = state.dag().task(e.taskId());
        publisher.sendDeadLetter(new DeadLetter(state.workflowId(), e.taskId(), e.attempt(), def.type(),
                def.input(), e.error(), e.occurredAt()));
        log.warn("Task {}/{} dead-lettered after {} attempts: {}", state.workflowId(), e.taskId(), e.attempt(), e.error());
    }

    // ----------------------------------------------------------------- timers

    private void scheduleRetry(String workflowId, String taskId, Instant at) {
        schedule(at, () -> {
            if (!owns(workflowId)) return;
            execute(workflowId, false, s -> {
                Instant now = clock.instant();
                TaskState t = s.task(taskId);
                if (t.status() == TaskStatus.RETRY_WAIT && now.isBefore(t.retryAt())) {
                    scheduleRetry(workflowId, taskId, t.retryAt());   // fired early; try again at retryAt
                }
                return decider.retryDue(s, taskId, now);
            });
        });
    }

    private void scheduleLeaseCheck(String workflowId, String taskId, int attempt, Instant expiresAt) {
        schedule(expiresAt, () -> {
            if (!owns(workflowId)) return;
            execute(workflowId, false, s -> {
                Instant now = clock.instant();
                TaskState t = s.task(taskId);
                if (t.status() == TaskStatus.RUNNING && t.attempt() == attempt && now.isBefore(t.leaseExpiresAt())) {
                    scheduleLeaseCheck(workflowId, taskId, attempt, t.leaseExpiresAt());
                }
                return decider.leaseExpired(s, taskId, attempt, now);
            });
        });
    }

    private void scheduleRepublish(String workflowId, String taskId, int attempt) {
        schedule(clock.instant().plus(REPUBLISH_DELAY), () -> {
            if (!owns(workflowId)) return;
            synchronized (lockFor(workflowId)) {
                WorkflowState state = cache.get(workflowId);
                if (state != null && state.task(taskId).status() == TaskStatus.QUEUED
                        && state.task(taskId).attempt() == attempt) {
                    publishTask(state, taskId, attempt);
                }
            }
        });
    }

    private void schedule(Instant at, Runnable task) {
        // Round UP to whole milliseconds. Truncating fired timers up to 1ms early; the "not due yet" path then
        // rescheduled with a truncated 0ms delay and spun until the deadline passed (seen as timer_early in metrics).
        long delayMs = Math.max(0, Duration.between(clock.instant(), at).plusNanos(999_999).toMillis());
        timers.schedule(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                log.error("Timer task failed", e);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private static Instant latest(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }

    private int stripeOf(String workflowId) {
        return Math.floorMod(workflowId.hashCode(), LOCK_STRIPES);
    }

    private Object lockFor(String workflowId) {
        return locks[stripeOf(workflowId)];
    }

    @PreDestroy
    void shutdown() {
        timers.shutdownNow();
    }
}
