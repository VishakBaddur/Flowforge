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
import com.flowforge.orchestrator.store.WorkflowRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
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

    private final WorkflowRepository repository;
    private final EventStore store;
    private final TaskPublisher publisher;
    private final Clock clock;
    private final WorkflowDecider decider = WorkflowDecider.withRandomJitter();

    private final Map<String, WorkflowState> cache = new ConcurrentHashMap<>();
    private final Set<Integer> ownedPartitions = ConcurrentHashMap.newKeySet();
    private final Object[] locks = new Object[LOCK_STRIPES];
    private final ScheduledExecutorService timers =
            Executors.newScheduledThreadPool(4, Thread.ofPlatform().name("ff-timer-", 0).daemon().factory());

    public WorkflowEngine(WorkflowRepository repository, EventStore store, TaskPublisher publisher, Clock clock) {
        this.repository = repository;
        this.store = store;
        this.publisher = publisher;
        this.clock = clock;
        for (int i = 0; i < LOCK_STRIPES; i++) locks[i] = new Object();
    }

    // ------------------------------------------------------------------ inputs

    public void handle(WorkflowCommand command) {
        Instant now = clock.instant();
        switch (command.kind()) {
            case START -> execute(command.workflowId(), true, s -> decider.start(s, command.definition(), command.owner(), now));
            case CANCEL -> execute(command.workflowId(), false, s -> decider.cancel(s, command.reason(), now));
        }
    }

    public void handle(TaskResult r) {
        Instant now = clock.instant();
        switch (r.kind()) {
            case STARTED -> execute(r.workflowId(), false,
                    s -> decider.taskStarted(s, r.taskId(), r.attempt(), r.workerId(), r.leaseExpiresAt(), now));
            case SUCCEEDED -> execute(r.workflowId(), false,
                    s -> decider.taskSucceeded(s, r.taskId(), r.attempt(), r.output(), now));
            case FAILED -> execute(r.workflowId(), false,
                    s -> decider.taskFailed(s, r.taskId(), r.attempt(), r.error(), now));
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
                        cacheIfActive(state);
                        return;
                    }
                    store.append(state, decision.events());
                } catch (ConcurrencyConflictException e) {
                    cache.remove(workflowId);
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
                case RUNNING -> scheduleLeaseCheck(id, t.taskId(), t.attempt(), t.leaseExpiresAt());
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
        long delayMs = Math.max(0, Duration.between(clock.instant(), at).toMillis());
        timers.schedule(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                log.error("Timer task failed", e);
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private Object lockFor(String workflowId) {
        return locks[Math.floorMod(workflowId.hashCode(), LOCK_STRIPES)];
    }

    @PreDestroy
    void shutdown() {
        timers.shutdownNow();
    }
}
