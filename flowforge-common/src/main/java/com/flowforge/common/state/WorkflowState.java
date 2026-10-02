package com.flowforge.common.state;

import com.flowforge.common.dag.Dag;
import com.flowforge.common.dag.DagValidator;
import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.TaskDeadLettered;
import com.flowforge.common.event.WorkflowEvent.TaskFailed;
import com.flowforge.common.event.WorkflowEvent.TaskQueued;
import com.flowforge.common.event.WorkflowEvent.TaskSkipped;
import com.flowforge.common.event.WorkflowEvent.TaskStarted;
import com.flowforge.common.event.WorkflowEvent.TaskSucceeded;
import com.flowforge.common.event.WorkflowEvent.WorkflowCancelled;
import com.flowforge.common.event.WorkflowEvent.WorkflowCompleted;
import com.flowforge.common.event.WorkflowEvent.WorkflowFailed;
import com.flowforge.common.event.WorkflowEvent.WorkflowStarted;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.model.WorkflowDefinition;
import com.flowforge.common.model.WorkflowStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.flowforge.common.model.TaskStatus.*;

/**
 * Event-sourced aggregate: current state = fold(apply, events).
 * apply() is the single place state changes, and it rejects illegal transitions.
 */
public final class WorkflowState {

    private final String workflowId;
    private final Map<String, TaskState> tasks = new LinkedHashMap<>();
    private WorkflowDefinition definition;
    private String owner;
    private Dag dag;
    private WorkflowStatus status;
    private long version;           // number of events applied == sequence of the last event
    private Instant startedAt;
    private Instant finishedAt;

    public WorkflowState(String workflowId) {
        this.workflowId = workflowId;
    }

    public static WorkflowState replay(String workflowId, List<? extends WorkflowEvent> events) {
        WorkflowState state = new WorkflowState(workflowId);
        events.forEach(state::apply);
        return state;
    }

    public void apply(WorkflowEvent event) {
        if (!workflowId.equals(event.workflowId()))
            throw new IllegalArgumentException("event for " + event.workflowId() + " applied to " + workflowId);
        if (definition == null && !(event instanceof WorkflowStarted))
            throw new IllegalStateException("first event must be WorkflowStarted, got " + event.type());
        if (status != null && status.isTerminal())
            throw new IllegalStateException("workflow is " + status + "; rejected " + event.type());

        switch (event) {
            case WorkflowStarted e -> {
                if (definition != null) throw new IllegalStateException("workflow already started");
                definition = e.definition();
                owner = e.owner();
                dag = DagValidator.validate(definition);
                status = WorkflowStatus.RUNNING;
                startedAt = e.occurredAt();
                definition.tasks().forEach(t -> tasks.put(t.id(), TaskState.pending(t.id())));
            }
            case TaskQueued e -> {
                int expected = task(e.taskId()).attempt() + 1;
                if (e.attempt() != expected)
                    throw new IllegalStateException("task " + e.taskId() + " queued as attempt " + e.attempt()
                            + ", expected " + expected);
                transition(e.taskId(), Set.of(PENDING, RETRY_WAIT), t -> t.queued(e.attempt(), e.occurredAt()));
            }
            case TaskStarted e -> {
                checkAttempt(e.taskId(), e.attempt());
                transition(e.taskId(), Set.of(QUEUED), t -> t.running(e.workerId(), e.leaseExpiresAt()));
            }
            case TaskSucceeded e -> {
                checkAttempt(e.taskId(), e.attempt());
                transition(e.taskId(), Set.of(QUEUED, RUNNING), t -> t.succeeded(e.output()));
            }
            case TaskFailed e -> {
                checkAttempt(e.taskId(), e.attempt());
                transition(e.taskId(), Set.of(QUEUED, RUNNING), t -> t.retryWait(e.error(), e.retryAt()));
            }
            case TaskDeadLettered e -> {
                checkAttempt(e.taskId(), e.attempt());
                transition(e.taskId(), Set.of(QUEUED, RUNNING), t -> t.deadLettered(e.error()));
            }
            case TaskSkipped e -> transition(e.taskId(), Set.of(PENDING), t -> t.skipped(e.reason()));
            case WorkflowCompleted e -> {
                if (!tasks.values().stream().allMatch(t -> t.status() == SUCCEEDED))
                    throw new IllegalStateException("cannot complete: not every task succeeded");
                status = WorkflowStatus.COMPLETED;
                finishedAt = e.occurredAt();
            }
            case WorkflowFailed e -> {
                if (!allTasksTerminal())
                    throw new IllegalStateException("cannot fail workflow while tasks are still active");
                status = WorkflowStatus.FAILED;
                finishedAt = e.occurredAt();
            }
            case WorkflowCancelled e -> {
                status = WorkflowStatus.CANCELLED;
                finishedAt = e.occurredAt();
            }
        }
        version++;
    }

    private void transition(String taskId, Set<TaskStatus> allowedFrom, UnaryOperator<TaskState> change) {
        TaskState current = task(taskId);
        if (!allowedFrom.contains(current.status()))
            throw new IllegalStateException("task " + taskId + " is " + current.status()
                    + "; expected one of " + allowedFrom);
        tasks.put(taskId, change.apply(current));
    }

    private void checkAttempt(String taskId, int attempt) {
        int current = task(taskId).attempt();
        if (attempt != current)
            throw new IllegalStateException("stale attempt " + attempt + " for task " + taskId + " (current " + current + ")");
    }

    // ---- queries ----

    public TaskState task(String taskId) {
        TaskState t = tasks.get(taskId);
        if (t == null) throw new IllegalArgumentException("unknown task '" + taskId + "'");
        return t;
    }

    public Map<String, TaskState> tasks() {
        return Collections.unmodifiableMap(tasks);
    }

    public Set<String> succeededTaskIds() {
        return tasks.values().stream().filter(t -> t.status() == SUCCEEDED)
                .map(TaskState::taskId).collect(Collectors.toUnmodifiableSet());
    }

    public boolean allTasksTerminal() {
        return tasks.values().stream().allMatch(t -> t.status().isTerminal());
    }

    public boolean anyDeadLettered() {
        return tasks.values().stream().anyMatch(t -> t.status() == DEAD_LETTERED);
    }

    public String workflowId() { return workflowId; }
    public WorkflowDefinition definition() { return definition; }
    public String owner() { return owner; }
    public Dag dag() { return dag; }
    public WorkflowStatus status() { return status; }
    public long version() { return version; }
    public Instant startedAt() { return startedAt; }
    public Instant finishedAt() { return finishedAt; }
}
