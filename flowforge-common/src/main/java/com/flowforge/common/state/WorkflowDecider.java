package com.flowforge.common.state;

import com.flowforge.common.event.WorkflowEvent;
import com.flowforge.common.event.WorkflowEvent.*;
import com.flowforge.common.model.TaskDefinition;
import com.flowforge.common.model.TaskStatus;
import com.flowforge.common.model.WorkflowDefinition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;

import static com.flowforge.common.model.TaskStatus.*;

/**
 * Pure decision logic: (state, input, now) -> events.
 * Each emitted event is applied to the given state immediately, so later checks in the
 * same call see it. If persisting the events fails (concurrency conflict), the caller must
 * discard this state and reload it from the event store.
 */
public final class WorkflowDecider {

    private final DoubleSupplier random;

    public WorkflowDecider(DoubleSupplier random) {
        this.random = random;
    }

    public static WorkflowDecider withRandomJitter() {
        return new WorkflowDecider(() -> ThreadLocalRandom.current().nextDouble());
    }

    public Decision start(WorkflowState state, WorkflowDefinition definition, Instant now) {
        if (state.definition() != null) return Decision.ignored("workflow already started");
        List<WorkflowEvent> out = new ArrayList<>();
        emit(state, out, new WorkflowStarted(state.workflowId(), now, definition));
        for (String root : state.dag().roots()) {
            emit(state, out, new TaskQueued(state.workflowId(), now, root, 1));
        }
        return Decision.of(out);
    }

    public Decision taskStarted(WorkflowState state, String taskId, int attempt, String workerId,
                                Instant leaseExpiresAt, Instant now) {
        String stale = checkCurrent(state, taskId, attempt, QUEUED);
        if (stale != null) return Decision.ignored(stale);
        List<WorkflowEvent> out = new ArrayList<>();
        emit(state, out, new TaskStarted(state.workflowId(), now, taskId, attempt, workerId, leaseExpiresAt));
        return Decision.of(out);
    }

    public Decision taskSucceeded(WorkflowState state, String taskId, int attempt,
                                  Map<String, String> output, Instant now) {
        String stale = checkCurrent(state, taskId, attempt, QUEUED, RUNNING);
        if (stale != null) return Decision.ignored(stale);

        List<WorkflowEvent> out = new ArrayList<>();
        emit(state, out, new TaskSucceeded(state.workflowId(), now, taskId, attempt, output));

        for (String dependent : state.dag().dependentsOf(taskId)) {
            if (state.task(dependent).status() == PENDING
                    && state.dag().isReady(dependent, state.succeededTaskIds())) {
                emit(state, out, new TaskQueued(state.workflowId(), now, dependent, 1));
            }
        }
        finishIfDone(state, out, now);
        return Decision.of(out);
    }

    public Decision taskFailed(WorkflowState state, String taskId, int attempt, String error, Instant now) {
        String stale = checkCurrent(state, taskId, attempt, QUEUED, RUNNING);
        if (stale != null) return Decision.ignored(stale);

        TaskDefinition def = state.dag().task(taskId);
        List<WorkflowEvent> out = new ArrayList<>();

        if (attempt < def.maxAttempts()) {
            Instant retryAt = now.plus(def.backoff().delay(attempt, random.getAsDouble()));
            emit(state, out, new TaskFailed(state.workflowId(), now, taskId, attempt, error, retryAt));
            return Decision.of(out);
        }

        emit(state, out, new TaskDeadLettered(state.workflowId(), now, taskId, attempt, error));
        for (String downstream : state.dag().downstreamOf(taskId)) {
            if (state.task(downstream).status() == PENDING) {
                emit(state, out, new TaskSkipped(state.workflowId(), now, downstream,
                        "upstream task '" + taskId + "' dead-lettered"));
            }
        }
        finishIfDone(state, out, now);
        return Decision.of(out);
    }

    public Decision retryDue(WorkflowState state, String taskId, Instant now) {
        String terminal = checkRunning(state);
        if (terminal != null) return Decision.ignored(terminal);
        TaskState task = state.task(taskId);
        if (task.status() != RETRY_WAIT) return Decision.ignored("task " + taskId + " is " + task.status());
        if (now.isBefore(task.retryAt())) return Decision.ignored("retry for " + taskId + " not due until " + task.retryAt());

        List<WorkflowEvent> out = new ArrayList<>();
        emit(state, out, new TaskQueued(state.workflowId(), now, taskId, task.attempt() + 1));
        return Decision.of(out);
    }

    public Decision leaseExpired(WorkflowState state, String taskId, int attempt, Instant now) {
        String stale = checkCurrent(state, taskId, attempt, RUNNING);
        if (stale != null) return Decision.ignored(stale);
        TaskState task = state.task(taskId);
        if (now.isBefore(task.leaseExpiresAt()))
            return Decision.ignored("lease for " + taskId + " valid until " + task.leaseExpiresAt());
        return taskFailed(state, taskId, attempt, "lease expired (worker " + task.workerId() + ")", now);
    }

    public Decision cancel(WorkflowState state, String reason, Instant now) {
        String terminal = checkRunning(state);
        if (terminal != null) return Decision.ignored(terminal);
        List<WorkflowEvent> out = new ArrayList<>();
        emit(state, out, new WorkflowCancelled(state.workflowId(), now, reason));
        return Decision.of(out);
    }

    // ---- helpers ----

    private static void finishIfDone(WorkflowState state, List<WorkflowEvent> out, Instant now) {
        if (!state.allTasksTerminal()) return;
        if (state.anyDeadLettered()) {
            String failed = state.tasks().values().stream()
                    .filter(t -> t.status() == DEAD_LETTERED).map(TaskState::taskId)
                    .collect(Collectors.joining(", "));
            emit(state, out, new WorkflowFailed(state.workflowId(), now, "dead-lettered: " + failed));
        } else {
            emit(state, out, new WorkflowCompleted(state.workflowId(), now));
        }
    }

    private static String checkRunning(WorkflowState state) {
        if (state.definition() == null) return "workflow not started";
        if (state.status().isTerminal()) return "workflow is " + state.status();
        return null;
    }

    /** Returns a reason to ignore the input, or null if it targets the task's current attempt in an allowed status. */
    private static String checkCurrent(WorkflowState state, String taskId, int attempt, TaskStatus... allowed) {
        String terminal = checkRunning(state);
        if (terminal != null) return terminal;
        TaskState task = state.task(taskId);
        if (attempt != task.attempt())
            return "stale attempt " + attempt + " for " + taskId + " (current " + task.attempt() + ")";
        for (TaskStatus s : allowed) if (task.status() == s) return null;
        return "task " + taskId + " is " + task.status();
    }

    private static void emit(WorkflowState state, List<WorkflowEvent> out, WorkflowEvent event) {
        state.apply(event);
        out.add(event);
    }
}
