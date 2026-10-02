package com.flowforge.common.state;

import com.flowforge.common.model.TaskStatus;

import java.time.Instant;
import java.util.Map;

/** Immutable snapshot of one task's runtime state. Transitions return a new instance. */
public record TaskState(
        String taskId,
        TaskStatus status,
        int attempt,
        Instant queuedAt,          // when the current attempt was queued (for latency metrics)
        String workerId,
        Instant leaseExpiresAt,
        Instant retryAt,
        String lastError,
        Map<String, String> output) {

    static TaskState pending(String taskId) {
        return new TaskState(taskId, TaskStatus.PENDING, 0, null, null, null, null, null, Map.of());
    }

    TaskState queued(int attempt, Instant at) {
        return new TaskState(taskId, TaskStatus.QUEUED, attempt, at, null, null, null, lastError, Map.of());
    }

    TaskState running(String workerId, Instant leaseExpiresAt) {
        return new TaskState(taskId, TaskStatus.RUNNING, attempt, queuedAt, workerId, leaseExpiresAt, null, lastError, Map.of());
    }

    TaskState succeeded(Map<String, String> output) {
        return new TaskState(taskId, TaskStatus.SUCCEEDED, attempt, queuedAt, workerId, null, null, lastError, output);
    }

    TaskState retryWait(String error, Instant retryAt) {
        return new TaskState(taskId, TaskStatus.RETRY_WAIT, attempt, queuedAt, null, null, retryAt, error, Map.of());
    }

    TaskState deadLettered(String error) {
        return new TaskState(taskId, TaskStatus.DEAD_LETTERED, attempt, queuedAt, null, null, null, error, Map.of());
    }

    TaskState skipped(String reason) {
        return new TaskState(taskId, TaskStatus.SKIPPED, attempt, queuedAt, null, null, null, reason, Map.of());
    }
}
