package com.flowforge.common.state;

import com.flowforge.common.model.TaskStatus;

import java.time.Instant;
import java.util.Map;

/** Immutable snapshot of one task's runtime state. Transitions return a new instance. */
public record TaskState(
        String taskId,
        TaskStatus status,
        int attempt,
        String workerId,
        Instant leaseExpiresAt,
        Instant retryAt,
        String lastError,
        Map<String, String> output) {

    static TaskState pending(String taskId) {
        return new TaskState(taskId, TaskStatus.PENDING, 0, null, null, null, null, Map.of());
    }

    TaskState queued(int attempt) {
        return new TaskState(taskId, TaskStatus.QUEUED, attempt, null, null, null, lastError, Map.of());
    }

    TaskState running(String workerId, Instant leaseExpiresAt) {
        return new TaskState(taskId, TaskStatus.RUNNING, attempt, workerId, leaseExpiresAt, null, lastError, Map.of());
    }

    TaskState succeeded(Map<String, String> output) {
        return new TaskState(taskId, TaskStatus.SUCCEEDED, attempt, workerId, null, null, lastError, output);
    }

    TaskState retryWait(String error, Instant retryAt) {
        return new TaskState(taskId, TaskStatus.RETRY_WAIT, attempt, null, null, retryAt, error, Map.of());
    }

    TaskState deadLettered(String error) {
        return new TaskState(taskId, TaskStatus.DEAD_LETTERED, attempt, null, null, null, error, Map.of());
    }

    TaskState skipped(String reason) {
        return new TaskState(taskId, TaskStatus.SKIPPED, attempt, null, null, null, reason, Map.of());
    }
}
