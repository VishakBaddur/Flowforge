package com.flowforge.common.messaging;

import java.time.Instant;
import java.util.Map;

/** Worker -> orchestrator. Topic: flowforge.task-results, key: workflowId. */
public record TaskResult(Kind kind, String workflowId, String taskId, int attempt, String workerId,
                         Instant leaseExpiresAt, Map<String, String> output, String error, Instant occurredAt) {

    public enum Kind { STARTED, SUCCEEDED, FAILED }

    public TaskResult {
        output = output == null ? Map.of() : Map.copyOf(output);
    }

    public static TaskResult started(TaskCommand c, String workerId, Instant leaseExpiresAt, Instant now) {
        return new TaskResult(Kind.STARTED, c.workflowId(), c.taskId(), c.attempt(), workerId, leaseExpiresAt, null, null, now);
    }

    public static TaskResult succeeded(TaskCommand c, String workerId, Map<String, String> output, Instant now) {
        return new TaskResult(Kind.SUCCEEDED, c.workflowId(), c.taskId(), c.attempt(), workerId, null, output, null, now);
    }

    public static TaskResult failed(TaskCommand c, String workerId, String error, Instant now) {
        return new TaskResult(Kind.FAILED, c.workflowId(), c.taskId(), c.attempt(), workerId, null, null, error, now);
    }
}
