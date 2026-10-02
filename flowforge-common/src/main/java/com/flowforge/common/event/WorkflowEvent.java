package com.flowforge.common.event;

import com.flowforge.common.model.WorkflowDefinition;

import java.time.Instant;
import java.util.Map;

/**
 * Every state change in a workflow is one of these immutable facts.
 * Sealed: the compiler knows the full set, so switches over events must be exhaustive.
 */
public sealed interface WorkflowEvent {

    String workflowId();

    Instant occurredAt();

    default String type() {
        return getClass().getSimpleName();
    }

    record WorkflowStarted(String workflowId, Instant occurredAt, WorkflowDefinition definition)
            implements WorkflowEvent {}

    record TaskQueued(String workflowId, Instant occurredAt, String taskId, int attempt)
            implements WorkflowEvent {}

    record TaskStarted(String workflowId, Instant occurredAt, String taskId, int attempt,
                       String workerId, Instant leaseExpiresAt)
            implements WorkflowEvent {}

    record TaskSucceeded(String workflowId, Instant occurredAt, String taskId, int attempt,
                         Map<String, String> output)
            implements WorkflowEvent {
        public TaskSucceeded {
            output = output == null ? Map.of() : Map.copyOf(output);
        }
    }

    /** A failed or timed-out attempt that will be retried at {@code retryAt}. */
    record TaskFailed(String workflowId, Instant occurredAt, String taskId, int attempt,
                      String error, Instant retryAt)
            implements WorkflowEvent {}

    /** Retries exhausted; the task goes to the dead-letter topic. */
    record TaskDeadLettered(String workflowId, Instant occurredAt, String taskId, int attempt, String error)
            implements WorkflowEvent {}

    record TaskSkipped(String workflowId, Instant occurredAt, String taskId, String reason)
            implements WorkflowEvent {}

    record WorkflowCompleted(String workflowId, Instant occurredAt)
            implements WorkflowEvent {}

    record WorkflowFailed(String workflowId, Instant occurredAt, String reason)
            implements WorkflowEvent {}

    record WorkflowCancelled(String workflowId, Instant occurredAt, String reason)
            implements WorkflowEvent {}
}
