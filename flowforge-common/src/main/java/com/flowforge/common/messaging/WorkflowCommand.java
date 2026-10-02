package com.flowforge.common.messaging;

import com.flowforge.common.model.WorkflowDefinition;

import java.time.Instant;

/** API -> orchestrator. Topic: flowforge.workflow-commands, key: workflowId. */
public record WorkflowCommand(Kind kind, String workflowId, WorkflowDefinition definition,
                              String reason, Instant issuedAt) {

    public enum Kind { START, CANCEL }

    public static WorkflowCommand start(String workflowId, WorkflowDefinition definition, Instant now) {
        return new WorkflowCommand(Kind.START, workflowId, definition, null, now);
    }

    public static WorkflowCommand cancel(String workflowId, String reason, Instant now) {
        return new WorkflowCommand(Kind.CANCEL, workflowId, null, reason, now);
    }
}
