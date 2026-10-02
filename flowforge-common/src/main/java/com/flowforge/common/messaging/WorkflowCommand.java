package com.flowforge.common.messaging;

import com.flowforge.common.model.WorkflowDefinition;

import java.time.Instant;

/** API -> orchestrator. Topic: flowforge.workflow-commands, key: workflowId. */
public record WorkflowCommand(Kind kind, String workflowId, WorkflowDefinition definition,
                              String owner, String reason, Instant issuedAt) {

    public enum Kind { START, CANCEL }

    public static WorkflowCommand start(String workflowId, WorkflowDefinition definition, String owner, Instant now) {
        return new WorkflowCommand(Kind.START, workflowId, definition, owner, null, now);
    }

    public static WorkflowCommand cancel(String workflowId, String reason, Instant now) {
        return new WorkflowCommand(Kind.CANCEL, workflowId, null, null, reason, now);
    }
}
