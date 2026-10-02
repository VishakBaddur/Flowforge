package com.flowforge.common.messaging;

import java.time.Instant;
import java.util.Map;

/** Orchestrator -> worker. Topic: flowforge.tasks, key: workflowId. */
public record TaskCommand(String workflowId, String taskId, int attempt, String type,
                          Map<String, String> input, long timeoutMillis, Instant issuedAt) {

    public TaskCommand {
        input = input == null ? Map.of() : Map.copyOf(input);
    }

    /**
     * Stable across retries: handlers pass this to external systems (payment APIs, DB upserts)
     * so a retried task never applies its side effect twice.
     */
    public String idempotencyKey() {
        return workflowId + ":" + taskId;
    }
}
