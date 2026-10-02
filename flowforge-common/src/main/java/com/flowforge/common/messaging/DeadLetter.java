package com.flowforge.common.messaging;

import java.time.Instant;
import java.util.Map;

/** Published by the orchestrator when a task exhausts its retries. Topic: flowforge.tasks.dlq. */
public record DeadLetter(String workflowId, String taskId, int attempts, String type,
                         Map<String, String> input, String error, Instant occurredAt) {

    public DeadLetter {
        input = input == null ? Map.of() : Map.copyOf(input);
    }
}
