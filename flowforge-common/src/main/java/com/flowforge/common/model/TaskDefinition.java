package com.flowforge.common.model;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/** One node in a workflow DAG. */
public record TaskDefinition(
        String id,
        String type,
        Map<String, String> input,
        List<String> dependsOn,
        int maxRetries,
        Duration timeout,
        BackoffPolicy backoff) {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    public TaskDefinition {
        if (id == null || id.isBlank())
            throw new IllegalArgumentException("task id is required");
        if (type == null || type.isBlank())
            throw new IllegalArgumentException("task type is required (task '" + id + "')");
        if (maxRetries < 0)
            throw new IllegalArgumentException("maxRetries must be >= 0 (task '" + id + "')");
        input = input == null ? Map.of() : Map.copyOf(input);
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
        if (timeout.isZero() || timeout.isNegative())
            throw new IllegalArgumentException("timeout must be > 0 (task '" + id + "')");
        backoff = backoff == null ? BackoffPolicy.DEFAULT : backoff;
    }

    /** Total attempts allowed = first try + retries. */
    public int maxAttempts() {
        return maxRetries + 1;
    }
}
