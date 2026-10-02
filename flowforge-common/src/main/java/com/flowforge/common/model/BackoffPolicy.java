package com.flowforge.common.model;

import java.time.Duration;

/**
 * Exponential backoff with an optional "full jitter" strategy.
 * delay(n) = random(0, min(maxDelay, initialDelay * multiplier^(n-1)))
 */
public record BackoffPolicy(Duration initialDelay, double multiplier, Duration maxDelay, boolean jitter) {

    public static final BackoffPolicy DEFAULT =
            new BackoffPolicy(Duration.ofMillis(500), 2.0, Duration.ofSeconds(30), true);

    public BackoffPolicy {
        if (initialDelay == null || initialDelay.isNegative())
            throw new IllegalArgumentException("initialDelay must be >= 0");
        if (multiplier < 1.0)
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        if (maxDelay == null || maxDelay.compareTo(initialDelay) < 0)
            throw new IllegalArgumentException("maxDelay must be >= initialDelay");
    }

    /** Exponential delay before retry number {@code attempt} (1-based), capped at maxDelay. */
    public Duration baseDelay(int attempt) {
        if (attempt < 1) throw new IllegalArgumentException("attempt must be >= 1");
        double millis = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);
        return Duration.ofMillis((long) Math.min(millis, maxDelay.toMillis()));
    }

    /** Actual delay to wait. {@code random} must be in [0, 1); injected so tests are deterministic. */
    public Duration delay(int attempt, double random) {
        if (random < 0 || random >= 1) throw new IllegalArgumentException("random must be in [0, 1)");
        Duration base = baseDelay(attempt);
        return jitter ? Duration.ofMillis((long) (base.toMillis() * random)) : base;
    }
}
