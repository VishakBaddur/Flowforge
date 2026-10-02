package com.flowforge.worker.runtime;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Redis-backed idempotency:
 *   ff:done:{workflowId:taskId}           hash of the task's output, kept 24h -> never re-run a completed task
 *   ff:claim:{workflowId:taskId}:{attempt} SET NX with TTL                    -> one worker per attempt
 */
@Component
public class IdempotencyStore {

    private static final Duration DONE_TTL = Duration.ofHours(24);
    private static final String DONE_MARKER = "__done";

    private final StringRedisTemplate redis;

    public IdempotencyStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Optional<Map<String, String>> completedOutput(String idempotencyKey) {
        Map<Object, Object> raw = redis.opsForHash().entries("ff:done:" + idempotencyKey);
        if (raw.isEmpty()) return Optional.empty();
        Map<String, String> output = new HashMap<>();
        raw.forEach((k, v) -> {
            if (!DONE_MARKER.equals(k)) output.put((String) k, (String) v);
        });
        return Optional.of(output);
    }

    public void markCompleted(String idempotencyKey, Map<String, String> output) {
        String key = "ff:done:" + idempotencyKey;
        Map<String, String> hash = new HashMap<>(output);
        hash.put(DONE_MARKER, "1");
        redis.opsForHash().putAll(key, hash);
        redis.expire(key, DONE_TTL);
    }

    public boolean tryClaim(String idempotencyKey, int attempt, String workerId, Duration ttl) {
        Boolean claimed = redis.opsForValue().setIfAbsent("ff:claim:" + idempotencyKey + ":" + attempt, workerId, ttl);
        return Boolean.TRUE.equals(claimed);
    }
}
