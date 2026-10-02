package com.flowforge.worker.runtime;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
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

    /** Extend the claim only if this worker still owns it (atomic check-and-set). */
    private static final RedisScript<Long> EXTEND_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
            Long.class);

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
        Boolean claimed = redis.opsForValue().setIfAbsent(claimKey(idempotencyKey, attempt), workerId, ttl);
        return Boolean.TRUE.equals(claimed);
    }

    /** Returns false if the claim was lost (expired and taken by another worker). */
    public boolean extendClaim(String idempotencyKey, int attempt, String workerId, Duration ttl) {
        Long result = redis.execute(EXTEND_IF_OWNER, List.of(claimKey(idempotencyKey, attempt)),
                workerId, String.valueOf(ttl.toMillis()));
        return result != null && result == 1L;
    }

    private static String claimKey(String idempotencyKey, int attempt) {
        return "ff:claim:" + idempotencyKey + ":" + attempt;
    }
}
