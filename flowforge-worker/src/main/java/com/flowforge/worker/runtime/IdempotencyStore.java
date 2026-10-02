package com.flowforge.worker.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Redis-backed idempotency:
 *   ff:done:{workflowId:taskId}           hash of the task's output, kept 24h -> never re-run a completed task
 *   ff:claim:{workflowId:taskId}:{attempt} SET NX with TTL                    -> one worker per attempt
 *
 * Redis is an OPTIMIZATION. If it is slow or down, every call falls back to a safe default and a
 * circuit breaker skips Redis for a few seconds, so work keeps flowing. Correctness of outcomes is
 * still guaranteed by the orchestrator's attempt numbers; only duplicate side-effect protection is lost.
 */
@Component
public class IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyStore.class);
    private static final Duration DONE_TTL = Duration.ofHours(24);
    private static final String DONE_MARKER = "__done";
    /** After a failure, skip Redis for this long instead of paying a timeout on every call. */
    private static final long OPEN_CIRCUIT_MS = 5_000;

    /** Extend the claim only if this worker still owns it (atomic check-and-set). */
    private static final RedisScript<Long> EXTEND_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private volatile long skipUntil = 0;
    private volatile boolean down = false;

    public IdempotencyStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public Optional<Map<String, String>> completedOutput(String idempotencyKey) {
        return guarded("read result cache", () -> {
            Map<Object, Object> raw = redis.opsForHash().entries("ff:done:" + idempotencyKey);
            if (raw.isEmpty()) return Optional.<Map<String, String>>empty();
            Map<String, String> output = new HashMap<>();
            raw.forEach((k, v) -> {
                if (!DONE_MARKER.equals(k)) output.put((String) k, (String) v);
            });
            return Optional.of(output);
        }, Optional.empty());
    }

    public void markCompleted(String idempotencyKey, Map<String, String> output) {
        guarded("write result cache", () -> {
            String key = "ff:done:" + idempotencyKey;
            Map<String, String> hash = new HashMap<>(output);
            hash.put(DONE_MARKER, "1");
            redis.opsForHash().putAll(key, hash);
            redis.expire(key, DONE_TTL);
            return null;
        }, null);
    }

    /** Fallback when Redis is down: proceed without a claim (duplicates are deduplicated by attempt number). */
    public boolean tryClaim(String idempotencyKey, int attempt, String workerId, Duration ttl) {
        return guarded("claim", () ->
                Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(claimKey(idempotencyKey, attempt), workerId, ttl)), true);
    }

    /** Returns false only if the claim was definitely lost to another worker. */
    public boolean extendClaim(String idempotencyKey, int attempt, String workerId, Duration ttl) {
        return guarded("extend claim", () -> {
            Long result = redis.execute(EXTEND_IF_OWNER, List.of(claimKey(idempotencyKey, attempt)),
                    workerId, String.valueOf(ttl.toMillis()));
            return result != null && result == 1L;
        }, true);
    }

    public boolean isDegraded() {
        return down;
    }

    private <T> T guarded(String operation, Supplier<T> call, T fallback) {
        if (System.currentTimeMillis() < skipUntil) return fallback;   // circuit open: don't even try
        try {
            T result = call.get();
            if (down) {
                down = false;
                log.info("Redis reachable again; result cache and claims re-enabled");
            }
            return result;
        } catch (DataAccessException e) {
            skipUntil = System.currentTimeMillis() + OPEN_CIRCUIT_MS;
            if (!down) {
                down = true;
                log.warn("Redis unavailable during '{}' ({}); running WITHOUT result cache and claims, retrying in {} ms",
                        operation, e.getClass().getSimpleName(), OPEN_CIRCUIT_MS);
            }
            return fallback;
        }
    }

    private static String claimKey(String idempotencyKey, int attempt) {
        return "ff:claim:" + idempotencyKey + ":" + attempt;
    }
}
