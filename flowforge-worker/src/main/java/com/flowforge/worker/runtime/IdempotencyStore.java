package com.flowforge.worker.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.flowforge.common.messaging.TaskCommand;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    /**
     * Atomically: extend the claim if this worker still owns it; re-take it if it expired and nobody else took it.
     * Returns 0 ONLY when another worker holds the claim.
     */
    private static final String EXTEND_LUA =
            "local owner = redis.call('get', KEYS[1]) " +
            "if owner == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) end " +
            "if not owner then redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 end " +
            "return 0";
    private static final RedisScript<Long> EXTEND_IF_OWNER = new DefaultRedisScript<>(EXTEND_LUA, Long.class);

    public enum Prep { CACHED, CLAIMED, TAKEN }

    public record Prepared(TaskCommand command, Prep outcome, Map<String, String> cachedOutput) {}

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

    /** HSET + EXPIRE in one pipelined round trip. */
    public void markCompleted(String idempotencyKey, Map<String, String> output) {
        guarded("write result cache", () -> {
            byte[] key = bytes("ff:done:" + idempotencyKey);
            Map<byte[], byte[]> hash = new HashMap<>();
            output.forEach((k, v) -> hash.put(bytes(k), bytes(v)));
            hash.put(bytes(DONE_MARKER), bytes("1"));
            redis.executePipelined((RedisCallback<Object>) conn -> {
                conn.hashCommands().hMSet(key, hash);
                conn.keyCommands().expire(key, DONE_TTL.toSeconds());
                return null;
            });
            return null;
        }, null);
    }

    /**
     * ONE round trip for a whole poll batch: result-cache lookup + phase-1 claim for every command.
     * Redis down => every command is treated as CLAIMED with no cache (attempt numbers still guarantee outcomes).
     */
    public List<Prepared> prepare(List<TaskCommand> commands, String workerId, Duration claimTtl) {
        List<Prepared> degraded = commands.stream().map(c -> new Prepared(c, Prep.CLAIMED, null)).toList();
        if (commands.isEmpty()) return degraded;
        return guarded("prepare batch", () -> {
            List<Object> results = redis.executePipelined((RedisCallback<Object>) conn -> {
                for (TaskCommand c : commands) {
                    conn.hashCommands().hGetAll(bytes("ff:done:" + c.idempotencyKey()));
                    conn.stringCommands().set(bytes(claimKey(c.idempotencyKey(), c.attempt())), bytes(workerId),
                            Expiration.from(claimTtl), RedisStringCommands.SetOption.ifAbsent());
                }
                return null;
            });
            List<Prepared> out = new ArrayList<>(commands.size());
            for (int i = 0; i < commands.size(); i++) {
                Map<?, ?> done = (Map<?, ?>) results.get(2 * i);
                if (done != null && !done.isEmpty()) {
                    Map<String, String> output = new HashMap<>();
                    done.forEach((k, v) -> {
                        if (!DONE_MARKER.equals(k)) output.put(String.valueOf(k), String.valueOf(v));
                    });
                    out.add(new Prepared(commands.get(i), Prep.CACHED, output));
                } else {
                    boolean claimed = Boolean.TRUE.equals(results.get(2 * i + 1));
                    out.add(new Prepared(commands.get(i), claimed ? Prep.CLAIMED : Prep.TAKEN, null));
                }
            }
            return out;
        }, degraded);
    }

    /**
     * Phase 2 for a whole batch in ONE round trip: extend each claim to the full lease (or re-take it if it
     * expired unclaimed). Returns the commands whose claim now belongs to ANOTHER worker; the caller must not
     * run those. Redis down => empty set (attempt numbers still guarantee outcomes, as in phase 1).
     */
    public Set<TaskCommand> extendClaims(List<TaskCommand> commands, String workerId, Function<TaskCommand, Duration> lease) {
        if (commands.isEmpty()) return Set.of();
        return guarded("extend claims", () -> {
            List<Object> results = redis.executePipelined((RedisCallback<Object>) conn -> {
                for (TaskCommand c : commands) {
                    conn.scriptingCommands().eval(bytes(EXTEND_LUA), ReturnType.INTEGER, 1,
                            bytes(claimKey(c.idempotencyKey(), c.attempt())), bytes(workerId),
                            bytes(String.valueOf(lease.apply(c).toMillis())));
                }
                return null;
            });
            Set<TaskCommand> lost = Collections.newSetFromMap(new IdentityHashMap<>());
            for (int i = 0; i < commands.size(); i++) {
                if (Long.valueOf(0L).equals(results.get(i))) lost.add(commands.get(i));   // only a definite "taken"
            }
            return lost;
        }, Set.<TaskCommand>of());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
