package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import com.flowforge.worker.handler.TaskHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Date;
import java.time.LocalDate;

/**
 * Shared plumbing for the ETL stages. Every stage is idempotent per (run_id, partition_date): it deletes and
 * rewrites its own partition inside one transaction, so a retried task never doubles data.
 */
abstract class EtlTask implements TaskHandler {

    static final String DEFAULT_PIPELINE = "nyc311";

    protected final JdbcTemplate jdbc;
    protected final TransactionTemplate tx;

    EtlTask(JdbcTemplate jdbc, PlatformTransactionManager txManager) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(txManager);
    }

    static String in(TaskCommand c, String key) {
        String v = c.input().get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("missing task input '" + key + "'");
        return v;
    }

    static String in(TaskCommand c, String key, String fallback) {
        String v = c.input().get(key);
        return v == null || v.isBlank() ? fallback : v;
    }

    static LocalDate day(TaskCommand c) {
        return LocalDate.parse(in(c, "partition_date"));
    }

    static long millisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    static long num(Object n) {
        return n == null ? 0 : ((Number) n).longValue();
    }

    /** Upsert, so a retried stage overwrites its numbers instead of double counting. */
    void recordStats(String runId, LocalDate day, String stage, long rowsIn, long rowsOut, long rejected, long duplicates,
                     Long inserted, Long updated, int attempt, long millis, String checksJson) {
        jdbc.update("""
                INSERT INTO etl.partition_stats (run_id, partition_date, stage, rows_in, rows_out, rows_rejected,
                    duplicates_removed, rows_inserted, rows_updated, attempt, duration_ms, checks, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, now())
                ON CONFLICT (run_id, partition_date, stage) DO UPDATE SET
                    rows_in = EXCLUDED.rows_in, rows_out = EXCLUDED.rows_out, rows_rejected = EXCLUDED.rows_rejected,
                    duplicates_removed = EXCLUDED.duplicates_removed, rows_inserted = EXCLUDED.rows_inserted,
                    rows_updated = EXCLUDED.rows_updated, attempt = EXCLUDED.attempt,
                    duration_ms = EXCLUDED.duration_ms, checks = EXCLUDED.checks, updated_at = now()
                """, runId, Date.valueOf(day), stage, (int) rowsIn, (int) rowsOut, (int) rejected, (int) duplicates,
                inserted == null ? null : inserted.intValue(), updated == null ? null : updated.intValue(),
                attempt, millis, checksJson);
    }
}
