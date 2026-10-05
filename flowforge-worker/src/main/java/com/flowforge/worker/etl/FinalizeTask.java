package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fan-in step. Runs only after every partition has loaded (the DAG guarantees it; this re-checks it).
 * Rebuilds aggregates, computes a fact checksum, writes the pipeline report, and is the ONLY step that
 * advances the watermark: forward only, and only if this run is contiguous with what is already complete.
 */
@Component
class FinalizeTask extends EtlTask {

    private static final DateTimeFormatter KEY = DateTimeFormatter.BASIC_ISO_DATE;

    private final JsonMapper json;

    FinalizeTask(JdbcTemplate jdbc, PlatformTransactionManager txManager, JsonMapper json) {
        super(jdbc, txManager);
        this.json = json;
    }

    @Override
    public String type() {
        return "etl.finalize";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) {
        String runId = in(c, "run_id");
        String pipeline = in(c, "pipeline", DEFAULT_PIPELINE);
        LocalDate startDay = LocalDate.parse(in(c, "start_date"));
        LocalDate endDay = LocalDate.parse(in(c, "end_date"));
        int partitions = Integer.parseInt(in(c, "partitions"));
        int startKey = Integer.parseInt(startDay.format(KEY));
        int endKey = Integer.parseInt(endDay.format(KEY));

        Map<String, Object> report = tx.execute(status -> {
            long loaded = num(jdbc.queryForObject(
                    "SELECT count(*) FROM etl.partition_stats WHERE run_id = ? AND stage = 'load'", Long.class, runId));
            if (loaded != partitions) {
                throw new IllegalStateException("only " + loaded + " of " + partitions + " partitions loaded");
            }

            jdbc.update("DELETE FROM analytics.agg_daily_complaints WHERE date_key BETWEEN ? AND ?", startKey, endKey);
            int aggRows = jdbc.update("""
                    INSERT INTO analytics.agg_daily_complaints
                        (date_key, borough, complaint_type, requests, closed, avg_resolution_hours)
                    SELECT f.created_date_key, l.borough, c.complaint_type, count(*), count(f.closed_at),
                           round(avg(f.resolution_hours), 2)
                    FROM analytics.fact_service_request f
                    JOIN analytics.dim_location l ON l.location_key = f.location_key
                    JOIN analytics.dim_complaint_type c ON c.complaint_key = f.complaint_key
                    WHERE f.created_date_key BETWEEN ? AND ?
                    GROUP BY f.created_date_key, l.borough, c.complaint_type
                    """, startKey, endKey);

            String before = watermark(pipeline);
            jdbc.update("""
                    INSERT INTO etl.watermark (pipeline, complete_through, last_run_id) VALUES (?, ?, ?)
                    ON CONFLICT (pipeline) DO UPDATE SET complete_through = EXCLUDED.complete_through,
                        last_run_id = EXCLUDED.last_run_id, updated_at = now()
                    WHERE etl.watermark.complete_through >= ?::date - 1
                      AND EXCLUDED.complete_through > etl.watermark.complete_through
                    """, pipeline, Date.valueOf(endDay), runId, Date.valueOf(startDay));
            String after = watermark(pipeline);

            Map<String, Object> s = jdbc.queryForMap("""
                    SELECT
                      (SELECT coalesce(sum(rows_out), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'extract') AS rows_extracted,
                      (SELECT coalesce(sum(rows_rejected), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'transform') AS rows_rejected,
                      (SELECT coalesce(sum(duplicates_removed), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'transform') AS duplicates_removed,
                      (SELECT coalesce(sum(rows_out), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'transform') AS rows_accepted,
                      (SELECT coalesce(sum(rows_inserted), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'load') AS rows_inserted,
                      (SELECT coalesce(sum(rows_updated), 0) FROM etl.partition_stats WHERE run_id = ? AND stage = 'load') AS rows_updated,
                      (SELECT count(*) FROM analytics.fact_service_request WHERE created_date_key BETWEEN ? AND ?) AS fact_rows,
                      (SELECT md5(coalesce(string_agg(unique_key || '|' || coalesce(status, '') || '|'
                                  || coalesce(closed_at::text, '') || '|' || agency_key || '|' || complaint_key
                                  || '|' || location_key, ',' ORDER BY unique_key), ''))
                       FROM analytics.fact_service_request WHERE created_date_key BETWEEN ? AND ?) AS fact_checksum,
                      (SELECT extract(epoch FROM clock_timestamp() - started_at) FROM etl.run WHERE run_id = ?) AS duration_s,
                      (SELECT mode FROM etl.run WHERE run_id = ?) AS mode
                    """, runId, runId, runId, runId, runId, runId, startKey, endKey, startKey, endKey, runId, runId);

            long extracted = num(s.get("rows_extracted"));
            long accepted = num(s.get("rows_accepted"));
            long inserted = num(s.get("rows_inserted"));
            long updated = num(s.get("rows_updated"));
            double seconds = ((Number) s.get("duration_s")).doubleValue();

            Map<String, Object> r = new LinkedHashMap<>();
            r.put("run_id", runId);
            r.put("mode", s.get("mode"));
            r.put("range", startDay + " to " + endDay);
            r.put("partitions", partitions);
            r.put("rows_extracted", extracted);
            r.put("rows_rejected", num(s.get("rows_rejected")));
            r.put("duplicates_removed", num(s.get("duplicates_removed")));
            r.put("rows_accepted", accepted);
            r.put("rows_inserted", inserted);
            r.put("rows_updated", updated);
            r.put("rows_unchanged", accepted - inserted - updated);
            r.put("fact_rows_in_range", num(s.get("fact_rows")));
            r.put("fact_checksum", s.get("fact_checksum"));
            r.put("aggregate_rows", aggRows);
            r.put("duration_s", Math.round(seconds * 100) / 100.0);
            r.put("rows_per_s", seconds > 0 ? Math.round(extracted / seconds) : 0);
            r.put("watermark_before", before);
            r.put("watermark_after", after);

            jdbc.update("UPDATE etl.run SET status = 'COMPLETED', finished_at = clock_timestamp(), report = ?::jsonb "
                    + "WHERE run_id = ?", json.writeValueAsString(r), runId);
            return r;
        });

        return Map.of("rows_extracted", String.valueOf(report.get("rows_extracted")),
                "fact_rows_in_range", String.valueOf(report.get("fact_rows_in_range")),
                "fact_checksum", String.valueOf(report.get("fact_checksum")),
                "watermark_after", String.valueOf(report.get("watermark_after")));
    }

    private String watermark(String pipeline) {
        return jdbc.query("SELECT complete_through::text FROM etl.watermark WHERE pipeline = ?",
                rs -> rs.next() ? rs.getString(1) : null, pipeline);
    }
}
