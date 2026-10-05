package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Data-quality gate: profiles the raw partition in one SQL pass and fails the task if any check fails. */
@Component
class ValidateTask extends EtlTask {

    private static final List<String> REQUIRED = List.of("unique_key", "created_date", "agency", "complaint_type");

    private final JsonMapper json;

    ValidateTask(JdbcTemplate jdbc, PlatformTransactionManager txManager, JsonMapper json) {
        super(jdbc, txManager);
        this.json = json;
    }

    @Override
    public String type() {
        return "etl.validate";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) {
        long start = System.nanoTime();
        String runId = in(c, "run_id");
        LocalDate day = day(c);
        long minRows = Long.parseLong(in(c, "min_rows", "1000"));
        long maxRows = Long.parseLong(in(c, "max_rows", "100000"));
        double maxMissingRequired = Double.parseDouble(in(c, "max_missing_required_pct", "1.0"));
        double maxMissingBorough = Double.parseDouble(in(c, "max_missing_borough_pct", "10"));
        double maxInvalidZip = Double.parseDouble(in(c, "max_invalid_zip_pct", "25"));
        double maxDuplicates = Double.parseDouble(in(c, "max_duplicate_pct", "5"));
        double minVolumeRatio = Double.parseDouble(in(c, "min_volume_ratio", "0.5"));
        long minHistoryDays = Long.parseLong(in(c, "min_history_days", "5"));

        Map<String, Object> p = jdbc.queryForMap("""
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE nullif(trim(payload->>'unique_key'), '') IS NULL) AS missing_unique_key,
                       count(*) FILTER (WHERE nullif(trim(payload->>'created_date'), '') IS NULL) AS missing_created_date,
                       count(*) FILTER (WHERE nullif(trim(payload->>'agency'), '') IS NULL) AS missing_agency,
                       count(*) FILTER (WHERE nullif(trim(payload->>'complaint_type'), '') IS NULL) AS missing_complaint_type,
                       count(*) FILTER (WHERE nullif(trim(payload->>'unique_key'), '') IS NULL
                                           OR nullif(trim(payload->>'created_date'), '') IS NULL
                                           OR nullif(trim(payload->>'agency'), '') IS NULL
                                           OR nullif(trim(payload->>'complaint_type'), '') IS NULL) AS missing_required,
                       count(*) FILTER (WHERE coalesce(upper(trim(payload->>'borough')), '') IN ('', 'UNSPECIFIED')) AS missing_borough,
                       count(*) FILTER (WHERE coalesce(trim(payload->>'incident_zip'), '') !~ '^\\d{5}') AS invalid_zip,
                       count(*) - count(DISTINCT payload->>'unique_key') AS duplicates
                FROM raw.service_requests
                WHERE run_id = ? AND partition_date = ?
                """, runId, Date.valueOf(day));

        long total = num(p.get("total"));
        Map<String, Object> checks = new LinkedHashMap<>();
        List<String> failures = new ArrayList<>();

        boolean countOk = total >= minRows && total <= maxRows;
        checks.put("row_count", Map.of("value", total, "min", minRows, "max", maxRows, "passed", countOk));
        if (!countOk) failures.add("row_count=" + total + " outside " + minRows + ".." + maxRows);

        // Volume anomaly: compare with the trailing 14-day median of our own extract history. Catches partitions
        // the source has not finished publishing, which a fixed min_rows cannot.
        Map<String, Object> h = jdbc.queryForMap("""
                SELECT count(*) AS days, percentile_cont(0.5) WITHIN GROUP (ORDER BY rows_out) AS median
                FROM (SELECT DISTINCT ON (partition_date) partition_date, rows_out
                      FROM etl.partition_stats
                      WHERE stage = 'extract' AND partition_date BETWEEN ?::date - 14 AND ?::date - 1
                      ORDER BY partition_date, updated_at DESC) history
                """, Date.valueOf(day), Date.valueOf(day));
        long historyDays = num(h.get("days"));
        Number median = (Number) h.get("median");
        if (historyDays >= minHistoryDays && median != null && median.doubleValue() > 0) {
            double ratio = Math.round(100.0 * total / median.doubleValue()) / 100.0;
            boolean volumeOk = ratio >= minVolumeRatio;
            checks.put("volume_vs_trailing_median", Map.of("value", ratio, "min", minVolumeRatio,
                    "median", median.longValue(), "history_days", historyDays, "passed", volumeOk));
            if (!volumeOk) failures.add("volume " + total + " rows = " + ratio + "x the trailing 14-day median ("
                    + median.longValue() + "), below " + minVolumeRatio + "x: source likely not fully published");
        } else {
            checks.put("volume_vs_trailing_median", Map.of("skipped", "only " + historyDays + " days of history"));
        }
        for (String col : REQUIRED) {
            percentCheck(checks, failures, "missing_" + col + "_pct", num(p.get("missing_" + col)), total, maxMissingRequired);
        }
        percentCheck(checks, failures, "missing_borough_pct", num(p.get("missing_borough")), total, maxMissingBorough);
        percentCheck(checks, failures, "invalid_zip_pct", num(p.get("invalid_zip")), total, maxInvalidZip);
        percentCheck(checks, failures, "duplicate_pct", num(p.get("duplicates")), total, maxDuplicates);
        checks.put("passed", failures.isEmpty());

        long rejected = num(p.get("missing_required"));
        recordStats(runId, day, "validate", total, total - rejected, rejected, num(p.get("duplicates")), null, null,
                c.attempt(), millisSince(start), json.writeValueAsString(checks));
        if (!failures.isEmpty()) {
            throw new DataQualityException("data quality gate failed for " + day + ": " + String.join("; ", failures));
        }
        return Map.of("rows", String.valueOf(total), "checks", "passed");
    }

    private static void percentCheck(Map<String, Object> checks, List<String> failures, String name,
                                     long count, long total, double maxPct) {
        double pct = total == 0 ? 100.0 : Math.round(10000.0 * count / total) / 100.0;
        boolean passed = pct <= maxPct;
        checks.put(name, Map.of("value", pct, "max", maxPct, "passed", passed));
        if (!passed) failures.add(name + "=" + pct + "% > " + maxPct + "%");
    }
}
