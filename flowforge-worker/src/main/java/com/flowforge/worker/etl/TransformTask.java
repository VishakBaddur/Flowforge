package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

/**
 * raw -> staging: type normalization (NYC local time -> timestamptz), cleaning, deduplication (latest version
 * of each request wins) and rejected rows with reasons. All in one transaction per partition.
 */
@Component
class TransformTask extends EtlTask {

    private static final String CREATE_SRC = """
            CREATE TEMP TABLE src (row_num INT, payload JSONB, unique_key TEXT, created_at TIMESTAMPTZ,
                                   closed_raw TIMESTAMPTZ, source_updated_at TIMESTAMPTZ, agency_code TEXT,
                                   complaint_raw TEXT) ON COMMIT DROP
            """;

    private static final String FILL_SRC = """
            INSERT INTO src
            SELECT row_num, payload,
                   nullif(trim(payload->>'unique_key'), ''),
                   CASE WHEN payload->>'created_date' ~ '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                        THEN (payload->>'created_date')::timestamp AT TIME ZONE 'America/New_York' END,
                   CASE WHEN payload->>'closed_date' ~ '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                        THEN (payload->>'closed_date')::timestamp AT TIME ZONE 'America/New_York' END,
                   CASE WHEN payload->>':updated_at' ~ '^\\d{4}-\\d{2}-\\d{2}T'
                        THEN (payload->>':updated_at')::timestamptz END,
                   nullif(upper(trim(payload->>'agency')), ''),
                   nullif(regexp_replace(trim(payload->>'complaint_type'), '\\s+', ' ', 'g'), '')
            FROM raw.service_requests
            WHERE run_id = ? AND partition_date = ?
            """;

    private static final String REJECTS = """
            INSERT INTO staging.rejected (run_id, partition_date, row_num, unique_key, reason)
            SELECT ?, ?, row_num, unique_key,
                   CASE WHEN unique_key IS NULL THEN 'missing unique_key'
                        WHEN created_at IS NULL THEN 'missing or invalid created_date'
                        WHEN agency_code IS NULL THEN 'missing agency'
                        WHEN complaint_raw IS NULL THEN 'missing complaint_type'
                        ELSE 'created_date outside partition' END
            FROM src
            WHERE unique_key IS NULL OR created_at IS NULL OR agency_code IS NULL OR complaint_raw IS NULL
               OR (created_at AT TIME ZONE 'America/New_York')::date <> ?
            """;

    private static final String STAGE = """
            INSERT INTO staging.service_requests (run_id, partition_date, unique_key, created_at, closed_at,
                agency_code, agency_name, complaint_type, descriptor, borough, zip, status, latitude, longitude,
                resolution_hours, source_updated_at)
            SELECT DISTINCT ON (unique_key)
                   ?, ?, unique_key, created_at, closed_at,
                   agency_code,
                   nullif(trim(payload->>'agency_name'), ''),
                   initcap(lower(complaint_raw)),
                   coalesce(initcap(lower(nullif(regexp_replace(trim(payload->>'descriptor'), '\\s+', ' ', 'g'), ''))), ''),
                   coalesce(nullif(nullif(upper(trim(payload->>'borough')), ''), 'UNSPECIFIED'), 'UNKNOWN'),
                   coalesce(substring(trim(payload->>'incident_zip') from '^\\d{5}'), ''),
                   initcap(lower(nullif(trim(payload->>'status'), ''))),
                   CASE WHEN payload->>'latitude' ~ '^-?\\d+(\\.\\d+)?$'
                         AND (payload->>'latitude')::numeric BETWEEN 40.4 AND 41.0
                        THEN round((payload->>'latitude')::numeric, 6) END,
                   CASE WHEN payload->>'longitude' ~ '^-?\\d+(\\.\\d+)?$'
                         AND (payload->>'longitude')::numeric BETWEEN -74.3 AND -73.6
                        THEN round((payload->>'longitude')::numeric, 6) END,
                   CASE WHEN closed_at IS NOT NULL
                        THEN round((extract(epoch FROM closed_at - created_at) / 3600)::numeric, 2) END,
                   source_updated_at
            FROM (SELECT src.*,
                         CASE WHEN closed_raw >= created_at AND closed_raw <= now() THEN closed_raw END AS closed_at
                  FROM src
                  WHERE unique_key IS NOT NULL AND created_at IS NOT NULL AND agency_code IS NOT NULL
                    AND complaint_raw IS NOT NULL
                    AND (created_at AT TIME ZONE 'America/New_York')::date = ?) v
            ORDER BY unique_key, source_updated_at DESC NULLS LAST, row_num DESC
            """;

    private static final String BAD_CLOSED = """
            SELECT count(*) FROM src
            WHERE created_at IS NOT NULL AND closed_raw IS NOT NULL
              AND NOT (closed_raw >= created_at AND closed_raw <= now())
            """;

    private final JsonMapper json;

    TransformTask(JdbcTemplate jdbc, PlatformTransactionManager txManager, JsonMapper json) {
        super(jdbc, txManager);
        this.json = json;
    }

    @Override
    public String type() {
        return "etl.transform";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) {
        long start = System.nanoTime();
        String runId = in(c, "run_id");
        LocalDate day = day(c);

        long[] r = tx.execute(status -> {
            Date d = Date.valueOf(day);
            jdbc.update("DELETE FROM staging.service_requests WHERE run_id = ? AND partition_date = ?", runId, d);
            jdbc.update("DELETE FROM staging.rejected WHERE run_id = ? AND partition_date = ?", runId, d);
            jdbc.execute(CREATE_SRC);
            long total = jdbc.update(FILL_SRC, runId, d);
            long rejected = jdbc.update(REJECTS, runId, d, d);
            long staged = jdbc.update(STAGE, runId, d, d);
            long badClosed = num(jdbc.queryForObject(BAD_CLOSED, Long.class));
            return new long[]{total, rejected, staged, badClosed};
        });

        long total = r[0], rejected = r[1], staged = r[2];
        long duplicates = (total - rejected) - staged;
        recordStats(runId, day, "transform", total, staged, rejected, duplicates, null, null, c.attempt(),
                millisSince(start), json.writeValueAsString(Map.of("invalid_closed_dates_nulled", r[3])));
        return Map.of("staged", String.valueOf(staged), "rejected", String.valueOf(rejected),
                "duplicates_removed", String.valueOf(duplicates));
    }
}
