package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * staging -> star schema in ONE transaction. Dimensions are upserted in sorted natural-key order (consistent
 * lock order, so parallel partitions don't deadlock); facts are upserted by natural key, newest version wins.
 */
@Component
class LoadTask extends EtlTask {

    private static final String DIM_DATE = """
            INSERT INTO analytics.dim_date (date_key, full_date, year, month, day, day_of_week, is_weekend)
            SELECT to_char(d, 'YYYYMMDD')::int, d, extract(year FROM d), extract(month FROM d), extract(day FROM d),
                   extract(isodow FROM d), extract(isodow FROM d) IN (6, 7)
            FROM (SELECT (created_at AT TIME ZONE 'America/New_York')::date AS d
                  FROM staging.service_requests WHERE run_id = ? AND partition_date = ?
                  UNION
                  SELECT (closed_at AT TIME ZONE 'America/New_York')::date
                  FROM staging.service_requests WHERE run_id = ? AND partition_date = ? AND closed_at IS NOT NULL) x
            ORDER BY 1
            ON CONFLICT (date_key) DO NOTHING
            """;

    private static final String DIM_AGENCY = """
            INSERT INTO analytics.dim_agency (agency_code, agency_name)
            SELECT DISTINCT ON (agency_code) agency_code, agency_name
            FROM staging.service_requests WHERE run_id = ? AND partition_date = ?
            ORDER BY agency_code, agency_name NULLS LAST
            ON CONFLICT (agency_code) DO UPDATE SET agency_name = EXCLUDED.agency_name
            WHERE EXCLUDED.agency_name IS NOT NULL
              AND analytics.dim_agency.agency_name IS DISTINCT FROM EXCLUDED.agency_name
            """;

    private static final String DIM_COMPLAINT = """
            INSERT INTO analytics.dim_complaint_type (complaint_type, descriptor)
            SELECT DISTINCT complaint_type, descriptor
            FROM staging.service_requests WHERE run_id = ? AND partition_date = ?
            ORDER BY 1, 2
            ON CONFLICT (complaint_type, descriptor) DO NOTHING
            """;

    private static final String DIM_LOCATION = """
            INSERT INTO analytics.dim_location (borough, zip)
            SELECT DISTINCT borough, zip
            FROM staging.service_requests WHERE run_id = ? AND partition_date = ?
            ORDER BY 1, 2
            ON CONFLICT (borough, zip) DO NOTHING
            """;

    private static final String FACT = """
            INSERT INTO analytics.fact_service_request (unique_key, created_date_key, closed_date_key, agency_key,
                complaint_key, location_key, status, created_at, closed_at, resolution_hours, latitude, longitude,
                source_updated_at, loaded_run_id, loaded_at)
            SELECT s.unique_key,
                   to_char(s.created_at AT TIME ZONE 'America/New_York', 'YYYYMMDD')::int,
                   CASE WHEN s.closed_at IS NOT NULL
                        THEN to_char(s.closed_at AT TIME ZONE 'America/New_York', 'YYYYMMDD')::int END,
                   a.agency_key, c.complaint_key, l.location_key, s.status, s.created_at, s.closed_at,
                   s.resolution_hours, s.latitude, s.longitude, s.source_updated_at, ?, now()
            FROM staging.service_requests s
            JOIN analytics.dim_agency a ON a.agency_code = s.agency_code
            JOIN analytics.dim_complaint_type c ON c.complaint_type = s.complaint_type AND c.descriptor = s.descriptor
            JOIN analytics.dim_location l ON l.borough = s.borough AND l.zip = s.zip
            WHERE s.run_id = ? AND s.partition_date = ?
            ORDER BY s.unique_key
            ON CONFLICT (unique_key) DO UPDATE SET
                created_date_key = EXCLUDED.created_date_key, closed_date_key = EXCLUDED.closed_date_key,
                agency_key = EXCLUDED.agency_key, complaint_key = EXCLUDED.complaint_key,
                location_key = EXCLUDED.location_key, status = EXCLUDED.status, created_at = EXCLUDED.created_at,
                closed_at = EXCLUDED.closed_at, resolution_hours = EXCLUDED.resolution_hours,
                latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                source_updated_at = EXCLUDED.source_updated_at, loaded_run_id = EXCLUDED.loaded_run_id,
                loaded_at = EXCLUDED.loaded_at
            WHERE (analytics.fact_service_request.source_updated_at IS NULL AND EXCLUDED.source_updated_at IS NOT NULL)
               OR EXCLUDED.source_updated_at > analytics.fact_service_request.source_updated_at
            RETURNING (xmax = 0) AS inserted
            """;

    private final JsonMapper json;

    LoadTask(JdbcTemplate jdbc, PlatformTransactionManager txManager, JsonMapper json) {
        super(jdbc, txManager);
        this.json = json;
    }

    @Override
    public String type() {
        return "etl.load";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) {
        long start = System.nanoTime();
        String runId = in(c, "run_id");
        LocalDate day = day(c);

        long[] r = tx.execute(status -> {
            Date d = Date.valueOf(day);
            long staged = num(jdbc.queryForObject(
                    "SELECT count(*) FROM staging.service_requests WHERE run_id = ? AND partition_date = ?",
                    Long.class, runId, d));
            jdbc.update(DIM_DATE, runId, d, runId, d);
            jdbc.update(DIM_AGENCY, runId, d);
            jdbc.update(DIM_COMPLAINT, runId, d);
            jdbc.update(DIM_LOCATION, runId, d);
            List<Boolean> upserted = jdbc.query(FACT, (rs, i) -> rs.getBoolean(1), runId, runId, d);
            long inserted = upserted.stream().filter(b -> b).count();
            return new long[]{staged, inserted, upserted.size() - inserted};
        });

        long staged = r[0], inserted = r[1], updated = r[2];
        recordStats(runId, day, "load", staged, inserted + updated, 0, 0, inserted, updated, c.attempt(),
                millisSince(start), json.writeValueAsString(Map.of("unchanged", staged - inserted - updated)));
        return Map.of("inserted", String.valueOf(inserted), "updated", String.valueOf(updated));
    }
}
