package com.flowforge.worker.etl;

import com.flowforge.common.messaging.TaskCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Extracts one day of NYC 311 service requests into raw.service_requests (payloads stored exactly as received). */
@Component
class ExtractTask extends EtlTask {

    private static final String API = "https://data.cityofnewyork.us/resource/erm2-nwe9.json";

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
    private final JsonMapper json;
    private final String appToken = System.getenv("SOCRATA_APP_TOKEN");   // optional: higher API rate limits

    ExtractTask(JdbcTemplate jdbc, PlatformTransactionManager txManager, JsonMapper json) {
        super(jdbc, txManager);
        this.json = json;
    }

    @Override
    public String type() {
        return "etl.extract";
    }

    @Override
    public Map<String, String> execute(TaskCommand c) throws Exception {
        long start = System.nanoTime();
        String runId = in(c, "run_id");
        LocalDate day = day(c);
        int pageSize = Integer.parseInt(in(c, "page_size", "50000"));
        String fault = in(c, "inject_fault", "none");
        boolean dropCreatedDate = "drop_created_date".equals(fault);   // simulates an upstream schema change

        // 1. Network first, with no database connection held: fetch every page for this day.
        List<String> pages = new ArrayList<>();
        long fetched = 0;
        for (int offset = 0; ; offset += pageSize) {
            String body = fetch(day, pageSize, offset);
            int n = json.readTree(body).size();
            pages.add(body);
            fetched += n;
            if (n < pageSize) break;
        }

        // 2. One transaction: replace this partition's raw rows. Postgres splits the JSON array itself.
        Integer stored = tx.execute(status -> {
            Date d = Date.valueOf(day);
            jdbc.update("DELETE FROM raw.service_requests WHERE run_id = ? AND partition_date = ?", runId, d);
            int rows = 0;
            for (int i = 0; i < pages.size(); i++) {
                rows += jdbc.update("""
                        INSERT INTO raw.service_requests (run_id, partition_date, row_num, payload, extracted_at)
                        SELECT ?, ?, (? + t.ord)::int,
                               CASE WHEN ? THEN t.elem - 'created_date' ELSE t.elem END,
                               now()
                        FROM jsonb_array_elements(?::jsonb) WITH ORDINALITY AS t(elem, ord)
                        """, runId, d, (long) i * pageSize, dropCreatedDate, pages.get(i));
            }
            return rows;
        });

        recordStats(runId, day, "extract", fetched, stored, 0, 0, null, null, c.attempt(), millisSince(start),
                json.writeValueAsString(Map.of("pages", pages.size(), "injected_fault", fault)));
        return Map.of("rows", String.valueOf(stored));
    }

    private String fetch(LocalDate day, int limit, int offset) throws Exception {
        String where = "created_date >= '" + day + "T00:00:00' AND created_date < '" + day.plusDays(1) + "T00:00:00'";
        String url = API + "?$select=" + enc(":*, *") + "&$where=" + enc(where) + "&$order=" + enc(":id")
                + "&$limit=" + limit + "&$offset=" + offset;
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(90))
                .header("Accept", "application/json");
        if (appToken != null && !appToken.isBlank()) request.header("X-App-Token", appToken);
        HttpResponse<String> response = http.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("NYC Open Data API returned HTTP " + response.statusCode() + " for " + day);
        }
        return response.body();
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
