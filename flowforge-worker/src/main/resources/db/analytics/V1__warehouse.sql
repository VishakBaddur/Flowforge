-- Layers: raw (as received) -> staging (typed, cleaned) -> analytics (star schema). etl = bookkeeping.
CREATE SCHEMA etl;
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA analytics;

-- Watermark: data is complete through this date. Advanced ONLY by finalize, after every partition committed.
CREATE TABLE etl.watermark (
    pipeline          TEXT PRIMARY KEY,
    complete_through  DATE        NOT NULL,
    last_run_id       TEXT        NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE etl.run (
    run_id       TEXT PRIMARY KEY,
    pipeline     TEXT        NOT NULL,
    mode         TEXT        NOT NULL,          -- backfill | incremental
    start_date   DATE        NOT NULL,
    end_date     DATE        NOT NULL,
    partitions   INT         NOT NULL,
    status       TEXT        NOT NULL,          -- RUNNING | COMPLETED
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    report       JSONB
);

-- One row per (run, day, stage). Upserted, so a retried stage overwrites instead of double counting.
CREATE TABLE etl.partition_stats (
    run_id              TEXT        NOT NULL,
    partition_date      DATE        NOT NULL,
    stage               TEXT        NOT NULL,   -- extract | validate | transform | load
    rows_in             INT         NOT NULL DEFAULT 0,
    rows_out            INT         NOT NULL DEFAULT 0,
    rows_rejected       INT         NOT NULL DEFAULT 0,
    duplicates_removed  INT         NOT NULL DEFAULT 0,
    rows_inserted       INT,
    rows_updated        INT,
    attempt             INT         NOT NULL,
    duration_ms         BIGINT      NOT NULL,
    checks              JSONB,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, partition_date, stage)
);

-- Raw API payloads, exactly as received (duplicates included, so validation can count them).
CREATE TABLE raw.service_requests (
    run_id          TEXT        NOT NULL,
    partition_date  DATE        NOT NULL,
    row_num         INT         NOT NULL,
    payload         JSONB       NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, partition_date, row_num)
);

CREATE TABLE staging.service_requests (
    run_id             TEXT        NOT NULL,
    partition_date     DATE        NOT NULL,
    unique_key         TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    closed_at          TIMESTAMPTZ,
    agency_code        TEXT        NOT NULL,
    agency_name        TEXT,
    complaint_type     TEXT        NOT NULL,
    descriptor         TEXT        NOT NULL DEFAULT '',
    borough            TEXT        NOT NULL DEFAULT 'UNKNOWN',
    zip                TEXT        NOT NULL DEFAULT '',
    status             TEXT,
    latitude           NUMERIC(9,6),
    longitude          NUMERIC(9,6),
    resolution_hours   NUMERIC(10,2),
    source_updated_at  TIMESTAMPTZ,
    PRIMARY KEY (run_id, partition_date, unique_key)
);

CREATE TABLE staging.rejected (
    run_id          TEXT NOT NULL,
    partition_date  DATE NOT NULL,
    row_num         INT  NOT NULL,
    unique_key      TEXT,
    reason          TEXT NOT NULL,
    PRIMARY KEY (run_id, partition_date, row_num)
);

-- Star schema. Natural keys are UNIQUE so dimension loads are idempotent upserts.
CREATE TABLE analytics.dim_date (
    date_key     INT PRIMARY KEY,               -- yyyymmdd
    full_date    DATE     NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,             -- ISO: 1 = Monday
    is_weekend   BOOLEAN  NOT NULL
);

CREATE TABLE analytics.dim_agency (
    agency_key   SERIAL PRIMARY KEY,
    agency_code  TEXT NOT NULL UNIQUE,
    agency_name  TEXT
);

CREATE TABLE analytics.dim_complaint_type (
    complaint_key   SERIAL PRIMARY KEY,
    complaint_type  TEXT NOT NULL,
    descriptor      TEXT NOT NULL DEFAULT '',
    UNIQUE (complaint_type, descriptor)
);

CREATE TABLE analytics.dim_location (
    location_key  SERIAL PRIMARY KEY,
    borough       TEXT NOT NULL DEFAULT 'UNKNOWN',
    zip           TEXT NOT NULL DEFAULT '',
    UNIQUE (borough, zip)
);

-- Grain: one row per service request. unique_key is the natural key: reloads upsert, never duplicate.
CREATE TABLE analytics.fact_service_request (
    unique_key         TEXT PRIMARY KEY,
    created_date_key   INT  NOT NULL REFERENCES analytics.dim_date (date_key),
    closed_date_key    INT  REFERENCES analytics.dim_date (date_key),
    agency_key         INT  NOT NULL REFERENCES analytics.dim_agency (agency_key),
    complaint_key      INT  NOT NULL REFERENCES analytics.dim_complaint_type (complaint_key),
    location_key       INT  NOT NULL REFERENCES analytics.dim_location (location_key),
    status             TEXT,
    created_at         TIMESTAMPTZ NOT NULL,
    closed_at          TIMESTAMPTZ,
    resolution_hours   NUMERIC(10,2),
    latitude           NUMERIC(9,6),
    longitude          NUMERIC(9,6),
    source_updated_at  TIMESTAMPTZ,
    loaded_run_id      TEXT        NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_fact_created_date ON analytics.fact_service_request (created_date_key);

-- Rebuilt by finalize for the run's date range.
CREATE TABLE analytics.agg_daily_complaints (
    date_key              INT  NOT NULL,
    borough               TEXT NOT NULL,
    complaint_type        TEXT NOT NULL,
    requests              INT  NOT NULL,
    closed                INT  NOT NULL,
    avg_resolution_hours  NUMERIC(10,2),
    PRIMARY KEY (date_key, borough, complaint_type)
);
