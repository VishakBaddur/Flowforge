-- Write side: append-only event log. Never UPDATE or DELETE rows here.
CREATE TABLE workflow_events (
    workflow_id  VARCHAR(64)  NOT NULL,
    sequence     BIGINT       NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (workflow_id, sequence)   -- optimistic concurrency guard
);

-- Read side: one row per workflow, updated in the same transaction as the events.
CREATE TABLE workflow_runs (
    workflow_id  VARCHAR(64)   PRIMARY KEY,
    name         VARCHAR(255)  NOT NULL,
    status       VARCHAR(32)   NOT NULL,
    version      BIGINT        NOT NULL,
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL
);

-- Partial index: recovery only ever asks for RUNNING workflows.
CREATE INDEX idx_workflow_runs_running ON workflow_runs (workflow_id) WHERE status = 'RUNNING';
