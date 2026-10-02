-- Additive change: existing rows keep owner = NULL (visible to admins only).
ALTER TABLE workflow_runs ADD COLUMN owner VARCHAR(255);
CREATE INDEX idx_workflow_runs_owner_created ON workflow_runs (owner, created_at DESC);
