-- migration: 2

-- Per-namespace state records: namespace becomes a key dimension alongside
-- run_id. Existing single rows are root-namespace rows (namespace = '').
ALTER TABLE workflow_state ADD COLUMN IF NOT EXISTS namespace TEXT NOT NULL DEFAULT '';

ALTER TABLE workflow_state DROP CONSTRAINT IF EXISTS workflow_state_pkey;
ALTER TABLE workflow_state ADD CONSTRAINT workflow_state_pkey PRIMARY KEY (run_id, namespace);
