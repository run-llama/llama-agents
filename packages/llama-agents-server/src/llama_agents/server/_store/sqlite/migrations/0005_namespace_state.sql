-- migration: 5

-- Per-namespace state records: namespace becomes a key dimension alongside
-- run_id. SQLite cannot ALTER a primary key, so rebuild the table with the
-- composite PK and migrate existing rows as root-namespace rows (namespace '').
CREATE TABLE workflow_state_new (
    run_id TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT '',
    state_json TEXT NOT NULL DEFAULT '{}',
    state_type TEXT NOT NULL DEFAULT 'DictState',
    state_module TEXT NOT NULL DEFAULT 'workflows.context.state_store',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, namespace)
);

INSERT INTO workflow_state_new (run_id, namespace, state_json, state_type, state_module, created_at, updated_at)
SELECT run_id, '', state_json, state_type, state_module, created_at, updated_at
FROM workflow_state;

DROP TABLE workflow_state;

ALTER TABLE workflow_state_new RENAME TO workflow_state;
