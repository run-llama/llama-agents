CREATE TABLE IF NOT EXISTS _migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS workflow_handlers (
    handler_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    run_id TEXT,
    error TEXT,
    result TEXT,
    started_at TEXT,
    updated_at TEXT,
    completed_at TEXT,
    idle_since TEXT
);
CREATE INDEX IF NOT EXISTS idx_handlers_run_id ON workflow_handlers(run_id);
CREATE INDEX IF NOT EXISTS idx_handlers_status ON workflow_handlers(status);
CREATE INDEX IF NOT EXISTS idx_handlers_workflow_name ON workflow_handlers(workflow_name);

CREATE TABLE IF NOT EXISTS workflow_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    event TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS workflow_ticks (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    tick_data TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS workflow_state (
    run_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (run_id, key)
);
