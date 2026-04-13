CREATE TABLE IF NOT EXISTS chaos_workflows (
    id              TEXT PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    namespace       TEXT NOT NULL DEFAULT 'regret-system',
    steps           TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS chaos_workflow_runs (
    id              TEXT PRIMARY KEY,
    workflow_id     TEXT NOT NULL REFERENCES chaos_workflows(id),
    workflow_name   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    finished_at     TEXT,
    error           TEXT
);
