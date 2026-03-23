CREATE TABLE IF NOT EXISTS hypotheses (
    id            TEXT PRIMARY KEY,
    name          TEXT UNIQUE NOT NULL,
    generator     TEXT NOT NULL,
    adapter       TEXT,
    adapter_addr  TEXT,
    duration      TEXT,
    tolerance     TEXT,
    key_space     TEXT NOT NULL DEFAULT '{}',
    checkpoint_every TEXT NOT NULL DEFAULT '10m',
    config        TEXT NOT NULL DEFAULT '{}',
    status        TEXT NOT NULL DEFAULT 'idle',
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_run_at   TEXT
);

CREATE TABLE IF NOT EXISTS adapters (
    id              TEXT PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    image           TEXT NOT NULL,
    env             TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS generators (
    name            TEXT PRIMARY KEY,
    description     TEXT NOT NULL DEFAULT '',
    workload        TEXT NOT NULL,
    rate            INTEGER NOT NULL DEFAULT 0,
    builtin         INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS hypothesis_results (
    id                   TEXT PRIMARY KEY,
    hypothesis_id        TEXT NOT NULL REFERENCES hypotheses(id) ON DELETE CASCADE,
    run_id               TEXT NOT NULL,
    total_batches        INTEGER NOT NULL DEFAULT 0,
    total_checkpoints    INTEGER NOT NULL DEFAULT 0,
    passed_checkpoints   INTEGER NOT NULL DEFAULT 0,
    failed_checkpoints   INTEGER NOT NULL DEFAULT 0,
    total_response_ops   INTEGER NOT NULL DEFAULT 0,
    safety_violations    INTEGER NOT NULL DEFAULT 0,
    stop_reason          TEXT,
    started_at           TEXT,
    finished_at          TEXT,
    created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS chaos_scenarios (
    id              TEXT PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    namespace       TEXT NOT NULL DEFAULT 'regret-system',
    actions         TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS chaos_injections (
    id              TEXT PRIMARY KEY,
    scenario_id     TEXT NOT NULL REFERENCES chaos_scenarios(id),
    scenario_name   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    finished_at     TEXT,
    error           TEXT
)
