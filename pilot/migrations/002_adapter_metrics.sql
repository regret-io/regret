CREATE TABLE IF NOT EXISTS adapter_metric_samples (
    hypothesis_id TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    ts            INTEGER NOT NULL,   -- unix epoch milliseconds
    metric        TEXT NOT NULL,      -- e.g. regret_adapter_op_duration_seconds_count
    labels        TEXT NOT NULL,      -- JSON object, e.g. {"op_type":"put","status":"ok"}
    value         REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ams_run_ts
    ON adapter_metric_samples(run_id, ts);

CREATE INDEX IF NOT EXISTS idx_ams_run_metric_ts
    ON adapter_metric_samples(run_id, metric, ts);
