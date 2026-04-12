CREATE TABLE IF NOT EXISTS adapter_metric_samples (
    hypothesis_id TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    ts            INTEGER NOT NULL,
    metric        TEXT NOT NULL,
    labels        TEXT NOT NULL,
    value         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ams_run_ts ON adapter_metric_samples(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_ams_run_metric_ts ON adapter_metric_samples(run_id, metric, ts);
