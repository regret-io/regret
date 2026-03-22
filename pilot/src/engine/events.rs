use chrono::Utc;
use serde::Serialize;

use crate::reference::{CheckpointFailure, ResponseFailure};

#[derive(Debug, Clone, Serialize)]
pub struct OpRecord {
    pub op_id: String,
    pub op_type: String,
    pub payload: serde_json::Value,
    pub status: String,
}

/// All event types written to events.jsonl.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Event {
    RunStarted {
        run_id: String,
        hypothesis_id: String,
        timestamp: String,
    },
    AdapterDeployed {
        adapter: String,
        address: String,
        timestamp: String,
    },
    AdapterReady {
        adapter: String,
        timestamp: String,
    },
    BatchStarted {
        run_id: String,
        batch_id: String,
        offset: usize,
        size: usize,
        timestamp: String,
    },
    BatchCompleted {
        run_id: String,
        batch_id: String,
        duration_ms: u64,
        timestamp: String,
    },
    BatchFailed {
        run_id: String,
        batch_id: String,
        attempts: u32,
        error: String,
        timestamp: String,
    },
    ResponseFailed {
        run_id: String,
        batch_id: String,
        op_id: String,
        op: String,
        expected: String,
        actual: String,
        timestamp: String,
    },
    OperationBatch {
        run_id: String,
        batch_id: String,
        ops: Vec<OpRecord>,
        timestamp: String,
    },
    CheckpointStarted {
        run_id: String,
        checkpoint_id: String,
        keys: usize,
        timestamp: String,
    },
    CheckpointPassed {
        run_id: String,
        checkpoint_id: String,
        keys_checked: usize,
        timestamp: String,
    },
    CheckpointFailed {
        run_id: String,
        checkpoint_id: String,
        failures: usize,
        timestamp: String,
    },
    AdapterTeardown {
        adapter: String,
        timestamp: String,
    },
    RunCompleted {
        run_id: String,
        stop_reason: String,
        pass_rate: f64,
        timestamp: String,
    },
    RunStopped {
        run_id: String,
        stop_reason: String,
        timestamp: String,
    },
}

impl Event {
    pub fn now() -> String {
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    pub fn operation_batch(run_id: &str, batch_id: &str, ops: Vec<OpRecord>) -> Self {
        Event::OperationBatch {
            run_id: run_id.to_string(),
            batch_id: batch_id.to_string(),
            ops,
            timestamp: Self::now(),
        }
    }

    pub fn run_started(run_id: &str, hypothesis_id: &str) -> Self {
        Event::RunStarted {
            run_id: run_id.to_string(),
            hypothesis_id: hypothesis_id.to_string(),
            timestamp: Self::now(),
        }
    }

    pub fn batch_started(run_id: &str, batch_id: &str, offset: usize, size: usize) -> Self {
        Event::BatchStarted {
            run_id: run_id.to_string(),
            batch_id: batch_id.to_string(),
            offset,
            size,
            timestamp: Self::now(),
        }
    }

    pub fn batch_completed(run_id: &str, batch_id: &str, duration_ms: u64) -> Self {
        Event::BatchCompleted {
            run_id: run_id.to_string(),
            batch_id: batch_id.to_string(),
            duration_ms,
            timestamp: Self::now(),
        }
    }

    pub fn batch_failed(run_id: &str, batch_id: &str, attempts: u32, error: &str) -> Self {
        Event::BatchFailed {
            run_id: run_id.to_string(),
            batch_id: batch_id.to_string(),
            attempts,
            error: error.to_string(),
            timestamp: Self::now(),
        }
    }

    pub fn response_failed(run_id: &str, batch_id: &str, failure: &ResponseFailure) -> Self {
        Event::ResponseFailed {
            run_id: run_id.to_string(),
            batch_id: batch_id.to_string(),
            op_id: failure.op_id.clone(),
            op: failure.op.clone(),
            expected: failure.expected.clone(),
            actual: failure.actual.clone(),
            timestamp: Self::now(),
        }
    }

    pub fn checkpoint_started(run_id: &str, checkpoint_id: &str, keys: usize) -> Self {
        Event::CheckpointStarted {
            run_id: run_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            keys,
            timestamp: Self::now(),
        }
    }

    pub fn checkpoint_passed(run_id: &str, checkpoint_id: &str, keys_checked: usize) -> Self {
        Event::CheckpointPassed {
            run_id: run_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            keys_checked,
            timestamp: Self::now(),
        }
    }

    pub fn checkpoint_failed(run_id: &str, checkpoint_id: &str, failures: usize) -> Self {
        Event::CheckpointFailed {
            run_id: run_id.to_string(),
            checkpoint_id: checkpoint_id.to_string(),
            failures,
            timestamp: Self::now(),
        }
    }

    pub fn run_completed(run_id: &str, stop_reason: &str, pass_rate: f64) -> Self {
        Event::RunCompleted {
            run_id: run_id.to_string(),
            stop_reason: stop_reason.to_string(),
            pass_rate,
            timestamp: Self::now(),
        }
    }

    pub fn run_stopped(run_id: &str, stop_reason: &str) -> Self {
        Event::RunStopped {
            run_id: run_id.to_string(),
            stop_reason: stop_reason.to_string(),
            timestamp: Self::now(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}
