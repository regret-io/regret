use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::reference::{
    AdapterBatchResponse, AdapterOpResult, OpKind, Operation, RecordState, ReferenceModel,
    Tolerance,
};
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::{HypothesisResult, SqliteStore};

use super::events::Event;

/// Execution configuration from the run request.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub batch_size: usize,
    pub checkpoint_every: usize,
    pub fail_fast: bool,
    pub timeout_secs: u64,
    pub max_retry_attempts: u32,
    pub initial_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            checkpoint_every: 10,
            fail_fast: true,
            timeout_secs: 1800,
            max_retry_attempts: 3,
            initial_retry_delay_ms: 1000,
            max_retry_delay_ms: 30000,
        }
    }
}

/// Progress tracking shared between executor and status endpoint.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ProgressInfo {
    pub total_ops: usize,
    pub completed_ops: usize,
    pub total_batches: usize,
    pub completed_batches: usize,
    pub total_checkpoints: usize,
    pub passed_checkpoints: usize,
    pub failed_checkpoints: usize,
    pub failed_response_ops: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StopReason {
    Completed,
    ResponseFailed,
    CheckpointFailed,
    Stopped,
    Error(String),
}

impl StopReason {
    pub fn as_str(&self) -> &str {
        match self {
            StopReason::Completed => "completed",
            StopReason::ResponseFailed => "response_failed",
            StopReason::CheckpointFailed => "checkpoint_failed",
            StopReason::Stopped => "stopped",
            StopReason::Error(_) => "error",
        }
    }
}

/// Adapter client abstraction for executing batches and reading state.
#[async_trait::async_trait]
pub trait AdapterClient: Send + Sync {
    async fn execute_batch(
        &self,
        batch_id: &str,
        trace_id: &str,
        ops: &[Operation],
    ) -> Result<AdapterBatchResponse>;

    async fn read_state(&self, keys: &[String]) -> Result<HashMap<String, Option<RecordState>>>;
}

/// The main execution loop.
pub struct Executor {
    pub hypothesis_id: String,
    pub run_id: String,
    pub config: ExecutionConfig,
    pub tolerance: Option<Tolerance>,
    pub reference: Box<dyn ReferenceModel>,
    pub cancel: CancellationToken,
    pub progress: Arc<RwLock<ProgressInfo>>,
    pub rocks: RocksStore,
    pub files: FileStore,
    pub sqlite: SqliteStore,
    pub adapter_client: Option<Box<dyn AdapterClient>>,
}

impl Executor {
    /// Run the main execution loop (spec §7.1).
    /// Returns the reference model back to the manager when done.
    pub async fn run(mut self) -> (Box<dyn ReferenceModel>, StopReason) {
        let result = self.run_inner().await;
        let stop_reason = match result {
            Ok(reason) => reason,
            Err(e) => {
                error!(hypothesis_id = %self.hypothesis_id, error = %e, "executor error");
                StopReason::Error(e.to_string())
            }
        };

        // Write result to SQLite
        let progress = self.progress.read().await.clone();
        let pass_rate = if progress.total_checkpoints > 0 {
            progress.passed_checkpoints as f64 / progress.total_checkpoints as f64
        } else if progress.failed_response_ops == 0 {
            1.0
        } else {
            0.0
        };

        let result_record = HypothesisResult {
            id: uuid::Uuid::now_v7().to_string(),
            hypothesis_id: self.hypothesis_id.clone(),
            run_id: self.run_id.clone(),
            total_batches: progress.completed_batches as i64,
            total_checkpoints: progress.total_checkpoints as i64,
            passed_checkpoints: progress.passed_checkpoints as i64,
            failed_checkpoints: progress.failed_checkpoints as i64,
            total_response_ops: progress.completed_ops as i64,
            failed_response_ops: progress.failed_response_ops as i64,
            stop_reason: Some(stop_reason.as_str().to_string()),
            started_at: None,
            finished_at: Some(Event::now()),
            created_at: Event::now(),
        };

        if let Err(e) = self.sqlite.create_result(&result_record).await {
            error!(error = %e, "failed to write result record");
        }

        // Update hypothesis status
        let status = match &stop_reason {
            StopReason::Completed if progress.failed_response_ops == 0 && progress.failed_checkpoints == 0 => "passed",
            StopReason::Stopped => "stopped",
            _ => "failed",
        };
        if let Err(e) = self.sqlite.update_hypothesis_status(&self.hypothesis_id, status).await {
            error!(error = %e, "failed to update hypothesis status");
        }

        // Emit final event
        let event = match &stop_reason {
            StopReason::Stopped => Event::run_stopped(&self.run_id, "manual"),
            _ => Event::run_completed(&self.run_id, stop_reason.as_str(), pass_rate),
        };
        let _ = self.files.append_event(&self.hypothesis_id, &event.to_json());

        info!(
            hypothesis_id = %self.hypothesis_id,
            run_id = %self.run_id,
            stop_reason = stop_reason.as_str(),
            pass_rate,
            "run finished"
        );

        (self.reference, stop_reason)
    }

    async fn run_inner(&mut self) -> Result<StopReason> {
        info!(
            hypothesis_id = %self.hypothesis_id,
            run_id = %self.run_id,
            "starting execution"
        );

        // 1. Emit RunStarted
        self.emit_event(Event::run_started(&self.run_id, &self.hypothesis_id));

        // 2. Clear reference model state
        self.reference.clear();

        // 3. Clear RocksDB state
        self.rocks
            .clear_state(&self.hypothesis_id)
            .context("failed to clear RocksDB state")?;

        // 4. Count total ops
        let total_ops = self
            .rocks
            .get_origin_count(&self.hypothesis_id)
            .context("failed to count origin ops")?;
        {
            let mut progress = self.progress.write().await;
            progress.total_ops = total_ops;
            progress.total_batches = (total_ops + self.config.batch_size - 1) / self.config.batch_size;
        }

        // 5. Main loop
        let mut offset = 0usize;
        let mut batch_counter = 0usize;
        let mut checkpoint_counter = 0usize;

        while offset < total_ops {
            // Check cancellation
            if self.cancel.is_cancelled() {
                return Ok(StopReason::Stopped);
            }

            // Read next batch from RocksDB
            let batch_data = self
                .rocks
                .read_origin_batch(&self.hypothesis_id, offset, self.config.batch_size)
                .context("failed to read origin batch")?;

            if batch_data.is_empty() {
                break;
            }

            let batch_size = batch_data.len();
            let batch_id = format!("batch-{batch_counter:04}");

            // Parse operations
            let mut ops = Vec::new();
            for data in &batch_data {
                let json: serde_json::Value = serde_json::from_slice(data)?;
                if let Some(op) = parse_origin_op(&json) {
                    ops.push(op);
                }
            }

            self.emit_event(Event::batch_started(&self.run_id, &batch_id, offset, batch_size));
            let batch_start = Instant::now();

            // 1. Send batch to adapter
            let adapter_response = if let Some(client) = &self.adapter_client {
                match self.execute_with_retry(client.as_ref(), &batch_id, &ops).await {
                    Ok(resp) => Some(resp),
                    Err(e) => {
                        self.emit_event(Event::batch_failed(
                            &self.run_id,
                            &batch_id,
                            self.config.max_retry_attempts,
                            &e.to_string(),
                        ));
                        return Ok(StopReason::Error(e.to_string()));
                    }
                }
            } else {
                // No adapter — apply writes directly to reference for local testing
                self.reference.process_response(&ops, &self.build_mock_response(&ops), &self.tolerance);
                None
            };

            let duration_ms = batch_start.elapsed().as_millis() as u64;
            self.emit_event(Event::batch_completed(&self.run_id, &batch_id, duration_ms));

            // 2. Reference processes response:
            //    - Successful writes → update reference state
            //    - Reads → verify against reference state → failures
            if let Some(response) = &adapter_response {
                let failures = self.reference.process_response(&ops, response, &self.tolerance);
                if !failures.is_empty() {
                    let mut progress = self.progress.write().await;
                    progress.failed_response_ops += failures.len();
                    drop(progress);

                    for failure in &failures {
                        self.emit_event(Event::response_failed(&self.run_id, &batch_id, failure));
                    }

                    if self.config.fail_fast {
                        return Ok(StopReason::ResponseFailed);
                    }
                }
            }

            // Update progress
            {
                let mut progress = self.progress.write().await;
                progress.completed_ops += batch_size;
                progress.completed_batches += 1;
            }

            offset += batch_size;
            batch_counter += 1;

            // Layer 2: Checkpoint
            if batch_counter % self.config.checkpoint_every == 0 {
                checkpoint_counter += 1;
                let checkpoint_result = self.run_checkpoint(checkpoint_counter).await?;
                if checkpoint_result == StopReason::CheckpointFailed && self.config.fail_fast {
                    return Ok(StopReason::CheckpointFailed);
                }
            }
        }

        // Final checkpoint
        checkpoint_counter += 1;
        self.run_checkpoint(checkpoint_counter).await?;

        Ok(StopReason::Completed)
    }

    async fn run_checkpoint(&mut self, checkpoint_num: usize) -> Result<StopReason> {
        let checkpoint_id = format!("ckpt-{checkpoint_num:04}");
        let touched_keys: Vec<String> = self.reference.touched_keys().iter().cloned().collect();

        self.emit_event(Event::checkpoint_started(
            &self.run_id,
            &checkpoint_id,
            touched_keys.len(),
        ));

        {
            let mut progress = self.progress.write().await;
            progress.total_checkpoints += 1;
        }

        // Read actual state from adapter
        let actual_state = if let Some(client) = &self.adapter_client {
            client.read_state(&touched_keys).await?
        } else {
            // No adapter — use reference state as actual (for local testing)
            self.reference.snapshot(self.reference.touched_keys())
        };

        let failures = self
            .reference
            .verify_checkpoint(&actual_state, &self.tolerance);

        if failures.is_empty() {
            let mut progress = self.progress.write().await;
            progress.passed_checkpoints += 1;
            drop(progress);

            self.emit_event(Event::checkpoint_passed(
                &self.run_id,
                &checkpoint_id,
                touched_keys.len(),
            ));

            Ok(StopReason::Completed)
        } else {
            let mut progress = self.progress.write().await;
            progress.failed_checkpoints += 1;
            drop(progress);

            // Write checkpoint files
            let expect_snapshot = self.reference.snapshot(self.reference.touched_keys());
            let expect_json = serde_json::to_value(&expect_snapshot)?;
            let actual_json = serde_json::to_value(&actual_state)?;
            self.files
                .write_checkpoint(&self.hypothesis_id, &expect_json, &actual_json)?;

            self.emit_event(Event::checkpoint_failed(
                &self.run_id,
                &checkpoint_id,
                failures.len(),
            ));

            if self.config.fail_fast {
                Ok(StopReason::CheckpointFailed)
            } else {
                Ok(StopReason::Completed)
            }
        }
    }

    async fn execute_with_retry(
        &self,
        client: &dyn AdapterClient,
        batch_id: &str,
        ops: &[Operation],
    ) -> Result<AdapterBatchResponse> {
        let mut attempt = 0u32;
        let mut delay_ms = self.config.initial_retry_delay_ms;

        loop {
            match client.execute_batch(batch_id, &self.run_id, ops).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.config.max_retry_attempts {
                        return Err(e.context(format!(
                            "batch {batch_id} failed after {attempt} attempts"
                        )));
                    }
                    warn!(
                        batch_id,
                        attempt,
                        error = %e,
                        "batch failed, retrying after {delay_ms}ms"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(self.config.max_retry_delay_ms);
                }
            }
        }
    }

    /// Build a mock response for local testing (no adapter).
    /// All writes succeed, reads return empty (reference model self-validates).
    fn build_mock_response(&self, ops: &[Operation]) -> AdapterBatchResponse {
        let results = ops
            .iter()
            .filter(|op| !op.id.is_empty())
            .map(|op| {
                let (status, op_type) = match &op.kind {
                    OpKind::Put { .. } => ("ok", "put"),
                    OpKind::Delete { .. } => ("ok", "delete"),
                    OpKind::DeleteRange { .. } => ("ok", "delete_range"),
                    OpKind::Cas { .. } => ("ok", "cas"),
                    OpKind::Get { .. } => ("not_found", "get"),
                    OpKind::RangeScan { .. } => ("ok", "range_scan"),
                    OpKind::List { .. } => ("ok", "list"),
                    OpKind::Fence => return None,
                };
                Some(AdapterOpResult {
                    op_id: op.id.clone(),
                    op: op_type.to_string(),
                    status: status.to_string(),
                    value: None,
                    version_id: None,
                    records: if op_type == "range_scan" { Some(vec![]) } else { None },
                    keys: if op_type == "list" { Some(vec![]) } else { None },
                    deleted_count: None,
                    message: None,
                })
            })
            .flatten()
            .collect();

        AdapterBatchResponse {
            batch_id: String::new(),
            results,
        }
    }

    fn emit_event(&self, event: Event) {
        let json = event.to_json();
        if let Err(e) = self.files.append_event(&self.hypothesis_id, &json) {
            error!(error = %e, "failed to write event");
        }
    }
}

/// Parse a JSONL origin line into an Operation.
fn parse_origin_op(json: &serde_json::Value) -> Option<Operation> {
    if json.get("type").and_then(|v| v.as_str()) == Some("fence") {
        return Some(Operation {
            id: String::new(),
            kind: OpKind::Fence,
        });
    }

    let id = json.get("id")?.as_str()?.to_string();
    let op = json.get("op")?.as_str()?;

    let kind = match op {
        "put" => OpKind::Put {
            key: json.get("key")?.as_str()?.to_string(),
            value: json.get("value")?.as_str()?.to_string(),
        },
        "delete" => OpKind::Delete {
            key: json.get("key")?.as_str()?.to_string(),
        },
        "delete_range" => OpKind::DeleteRange {
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "cas" => OpKind::Cas {
            key: json.get("key")?.as_str()?.to_string(),
            expected_version_id: json.get("expected_version_id")?.as_u64()?,
            new_value: json.get("new_value")?.as_str()?.to_string(),
        },
        "get" => OpKind::Get {
            key: json.get("key")?.as_str()?.to_string(),
        },
        "range_scan" => OpKind::RangeScan {
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "list" => OpKind::List {
            prefix: json.get("prefix")?.as_str()?.to_string(),
        },
        _ => return None,
    };

    Some(Operation { id, kind })
}
