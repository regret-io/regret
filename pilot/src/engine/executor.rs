use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::reference::{
    AdapterBatchResponse, AdapterOpResult, OpKind, OpStatus, Operation, RecordState,
    ReferenceModel, Tolerance,
};
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::{HypothesisResult, SqliteStore};
use crate::types::{HypothesisStatus, OpType};

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

impl std::fmt::Display for StopReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StopReason::Error(msg) => write!(f, "error: {msg}"),
            other => f.write_str(other.as_str()),
        }
    }
}

/// Adapter client abstraction.
#[async_trait::async_trait]
pub trait AdapterClient: Send + Sync {
    /// Stream ops to adapter via bidirectional gRPC, return all results.
    /// Fences in the ops list act as sync points.
    async fn execute_ops(&self, ops: &[Operation]) -> Result<Vec<AdapterOpResult>>;

    /// Read all records under a key prefix (checkpoint).
    async fn read_state(&self, key_prefix: &str) -> Result<HashMap<String, Option<RecordState>>>;

    /// Delete all data under a key prefix (hypothesis deletion).
    async fn cleanup(&self, key_prefix: &str) -> Result<()>;
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
            StopReason::Completed if progress.failed_response_ops == 0 && progress.failed_checkpoints == 0 => HypothesisStatus::Passed,
            StopReason::Stopped => HypothesisStatus::Stopped,
            _ => HypothesisStatus::Failed,
        };
        if let Err(e) = self.sqlite.update_hypothesis_status(&self.hypothesis_id, &status.to_string()).await {
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
        }

        // 5. Read all ops from origin
        let mut all_ops = Vec::new();
        let mut read_offset = 0usize;
        loop {
            let chunk = self
                .rocks
                .read_origin_batch(&self.hypothesis_id, read_offset, 1000)
                .context("failed to read origin")?;
            if chunk.is_empty() {
                break;
            }
            for data in &chunk {
                let json: serde_json::Value = serde_json::from_slice(data)?;
                if let Some(op) = parse_origin_op(&json) {
                    all_ops.push(op);
                }
            }
            read_offset += chunk.len();
        }

        // 6. Stream all ops to adapter via bidirectional gRPC
        self.emit_event(Event::batch_started(&self.run_id, "stream", 0, all_ops.len()));
        let stream_start = Instant::now();

        let adapter_results = if let Some(client) = &self.adapter_client {
            match client.execute_ops(&all_ops).await {
                Ok(results) => Some(results),
                Err(e) => {
                    self.emit_event(Event::batch_failed(&self.run_id, "stream", 1, &e.to_string()));
                    return Ok(StopReason::Error(e.to_string()));
                }
            }
        } else {
            // No adapter — build mock results for local testing
            let mock = self.build_mock_response(&all_ops);
            Some(mock.results)
        };

        let duration_ms = stream_start.elapsed().as_millis() as u64;
        self.emit_event(Event::batch_completed(&self.run_id, "stream", duration_ms));

        // 7. Reference processes results in fence-delimited segments.
        //    Writes before a fence are applied, reads after a fence are verified.
        if let Some(results) = &adapter_results {
            // Build result lookup by op_id
            let result_map: HashMap<String, &AdapterOpResult> = results.iter()
                .map(|r| (r.op_id.clone(), r))
                .collect();

            // Split ops at fence boundaries, process each segment
            let mut segment: Vec<Operation> = Vec::new();
            let mut total_failures = Vec::new();

            for op in &all_ops {
                if matches!(op.kind, OpKind::Fence) {
                    // Process accumulated segment
                    if !segment.is_empty() {
                        let seg_results: Vec<AdapterOpResult> = segment.iter()
                            .filter_map(|o| result_map.get(&o.id).map(|r| (*r).clone()))
                            .collect();
                        let response = crate::reference::AdapterBatchResponse {
                            batch_id: "stream".to_string(),
                            results: seg_results,
                        };
                        let failures = self.reference.process_response(&segment, &response, &self.tolerance);
                        total_failures.extend(failures);
                    }
                    segment.clear();
                } else {
                    segment.push(op.clone());
                }
            }
            // Process trailing segment
            if !segment.is_empty() {
                let seg_results: Vec<AdapterOpResult> = segment.iter()
                    .filter_map(|o| result_map.get(&o.id).map(|r| (*r).clone()))
                    .collect();
                let response = crate::reference::AdapterBatchResponse {
                    batch_id: "stream".to_string(),
                    results: seg_results,
                };
                let failures = self.reference.process_response(&segment, &response, &self.tolerance);
                total_failures.extend(failures);
            }

            if !total_failures.is_empty() {
                let mut progress = self.progress.write().await;
                progress.failed_response_ops += total_failures.len();
                drop(progress);

                for failure in &total_failures {
                    self.emit_event(Event::response_failed(&self.run_id, "stream", failure));
                }

                if self.config.fail_fast {
                    return Ok(StopReason::ResponseFailed);
                }
            }
        }

        {
            let mut progress = self.progress.write().await;
            progress.completed_ops = all_ops.len();
            progress.completed_batches = 1;
        }

        // 8. Checkpoint
        let mut checkpoint_counter = 0usize;
        checkpoint_counter += 1;
        self.run_checkpoint(checkpoint_counter).await?;

        Ok(StopReason::Completed)
    }

    async fn run_checkpoint(&mut self, checkpoint_num: usize) -> Result<StopReason> {
        let checkpoint_id = format!("ckpt-{checkpoint_num:04}");
        let key_prefix = format!("/{}/", self.hypothesis_id);
        let touched_count = self.reference.touched_keys().len();

        self.emit_event(Event::checkpoint_started(
            &self.run_id,
            &checkpoint_id,
            touched_count,
        ));

        {
            let mut progress = self.progress.write().await;
            progress.total_checkpoints += 1;
        }

        // Read actual state from adapter by scanning the hypothesis prefix
        let actual_state = if let Some(client) = &self.adapter_client {
            client.read_state(&key_prefix).await?
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
                touched_count,
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

    /// Build a mock response for local testing (no adapter).
    /// All writes succeed, reads return empty (reference model self-validates).
    fn build_mock_response(&self, ops: &[Operation]) -> AdapterBatchResponse {
        let results = ops
            .iter()
            .filter(|op| !op.id.is_empty())
            .map(|op| {
                let (status, op_type) = match &op.kind {
                    OpKind::Put { .. } => (OpStatus::Ok, OpType::Put),
                    OpKind::Get { .. } => (OpStatus::NotFound, OpType::Get),
                    OpKind::Delete { .. } => (OpStatus::Ok, OpType::Delete),
                    OpKind::DeleteRange { .. } => (OpStatus::Ok, OpType::DeleteRange),
                    OpKind::List { .. } => (OpStatus::Ok, OpType::List),
                    OpKind::RangeScan { .. } => (OpStatus::Ok, OpType::RangeScan),
                    OpKind::Cas { .. } => (OpStatus::Ok, OpType::Cas),
                    OpKind::EphemeralPut { .. } => (OpStatus::Ok, OpType::EphemeralPut),
                    OpKind::IndexedPut { .. } => (OpStatus::Ok, OpType::IndexedPut),
                    OpKind::IndexedGet { .. } => (OpStatus::NotFound, OpType::IndexedGet),
                    OpKind::IndexedList { .. } => (OpStatus::Ok, OpType::IndexedList),
                    OpKind::IndexedRangeScan { .. } => (OpStatus::Ok, OpType::IndexedRangeScan),
                    OpKind::SequencePut { .. } => (OpStatus::Ok, OpType::SequencePut),
                    OpKind::Fence => return None,
                };
                Some(AdapterOpResult {
                    op_id: op.id.clone(),
                    op: op_type.to_string(),
                    status: status.to_string(),
                    value: None,
                    version_id: None,
                    records: if op_type == OpType::RangeScan { Some(vec![]) } else { None },
                    keys: if op_type == OpType::List { Some(vec![]) } else { None },
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
        "get" => OpKind::Get {
            key: json.get("key")?.as_str()?.to_string(),
        },
        "delete" => OpKind::Delete {
            key: json.get("key")?.as_str()?.to_string(),
        },
        "delete_range" => OpKind::DeleteRange {
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "list" => OpKind::List {
            prefix: json.get("prefix")?.as_str()?.to_string(),
        },
        "range_scan" => OpKind::RangeScan {
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "cas" => OpKind::Cas {
            key: json.get("key")?.as_str()?.to_string(),
            expected_version_id: json.get("expected_version_id")?.as_u64()?,
            new_value: json.get("new_value")?.as_str()?.to_string(),
        },
        "ephemeral_put" => OpKind::EphemeralPut {
            key: json.get("key")?.as_str()?.to_string(),
            value: json.get("value")?.as_str()?.to_string(),
        },
        "indexed_put" => OpKind::IndexedPut {
            key: json.get("key")?.as_str()?.to_string(),
            value: json.get("value")?.as_str()?.to_string(),
            index_name: json.get("index_name")?.as_str()?.to_string(),
            index_key: json.get("index_key")?.as_str()?.to_string(),
        },
        "indexed_get" => OpKind::IndexedGet {
            index_name: json.get("index_name")?.as_str()?.to_string(),
            index_key: json.get("index_key")?.as_str()?.to_string(),
        },
        "indexed_list" => OpKind::IndexedList {
            index_name: json.get("index_name")?.as_str()?.to_string(),
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "indexed_range_scan" => OpKind::IndexedRangeScan {
            index_name: json.get("index_name")?.as_str()?.to_string(),
            start: json.get("start")?.as_str()?.to_string(),
            end: json.get("end")?.as_str()?.to_string(),
        },
        "sequence_put" => OpKind::SequencePut {
            prefix: json.get("prefix")?.as_str()?.to_string(),
            value: json.get("value")?.as_str()?.to_string(),
            delta: json.get("delta")?.as_u64().unwrap_or(1),
        },
        _ => return None,
    };

    Some(Operation { id, kind })
}
