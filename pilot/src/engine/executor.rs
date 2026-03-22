use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::reference::{
    AdapterBatchResponse, AdapterOpResult, OpKind, OpStatus, Operation, RecordState,
    ReferenceModel, Tolerance,
};
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::{HypothesisResult, SqliteStore};
use crate::types::{HypothesisStatus, OpType};

use super::events::Event;

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub batch_size: usize,
    pub checkpoint_every: usize,
    pub fail_fast: bool,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self { batch_size: 100, checkpoint_every: 10, fail_fast: true }
    }
}

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
    async fn execute_batch(&self, batch_id: &str, ops: &[Operation]) -> Result<Vec<AdapterOpResult>>;
    async fn read_state(&self, key_prefix: &str) -> Result<HashMap<String, Option<RecordState>>>;
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
    pub async fn run(mut self) -> (Box<dyn ReferenceModel>, StopReason) {
        let result = self.run_inner().await;
        let stop_reason = match result {
            Ok(r) => r,
            Err(e) => { error!(error = %e, "executor error"); StopReason::Error(e.to_string()) }
        };

        let progress = self.progress.read().await.clone();
        let pass_rate = if progress.total_checkpoints > 0 {
            progress.passed_checkpoints as f64 / progress.total_checkpoints as f64
        } else if progress.failed_response_ops == 0 { 1.0 } else { 0.0 };

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
            started_at: None, finished_at: Some(Event::now()), created_at: Event::now(),
        };
        let _ = self.sqlite.create_result(&result_record).await;

        let status = match &stop_reason {
            StopReason::Completed if progress.failed_response_ops == 0 && progress.failed_checkpoints == 0 => HypothesisStatus::Passed,
            StopReason::Stopped => HypothesisStatus::Stopped,
            _ => HypothesisStatus::Failed,
        };
        let _ = self.sqlite.update_hypothesis_status(&self.hypothesis_id, &status.to_string()).await;

        let event = match &stop_reason {
            StopReason::Stopped => Event::run_stopped(&self.run_id, "manual"),
            _ => Event::run_completed(&self.run_id, stop_reason.as_str(), pass_rate),
        };
        let _ = self.files.append_event(&self.hypothesis_id, &event.to_json());

        info!(hypothesis_id = %self.hypothesis_id, run_id = %self.run_id, stop_reason = stop_reason.as_str(), pass_rate, "run finished");
        (self.reference, stop_reason)
    }

    async fn run_inner(&mut self) -> Result<StopReason> {
        info!(hypothesis_id = %self.hypothesis_id, run_id = %self.run_id, "starting execution");
        self.emit_event(Event::run_started(&self.run_id, &self.hypothesis_id));
        self.reference.clear();
        self.rocks.clear_state(&self.hypothesis_id).context("failed to clear state")?;

        // Read all ops from origin
        let mut all_ops = Vec::new();
        let mut offset = 0usize;
        loop {
            let chunk = self.rocks.read_origin_batch(&self.hypothesis_id, offset, 1000)?;
            if chunk.is_empty() { break; }
            for data in &chunk {
                let json: serde_json::Value = serde_json::from_slice(data)?;
                if let Some(op) = parse_origin_op(&json) {
                    all_ops.push(op);
                }
            }
            offset += chunk.len();
        }

        {
            let mut p = self.progress.write().await;
            p.total_ops = all_ops.len();
        }

        // Split into batches: delete_range gets its own batch, others grouped by batch_size
        let batches = self.split_into_batches(&all_ops);
        {
            let mut p = self.progress.write().await;
            p.total_batches = batches.len();
        }

        let mut batch_counter = 0usize;
        let mut checkpoint_counter = 0usize;

        for batch in &batches {
            if self.cancel.is_cancelled() {
                return Ok(StopReason::Stopped);
            }

            let batch_id = format!("batch-{batch_counter:04}");
            self.emit_event(Event::batch_started(&self.run_id, &batch_id, batch_counter, batch.len()));
            let start = Instant::now();

            // Send batch to adapter
            let results = if let Some(client) = &self.adapter_client {
                match client.execute_batch(&batch_id, batch).await {
                    Ok(r) => r,
                    Err(e) => {
                        self.emit_event(Event::batch_failed(&self.run_id, &batch_id, 1, &e.to_string()));
                        return Ok(StopReason::Error(e.to_string()));
                    }
                }
            } else {
                self.build_mock_results(batch)
            };

            let duration_ms = start.elapsed().as_millis() as u64;
            self.emit_event(Event::batch_completed(&self.run_id, &batch_id, duration_ms));

            // Reference processes results
            let response = AdapterBatchResponse { batch_id: batch_id.clone(), results };
            let failures = self.reference.process_response(batch, &response, &self.tolerance);
            if !failures.is_empty() {
                let mut p = self.progress.write().await;
                p.failed_response_ops += failures.len();
                drop(p);
                for f in &failures {
                    self.emit_event(Event::response_failed(&self.run_id, &batch_id, f));
                }
                if self.config.fail_fast {
                    return Ok(StopReason::ResponseFailed);
                }
            }

            {
                let mut p = self.progress.write().await;
                p.completed_ops += batch.len();
                p.completed_batches += 1;
            }

            batch_counter += 1;

            // Checkpoint
            if batch_counter % self.config.checkpoint_every == 0 {
                checkpoint_counter += 1;
                let r = self.run_checkpoint(checkpoint_counter).await?;
                if r == StopReason::CheckpointFailed && self.config.fail_fast {
                    return Ok(StopReason::CheckpointFailed);
                }
            }
        }

        // Final checkpoint
        checkpoint_counter += 1;
        self.run_checkpoint(checkpoint_counter).await?;

        Ok(StopReason::Completed)
    }

    /// Split ops into conflict-free batches.
    /// Rules:
    /// - Writes and reads are never in the same batch
    /// - delete_range always gets its own batch
    /// - No two write ops in a batch touch the same key
    /// - Batch size capped at config.batch_size
    fn split_into_batches(&self, ops: &[Operation]) -> Vec<Vec<Operation>> {
        let mut batches = Vec::new();
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        let mut write_keys = std::collections::HashSet::new();

        for op in ops {
            let is_read = matches!(op.kind,
                OpKind::Get { .. } | OpKind::List { .. } | OpKind::RangeScan { .. }
                | OpKind::IndexedGet { .. } | OpKind::IndexedList { .. } | OpKind::IndexedRangeScan { .. }
            );

            if is_read {
                // Flush pending writes first, then accumulate reads
                if !writes.is_empty() {
                    batches.push(std::mem::take(&mut writes));
                    write_keys.clear();
                }
                reads.push(op.clone());
                if reads.len() >= self.config.batch_size {
                    batches.push(std::mem::take(&mut reads));
                }
            } else if matches!(op.kind, OpKind::DeleteRange { .. }) {
                // Flush everything, then delete_range alone
                if !writes.is_empty() {
                    batches.push(std::mem::take(&mut writes));
                    write_keys.clear();
                }
                if !reads.is_empty() {
                    batches.push(std::mem::take(&mut reads));
                }
                batches.push(vec![op.clone()]);
            } else {
                // Write op — flush reads first if any
                if !reads.is_empty() {
                    batches.push(std::mem::take(&mut reads));
                }

                let key = op_key(&op.kind);
                if let Some(k) = &key {
                    if write_keys.contains(k.as_str()) {
                        batches.push(std::mem::take(&mut writes));
                        write_keys.clear();
                    }
                    write_keys.insert(k.clone());
                }

                writes.push(op.clone());
                if writes.len() >= self.config.batch_size {
                    batches.push(std::mem::take(&mut writes));
                    write_keys.clear();
                }
            }
        }
        if !writes.is_empty() { batches.push(writes); }
        if !reads.is_empty() { batches.push(reads); }
        batches
    }

    async fn run_checkpoint(&mut self, num: usize) -> Result<StopReason> {
        let id = format!("ckpt-{num:04}");
        let prefix = format!("/{}/", self.hypothesis_id);
        let count = self.reference.touched_keys().len();

        self.emit_event(Event::checkpoint_started(&self.run_id, &id, count));
        { self.progress.write().await.total_checkpoints += 1; }

        let actual = if let Some(client) = &self.adapter_client {
            client.read_state(&prefix).await?
        } else {
            self.reference.snapshot(self.reference.touched_keys())
        };

        let failures = self.reference.verify_checkpoint(&actual, &self.tolerance);

        if failures.is_empty() {
            { self.progress.write().await.passed_checkpoints += 1; }
            self.emit_event(Event::checkpoint_passed(&self.run_id, &id, count));
            Ok(StopReason::Completed)
        } else {
            { self.progress.write().await.failed_checkpoints += 1; }
            let expect = self.reference.snapshot(self.reference.touched_keys());
            let _ = self.files.write_checkpoint(&self.hypothesis_id, &serde_json::to_value(&expect)?, &serde_json::to_value(&actual)?);
            self.emit_event(Event::checkpoint_failed(&self.run_id, &id, failures.len()));
            if self.config.fail_fast { Ok(StopReason::CheckpointFailed) } else { Ok(StopReason::Completed) }
        }
    }

    fn build_mock_results(&self, ops: &[Operation]) -> Vec<AdapterOpResult> {
        ops.iter()
            .filter(|o| !o.id.is_empty())
            .filter_map(|op| {
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
                    op_id: op.id.clone(), op: op_type.to_string(), status: status.to_string(),
                    value: None, version_id: None,
                    records: if op_type == OpType::RangeScan { Some(vec![]) } else { None },
                    keys: if op_type == OpType::List { Some(vec![]) } else { None },
                    deleted_count: None, message: None,
                })
            })
            .collect()
    }

    fn emit_event(&self, event: Event) {
        if let Err(e) = self.files.append_event(&self.hypothesis_id, &event.to_json()) {
            error!(error = %e, "failed to write event");
        }
    }
}

/// Extract the primary key from an operation (for conflict detection).
fn op_key(kind: &OpKind) -> Option<String> {
    match kind {
        OpKind::Put { key, .. } | OpKind::Get { key } | OpKind::Delete { key }
        | OpKind::Cas { key, .. } | OpKind::EphemeralPut { key, .. }
        | OpKind::IndexedPut { key, .. } => Some(key.clone()),
        OpKind::SequencePut { prefix, .. } => Some(prefix.clone()),
        OpKind::List { .. } | OpKind::RangeScan { .. }
        | OpKind::IndexedGet { .. } | OpKind::IndexedList { .. }
        | OpKind::IndexedRangeScan { .. } => None, // reads don't conflict
        OpKind::DeleteRange { .. } | OpKind::Fence => None,
    }
}

fn parse_origin_op(json: &serde_json::Value) -> Option<Operation> {
    let id = json.get("id")?.as_str()?.to_string();
    let op = json.get("op")?.as_str()?;

    let kind = match op {
        "put" => OpKind::Put { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string() },
        "get" => OpKind::Get { key: json.get("key")?.as_str()?.to_string() },
        "delete" => OpKind::Delete { key: json.get("key")?.as_str()?.to_string() },
        "delete_range" => OpKind::DeleteRange { start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "list" => OpKind::List { prefix: json.get("prefix")?.as_str()?.to_string() },
        "range_scan" => OpKind::RangeScan { start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "cas" => OpKind::Cas { key: json.get("key")?.as_str()?.to_string(), expected_version_id: json.get("expected_version_id")?.as_u64()?, new_value: json.get("new_value")?.as_str()?.to_string() },
        "ephemeral_put" => OpKind::EphemeralPut { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string() },
        "indexed_put" => OpKind::IndexedPut { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string(), index_name: json.get("index_name")?.as_str()?.to_string(), index_key: json.get("index_key")?.as_str()?.to_string() },
        "indexed_get" => OpKind::IndexedGet { index_name: json.get("index_name")?.as_str()?.to_string(), index_key: json.get("index_key")?.as_str()?.to_string() },
        "indexed_list" => OpKind::IndexedList { index_name: json.get("index_name")?.as_str()?.to_string(), start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "indexed_range_scan" => OpKind::IndexedRangeScan { index_name: json.get("index_name")?.as_str()?.to_string(), start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "sequence_put" => OpKind::SequencePut { prefix: json.get("prefix")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string(), delta: json.get("delta")?.as_u64().unwrap_or(1) },
        _ => return None,
    };
    Some(Operation { id, kind })
}
