use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::reference::{
    AdapterBatchResponse, AdapterOpResult, GetComparison, OpKind, OpStatus, Operation,
    RecordState, ReferenceModel, Tolerance,
};
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::{HypothesisResult, SqliteStore};
use crate::types::{HypothesisStatus, OpType};

use super::events::Event;

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub batch_size: usize,
    /// Checkpoint interval in seconds. Default 600 (10 minutes).
    pub checkpoint_interval_secs: u64,
    pub fail_fast: bool,
    /// Run duration in seconds. None = run forever until stopped.
    pub duration_secs: Option<u64>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            checkpoint_interval_secs: 600, // 10m
            fail_fast: true,
            duration_secs: None,
        }
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
    pub safety_violations: usize,
    pub elapsed_secs: u64,
    pub ops_per_sec: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StopReason {
    Completed,
    SafetyViolation,
    CheckpointFailed,
    Stopped,
    Error(String),
}

impl StopReason {
    pub fn as_str(&self) -> &str {
        match self {
            StopReason::Completed => "completed",
            StopReason::SafetyViolation => "safety_violation",
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
    pub generate_params: crate::generator::GenerateParams,
    pub reference: Box<dyn ReferenceModel>,
    pub cancel: CancellationToken,
    pub progress: Arc<RwLock<ProgressInfo>>,
    pub files: FileStore,
    pub sqlite: SqliteStore,
    pub rocks: Option<RocksStore>,
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
        } else if progress.safety_violations == 0 { 1.0 } else { 0.0 };

        let result_record = HypothesisResult {
            id: uuid::Uuid::now_v7().to_string(),
            hypothesis_id: self.hypothesis_id.clone(),
            run_id: self.run_id.clone(),
            total_batches: progress.completed_batches as i64,
            total_checkpoints: progress.total_checkpoints as i64,
            passed_checkpoints: progress.passed_checkpoints as i64,
            failed_checkpoints: progress.failed_checkpoints as i64,
            total_response_ops: progress.completed_ops as i64,
            safety_violations: progress.safety_violations as i64,
            stop_reason: Some(stop_reason.as_str().to_string()),
            started_at: None, finished_at: Some(Event::now()), created_at: Event::now(),
        };
        let _ = self.sqlite.create_result(&result_record).await;

        let status = match &stop_reason {
            StopReason::Completed if progress.safety_violations == 0 && progress.failed_checkpoints == 0 => HypothesisStatus::Passed,
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
        self.reference.set_run_id(&self.run_id);
        self.reference.clear();

        // Clean adapter data from previous run
        let prefix = self.reference.key_prefix();
        if let Some(client) = &self.adapter_client {
            if let Err(e) = client.cleanup(&prefix).await {
                warn!(error = %e, "failed to cleanup adapter, continuing anyway");
            }
        }

        // Start watching notifications (precondition for ephemeral/notification generators)
        let workload = self.generate_params.resolved_workload();
        let needs_watch = workload.contains_key("get_notifications")
            || workload.contains_key("session_restart");
        if needs_watch {
            if let Some(client) = &self.adapter_client {
                let watch_op = crate::reference::Operation {
                    id: "precondition-watch".to_string(),
                    kind: crate::reference::OpKind::WatchStart { prefix: prefix.clone() },
                };
                match client.execute_batch("precondition", &[watch_op]).await {
                    Ok(results) => {
                        for r in &results {
                            info!(op_id = %r.op_id, status = %r.status, message = ?r.message, "watch_start result");
                        }
                    }
                    Err(e) => { warn!(error = %e, "watch_start precondition failed"); }
                }
            }
        }

        // Update key prefix to include run_id, then create generator
        self.generate_params.key_space.prefix = format!("/ref/{}/{}/", self.hypothesis_id, self.run_id);
        let mut generator = crate::generator::create_generator(
            &self.generate_params,
            self.rocks.clone(),
        );
        let run_start = Instant::now();
        let duration_secs = self.config.duration_secs.unwrap_or(0);

        let mut total_ops = 0usize;
        let mut batch_counter = 0usize;
        let mut checkpoint_counter = 0usize;
        let mut last_checkpoint = Instant::now();
        let checkpoint_interval = std::time::Duration::from_secs(self.config.checkpoint_interval_secs);

        loop {
            // Check stop conditions
            if self.cancel.is_cancelled() {
                return Ok(StopReason::Stopped);
            }
            if duration_secs > 0 && run_start.elapsed().as_secs() >= duration_secs {
                break;
            }

            let batch_count = self.config.batch_size;
            let raw_ops = generator.gen_batch(batch_count);

            // Convert OriginOp to Operation
            let ops: Vec<Operation> = raw_ops.iter()
                .filter_map(|o| {
                    if let crate::generator::types::OriginOp::Operation(op) = o {
                        parse_origin_op(&serde_json::to_value(o).ok()?)
                    } else {
                        None
                    }
                })
                .collect();

            if ops.is_empty() {
                break;
            }

            // Split into conflict-free sub-batches
            let batches = self.split_into_batches(&ops);

            for batch in &batches {
                if self.cancel.is_cancelled() {
                    return Ok(StopReason::Stopped);
                }

                let batch_id = format!("batch-{batch_counter:04}");
                let start = Instant::now();

                let failures = if let Some(client) = &self.adapter_client {
                    let mut last_err = String::new();
                    let mut results = None;
                    for attempt in 1..=3u32 {
                        match client.execute_batch(&batch_id, batch).await {
                            Ok(r) => { results = Some(r); break; }
                            Err(e) => {
                                last_err = e.to_string();
                                if attempt < 3 {
                                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
                                }
                            }
                        }
                    }
                    let results = match results {
                        Some(r) => r,
                        None => {
                            self.emit_event(Event::batch_failed(&self.run_id, &batch_id, 3, &last_err));
                            return Ok(StopReason::Error(last_err));
                        }
                    };
                    let duration_ms = start.elapsed().as_millis() as u64;
                    let response = AdapterBatchResponse { batch_id: batch_id.clone(), results };
                    let failures = self.reference.process_response(batch, &response, &self.tolerance);

                    // Build op records with verification info
                    let failed_ops: std::collections::HashSet<&str> =
                        failures.iter().map(|f| f.op_id.as_str()).collect();
                    let op_records: Vec<super::events::OpRecord> = batch.iter().zip(response.results.iter())
                        .map(|(op, res)| {
                            let is_failed = failed_ops.contains(op.id.as_str());
                            let failure = failures.iter().find(|f| f.op_id == op.id);
                            // Only include full response (records, keys) when verification failed
                            let resp = if is_failed { op_response(res) } else { op_response_brief(res) };
                            super::events::OpRecord {
                                op_id: op.id.clone(),
                                op_type: op_type_str(&op.kind),
                                payload: op_payload(&op.kind),
                                status: res.status.clone(),
                                response: resp,
                                expected: failure.map(|f| serde_json::json!(f.expected)),
                                actual: failure.map(|f| serde_json::json!(f.actual)),
                                verified: Some(!is_failed),
                            }
                        })
                        .collect();
                    self.emit_event(Event::operation_batch(&self.run_id, &batch_id, batch_counter, duration_ms, op_records));
                    failures
                } else {
                    let mock = self.build_mock_results(batch);
                    let duration_ms = start.elapsed().as_millis() as u64;
                    let response = AdapterBatchResponse { batch_id: batch_id.clone(), results: mock };
                    let _ = self.reference.process_response(batch, &response, &self.tolerance);
                    let op_records: Vec<super::events::OpRecord> = batch.iter().zip(response.results.iter())
                        .map(|(op, res)| super::events::OpRecord {
                            op_id: op.id.clone(),
                            op_type: op_type_str(&op.kind),
                            payload: op_payload(&op.kind),
                            status: res.status.clone(),
                            response: op_response_brief(res),
                            expected: None,
                            actual: None,
                            verified: None,
                        })
                        .collect();
                    self.emit_event(Event::operation_batch(&self.run_id, &batch_id, batch_counter, duration_ms, op_records));
                    vec![]
                };
                if !failures.is_empty() {
                    let mut p = self.progress.write().await;
                    p.safety_violations += failures.len();
                    drop(p);
                    for f in &failures {
                        self.emit_event(Event::safety_violation(&self.run_id, &batch_id, f));
                    }
                    if self.config.fail_fast {
                        return Ok(StopReason::SafetyViolation);
                    }
                }

                {
                    let mut p = self.progress.write().await;
                    p.completed_ops += batch.len();
                    p.completed_batches += 1;
                    let elapsed = run_start.elapsed().as_secs();
                    p.elapsed_secs = elapsed;
                    p.ops_per_sec = if elapsed > 0 { p.completed_ops as f64 / elapsed as f64 } else { 0.0 };
                }

                batch_counter += 1;

                if last_checkpoint.elapsed() >= checkpoint_interval {
                    checkpoint_counter += 1;
                    last_checkpoint = Instant::now();
                    let r = self.run_checkpoint(checkpoint_counter).await?;
                    if r == StopReason::CheckpointFailed && self.config.fail_fast {
                        return Ok(StopReason::CheckpointFailed);
                    }
                }
            }

            total_ops += ops.len();
            {
                let mut p = self.progress.write().await;
                p.total_ops = total_ops;
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
    /// - No two write ops in a batch touch the same key (unless allow_key_conflicts)
    /// - Batch size capped at config.batch_size
    ///
    /// For kv-cas generator, key conflicts within a batch are intentional and
    /// must be preserved — the whole point is testing the exactly-one-wins invariant.
    fn split_into_batches(&self, ops: &[Operation]) -> Vec<Vec<Operation>> {
        let allow_key_conflicts = self.generate_params.generator == "kv-cas";

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
            } else if matches!(op.kind, OpKind::DeleteRange { .. } | OpKind::SessionRestart | OpKind::WatchStart { .. }) {
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

                if !allow_key_conflicts {
                    let key = op_key(&op.kind);
                    if let Some(k) = &key {
                        if write_keys.contains(k.as_str()) {
                            batches.push(std::mem::take(&mut writes));
                            write_keys.clear();
                        }
                        write_keys.insert(k.clone());
                    }
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
        let prefix = self.reference.key_prefix();

        { self.progress.write().await.total_checkpoints += 1; }

        let start = Instant::now();
        let actual = if let Some(client) = &self.adapter_client {
            client.read_state(&prefix).await?
        } else {
            self.reference.snapshot_all()
        };

        let failures = self.reference.verify_checkpoint(&actual, &self.tolerance);
        let duration_ms = start.elapsed().as_millis() as u64;
        let expect = self.reference.snapshot_all();

        let passed = failures.is_empty();
        self.emit_event(Event::checkpoint(&self.run_id, &id, duration_ms, &expect, &actual, &failures));

        if passed {
            { self.progress.write().await.passed_checkpoints += 1; }
            Ok(StopReason::Completed)
        } else {
            { self.progress.write().await.failed_checkpoints += 1; }
            let _ = self.files.write_checkpoint(&self.hypothesis_id, &serde_json::to_value(&expect)?, &serde_json::to_value(&actual)?);
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
                    OpKind::WatchStart { .. } | OpKind::SessionRestart | OpKind::GetNotifications => (OpStatus::Ok, OpType::Get),
                    OpKind::Fence => return None,
                };
                Some(AdapterOpResult {
                    op_id: op.id.clone(), op: op_type.to_string(), status: status.to_string(),
                    key: None, value: None, version_id: None,
                    records: if op_type == OpType::RangeScan { Some(vec![]) } else { None },
                    keys: if op_type == OpType::List { Some(vec![]) } else { None },
                    deleted_count: None, notifications: None, message: None,
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

fn op_type_str(kind: &OpKind) -> String {
    match kind {
        OpKind::Put { .. } => "put",
        OpKind::Get { comparison: GetComparison::Equal, .. } => "get",
        OpKind::Get { comparison: GetComparison::Floor, .. } => "get_floor",
        OpKind::Get { comparison: GetComparison::Ceiling, .. } => "get_ceiling",
        OpKind::Get { comparison: GetComparison::Lower, .. } => "get_lower",
        OpKind::Get { comparison: GetComparison::Higher, .. } => "get_higher",
        OpKind::Delete { .. } => "delete",
        OpKind::DeleteRange { .. } => "delete_range", OpKind::List { .. } => "list",
        OpKind::RangeScan { .. } => "range_scan", OpKind::Cas { .. } => "cas",
        OpKind::EphemeralPut { .. } => "ephemeral_put", OpKind::IndexedPut { .. } => "indexed_put",
        OpKind::IndexedGet { .. } => "indexed_get", OpKind::IndexedList { .. } => "indexed_list",
        OpKind::IndexedRangeScan { .. } => "indexed_range_scan",
        OpKind::SequencePut { .. } => "sequence_put",
        OpKind::WatchStart { .. } => "watch_start",
        OpKind::SessionRestart => "session_restart",
        OpKind::GetNotifications => "get_notifications",
        OpKind::Fence => "fence",
    }.to_string()
}

fn op_payload(kind: &OpKind) -> serde_json::Value {
    match kind {
        OpKind::Put { key, value } => serde_json::json!({"key": key, "value": value}),
        OpKind::Get { key, .. } => serde_json::json!({"key": key}),
        OpKind::Delete { key } => serde_json::json!({"key": key}),
        OpKind::DeleteRange { start, end } => serde_json::json!({"start": start, "end": end}),
        OpKind::List { start, end } => serde_json::json!({"start": start, "end": end}),
        OpKind::RangeScan { start, end } => serde_json::json!({"start": start, "end": end}),
        OpKind::Cas { key, expected_version_id, new_value } => serde_json::json!({"key": key, "expected_version_id": expected_version_id, "new_value": new_value}),
        OpKind::EphemeralPut { key, value } => serde_json::json!({"key": key, "value": value}),
        OpKind::IndexedPut { key, value, index_name, index_key } => serde_json::json!({"key": key, "value": value, "index_name": index_name, "index_key": index_key}),
        OpKind::IndexedGet { index_name, index_key } => serde_json::json!({"index_name": index_name, "index_key": index_key}),
        OpKind::IndexedList { index_name, start, end } => serde_json::json!({"index_name": index_name, "start": start, "end": end}),
        OpKind::IndexedRangeScan { index_name, start, end } => serde_json::json!({"index_name": index_name, "start": start, "end": end}),
        OpKind::SequencePut { prefix, value, delta } => serde_json::json!({"prefix": prefix, "value": value, "delta": delta}),
        OpKind::WatchStart { prefix } => serde_json::json!({"prefix": prefix}),
        OpKind::SessionRestart => serde_json::json!({}),
        OpKind::GetNotifications => serde_json::json!({}),
        OpKind::Fence => serde_json::json!({}),
    }
}

/// Extract the primary key from an operation (for conflict detection).
fn op_key(kind: &OpKind) -> Option<String> {
    match kind {
        OpKind::Put { key, .. } | OpKind::Get { key, .. } | OpKind::Delete { key }
        | OpKind::Cas { key, .. } | OpKind::EphemeralPut { key, .. }
        | OpKind::IndexedPut { key, .. } => Some(key.clone()),
        OpKind::SequencePut { prefix, .. } => Some(prefix.clone()),
        OpKind::List { .. } | OpKind::RangeScan { .. }
        | OpKind::IndexedGet { .. } | OpKind::IndexedList { .. }
        | OpKind::IndexedRangeScan { .. } => None, // reads don't conflict
        OpKind::DeleteRange { .. } | OpKind::WatchStart { .. }
        | OpKind::SessionRestart | OpKind::GetNotifications | OpKind::Fence => None,
    }
}

/// Build response JSON from adapter result — includes value, version_id, records, keys, etc.
fn op_response(res: &AdapterOpResult) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    if let Some(v) = &res.value {
        m.insert("value".into(), serde_json::json!(v));
    }
    if let Some(v) = res.version_id {
        m.insert("version_id".into(), serde_json::json!(v));
    }
    if let Some(recs) = &res.records {
        m.insert("records".into(), serde_json::to_value(recs).unwrap_or_default());
    }
    if let Some(keys) = &res.keys {
        m.insert("keys".into(), serde_json::json!(keys));
    }
    if let Some(c) = res.deleted_count {
        m.insert("deleted_count".into(), serde_json::json!(c));
    }
    if let Some(msg) = &res.message {
        m.insert("message".into(), serde_json::json!(msg));
    }
    serde_json::Value::Object(m)
}

/// Brief response — only scalar fields (value, version_id), skips records/keys arrays.
fn op_response_brief(res: &AdapterOpResult) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    if let Some(v) = &res.value {
        m.insert("value".into(), serde_json::json!(v));
    }
    if let Some(v) = res.version_id {
        m.insert("version_id".into(), serde_json::json!(v));
    }
    if let Some(c) = res.deleted_count {
        m.insert("deleted_count".into(), serde_json::json!(c));
    }
    if let Some(msg) = &res.message {
        m.insert("message".into(), serde_json::json!(msg));
    }
    serde_json::Value::Object(m)
}

fn parse_origin_op(json: &serde_json::Value) -> Option<Operation> {
    let id = json.get("id")?.as_str()?.to_string();
    let op = json.get("op")?.as_str()?;

    let kind = match op {
        "put" => OpKind::Put { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string() },
        "get" => OpKind::Get { key: json.get("key")?.as_str()?.to_string(), comparison: GetComparison::Equal },
        "get_floor" => OpKind::Get { key: json.get("key")?.as_str()?.to_string(), comparison: GetComparison::Floor },
        "get_ceiling" => OpKind::Get { key: json.get("key")?.as_str()?.to_string(), comparison: GetComparison::Ceiling },
        "get_lower" => OpKind::Get { key: json.get("key")?.as_str()?.to_string(), comparison: GetComparison::Lower },
        "get_higher" => OpKind::Get { key: json.get("key")?.as_str()?.to_string(), comparison: GetComparison::Higher },
        "delete" => OpKind::Delete { key: json.get("key")?.as_str()?.to_string() },
        "delete_range" => OpKind::DeleteRange { start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "list" => OpKind::List { start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "range_scan" => OpKind::RangeScan { start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "cas" => OpKind::Cas { key: json.get("key")?.as_str()?.to_string(), expected_version_id: json.get("expected_version_id")?.as_u64()?, new_value: json.get("new_value")?.as_str()?.to_string() },
        "ephemeral_put" => OpKind::EphemeralPut { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string() },
        "indexed_put" => OpKind::IndexedPut { key: json.get("key")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string(), index_name: json.get("index_name")?.as_str()?.to_string(), index_key: json.get("index_key")?.as_str()?.to_string() },
        "indexed_get" => OpKind::IndexedGet { index_name: json.get("index_name")?.as_str()?.to_string(), index_key: json.get("index_key")?.as_str()?.to_string() },
        "indexed_list" => OpKind::IndexedList { index_name: json.get("index_name")?.as_str()?.to_string(), start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "indexed_range_scan" => OpKind::IndexedRangeScan { index_name: json.get("index_name")?.as_str()?.to_string(), start: json.get("start")?.as_str()?.to_string(), end: json.get("end")?.as_str()?.to_string() },
        "sequence_put" => OpKind::SequencePut { prefix: json.get("prefix")?.as_str()?.to_string(), value: json.get("value")?.as_str()?.to_string(), delta: json.get("delta")?.as_u64().unwrap_or(1) },
        "watch_start" => OpKind::WatchStart { prefix: json.get("key")?.as_str()?.to_string() },
        "session_restart" => OpKind::SessionRestart,
        "get_notifications" => OpKind::GetNotifications,
        _ => return None,
    };
    Some(Operation { id, kind })
}
