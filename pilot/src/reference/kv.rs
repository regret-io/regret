use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::storage::rocks::RocksStore;
use crate::types::OpType;

use super::{
    AdapterBatchResponse, AdapterOpResult, CheckpointFailure, OpKind, OpStatus, Operation,
    RangeRecord, RecordState, ReferenceModel, SafetyViolation, Tolerance,
};

/// Persisted KV record stored in RocksDB state:{key}.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KVRecord {
    value: Option<String>,
    version_id: u64,
}

pub struct BasicKvReference {
    rocks: RocksStore,
    hypothesis_id: String,
    touched_keys: HashSet<String>,
}

impl BasicKvReference {
    pub fn new(rocks: RocksStore, hypothesis_id: String) -> Self {
        Self {
            rocks,
            hypothesis_id,
            touched_keys: HashSet::new(),
        }
    }

    fn should_ignore_field(field: &str, tolerance: &Option<Tolerance>) -> bool {
        if let Some(t) = tolerance {
            t.structural.iter().any(|s| s.field == field && s.ignore)
        } else {
            false
        }
    }

    /// Read a record from RocksDB.
    fn get_record(&self, key: &str) -> Option<KVRecord> {
        match self.rocks.read_state(&self.hypothesis_id, key) {
            Ok(Some(data)) => serde_json::from_slice(&data).ok(),
            _ => None,
        }
    }

    /// Write a record to RocksDB.
    fn put_record(&self, key: &str, record: &KVRecord) {
        if let Ok(data) = serde_json::to_vec(record) {
            if let Err(e) = self.rocks.write_state(&self.hypothesis_id, key, &data) {
                warn!(key, error = %e, "failed to write reference state");
            }
        }
    }

    /// Apply a successful write to the reference state (persisted in RocksDB).
    fn apply_write(&mut self, op: &Operation) {
        match &op.kind {
            OpKind::Put { key, value } => {
                let mut rec = self.get_record(key).unwrap_or_default();
                rec.value = Some(value.clone());
                rec.version_id += 1;
                self.put_record(key, &rec);
                self.touched_keys.insert(key.clone());
            }
            OpKind::Delete { key } => {
                if let Some(mut rec) = self.get_record(key) {
                    rec.value = None;
                    self.put_record(key, &rec);
                }
                self.touched_keys.insert(key.clone());
            }
            OpKind::DeleteRange { start, end } => {
                // Read all keys in range from RocksDB
                let keys = self.keys_in_range(start, end);
                for k in keys {
                    if let Some(mut rec) = self.get_record(&k) {
                        if rec.value.is_some() {
                            rec.value = None;
                            self.put_record(&k, &rec);
                        }
                    }
                    self.touched_keys.insert(k);
                }
            }
            OpKind::Cas { key, new_value, .. } => {
                let mut rec = self.get_record(key).unwrap_or_default();
                rec.value = Some(new_value.clone());
                rec.version_id += 1;
                self.put_record(key, &rec);
                self.touched_keys.insert(key.clone());
            }
            OpKind::EphemeralPut { key, value } => {
                let mut rec = self.get_record(key).unwrap_or_default();
                rec.value = Some(value.clone());
                rec.version_id += 1;
                self.put_record(key, &rec);
                self.touched_keys.insert(key.clone());
            }
            OpKind::IndexedPut { key, value, .. } => {
                let mut rec = self.get_record(key).unwrap_or_default();
                rec.value = Some(value.clone());
                rec.version_id += 1;
                self.put_record(key, &rec);
                self.touched_keys.insert(key.clone());
            }
            OpKind::SequencePut { prefix, .. } => {
                self.touched_keys.insert(prefix.clone());
            }
            _ => {}
        }
    }

    /// Get all state keys in a range by scanning touched_keys.
    /// Since RocksDB doesn't have efficient range scans on state: prefix with
    /// arbitrary key ranges, we scan touched_keys.
    fn keys_in_range(&self, start: &str, end: &str) -> Vec<String> {
        self.touched_keys
            .iter()
            .filter(|k| k.as_str() >= start && k.as_str() < end)
            .cloned()
            .collect()
    }

    /// Verify a read result against reference state.
    fn verify_read(
        &self,
        op: &Operation,
        result: &AdapterOpResult,
        tolerance: &Option<Tolerance>,
    ) -> Option<SafetyViolation> {
        let ignore_version = Self::should_ignore_field("metadata.version_id", tolerance);

        let parsed_status = OpStatus::from_str(&result.status).ok();

        match &op.kind {
            OpKind::Get { key } => {
                let rec = self.get_record(key).filter(|r| r.value.is_some());
                match rec {
                    None => {
                        if parsed_status != Some(OpStatus::NotFound) {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: OpType::Get.to_string(),
                                expected: format!("status={}", OpStatus::NotFound),
                                actual: format!("status={}", result.status),
                            });
                        }
                    }
                    Some(r) => {
                        if parsed_status != Some(OpStatus::Ok) {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: OpType::Get.to_string(),
                                expected: format!("status={}", OpStatus::Ok),
                                actual: format!("status={}", result.status),
                            });
                        }
                        if result.value.as_ref() != r.value.as_ref() {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: OpType::Get.to_string(),
                                expected: format!("value={:?}", r.value),
                                actual: format!("value={:?}", result.value),
                            });
                        }
                        if !ignore_version {
                            if let Some(actual_vid) = result.version_id {
                                if actual_vid != r.version_id {
                                    return Some(SafetyViolation {
                                        op_id: op.id.clone(),
                                        op: OpType::Get.to_string(),
                                        expected: format!("version_id={}", r.version_id),
                                        actual: format!("version_id={actual_vid}"),
                                    });
                                }
                            }
                        }
                    }
                }
                None
            }
            OpKind::RangeScan { start, end } => {
                // Build expected records from RocksDB state
                let mut expected: Vec<RangeRecord> = Vec::new();
                let mut all_keys: Vec<String> = self.touched_keys
                    .iter()
                    .filter(|k| k.as_str() >= start.as_str() && k.as_str() < end.as_str())
                    .cloned()
                    .collect();
                all_keys.sort();

                for k in &all_keys {
                    if let Some(rec) = self.get_record(k) {
                        if let Some(v) = &rec.value {
                            expected.push(RangeRecord {
                                key: k.clone(),
                                value: v.clone(),
                                version_id: rec.version_id,
                            });
                        }
                    }
                }

                if let Some(actual_records) = &result.records {
                    if actual_records.len() != expected.len() {
                        return Some(SafetyViolation {
                            op_id: op.id.clone(),
                            op: OpType::RangeScan.to_string(),
                            expected: format!("{} records", expected.len()),
                            actual: format!("{} records", actual_records.len()),
                        });
                    }
                    for (i, (exp, act)) in expected.iter().zip(actual_records.iter()).enumerate() {
                        let mut mismatch = exp.key != act.key || exp.value != act.value;
                        if !ignore_version && exp.version_id != act.version_id {
                            mismatch = true;
                        }
                        if mismatch {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: OpType::RangeScan.to_string(),
                                expected: format!("record[{i}]={{key={},value={}}}", exp.key, exp.value),
                                actual: format!("record[{i}]={{key={},value={}}}", act.key, act.value),
                            });
                        }
                    }
                }
                None
            }
            OpKind::List { prefix } => {
                let mut expected_keys: Vec<String> = self.touched_keys
                    .iter()
                    .filter(|k| k.starts_with(prefix.as_str()))
                    .filter(|k| {
                        self.get_record(k)
                            .map(|r| r.value.is_some())
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect();
                expected_keys.sort();

                if let Some(actual_keys) = &result.keys {
                    if actual_keys != &expected_keys {
                        return Some(SafetyViolation {
                            op_id: op.id.clone(),
                            op: OpType::List.to_string(),
                            expected: format!("keys={expected_keys:?}"),
                            actual: format!("keys={actual_keys:?}"),
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn is_write(op: &OpKind) -> bool {
        matches!(
            op,
            OpKind::Put { .. }
                | OpKind::Delete { .. }
                | OpKind::DeleteRange { .. }
                | OpKind::Cas { .. }
                | OpKind::EphemeralPut { .. }
                | OpKind::IndexedPut { .. }
                | OpKind::SequencePut { .. }
        )
    }

    fn is_read(op: &OpKind) -> bool {
        matches!(
            op,
            OpKind::Get { .. }
                | OpKind::RangeScan { .. }
                | OpKind::List { .. }
                | OpKind::IndexedGet { .. }
                | OpKind::IndexedList { .. }
                | OpKind::IndexedRangeScan { .. }
        )
    }
}

impl Default for KVRecord {
    fn default() -> Self {
        Self { value: None, version_id: 0 }
    }
}

impl ReferenceModel for BasicKvReference {
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<SafetyViolation> {
        let mut failures = Vec::new();

        let op_map: HashMap<&str, &Operation> = ops
            .iter()
            .filter(|op| !op.id.is_empty())
            .map(|op| (op.id.as_str(), op))
            .collect();

        for result in &response.results {
            let Some(op) = op_map.get(result.op_id.as_str()) else {
                continue;
            };

            let result_status = OpStatus::from_str(&result.status).ok();

            if Self::is_write(&op.kind) {
                if result_status == Some(OpStatus::Ok) {
                    self.apply_write(op);
                }
            } else if Self::is_read(&op.kind) {
                if let Some(failure) = self.verify_read(op, result, tolerance) {
                    failures.push(failure);
                }
            }
        }

        failures
    }

    fn verify_checkpoint(
        &self,
        actual_state: &HashMap<String, Option<RecordState>>,
        tolerance: &Option<Tolerance>,
    ) -> Vec<CheckpointFailure> {
        let mut failures = Vec::new();
        let expect = self.snapshot(&self.touched_keys);
        let ignore_version = Self::should_ignore_field("metadata.version_id", tolerance);

        let all_keys: HashSet<&String> = expect.keys().chain(actual_state.keys()).collect();

        for key in all_keys {
            let exp = expect.get(key).cloned().flatten();
            let act = actual_state.get(key).cloned().flatten();

            match (&exp, &act) {
                (None, None) => {}
                (Some(e), Some(a)) => {
                    let value_mismatch = e.value != a.value;
                    let version_mismatch = !ignore_version && e.version_id != a.version_id;
                    if value_mismatch || version_mismatch {
                        failures.push(CheckpointFailure {
                            key: key.clone(),
                            expected: Some(e.clone()),
                            actual: Some(a.clone()),
                        });
                    }
                }
                _ => {
                    failures.push(CheckpointFailure {
                        key: key.clone(),
                        expected: exp,
                        actual: act,
                    });
                }
            }
        }

        failures
    }

    fn touched_keys(&self) -> &HashSet<String> {
        &self.touched_keys
    }

    fn snapshot(&self, keys: &HashSet<String>) -> HashMap<String, Option<RecordState>> {
        keys.iter()
            .map(|k| {
                let state = self.get_record(k).and_then(|r| {
                    if r.value.is_none() {
                        None
                    } else {
                        Some(RecordState {
                            value: r.value,
                            version_id: r.version_id,
                            metadata: HashMap::new(),
                        })
                    }
                });
                (k.clone(), state)
            })
            .collect()
    }

    fn clear(&mut self) {
        if let Err(e) = self.rocks.clear_state(&self.hypothesis_id) {
            warn!(error = %e, "failed to clear reference state in RocksDB");
        }
        self.touched_keys.clear();
    }
}
