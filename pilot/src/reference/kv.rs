use std::collections::{BTreeMap, HashMap, HashSet};

use super::{
    AdapterBatchResponse, AdapterOpResult, CheckpointFailure, OpKind, Operation, RangeRecord,
    RecordState, ReferenceModel, ResponseFailure, Tolerance,
};

#[derive(Debug, Clone)]
struct KVRecord {
    value: Option<String>,
    version_id: u64,
}

impl Default for KVRecord {
    fn default() -> Self {
        Self {
            value: None,
            version_id: 0,
        }
    }
}

pub struct BasicKvReference {
    store: BTreeMap<String, KVRecord>,
    touched_keys: HashSet<String>,
}

impl BasicKvReference {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
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

    /// Apply a successful write to the reference state.
    fn apply_write(&mut self, op: &Operation) {
        match &op.kind {
            OpKind::Put { key, value } => {
                let rec = self.store.entry(key.clone()).or_default();
                rec.value = Some(value.clone());
                rec.version_id += 1;
                self.touched_keys.insert(key.clone());
            }
            OpKind::Delete { key } => {
                if let Some(rec) = self.store.get_mut(key) {
                    rec.value = None;
                }
                self.touched_keys.insert(key.clone());
            }
            OpKind::DeleteRange { start, end } => {
                let keys: Vec<String> = self
                    .store
                    .range(start.clone()..end.clone())
                    .filter(|(_, r)| r.value.is_some())
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in &keys {
                    if let Some(rec) = self.store.get_mut(k) {
                        rec.value = None;
                    }
                    self.touched_keys.insert(k.clone());
                }
            }
            OpKind::Cas { key, new_value, .. } => {
                let rec = self.store.entry(key.clone()).or_default();
                rec.value = Some(new_value.clone());
                rec.version_id += 1;
                self.touched_keys.insert(key.clone());
            }
            _ => {} // reads and fences don't modify state
        }
    }

    /// Verify a read result against reference state.
    fn verify_read(
        &self,
        op: &Operation,
        result: &AdapterOpResult,
        tolerance: &Option<Tolerance>,
    ) -> Option<ResponseFailure> {
        let ignore_version = Self::should_ignore_field("metadata.version_id", tolerance);

        match &op.kind {
            OpKind::Get { key } => {
                let rec = self.store.get(key).filter(|r| r.value.is_some());
                match rec {
                    None => {
                        // Key doesn't exist in reference — adapter should return not_found
                        if result.status != "not_found" {
                            return Some(ResponseFailure {
                                op_id: op.id.clone(),
                                op: "get".to_string(),
                                expected: "status=not_found".to_string(),
                                actual: format!("status={}", result.status),
                            });
                        }
                    }
                    Some(r) => {
                        if result.status != "ok" {
                            return Some(ResponseFailure {
                                op_id: op.id.clone(),
                                op: "get".to_string(),
                                expected: "status=ok".to_string(),
                                actual: format!("status={}", result.status),
                            });
                        }
                        // Check value
                        if result.value.as_ref() != r.value.as_ref() {
                            return Some(ResponseFailure {
                                op_id: op.id.clone(),
                                op: "get".to_string(),
                                expected: format!("value={:?}", r.value),
                                actual: format!("value={:?}", result.value),
                            });
                        }
                        // Check version_id
                        if !ignore_version {
                            if let Some(actual_vid) = result.version_id {
                                if actual_vid != r.version_id {
                                    return Some(ResponseFailure {
                                        op_id: op.id.clone(),
                                        op: "get".to_string(),
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
                let expected: Vec<RangeRecord> = self
                    .store
                    .range(start.clone()..end.clone())
                    .filter(|(_, r)| r.value.is_some())
                    .filter_map(|(k, r)| {
                        r.value.as_ref().map(|v| RangeRecord {
                            key: k.clone(),
                            value: v.clone(),
                            version_id: r.version_id,
                        })
                    })
                    .collect();

                if let Some(actual_records) = &result.records {
                    if actual_records.len() != expected.len() {
                        return Some(ResponseFailure {
                            op_id: op.id.clone(),
                            op: "range_scan".to_string(),
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
                            return Some(ResponseFailure {
                                op_id: op.id.clone(),
                                op: "range_scan".to_string(),
                                expected: format!("record[{i}]={{key={},value={}}}", exp.key, exp.value),
                                actual: format!("record[{i}]={{key={},value={}}}", act.key, act.value),
                            });
                        }
                    }
                }
                None
            }
            OpKind::List { prefix } => {
                let expected_keys: Vec<String> = self
                    .store
                    .range(prefix.clone()..)
                    .take_while(|(k, _)| k.starts_with(prefix.as_str()))
                    .filter(|(_, r)| r.value.is_some())
                    .map(|(k, _)| k.clone())
                    .collect();

                if let Some(actual_keys) = &result.keys {
                    if actual_keys != &expected_keys {
                        return Some(ResponseFailure {
                            op_id: op.id.clone(),
                            op: "list".to_string(),
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
        )
    }

    fn is_read(op: &OpKind) -> bool {
        matches!(
            op,
            OpKind::Get { .. } | OpKind::RangeScan { .. } | OpKind::List { .. }
        )
    }
}

impl ReferenceModel for BasicKvReference {
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<ResponseFailure> {
        let mut failures = Vec::new();

        // Build op_id -> Operation map
        let op_map: HashMap<&str, &Operation> = ops
            .iter()
            .filter(|op| !op.id.is_empty()) // skip fences (empty id)
            .map(|op| (op.id.as_str(), op))
            .collect();

        for result in &response.results {
            let Some(op) = op_map.get(result.op_id.as_str()) else {
                continue;
            };

            if Self::is_write(&op.kind) {
                if result.status == "ok" {
                    // Write succeeded — apply to reference state
                    self.apply_write(op);
                }
                // Write failures (not_found, version_mismatch) are expected for
                // delete-nonexistent and cas-mismatch — these are not verification errors.
                // The reference state is NOT updated for failed writes.
            } else if Self::is_read(&op.kind) {
                // Read — verify against current reference state
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
                let state = self.store.get(k).and_then(|r| {
                    if r.value.is_none() {
                        None
                    } else {
                        Some(RecordState {
                            value: r.value.clone(),
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
        self.store.clear();
        self.touched_keys.clear();
    }
}
