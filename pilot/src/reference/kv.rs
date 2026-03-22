use std::collections::{BTreeMap, HashMap, HashSet};

use super::{
    AdapterBatchResponse, CheckpointFailure, OpExpect, OpKind, OpStatus, Operation, RangeRecord,
    RecordState, ReferenceModel, ResponseFailure, StructuralTolerance, Tolerance,
};

#[derive(Debug, Clone)]
struct KVRecord {
    value: Option<String>,
    version_id: u64,
    deleted: bool,
}

impl Default for KVRecord {
    fn default() -> Self {
        Self {
            value: None,
            version_id: 0,
            deleted: false,
        }
    }
}

pub struct BasicKvReference {
    store: BTreeMap<String, KVRecord>,
    touched_keys: HashSet<String>,
    pending_expects: HashMap<String, OpExpect>,
}

impl BasicKvReference {
    pub fn new() -> Self {
        Self {
            store: BTreeMap::new(),
            touched_keys: HashSet::new(),
            pending_expects: HashMap::new(),
        }
    }

    /// Check if a field should be ignored per tolerance config.
    fn should_ignore_field(field: &str, tolerance: &Option<Tolerance>) -> bool {
        if let Some(t) = tolerance {
            for s in &t.structural {
                if s.field == field && s.ignore {
                    return true;
                }
            }
        }
        false
    }
}

impl ReferenceModel for BasicKvReference {
    fn apply(&mut self, ops: &[Operation]) {
        for op in ops {
            match &op.kind {
                OpKind::Put { key, value } => {
                    let rec = self.store.entry(key.clone()).or_default();
                    rec.value = Some(value.clone());
                    rec.version_id += 1;
                    rec.deleted = false;
                    self.touched_keys.insert(key.clone());
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status: OpStatus::Ok });
                }

                OpKind::Delete { key } => {
                    let existed = self.store.get(key).is_some_and(|r| !r.deleted && r.value.is_some());
                    // Mark as deleted instead of removing — preserves key existence tracking
                    if let Some(rec) = self.store.get_mut(key) {
                        rec.value = None;
                        rec.deleted = true;
                    }
                    self.touched_keys.insert(key.clone());
                    self.pending_expects.insert(
                        op.id.clone(),
                        OpExpect::Write {
                            status: if existed {
                                OpStatus::Ok
                            } else {
                                OpStatus::NotFound
                            },
                        },
                    );
                }

                OpKind::DeleteRange { start, end } => {
                    let keys: Vec<String> = self
                        .store
                        .range(start.clone()..end.clone())
                        .filter(|(_, r)| !r.deleted)
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in &keys {
                        if let Some(rec) = self.store.get_mut(k) {
                            rec.value = None;
                            rec.deleted = true;
                        }
                        self.touched_keys.insert(k.clone());
                    }
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status: OpStatus::Ok });
                }

                OpKind::Cas {
                    key,
                    expected_version_id,
                    new_value,
                } => {
                    let rec = self.store.entry(key.clone()).or_default();
                    let status = if rec.version_id == *expected_version_id {
                        rec.value = Some(new_value.clone());
                        rec.version_id += 1;
                        rec.deleted = false;
                        OpStatus::Ok
                    } else {
                        OpStatus::VersionMismatch
                    };
                    self.touched_keys.insert(key.clone());
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status });
                }

                OpKind::Get { key } => {
                    let rec = self.store.get(key).filter(|r| !r.deleted);
                    self.pending_expects.insert(
                        op.id.clone(),
                        OpExpect::Get {
                            value: rec.and_then(|r| r.value.clone()),
                            version_id: rec.map(|r| r.version_id).unwrap_or(0),
                        },
                    );
                }

                OpKind::RangeScan { start, end } => {
                    let records: Vec<RangeRecord> = self
                        .store
                        .range(start.clone()..end.clone())
                        .filter(|(_, r)| !r.deleted)
                        .filter_map(|(k, r)| {
                            r.value.as_ref().map(|v| RangeRecord {
                                key: k.clone(),
                                value: v.clone(),
                                version_id: r.version_id,
                            })
                        })
                        .collect();
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Range { records });
                }

                OpKind::List { prefix } => {
                    let keys: Vec<String> = self
                        .store
                        .range(prefix.clone()..)
                        .take_while(|(k, _)| k.starts_with(prefix.as_str()))
                        .filter(|(_, r)| !r.deleted && r.value.is_some())
                        .map(|(k, _)| k.clone())
                        .collect();
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::List { keys });
                }

                OpKind::Fence => {}
            }
        }
    }

    fn take_expects(&mut self) -> HashMap<String, OpExpect> {
        std::mem::take(&mut self.pending_expects)
    }

    fn verify_response(
        &self,
        expects: &HashMap<String, OpExpect>,
        actual: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<ResponseFailure> {
        let mut failures = Vec::new();
        let ignore_version = Self::should_ignore_field("metadata.version_id", tolerance);

        for result in &actual.results {
            let Some(expected) = expects.get(&result.op_id) else {
                continue;
            };

            match expected {
                OpExpect::Write { status } => {
                    if result.status != status.as_str() {
                        failures.push(ResponseFailure {
                            op_id: result.op_id.clone(),
                            op: result.op.clone(),
                            expected: format!("status={}", status.as_str()),
                            actual: format!("status={}", result.status),
                        });
                    }
                }
                OpExpect::Get { value, version_id } => {
                    if value.is_none() {
                        if result.status != "not_found" {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: "status=not_found".to_string(),
                                actual: format!("status={}", result.status),
                            });
                        }
                    } else if result.status != "ok" {
                        failures.push(ResponseFailure {
                            op_id: result.op_id.clone(),
                            op: result.op.clone(),
                            expected: "status=ok".to_string(),
                            actual: format!("status={}", result.status),
                        });
                    } else {
                        // Check value
                        if result.value.as_ref() != value.as_ref() {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: format!("value={:?}", value),
                                actual: format!("value={:?}", result.value),
                            });
                        }
                        // Check version_id (unless ignored by tolerance)
                        if !ignore_version {
                            if let Some(actual_vid) = result.version_id {
                                if actual_vid != *version_id {
                                    failures.push(ResponseFailure {
                                        op_id: result.op_id.clone(),
                                        op: result.op.clone(),
                                        expected: format!("version_id={version_id}"),
                                        actual: format!("version_id={actual_vid}"),
                                    });
                                }
                            }
                        }
                    }
                }
                OpExpect::Range { records } => {
                    if let Some(actual_records) = &result.records {
                        if actual_records.len() != records.len() {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: format!("{} records", records.len()),
                                actual: format!("{} records", actual_records.len()),
                            });
                        } else {
                            for (i, (exp, act)) in
                                records.iter().zip(actual_records.iter()).enumerate()
                            {
                                let mut mismatch = false;
                                let mut exp_str = format!("key={},value={}", exp.key, exp.value);
                                let mut act_str = format!("key={},value={}", act.key, act.value);

                                if exp.key != act.key || exp.value != act.value {
                                    mismatch = true;
                                }

                                if !ignore_version && exp.version_id != act.version_id {
                                    mismatch = true;
                                    exp_str = format!("{exp_str},vid={}", exp.version_id);
                                    act_str = format!("{act_str},vid={}", act.version_id);
                                }

                                if mismatch {
                                    failures.push(ResponseFailure {
                                        op_id: result.op_id.clone(),
                                        op: result.op.clone(),
                                        expected: format!("record[{i}]{{{exp_str}}}"),
                                        actual: format!("record[{i}]{{{act_str}}}"),
                                    });
                                }
                            }
                        }
                    }
                }
                OpExpect::List { keys } => {
                    if let Some(actual_keys) = &result.keys {
                        if actual_keys != keys {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: format!("keys={keys:?}"),
                                actual: format!("keys={actual_keys:?}"),
                            });
                        }
                    }
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
                    if r.deleted || r.value.is_none() {
                        None // Deleted or never had value → absent
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
        self.pending_expects.clear();
    }
}
