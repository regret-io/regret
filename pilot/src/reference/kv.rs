use std::collections::{BTreeMap, HashMap, HashSet};

use super::{
    AdapterBatchResponse, CheckpointFailure, OpExpect, OpKind, OpStatus, Operation,
    RangeRecord, RecordState, ReferenceModel, ResponseFailure, Tolerance,
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
}

impl ReferenceModel for BasicKvReference {
    fn apply(&mut self, ops: &[Operation]) {
        for op in ops {
            match &op.kind {
                OpKind::Put { key, value } => {
                    let rec = self.store.entry(key.clone()).or_default();
                    rec.value = Some(value.clone());
                    rec.version_id += 1;
                    self.touched_keys.insert(key.clone());
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status: OpStatus::Ok });
                }

                OpKind::Delete { key } => {
                    let existed = self.store.remove(key).is_some();
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
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in &keys {
                        self.store.remove(k);
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
                        OpStatus::Ok
                    } else {
                        OpStatus::VersionMismatch
                    };
                    self.touched_keys.insert(key.clone());
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status });
                }

                OpKind::Get { key } => {
                    let rec = self.store.get(key);
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
                        .filter(|(_, r)| r.value.is_some())
                        .map(|(k, _)| k.clone())
                        .collect();
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::List { keys });
                }

                OpKind::Fence => {
                    // Fence is a sync point, no expectation needed
                }
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
        _tolerance: &Option<Tolerance>,
    ) -> Vec<ResponseFailure> {
        let mut failures = Vec::new();

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
                    } else {
                        if result.status != "ok" {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: "status=ok".to_string(),
                                actual: format!("status={}", result.status),
                            });
                        } else if result.value.as_ref() != value.as_ref() {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: format!("value={:?}", value),
                                actual: format!("value={:?}", result.value),
                            });
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
                                if exp.key != act.key || exp.value != act.value {
                                    failures.push(ResponseFailure {
                                        op_id: result.op_id.clone(),
                                        op: result.op.clone(),
                                        expected: format!(
                                            "record[{i}]={{key={},value={}}}",
                                            exp.key, exp.value
                                        ),
                                        actual: format!(
                                            "record[{i}]={{key={},value={}}}",
                                            act.key, act.value
                                        ),
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
        _tolerance: &Option<Tolerance>,
    ) -> Vec<CheckpointFailure> {
        let mut failures = Vec::new();
        let expect = self.snapshot(&self.touched_keys);

        let all_keys: HashSet<&String> = expect
            .keys()
            .chain(actual_state.keys())
            .collect();

        for key in all_keys {
            let exp = expect.get(key).cloned().flatten();
            let act = actual_state.get(key).cloned().flatten();

            match (&exp, &act) {
                (None, None) => {} // both absent
                (Some(e), Some(a)) => {
                    // Compare value and version
                    if e.value != a.value || e.version_id != a.version_id {
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
                    r.value.as_ref().map(|v| RecordState {
                        value: Some(v.clone()),
                        version_id: r.version_id,
                        metadata: HashMap::new(),
                    })
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
