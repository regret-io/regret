use std::collections::{HashMap, HashSet};

use super::{
    AdapterBatchResponse, CheckpointFailure, OpExpect, OpKind, OpStatus, Operation, RecordState,
    ReferenceModel, ResponseFailure, Tolerance,
};

#[derive(Debug, Clone)]
struct Message {
    offset: u64,
    key: Option<String>,
    value: String,
}

/// Basic streaming reference model.
///
/// Tracks produced messages per topic-partition and consumed offsets.
/// Verification checks:
/// - Layer 1: produce acknowledged, consume returns expected messages
/// - Layer 2: no data loss, correct ordering, no duplicates
pub struct BasicStreamingReference {
    /// topic -> partition -> messages
    produced: HashMap<String, HashMap<u32, Vec<Message>>>,
    /// topic -> partition -> next expected offset
    consumed: HashMap<String, HashMap<u32, u64>>,
    touched_keys: HashSet<String>,
    pending_expects: HashMap<String, OpExpect>,
}

impl BasicStreamingReference {
    pub fn new() -> Self {
        Self {
            produced: HashMap::new(),
            consumed: HashMap::new(),
            touched_keys: HashSet::new(),
            pending_expects: HashMap::new(),
        }
    }

    fn topic_partition_key(topic: &str, partition: u32) -> String {
        format!("{topic}:{partition}")
    }
}

impl ReferenceModel for BasicStreamingReference {
    fn apply(&mut self, ops: &[Operation]) {
        for op in ops {
            match &op.kind {
                // For streaming, we reuse KV-style ops with semantic mapping:
                // put → produce (key=topic:partition, value=message)
                // get → consume (key=topic:partition)
                OpKind::Put { key, value } => {
                    // Parse key as "topic:partition"
                    let parts: Vec<&str> = key.rsplitn(2, ':').collect();
                    if parts.len() == 2 {
                        let partition: u32 = parts[0].parse().unwrap_or(0);
                        let topic = parts[1].to_string();
                        let messages = self
                            .produced
                            .entry(topic)
                            .or_default()
                            .entry(partition)
                            .or_default();
                        let offset = messages.len() as u64;
                        messages.push(Message {
                            offset,
                            key: None,
                            value: value.clone(),
                        });
                        self.touched_keys.insert(key.clone());
                    }
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status: OpStatus::Ok });
                }

                OpKind::Get { key } => {
                    // Consume next message from topic:partition
                    let parts: Vec<&str> = key.rsplitn(2, ':').collect();
                    if parts.len() == 2 {
                        let partition: u32 = parts[0].parse().unwrap_or(0);
                        let topic = parts[1].to_string();
                        let offset = self
                            .consumed
                            .entry(topic.clone())
                            .or_default()
                            .entry(partition)
                            .or_insert(0);

                        let messages = self
                            .produced
                            .get(&topic)
                            .and_then(|t| t.get(&partition));

                        if let Some(msgs) = messages {
                            if (*offset as usize) < msgs.len() {
                                let msg = &msgs[*offset as usize];
                                self.pending_expects.insert(
                                    op.id.clone(),
                                    OpExpect::Get {
                                        value: Some(msg.value.clone()),
                                        version_id: msg.offset,
                                    },
                                );
                                *offset += 1;
                            } else {
                                self.pending_expects.insert(
                                    op.id.clone(),
                                    OpExpect::Get {
                                        value: None,
                                        version_id: 0,
                                    },
                                );
                            }
                        } else {
                            self.pending_expects.insert(
                                op.id.clone(),
                                OpExpect::Get {
                                    value: None,
                                    version_id: 0,
                                },
                            );
                        }
                        self.touched_keys.insert(key.clone());
                    }
                }

                OpKind::Fence => {}

                // Other ops are no-ops for streaming
                _ => {
                    self.pending_expects
                        .insert(op.id.clone(), OpExpect::Write { status: OpStatus::Ok });
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
                OpExpect::Get { value, .. } => {
                    if value.is_none() {
                        if result.status != "not_found" {
                            failures.push(ResponseFailure {
                                op_id: result.op_id.clone(),
                                op: result.op.clone(),
                                expected: "no message available".to_string(),
                                actual: format!("status={}", result.status),
                            });
                        }
                    } else if result.value.as_ref() != value.as_ref() {
                        failures.push(ResponseFailure {
                            op_id: result.op_id.clone(),
                            op: result.op.clone(),
                            expected: format!("value={:?}", value),
                            actual: format!("value={:?}", result.value),
                        });
                    }
                }
                _ => {}
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

        // For streaming, checkpoint verifies no data loss:
        // each topic:partition should have all produced messages consumable
        for (topic, partitions) in &self.produced {
            for (partition, messages) in partitions {
                let key = Self::topic_partition_key(topic, *partition);
                let actual = actual_state.get(&key).cloned().flatten();

                // Expected: the total count of produced messages
                let expected_count = messages.len() as u64;

                match actual {
                    Some(record) => {
                        // version_id represents the message count in the adapter
                        if record.version_id != expected_count {
                            failures.push(CheckpointFailure {
                                key: key.clone(),
                                expected: Some(RecordState {
                                    value: None,
                                    version_id: expected_count,
                                    metadata: HashMap::from([(
                                        "message_count".to_string(),
                                        expected_count.to_string(),
                                    )]),
                                }),
                                actual: Some(record),
                            });
                        }
                    }
                    None if expected_count > 0 => {
                        failures.push(CheckpointFailure {
                            key,
                            expected: Some(RecordState {
                                value: None,
                                version_id: expected_count,
                                metadata: HashMap::from([(
                                    "message_count".to_string(),
                                    expected_count.to_string(),
                                )]),
                            }),
                            actual: None,
                        });
                    }
                    _ => {}
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
                let parts: Vec<&str> = k.rsplitn(2, ':').collect();
                let state = if parts.len() == 2 {
                    let partition: u32 = parts[0].parse().unwrap_or(0);
                    let topic = parts[1];
                    self.produced
                        .get(topic)
                        .and_then(|t| t.get(&partition))
                        .map(|msgs| RecordState {
                            value: None,
                            version_id: msgs.len() as u64,
                            metadata: HashMap::from([(
                                "message_count".to_string(),
                                msgs.len().to_string(),
                            )]),
                        })
                } else {
                    None
                };
                (k.clone(), state)
            })
            .collect()
    }

    fn clear(&mut self) {
        self.produced.clear();
        self.consumed.clear();
        self.touched_keys.clear();
        self.pending_expects.clear();
    }
}
