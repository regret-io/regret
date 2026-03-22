use std::collections::{HashMap, HashSet};

use std::str::FromStr;

use crate::types::OpType;

use super::{
    AdapterBatchResponse, CheckpointFailure, OpKind, OpStatus, Operation, RecordState,
    ReferenceModel, SafetyViolation, Tolerance,
};

#[derive(Debug, Clone)]
struct Message {
    offset: u64,
    value: String,
}

/// Basic streaming reference model.
pub struct BasicStreamingReference {
    /// topic -> partition -> messages
    produced: HashMap<String, HashMap<u32, Vec<Message>>>,
    /// topic -> partition -> consumed offset
    consumed: HashMap<String, HashMap<u32, u64>>,
    touched_keys: HashSet<String>,
}

impl BasicStreamingReference {
    pub fn new() -> Self {
        Self {
            produced: HashMap::new(),
            consumed: HashMap::new(),
            touched_keys: HashSet::new(),
        }
    }

    fn topic_partition_key(topic: &str, partition: u32) -> String {
        format!("{topic}:{partition}")
    }

    fn parse_topic_partition(key: &str) -> Option<(String, u32)> {
        let parts: Vec<&str> = key.rsplitn(2, ':').collect();
        if parts.len() == 2 {
            let partition: u32 = parts[0].parse().ok()?;
            Some((parts[1].to_string(), partition))
        } else {
            None
        }
    }
}

impl ReferenceModel for BasicStreamingReference {
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        _tolerance: &Option<Tolerance>,
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

            match &op.kind {
                OpKind::Put { key, value } if OpStatus::from_str(&result.status).ok() == Some(OpStatus::Ok) => {
                    // Produce succeeded — record message
                    if let Some((topic, partition)) = Self::parse_topic_partition(key) {
                        let messages = self
                            .produced
                            .entry(topic)
                            .or_default()
                            .entry(partition)
                            .or_default();
                        let offset = messages.len() as u64;
                        messages.push(Message {
                            offset,
                            value: value.clone(),
                        });
                        self.touched_keys.insert(key.clone());
                    }
                }
                OpKind::Get { key } => {
                    // Consume — verify against produced messages
                    if let Some((topic, partition)) = Self::parse_topic_partition(key) {
                        let consumed_offset = self
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
                            if (*consumed_offset as usize) < msgs.len() {
                                let expected_msg = &msgs[*consumed_offset as usize];
                                if result.value.as_deref() != Some(&expected_msg.value) {
                                    failures.push(SafetyViolation {
                                        op_id: op.id.clone(),
                                        op: OpType::Get.to_string(),
                                        expected: format!("value={:?}", expected_msg.value),
                                        actual: format!("value={:?}", result.value),
                                    });
                                }
                                *consumed_offset += 1;
                            }
                        }
                        self.touched_keys.insert(key.clone());
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

        for (topic, partitions) in &self.produced {
            for (partition, messages) in partitions {
                let key = Self::topic_partition_key(topic, *partition);
                let actual = actual_state.get(&key).cloned().flatten();
                let expected_count = messages.len() as u64;

                match actual {
                    Some(record) => {
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
                let state = Self::parse_topic_partition(k).and_then(|(topic, partition)| {
                    self.produced
                        .get(&topic)
                        .and_then(|t| t.get(&partition))
                        .map(|msgs| RecordState {
                            value: None,
                            version_id: msgs.len() as u64,
                            metadata: HashMap::from([(
                                "message_count".to_string(),
                                msgs.len().to_string(),
                            )]),
                        })
                });
                (k.clone(), state)
            })
            .collect()
    }

    fn clear(&mut self) {
        self.produced.clear();
        self.consumed.clear();
        self.touched_keys.clear();
    }
}
