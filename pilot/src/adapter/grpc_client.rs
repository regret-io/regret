use std::collections::HashMap;

use anyhow::{Context, Result};
use tonic::transport::Channel;

use regret_proto::regret_v1::adapter_service_client::AdapterServiceClient;
use regret_proto::regret_v1::{self as proto};

use crate::engine::executor::AdapterClient;
use crate::reference::{
    AdapterBatchResponse, AdapterOpResult, OpKind, Operation, RangeRecord, RecordState,
};

/// gRPC adapter client that talks to a real adapter.
pub struct GrpcAdapterClient {
    client: AdapterServiceClient<Channel>,
}

impl GrpcAdapterClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let url = if addr.starts_with("http") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        };
        let channel = Channel::from_shared(url)?
            .connect()
            .await
            .context("failed to connect to adapter")?;
        Ok(Self {
            client: AdapterServiceClient::new(channel),
        })
    }
}

#[async_trait::async_trait]
impl AdapterClient for GrpcAdapterClient {
    async fn execute_batch(
        &self,
        batch_id: &str,
        trace_id: &str,
        ops: &[Operation],
    ) -> Result<AdapterBatchResponse> {
        // Build op_id -> op_type map for response mapping
        let mut op_type_map: HashMap<String, String> = HashMap::new();
        let mut items = Vec::new();

        for op in ops {
            match &op.kind {
                OpKind::Fence => {
                    items.push(proto::Item {
                        item: Some(proto::item::Item::Fence(proto::Fence {})),
                    });
                }
                _ => {
                    let (op_type, payload) = serialize_op(&op.kind);
                    op_type_map.insert(op.id.clone(), op_type.clone());
                    items.push(proto::Item {
                        item: Some(proto::item::Item::Op(proto::Operation {
                            op_id: op.id.clone(),
                            op_type,
                            payload: payload.into(),
                        })),
                    });
                }
            }
        }

        let request = proto::BatchRequest {
            batch_id: batch_id.to_string(),
            trace_id: trace_id.to_string(),
            items,
        };

        let mut client = self.client.clone();
        let response = client
            .execute_batch(request)
            .await
            .context("ExecuteBatch RPC failed")?
            .into_inner();

        let results = response
            .results
            .into_iter()
            .map(|r| {
                // Parse payload JSON for result-specific fields
                let payload: serde_json::Value = if r.payload.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&r.payload).unwrap_or_default()
                };

                // Look up op type from our map
                let op_type = op_type_map
                    .get(&r.op_id)
                    .cloned()
                    .unwrap_or_default();

                AdapterOpResult {
                    op_id: r.op_id,
                    op: op_type,
                    status: r.status,
                    value: payload.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    version_id: payload.get("version_id").and_then(|v| v.as_u64()),
                    records: payload.get("records").and_then(|v| {
                        v.as_array().map(|arr| {
                            arr.iter()
                                .filter_map(|r| {
                                    Some(RangeRecord {
                                        key: r.get("key")?.as_str()?.to_string(),
                                        value: r.get("value")?.as_str()?.to_string(),
                                        version_id: r.get("version_id")?.as_u64().unwrap_or(0),
                                    })
                                })
                                .collect()
                        })
                    }),
                    keys: payload.get("keys").and_then(|v| {
                        v.as_array().map(|arr| {
                            arr.iter()
                                .filter_map(|k| k.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                    }),
                    deleted_count: payload.get("deleted_count").and_then(|v| v.as_u64()),
                    message: if r.message.is_empty() {
                        None
                    } else {
                        Some(r.message)
                    },
                }
            })
            .collect();

        Ok(AdapterBatchResponse {
            batch_id: response.batch_id,
            results,
        })
    }

    async fn read_state(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<RecordState>>> {
        let request = proto::ReadStateRequest {
            keys: keys.to_vec(),
        };

        let mut client = self.client.clone();
        let response = client
            .read_state(request)
            .await
            .context("ReadState RPC failed")?
            .into_inner();

        let mut result = HashMap::new();
        for record in response.records {
            let state = match record.value {
                Some(value_bytes) => {
                    let value_str = String::from_utf8_lossy(&value_bytes).to_string();
                    let version_id = record
                        .metadata
                        .get("version_id")
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    Some(RecordState {
                        value: Some(value_str),
                        version_id,
                        metadata: record.metadata,
                    })
                }
                None => None, // Key does not exist in adapter
            };
            result.insert(record.key, state);
        }

        Ok(result)
    }
}

fn serialize_op(kind: &OpKind) -> (String, Vec<u8>) {
    match kind {
        OpKind::Put { key, value } => (
            "put".to_string(),
            serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap(),
        ),
        OpKind::Delete { key } => (
            "delete".to_string(),
            serde_json::to_vec(&serde_json::json!({"key": key})).unwrap(),
        ),
        OpKind::DeleteRange { start, end } => (
            "delete_range".to_string(),
            serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap(),
        ),
        OpKind::Cas {
            key,
            expected_version_id,
            new_value,
        } => (
            "cas".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "key": key,
                "expected_version_id": expected_version_id,
                "new_value": new_value
            }))
            .unwrap(),
        ),
        OpKind::Get { key } => (
            "get".to_string(),
            serde_json::to_vec(&serde_json::json!({"key": key})).unwrap(),
        ),
        OpKind::RangeScan { start, end } => (
            "range_scan".to_string(),
            serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap(),
        ),
        OpKind::List { prefix } => (
            "list".to_string(),
            serde_json::to_vec(&serde_json::json!({"prefix": prefix})).unwrap(),
        ),
        OpKind::EphemeralPut { key, value } => (
            "ephemeral_put".to_string(),
            serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap(),
        ),
        OpKind::IndexedPut { key, value, index_name, index_key } => (
            "indexed_put".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "key": key, "value": value,
                "index_name": index_name, "index_key": index_key
            })).unwrap(),
        ),
        OpKind::IndexedGet { index_name, index_key } => (
            "indexed_get".to_string(),
            serde_json::to_vec(&serde_json::json!({"index_name": index_name, "index_key": index_key})).unwrap(),
        ),
        OpKind::IndexedList { index_name, start, end } => (
            "indexed_list".to_string(),
            serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap(),
        ),
        OpKind::IndexedRangeScan { index_name, start, end } => (
            "indexed_range_scan".to_string(),
            serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap(),
        ),
        OpKind::SequencePut { prefix, value, delta } => (
            "sequence_put".to_string(),
            serde_json::to_vec(&serde_json::json!({"prefix": prefix, "value": value, "delta": delta})).unwrap(),
        ),
        OpKind::Fence => ("fence".to_string(), vec![]),
    }
}
