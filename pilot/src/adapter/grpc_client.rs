use std::collections::HashMap;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{error, info};

use regret_proto::regret_v1::adapter_service_client::AdapterServiceClient;
use regret_proto::regret_v1::{self as proto};

use crate::engine::executor::AdapterClient;
use crate::reference::{AdapterOpResult, OpKind, Operation, RangeRecord, RecordState};

/// gRPC adapter client using bidirectional streaming.
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
    /// Stream operations to the adapter:
    /// - Send all ops, inserting fences between write/read boundaries
    /// - Collect results and fence acks
    /// - Return all OpResults
    async fn execute_ops(
        &self,
        ops: &[Operation],
    ) -> Result<Vec<AdapterOpResult>> {
        let mut client = self.client.clone();

        // Create the request stream
        let (tx, rx) = mpsc::channel::<proto::ExecuteRequest>(256);
        let request_stream = ReceiverStream::new(rx);

        // Start the bidirectional stream
        let response = client
            .execute(request_stream)
            .await
            .context("Execute stream failed")?;
        let mut response_stream = response.into_inner();

        // Send ops in a background task, collect responses in foreground
        let ops_owned: Vec<Operation> = ops.to_vec();
        let send_handle = tokio::spawn(async move {
            let mut fence_counter = 0u64;
            for op in &ops_owned {
                let request = match &op.kind {
                    OpKind::Fence => {
                        fence_counter += 1;
                        proto::ExecuteRequest {
                            request: Some(proto::execute_request::Request::Fence(
                                proto::Fence { fence_id: fence_counter },
                            )),
                        }
                    }
                    _ => {
                        let (op_type, payload) = serialize_op(&op.kind);
                        proto::ExecuteRequest {
                            request: Some(proto::execute_request::Request::Op(
                                proto::Operation {
                                    op_id: op.id.clone(),
                                    op_type,
                                    payload: payload.into(),
                                },
                            )),
                        }
                    }
                };
                if tx.send(request).await.is_err() {
                    break;
                }
            }
            // Drop tx to signal end of stream
        });

        // Collect all results
        let mut results = Vec::new();
        while let Some(resp) = response_stream.message().await.context("reading response stream")? {
            match resp.response {
                Some(proto::execute_response::Response::Result(r)) => {
                    results.push(parse_op_result(r));
                }
                Some(proto::execute_response::Response::FenceAck(_)) => {
                    // Fence acknowledged — all prior ops completed
                }
                None => {}
            }
        }

        send_handle.await.context("send task failed")?;
        Ok(results)
    }

    async fn read_state(
        &self,
        key_prefix: &str,
    ) -> Result<HashMap<String, Option<RecordState>>> {
        let request = proto::ReadStateRequest {
            key_prefix: key_prefix.to_string(),
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
                None => None,
            };
            result.insert(record.key, state);
        }

        Ok(result)
    }

    async fn cleanup(&self, key_prefix: &str) -> Result<()> {
        let mut client = self.client.clone();
        client
            .cleanup(proto::CleanupRequest {
                key_prefix: key_prefix.to_string(),
            })
            .await
            .context("Cleanup RPC failed")?;
        Ok(())
    }
}

fn parse_op_result(r: proto::OpResult) -> AdapterOpResult {
    let payload: serde_json::Value = if r.payload.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&r.payload).unwrap_or_default()
    };

    AdapterOpResult {
        op_id: r.op_id.clone(),
        op: String::new(), // filled by executor from op map
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
        message: if r.message.is_empty() { None } else { Some(r.message) },
    }
}

fn serialize_op(kind: &OpKind) -> (String, Vec<u8>) {
    match kind {
        OpKind::Put { key, value } => ("put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap()),
        OpKind::Get { key } => ("get".into(), serde_json::to_vec(&serde_json::json!({"key": key})).unwrap()),
        OpKind::Delete { key } => ("delete".into(), serde_json::to_vec(&serde_json::json!({"key": key})).unwrap()),
        OpKind::DeleteRange { start, end } => ("delete_range".into(), serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap()),
        OpKind::List { prefix } => ("list".into(), serde_json::to_vec(&serde_json::json!({"prefix": prefix})).unwrap()),
        OpKind::RangeScan { start, end } => ("range_scan".into(), serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap()),
        OpKind::Cas { key, expected_version_id, new_value } => ("cas".into(), serde_json::to_vec(&serde_json::json!({"key": key, "expected_version_id": expected_version_id, "new_value": new_value})).unwrap()),
        OpKind::EphemeralPut { key, value } => ("ephemeral_put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap()),
        OpKind::IndexedPut { key, value, index_name, index_key } => ("indexed_put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value, "index_name": index_name, "index_key": index_key})).unwrap()),
        OpKind::IndexedGet { index_name, index_key } => ("indexed_get".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "index_key": index_key})).unwrap()),
        OpKind::IndexedList { index_name, start, end } => ("indexed_list".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap()),
        OpKind::IndexedRangeScan { index_name, start, end } => ("indexed_range_scan".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap()),
        OpKind::SequencePut { prefix, value, delta } => ("sequence_put".into(), serde_json::to_vec(&serde_json::json!({"prefix": prefix, "value": value, "delta": delta})).unwrap()),
        OpKind::Fence => ("fence".into(), vec![]),
    }
}
