use std::collections::HashMap;

use anyhow::{Context, Result};
use tonic::transport::Channel;

use regret_proto::regret_v1::adapter_service_client::AdapterServiceClient;
use regret_proto::regret_v1::{self as proto};

use crate::engine::executor::AdapterClient;
use crate::reference::{AdapterOpResult, OpKind, Operation, RangeRecord, RecordState};

pub struct GrpcAdapterClient {
    client: AdapterServiceClient<Channel>,
}

impl GrpcAdapterClient {
    pub async fn cleanup_prefix(addr: &str, key_prefix: &str) -> Result<()> {
        let client = Self::connect(addr).await?;
        client.cleanup(key_prefix).await
    }

    pub async fn connect(addr: &str) -> Result<Self> {
        let url = if addr.starts_with("http") { addr.to_string() } else { format!("http://{addr}") };
        let channel = Channel::from_shared(url)?.connect().await.context("failed to connect")?;
        Ok(Self { client: AdapterServiceClient::new(channel) })
    }
}

#[async_trait::async_trait]
impl AdapterClient for GrpcAdapterClient {
    async fn execute_batch(
        &self,
        batch_id: &str,
        ops: &[Operation],
    ) -> Result<Vec<AdapterOpResult>> {
        let mut client = self.client.clone();

        let proto_ops: Vec<proto::Operation> = ops.iter()
            .filter(|o| !o.id.is_empty())
            .map(|o| {
                let (op_type, payload) = serialize_op(&o.kind);
                proto::Operation { op_id: o.id.clone(), op_type, payload: payload.into() }
            })
            .collect();

        let response = client
            .execute_batch(proto::BatchRequest {
                batch_id: batch_id.to_string(),
                ops: proto_ops,
            })
            .await
            .context("ExecuteBatch failed")?
            .into_inner();

        Ok(response.results.into_iter().map(parse_op_result).collect())
    }

    async fn read_state(&self, key_prefix: &str) -> Result<HashMap<String, Option<RecordState>>> {
        let mut client = self.client.clone();
        let response = client
            .read_state(proto::ReadStateRequest { key_prefix: key_prefix.to_string() })
            .await.context("ReadState failed")?.into_inner();

        let mut result = HashMap::new();
        for record in response.records {
            let state = match record.value {
                Some(bytes) => {
                    let value = String::from_utf8_lossy(&bytes).to_string();
                    let vid = record.metadata.get("version_id").and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
                    Some(RecordState { value: Some(value), version_id: vid })
                }
                None => None,
            };
            result.insert(record.key, state);
        }
        Ok(result)
    }

    async fn cleanup(&self, key_prefix: &str) -> Result<()> {
        let mut client = self.client.clone();
        client.cleanup(proto::CleanupRequest { key_prefix: key_prefix.to_string() }).await?;
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
        op_id: r.op_id, op: String::new(), status: r.status,
        key: payload.get("key").and_then(|v| v.as_str()).map(|s| s.to_string()),
        value: payload.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()),
        version_id: payload.get("version_id").and_then(|v| v.as_u64()),
        records: payload.get("records").and_then(|v| v.as_array().map(|arr| arr.iter().filter_map(|r| Some(RangeRecord { key: r.get("key")?.as_str()?.to_string(), value: r.get("value")?.as_str()?.to_string(), version_id: r.get("version_id")?.as_u64().unwrap_or(0) })).collect())),
        keys: payload.get("keys").and_then(|v| v.as_array().map(|arr| arr.iter().filter_map(|k| k.as_str().map(|s| s.to_string())).collect())),
        deleted_count: payload.get("deleted_count").and_then(|v| v.as_u64()),
        notifications: payload.get("notifications").and_then(|v| serde_json::from_value(v.clone()).ok()),
        message: if r.message.is_empty() { None } else { Some(r.message) },
    }
}

fn serialize_op(kind: &OpKind) -> (String, Vec<u8>) {
    match kind {
        OpKind::Put { key, value } => ("put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap()),
        OpKind::Get { key, comparison } => {
            let op_type = match comparison {
                crate::reference::GetComparison::Equal => "get",
                crate::reference::GetComparison::Floor => "get_floor",
                crate::reference::GetComparison::Ceiling => "get_ceiling",
                crate::reference::GetComparison::Lower => "get_lower",
                crate::reference::GetComparison::Higher => "get_higher",
            };
            (op_type.into(), serde_json::to_vec(&serde_json::json!({"key": key})).unwrap())
        }
        OpKind::Delete { key } => ("delete".into(), serde_json::to_vec(&serde_json::json!({"key": key})).unwrap()),
        OpKind::DeleteRange { start, end } => ("delete_range".into(), serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap()),
        OpKind::List { start, end } => ("list".into(), serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap()),
        OpKind::RangeScan { start, end } => ("range_scan".into(), serde_json::to_vec(&serde_json::json!({"start": start, "end": end})).unwrap()),
        OpKind::Cas { key, expected_version_id, new_value } => ("cas".into(), serde_json::to_vec(&serde_json::json!({"key": key, "expected_version_id": expected_version_id, "new_value": new_value})).unwrap()),
        OpKind::EphemeralPut { key, value } => ("ephemeral_put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value})).unwrap()),
        OpKind::IndexedPut { key, value, index_name, index_key } => ("indexed_put".into(), serde_json::to_vec(&serde_json::json!({"key": key, "value": value, "index_name": index_name, "index_key": index_key})).unwrap()),
        OpKind::IndexedGet { index_name, index_key } => ("indexed_get".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "index_key": index_key})).unwrap()),
        OpKind::IndexedList { index_name, start, end } => ("indexed_list".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap()),
        OpKind::IndexedRangeScan { index_name, start, end } => ("indexed_range_scan".into(), serde_json::to_vec(&serde_json::json!({"index_name": index_name, "start": start, "end": end})).unwrap()),
        OpKind::SequencePut { prefix, value, delta } => ("sequence_put".into(), serde_json::to_vec(&serde_json::json!({"prefix": prefix, "value": value, "delta": delta})).unwrap()),
        OpKind::WatchStart { prefix } => ("watch_start".into(), serde_json::to_vec(&serde_json::json!({"key": prefix})).unwrap()),
        OpKind::SessionRestart => ("session_restart".into(), vec![]),
        OpKind::GetNotifications => ("get_notifications".into(), vec![]),
        OpKind::Fence => ("fence".into(), vec![]),
    }
}
