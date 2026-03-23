use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};

/// A single line in the origin JSONL file.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OriginOp {
    Fence(FenceOp),
    Operation(OperationOp),
}

impl Serialize for OriginOp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            OriginOp::Fence(_) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "fence")?;
                map.end()
            }
            OriginOp::Operation(op) => op.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FenceOp {
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OperationOp {
    pub id: String,
    pub op: String,
    pub fields: OpFields,
}

impl Serialize for OperationOp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("id", &self.id)?;
        map.serialize_entry("op", &self.op)?;

        match &self.fields {
            OpFields::Put { key, value } => {
                map.serialize_entry("key", key)?;
                map.serialize_entry("value", value)?;
            }
            OpFields::Delete { key } => {
                map.serialize_entry("key", key)?;
            }
            OpFields::DeleteRange { start, end } => {
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
            }
            OpFields::Cas { key, expected_version_id, new_value } => {
                map.serialize_entry("key", key)?;
                map.serialize_entry("expected_version_id", expected_version_id)?;
                map.serialize_entry("new_value", new_value)?;
            }
            OpFields::Get { key } => {
                map.serialize_entry("key", key)?;
            }
            OpFields::RangeScan { start, end } => {
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
            }
            OpFields::List { start, end } => {
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
            }
            OpFields::EphemeralPut { key, value } => {
                map.serialize_entry("key", key)?;
                map.serialize_entry("value", value)?;
            }
            OpFields::IndexedPut { key, value, index_name, index_key } => {
                map.serialize_entry("key", key)?;
                map.serialize_entry("value", value)?;
                map.serialize_entry("index_name", index_name)?;
                map.serialize_entry("index_key", index_key)?;
            }
            OpFields::IndexedGet { index_name, index_key } => {
                map.serialize_entry("index_name", index_name)?;
                map.serialize_entry("index_key", index_key)?;
            }
            OpFields::IndexedList { index_name, start, end } => {
                map.serialize_entry("index_name", index_name)?;
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
            }
            OpFields::IndexedRangeScan { index_name, start, end } => {
                map.serialize_entry("index_name", index_name)?;
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
            }
            OpFields::SequencePut { prefix, value, delta } => {
                map.serialize_entry("prefix", prefix)?;
                map.serialize_entry("value", value)?;
                map.serialize_entry("delta", delta)?;
            }
        }
        map.end()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpFields {
    // Basic KV
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    DeleteRange { start: String, end: String },
    List { start: String, end: String },
    RangeScan { start: String, end: String },

    // CAS
    Cas { key: String, expected_version_id: u64, new_value: String },

    // Ephemeral
    EphemeralPut { key: String, value: String },

    // Secondary index
    IndexedPut { key: String, value: String, index_name: String, index_key: String },
    IndexedGet { index_name: String, index_key: String },
    IndexedList { index_name: String, start: String, end: String },
    IndexedRangeScan { index_name: String, start: String, end: String },

    // Sequence keys
    SequencePut { prefix: String, value: String, delta: u64 },
}

impl OriginOp {
    pub fn fence() -> Self {
        OriginOp::Fence(FenceOp { typ: "fence".to_string() })
    }

    pub fn put(id: String, key: String, value: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "put".to_string(), fields: OpFields::Put { key, value } })
    }

    pub fn get(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get".to_string(), fields: OpFields::Get { key } })
    }

    pub fn get_floor(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get_floor".to_string(), fields: OpFields::Get { key } })
    }

    pub fn get_ceiling(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get_ceiling".to_string(), fields: OpFields::Get { key } })
    }

    pub fn get_lower(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get_lower".to_string(), fields: OpFields::Get { key } })
    }

    pub fn get_higher(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get_higher".to_string(), fields: OpFields::Get { key } })
    }

    pub fn delete(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "delete".to_string(), fields: OpFields::Delete { key } })
    }

    pub fn delete_range(id: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "delete_range".to_string(), fields: OpFields::DeleteRange { start, end } })
    }

    pub fn list(id: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "list".to_string(), fields: OpFields::List { start, end } })
    }

    pub fn range_scan(id: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "range_scan".to_string(), fields: OpFields::RangeScan { start, end } })
    }

    pub fn cas(id: String, key: String, expected_version_id: u64, new_value: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "cas".to_string(), fields: OpFields::Cas { key, expected_version_id, new_value } })
    }

    pub fn ephemeral_put(id: String, key: String, value: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "ephemeral_put".to_string(), fields: OpFields::EphemeralPut { key, value } })
    }

    pub fn indexed_put(id: String, key: String, value: String, index_name: String, index_key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "indexed_put".to_string(), fields: OpFields::IndexedPut { key, value, index_name, index_key } })
    }

    pub fn indexed_get(id: String, index_name: String, index_key: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "indexed_get".to_string(), fields: OpFields::IndexedGet { index_name, index_key } })
    }

    pub fn indexed_list(id: String, index_name: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "indexed_list".to_string(), fields: OpFields::IndexedList { index_name, start, end } })
    }

    pub fn indexed_range_scan(id: String, index_name: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "indexed_range_scan".to_string(), fields: OpFields::IndexedRangeScan { index_name, start, end } })
    }

    pub fn sequence_put(id: String, prefix: String, value: String, delta: u64) -> Self {
        OriginOp::Operation(OperationOp { id, op: "sequence_put".to_string(), fields: OpFields::SequencePut { prefix, value, delta } })
    }

    pub fn watch_start(id: String, prefix: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "watch_start".to_string(), fields: OpFields::Get { key: prefix } })
    }

    pub fn session_restart(id: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "session_restart".to_string(), fields: OpFields::Get { key: String::new() } })
    }

    pub fn get_notifications(id: String) -> Self {
        OriginOp::Operation(OperationOp { id, op: "get_notifications".to_string(), fields: OpFields::Get { key: String::new() } })
    }
}
