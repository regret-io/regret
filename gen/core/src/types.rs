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
        // Flatten: {"id":..., "op":..., ...fields}
        match &self.fields {
            OpFields::Put { key, value } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("key", key)?;
                map.serialize_entry("value", value)?;
                map.end()
            }
            OpFields::Delete { key } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("key", key)?;
                map.end()
            }
            OpFields::DeleteRange { start, end } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
                map.end()
            }
            OpFields::Cas {
                key,
                expected_version_id,
                new_value,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("key", key)?;
                map.serialize_entry("expected_version_id", expected_version_id)?;
                map.serialize_entry("new_value", new_value)?;
                map.end()
            }
            OpFields::Get { key } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("key", key)?;
                map.end()
            }
            OpFields::RangeScan { start, end } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("start", start)?;
                map.serialize_entry("end", end)?;
                map.end()
            }
            OpFields::List { prefix } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("id", &self.id)?;
                map.serialize_entry("op", &self.op)?;
                map.serialize_entry("prefix", prefix)?;
                map.end()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpFields {
    Put { key: String, value: String },
    Delete { key: String },
    DeleteRange { start: String, end: String },
    Cas { key: String, expected_version_id: u64, new_value: String },
    Get { key: String },
    RangeScan { start: String, end: String },
    List { prefix: String },
}

impl OriginOp {
    pub fn fence() -> Self {
        OriginOp::Fence(FenceOp {
            typ: "fence".to_string(),
        })
    }

    pub fn put(id: String, key: String, value: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "put".to_string(),
            fields: OpFields::Put { key, value },
        })
    }

    pub fn delete(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "delete".to_string(),
            fields: OpFields::Delete { key },
        })
    }

    pub fn delete_range(id: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "delete_range".to_string(),
            fields: OpFields::DeleteRange { start, end },
        })
    }

    pub fn cas(id: String, key: String, expected_version_id: u64, new_value: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "cas".to_string(),
            fields: OpFields::Cas {
                key,
                expected_version_id,
                new_value,
            },
        })
    }

    pub fn get(id: String, key: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "get".to_string(),
            fields: OpFields::Get { key },
        })
    }

    pub fn range_scan(id: String, start: String, end: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "range_scan".to_string(),
            fields: OpFields::RangeScan { start, end },
        })
    }

    pub fn list(id: String, prefix: String) -> Self {
        OriginOp::Operation(OperationOp {
            id,
            op: "list".to_string(),
            fields: OpFields::List { prefix },
        })
    }
}
