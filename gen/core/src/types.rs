use serde::{Deserialize, Serialize};

/// A single line in the origin JSONL file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OriginOp {
    Fence(FenceOp),
    Operation(OperationOp),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FenceOp {
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationOp {
    pub id: String,
    pub op: String,
    #[serde(flatten)]
    pub fields: OpFields,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OpFields {
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    DeleteRange {
        start: String,
        end: String,
    },
    Cas {
        key: String,
        expected_version_id: u64,
        new_value: String,
    },
    Get {
        key: String,
    },
    RangeScan {
        start: String,
        end: String,
    },
    List {
        prefix: String,
    },
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
