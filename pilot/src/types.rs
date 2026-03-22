use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// Status of a hypothesis lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum HypothesisStatus {
    Idle,
    Running,
    Passed,
    Failed,
    Stopped,
}

/// All supported operation types.
///
/// Basic KV: put, get, delete, delete_range, list, range_scan
/// CAS: cas
/// Ephemeral: ephemeral_put
/// Secondary index: indexed_put, indexed_get, indexed_list, indexed_range_scan
/// Sequence keys: sequence_put
/// Fence: synchronization barrier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OpType {
    // Basic KV
    Put,
    Get,
    Delete,
    DeleteRange,
    List,
    RangeScan,

    // CAS (versioned conditional write)
    Cas,

    // Ephemeral records (auto-deleted when session expires)
    EphemeralPut,

    // Secondary index operations
    IndexedPut,
    IndexedGet,
    IndexedList,
    IndexedRangeScan,

    // Sequence keys (server-assigned monotonic key suffix)
    SequencePut,

    // Synchronization barrier
    Fence,
}

impl OpType {
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            OpType::Put
                | OpType::Delete
                | OpType::DeleteRange
                | OpType::Cas
                | OpType::EphemeralPut
                | OpType::IndexedPut
                | OpType::SequencePut
        )
    }

    pub fn is_read(&self) -> bool {
        matches!(
            self,
            OpType::Get
                | OpType::List
                | OpType::RangeScan
                | OpType::IndexedGet
                | OpType::IndexedList
                | OpType::IndexedRangeScan
        )
    }
}
