pub mod kv;
pub mod streaming;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::storage::rocks::RocksStore;

/// Unified record state for checkpoint comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordState {
    pub value: Option<String>,
    pub version_id: u64,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum OpStatus {
    Ok,
    NotFound,
    VersionMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeRecord {
    pub key: String,
    pub value: String,
    pub version_id: u64,
}

/// A failure detected during verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyViolation {
    pub op_id: String,
    pub op: String,
    pub expected: String,
    pub actual: String,
}

/// A failure detected during checkpoint verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointFailure {
    pub key: String,
    pub expected: Option<RecordState>,
    pub actual: Option<RecordState>,
}

/// Tolerance configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tolerance {
    #[serde(default = "default_ordering")]
    pub ordering: String,
    #[serde(default = "default_duplicates")]
    pub duplicates: String,
    #[serde(default)]
    pub structural: Vec<StructuralTolerance>,
}

fn default_ordering() -> String { "strict".to_string() }
fn default_duplicates() -> String { "deny".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuralTolerance {
    pub field: String,
    pub ignore: bool,
}

/// Adapter batch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterBatchResponse {
    pub batch_id: String,
    pub results: Vec<AdapterOpResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterOpResult {
    pub op_id: String,
    pub op: String,
    pub status: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub version_id: Option<u64>,
    #[serde(default)]
    pub records: Option<Vec<RangeRecord>>,
    #[serde(default)]
    pub keys: Option<Vec<String>>,
    #[serde(default)]
    pub deleted_count: Option<u64>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Parsed operation.
#[derive(Debug, Clone)]
pub struct Operation {
    pub id: String,
    pub kind: OpKind,
}

#[derive(Debug, Clone)]
pub enum OpKind {
    // Basic KV
    Put { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    DeleteRange { start: String, end: String },
    List { prefix: String },
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

    // Synchronization barrier
    Fence,
}

/// The core reference model trait.
///
/// The reference model owns the "truth state" persisted in RocksDB.
/// It processes adapter responses to maintain what the state SHOULD be
/// after all successful operations.
pub trait ReferenceModel: Send + Sync {
    /// Process adapter response for a batch of operations.
    ///
    /// - Write succeeded → update state in RocksDB
    /// - Read succeeded → verify value against state in RocksDB
    ///
    /// Returns list of read verification failures.
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<SafetyViolation>;

    /// Layer 2: verify adapter state snapshot against reference state.
    fn verify_checkpoint(
        &self,
        actual_state: &HashMap<String, Option<RecordState>>,
        tolerance: &Option<Tolerance>,
    ) -> Vec<CheckpointFailure>;

    /// Keys touched since last clear.
    fn touched_keys(&self) -> &HashSet<String>;

    /// Snapshot reference state for given keys.
    fn snapshot(&self, keys: &HashSet<String>) -> HashMap<String, Option<RecordState>>;

    /// Clear all state (start of each run).
    fn clear(&mut self);
}

/// Create a reference model backed by RocksDB.
pub fn create_reference(
    generator_name: &str,
    rocks: RocksStore,
    hypothesis_id: String,
) -> Box<dyn ReferenceModel> {
    // All KV-family generators use the same reference model
    match generator_name {
        "basic-streaming" => Box::new(streaming::BasicStreamingReference::new()),
        _ => Box::new(kv::BasicKvReference::new(rocks, hypothesis_id)),
    }
}
