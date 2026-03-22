pub mod kv;
pub mod streaming;

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// Unified record state for checkpoint comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordState {
    pub value: Option<String>,
    pub version_id: u64,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OpStatus {
    Ok,
    NotFound,
    VersionMismatch,
}

impl OpStatus {
    pub fn as_str(&self) -> &str {
        match self {
            OpStatus::Ok => "ok",
            OpStatus::NotFound => "not_found",
            OpStatus::VersionMismatch => "version_mismatch",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeRecord {
    pub key: String,
    pub value: String,
    pub version_id: u64,
}

/// A failure detected during verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseFailure {
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
    Put { key: String, value: String },
    Delete { key: String },
    DeleteRange { start: String, end: String },
    Cas { key: String, expected_version_id: u64, new_value: String },
    Get { key: String },
    RangeScan { start: String, end: String },
    List { prefix: String },
    Fence,
}

/// The core reference model trait.
///
/// The reference model owns the "truth state" — it processes adapter responses
/// to maintain what the state SHOULD be after all successful operations.
///
/// Flow:
///   1. Executor sends ops to adapter → gets response
///   2. Reference.process_response(ops, response) →
///      - Writes that succeeded → update reference state
///      - Reads that succeeded → verify value against reference state → return failures
///   3. Checkpoint: reference.snapshot() vs adapter.readState() → return failures
pub trait ReferenceModel: Send + Sync {
    /// Process adapter response for a batch of operations.
    ///
    /// For each operation result:
    /// - Write (put/delete/cas) with status=ok → update internal state
    /// - Read (get/range_scan/list) → verify returned value against internal state
    ///
    /// Returns list of read verification failures (empty = all reads correct).
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<ResponseFailure>;

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

/// Create a reference model based on profile name.
pub fn create_reference(profile: &str) -> Box<dyn ReferenceModel> {
    match profile {
        "basic-kv" => Box::new(kv::BasicKvReference::new()),
        "basic-streaming" => Box::new(streaming::BasicStreamingReference::new()),
        _ => panic!("unsupported profile: {profile}"),
    }
}
