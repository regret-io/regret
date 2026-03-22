use serde::{Deserialize, Serialize};

use crate::engine::executor::ProgressInfo;

// --- Hypothesis CRUD ---

#[derive(Debug, Deserialize)]
pub struct CreateHypothesisRequest {
    pub name: String,
    pub generator: String,
    pub state_machine: serde_json::Value,
    #[serde(default)]
    pub tolerance: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct HypothesisResponse {
    pub id: String,
    pub name: String,
    pub generator: String,
    pub state_machine: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tolerance: Option<serde_json::Value>,
    pub status: String,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HypothesisListResponse {
    pub items: Vec<HypothesisResponse>,
}

// --- Origin ---

#[derive(Debug, Serialize)]
pub struct OriginUploadResponse {
    pub hypothesis_id: String,
    pub total_ops: usize,
    pub total_fences: usize,
}

// --- Generate ---
// GenerateRequest is just GenerateParams from the generator module.
// Re-exported here so the API handler can deserialize directly.
pub use crate::generator::GenerateParams as GenerateRequest;

// --- Run Control ---

#[derive(Debug, Deserialize)]
pub struct StartRunRequest {
    /// Adapter name to use for this run (must be pre-registered via POST /api/adapters).
    /// If omitted, runs reference model only (no real system verification).
    pub adapter: Option<String>,
    /// Override adapter gRPC address (for local dev when K8s service names aren't resolvable).
    pub adapter_addr: Option<String>,
    #[serde(default)]
    pub execution: ExecutionConfigRequest,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionConfigRequest {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_checkpoint_every")]
    pub checkpoint_every: usize,
    #[serde(default = "default_fail_fast")]
    pub fail_fast: bool,
    /// Max ops to execute. Omit for unlimited.
    pub max_ops: Option<usize>,
    /// Max duration (e.g. "30m", "1h", "300s"). Omit for unlimited.
    pub duration: Option<String>,
}

fn default_batch_size() -> usize { 100 }
fn default_checkpoint_every() -> usize { 10 }
fn default_fail_fast() -> bool { true }

impl Default for ExecutionConfigRequest {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            checkpoint_every: default_checkpoint_every(),
            fail_fast: default_fail_fast(),
            max_ops: None,
            duration: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StartRunResponse {
    pub run_id: String,
    pub hypothesis_id: String,
    pub status: String,
}

// --- Status ---

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub hypothesis_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressInfo>,
}

// --- Events query ---

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub run_id: Option<String>,
    #[serde(rename = "type")]
    pub event_type: Option<String>,
    pub since: Option<String>,
}

// --- Bundle query ---

#[derive(Debug, Deserialize)]
pub struct BundleQuery {
    pub run_id: Option<String>,
}
