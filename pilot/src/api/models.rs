use serde::{Deserialize, Serialize};

use crate::engine::executor::ProgressInfo;

// --- Hypothesis CRUD ---

#[derive(Debug, Deserialize)]
pub struct CreateHypothesisRequest {
    pub name: String,
    pub profile: String,
    pub state_machine: serde_json::Value,
    #[serde(default)]
    pub tolerance: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct HypothesisResponse {
    pub id: String,
    pub name: String,
    pub profile: String,
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
    #[serde(default = "default_timeout")]
    pub timeout: String,
    #[serde(default)]
    pub retry: RetryConfigRequest,
}

fn default_batch_size() -> usize { 100 }
fn default_checkpoint_every() -> usize { 10 }
fn default_fail_fast() -> bool { true }
fn default_timeout() -> String { "30m".to_string() }

impl Default for ExecutionConfigRequest {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            checkpoint_every: default_checkpoint_every(),
            fail_fast: default_fail_fast(),
            timeout: default_timeout(),
            retry: RetryConfigRequest::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RetryConfigRequest {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_initial_delay")]
    pub initial_delay: String,
    #[serde(default = "default_max_delay")]
    pub max_delay: String,
}

fn default_max_attempts() -> u32 { 3 }
fn default_initial_delay() -> String { "1s".to_string() }
fn default_max_delay() -> String { "30s".to_string() }

impl Default for RetryConfigRequest {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
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
