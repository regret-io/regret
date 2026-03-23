use serde::{Deserialize, Serialize};

use crate::engine::executor::ProgressInfo;

// --- Hypothesis CRUD ---

#[derive(Debug, Deserialize)]
pub struct CreateHypothesisRequest {
    pub name: String,
    pub generator: String,
    pub adapter: Option<String>,
    pub adapter_addr: Option<String>,
    pub duration: Option<String>,
    #[serde(default = "default_checkpoint_every")]
    pub checkpoint_every: String,
    #[serde(default)]
    pub tolerance: Option<serde_json::Value>,
    #[serde(default)]
    pub key_space: Option<serde_json::Value>,
}

fn default_checkpoint_every() -> String { "10m".to_string() }

#[derive(Debug, Serialize)]
pub struct HypothesisResponse {
    pub id: String,
    pub name: String,
    pub generator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adapter_addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    pub checkpoint_every: String,
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

// --- Run Control ---

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
    /// Return only the last N events (applied after filtering).
    pub last: Option<usize>,
}

// --- Bundle query ---

#[derive(Debug, Deserialize)]
pub struct BundleQuery {
    pub run_id: Option<String>,
}
