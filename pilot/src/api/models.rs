use serde::{Deserialize, Serialize};

use crate::engine::executor::ProgressInfo;

// --- Hypothesis CRUD ---

#[derive(Debug, Deserialize)]
pub struct CreateHypothesisRequest {
    pub name: String,
    pub generator: String,
    /// Adapter name. Omit for reference-only mode.
    pub adapter: Option<String>,
    /// Override adapter gRPC address (local dev).
    pub adapter_addr: Option<String>,
    /// Run duration (e.g. "30s", "5m", "1h"). Omit for forever.
    pub duration: Option<String>,
    /// Tolerance config for verification.
    #[serde(default)]
    pub tolerance: Option<serde_json::Value>,
    /// Key space config override.
    #[serde(default)]
    pub key_space: Option<serde_json::Value>,
}

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

// --- Run Control (now just starts — config is on hypothesis) ---

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
