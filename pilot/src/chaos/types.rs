use serde::{Deserialize, Serialize};

/// A chaos scenario — reusable template defining what chaos to inject.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosScenario {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub actions: Vec<ChaosAction>,
}

/// A single chaos action definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosAction {
    /// Action type: pod_kill, pod_restart, network_partition, network_delay, network_loss, custom
    #[serde(rename = "type")]
    pub action_type: String,

    /// Label selector for target pods (like Chaos Mesh)
    /// e.g. "app.kubernetes.io/name=oxia"
    #[serde(default)]
    pub selector: LabelSelector,

    /// Specific pod name (overrides selector)
    #[serde(default)]
    pub target_pod: Option<String>,

    /// Interval between repeated injections (e.g. "30s", "1m")
    #[serde(default)]
    pub interval: Option<String>,

    /// One-time injection at this offset from start (e.g. "2m", "5m")
    #[serde(default)]
    pub at: Option<String>,

    /// Duration of the chaos effect (for network chaos)
    #[serde(default)]
    pub duration: Option<String>,

    /// Parameters for specific action types
    #[serde(default)]
    pub params: serde_json::Value,
}

/// Label-based pod selector, like Chaos Mesh.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LabelSelector {
    /// Match labels: {"app": "oxia", "component": "server"}
    #[serde(default)]
    pub match_labels: std::collections::HashMap<String, String>,

    /// Percentage of matching pods to target (0-100, default 100)
    #[serde(default = "default_percentage")]
    pub percentage: u32,

    /// Mode: "one" (random one), "all", "fixed" (exact count), "percentage"
    #[serde(default = "default_mode")]
    pub mode: String,

    /// Fixed count when mode="fixed"
    #[serde(default)]
    pub count: Option<u32>,
}

fn default_percentage() -> u32 { 100 }
fn default_mode() -> String { "one".to_string() }
