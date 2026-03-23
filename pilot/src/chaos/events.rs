use chrono::Utc;
use serde::Serialize;

/// Chaos events written to /data/chaos/events.jsonl
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ChaosEvent {
    ChaosInjected {
        injection_id: String,
        scenario_name: String,
        action_type: String,
        target_pods: Vec<String>,
        namespace: String,
        timestamp: String,
    },
    ChaosRecovered {
        injection_id: String,
        scenario_name: String,
        action_type: String,
        target_pods: Vec<String>,
        duration_ms: u64,
        timestamp: String,
    },
    ChaosError {
        injection_id: String,
        scenario_name: String,
        action_type: String,
        error: String,
        timestamp: String,
    },
    InjectionStarted {
        injection_id: String,
        scenario_name: String,
        timestamp: String,
    },
    InjectionStopped {
        injection_id: String,
        scenario_name: String,
        reason: String,
        timestamp: String,
    },
}

impl ChaosEvent {
    pub fn now() -> String {
        Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}
