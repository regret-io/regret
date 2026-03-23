use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use crate::chaos::types::{ChaosAction, ChaosScenario};

use super::error::ApiError;

// --- Request / Response types ---

#[derive(Debug, Deserialize)]
pub struct CreateScenarioRequest {
    pub name: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    pub actions: Vec<ChaosAction>,
}

fn default_namespace() -> String {
    "regret-system".to_string()
}

#[derive(Debug, Serialize)]
pub struct ScenarioResponse {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub actions: Vec<ChaosAction>,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct InjectionResponse {
    pub id: String,
    pub scenario_id: String,
    pub scenario_name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StartInjectionResponse {
    pub injection_id: String,
    pub scenario_id: String,
    pub status: String,
}

// --- Scenario CRUD ---

pub async fn create_scenario(
    State(state): State<AppState>,
    Json(req): Json<CreateScenarioRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if req.actions.is_empty() {
        return Err(ApiError::BadRequest("actions must not be empty".to_string()));
    }

    let id = format!("cs-{}", uuid::Uuid::now_v7());
    let actions_json = serde_json::to_string(&req.actions)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let record = state
        .sqlite
        .create_chaos_scenario(&id, &req.name, &req.namespace, &actions_json)
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(scenario_to_response(&record)),
    ))
}

pub async fn list_scenarios(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state.sqlite.list_chaos_scenarios().await?;
    let items: Vec<ScenarioResponse> = records.iter().map(scenario_to_response).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_scenario(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .sqlite
        .get_chaos_scenario(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("chaos scenario {id} not found")))?;
    Ok(Json(scenario_to_response(&record)))
}

pub async fn update_scenario(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CreateScenarioRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .sqlite
        .get_chaos_scenario(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("chaos scenario {id} not found")))?;

    if req.actions.is_empty() {
        return Err(ApiError::BadRequest("actions must not be empty".to_string()));
    }

    let actions_json = serde_json::to_string(&req.actions)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let record = state
        .sqlite
        .update_chaos_scenario(&id, &req.name, &req.namespace, &actions_json)
        .await?
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("update failed")))?;

    Ok(Json(scenario_to_response(&record)))
}

pub async fn delete_scenario(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let deleted = state.sqlite.delete_chaos_scenario(&id).await?;
    if !deleted {
        return Err(ApiError::NotFound(format!("chaos scenario {id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

// --- Injection control ---

#[derive(Debug, Deserialize, Default)]
pub struct InjectRequest {
    /// Override action params by action type. e.g. {"upgrade_test": {"candidate_image": "..."}}
    #[serde(default)]
    pub overrides: HashMap<String, serde_json::Value>,
}

pub async fn start_injection(
    State(state): State<AppState>,
    Path(scenario_id): Path<String>,
    body: Option<Json<InjectRequest>>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .sqlite
        .get_chaos_scenario(&scenario_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("chaos scenario {scenario_id} not found")))?;

    let mut actions: Vec<ChaosAction> = serde_json::from_str(&record.actions)
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("invalid actions JSON: {e}")))?;

    // Apply overrides — merge override params into matching action types
    if let Some(Json(req)) = body {
        for action in &mut actions {
            if let Some(overrides) = req.overrides.get(&action.action_type) {
                if let Some(override_obj) = overrides.as_object() {
                    if let Some(params_obj) = action.params.as_object_mut() {
                        for (k, v) in override_obj {
                            params_obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
    }

    let scenario = ChaosScenario {
        id: record.id.clone(),
        name: record.name.clone(),
        namespace: record.namespace.clone(),
        actions,
    };

    let injection_id = state.chaos.start_injection(scenario).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(StartInjectionResponse {
            injection_id,
            scenario_id: record.id,
            status: "running".to_string(),
        }),
    ))
}

pub async fn stop_injection(
    State(state): State<AppState>,
    Path(injection_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.chaos.stop_injection(&injection_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_injections(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let records = state.sqlite.list_chaos_injections().await?;
    let items: Vec<InjectionResponse> = records
        .iter()
        .map(|r| InjectionResponse {
            id: r.id.clone(),
            scenario_id: r.scenario_id.clone(),
            scenario_name: r.scenario_name.clone(),
            status: r.status.clone(),
            started_at: r.started_at.clone(),
            finished_at: r.finished_at.clone(),
            error: r.error.clone(),
        })
        .collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_injection(
    State(state): State<AppState>,
    Path(injection_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .sqlite
        .get_chaos_injection(&injection_id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("chaos injection {injection_id} not found")))?;
    Ok(Json(InjectionResponse {
        id: record.id,
        scenario_id: record.scenario_id,
        scenario_name: record.scenario_name,
        status: record.status,
        started_at: record.started_at,
        finished_at: record.finished_at,
        error: record.error,
    }))
}

pub async fn delete_injection(
    State(state): State<AppState>,
    Path(injection_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Stop if still running
    if state.chaos.is_active(&injection_id).await {
        state.chaos.stop_injection(&injection_id).await?;
    }
    let deleted = state.sqlite.delete_chaos_injection(&injection_id).await?;
    if !deleted {
        return Err(ApiError::NotFound(format!(
            "chaos injection {injection_id} not found"
        )));
    }
    Ok(StatusCode::NO_CONTENT)
}

// --- Chaos events ---

pub async fn chaos_events(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let events = state.files.read_chaos_events(None)?;
    let mut body = String::new();
    for event in &events {
        body.push_str(&serde_json::to_string(event).unwrap_or_default());
        body.push('\n');
    }
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    ))
}

// --- Helpers ---

fn scenario_to_response(
    r: &crate::storage::sqlite::ChaosScenarioRecord,
) -> ScenarioResponse {
    let actions: Vec<ChaosAction> =
        serde_json::from_str(&r.actions).unwrap_or_default();
    ScenarioResponse {
        id: r.id.clone(),
        name: r.name.clone(),
        namespace: r.namespace.clone(),
        actions,
        created_at: r.created_at.clone(),
    }
}
