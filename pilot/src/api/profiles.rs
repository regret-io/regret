use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use super::error::ApiError;

#[derive(Debug, Deserialize)]
pub struct CreateProfileRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub workload: std::collections::HashMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct ProfileResponse {
    pub name: String,
    pub description: String,
    pub workload: serde_json::Value,
    pub builtin: bool,
    pub created_at: String,
}

pub async fn list(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let profiles = state.sqlite.list_profiles().await?;
    let items: Vec<ProfileResponse> = profiles.iter().map(to_response).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_one(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let profile = state
        .sqlite
        .get_profile(&name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("profile '{name}' not found")))?;
    Ok(Json(to_response(&profile)))
}

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateProfileRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if state.sqlite.get_profile(&req.name).await?.is_some() {
        return Err(ApiError::Conflict(format!("profile '{}' already exists", req.name)));
    }

    let workload_json = serde_json::to_string(&req.workload)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    state
        .sqlite
        .upsert_profile(&req.name, &req.description, &workload_json, false)
        .await?;

    let profile = state
        .sqlite
        .get_profile(&req.name)
        .await?
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("failed to read just-created profile")))?;

    Ok((StatusCode::CREATED, Json(to_response(&profile))))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let profile = state
        .sqlite
        .get_profile(&name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("profile '{name}' not found")))?;

    if profile.builtin != 0 {
        return Err(ApiError::Conflict("cannot delete built-in profile".to_string()));
    }

    state.sqlite.delete_profile(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn to_response(p: &crate::storage::sqlite::ProfileRecord) -> ProfileResponse {
    ProfileResponse {
        name: p.name.clone(),
        description: p.description.clone(),
        workload: serde_json::from_str(&p.workload).unwrap_or_default(),
        builtin: p.builtin != 0,
        created_at: p.created_at.clone(),
    }
}
