use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;
use super::error::ApiError;

#[derive(Debug, Deserialize)]
pub struct CreateGeneratorRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub workload: std::collections::HashMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct GeneratorResponse {
    pub name: String,
    pub description: String,
    pub workload: serde_json::Value,
    pub builtin: bool,
    pub created_at: String,
}

pub async fn list(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let generators = state.sqlite.list_generators().await?;
    let items: Vec<GeneratorResponse> = generators.iter().map(to_response).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_one(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .sqlite
        .get_generator(&name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("generator '{name}' not found")))?;
    Ok(Json(to_response(&record)))
}

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateGeneratorRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if state.sqlite.get_generator(&req.name).await?.is_some() {
        return Err(ApiError::Conflict(format!("generator '{}' already exists", req.name)));
    }

    let workload_json = serde_json::to_string(&req.workload)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    state
        .sqlite
        .upsert_generator(&req.name, &req.description, &workload_json, false)
        .await?;

    let record = state
        .sqlite
        .get_generator(&req.name)
        .await?
        .ok_or_else(|| ApiError::Internal(anyhow::anyhow!("failed to read just-created generator")))?;

    Ok((StatusCode::CREATED, Json(to_response(&record))))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let record = state
        .sqlite
        .get_generator(&name)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("generator '{name}' not found")))?;

    if record.builtin != 0 {
        return Err(ApiError::Conflict("cannot delete built-in generator".to_string()));
    }

    state.sqlite.delete_generator(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn to_response(g: &crate::storage::sqlite::GeneratorRecord) -> GeneratorResponse {
    GeneratorResponse {
        name: g.name.clone(),
        description: g.description.clone(),
        workload: serde_json::from_str(&g.workload).unwrap_or_default(),
        builtin: g.builtin != 0,
        created_at: g.created_at.clone(),
    }
}
