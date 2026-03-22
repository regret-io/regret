use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app_state::AppState;
use super::error::ApiError;

#[derive(Debug, Deserialize)]
pub struct CreateAdapterRequest {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct AdapterResponse {
    pub id: String,
    pub name: String,
    pub image: String,
    pub env: serde_json::Value,
    pub created_at: String,
}

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateAdapterRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Check for duplicate name
    if state.sqlite.get_adapter_by_name(&req.name).await?.is_some() {
        return Err(ApiError::Conflict(format!(
            "adapter '{}' already exists",
            req.name
        )));
    }

    let id = format!("adp-{}", Uuid::now_v7());
    let env_json = serde_json::to_string(&req.env)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let adapter = state.sqlite.create_adapter(&id, &req.name, &req.image, &env_json).await?;

    Ok((StatusCode::CREATED, Json(to_response(&adapter))))
}

pub async fn list(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let adapters = state.sqlite.list_adapters_all().await?;
    let items: Vec<AdapterResponse> = adapters.iter().map(to_response).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn get_one(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let adapter = state
        .sqlite
        .get_adapter(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("adapter {id} not found")))?;
    Ok(Json(to_response(&adapter)))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let deleted = state.sqlite.delete_adapter(&id).await?;
    if !deleted {
        return Err(ApiError::NotFound(format!("adapter {id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

fn to_response(a: &crate::storage::sqlite::AdapterRecord) -> AdapterResponse {
    AdapterResponse {
        id: a.id.clone(),
        name: a.name.clone(),
        image: a.image.clone(),
        env: serde_json::from_str(&a.env).unwrap_or_default(),
        created_at: a.created_at.clone(),
    }
}
