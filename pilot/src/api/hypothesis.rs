use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::Json;
use uuid::Uuid;

use crate::app_state::AppState;
use crate::engine::executor::ExecutionConfig;
use crate::types::HypothesisStatus;

use super::error::ApiError;
use super::models::*;

// --- CRUD ---

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateHypothesisRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Check for duplicate name
    if state
        .sqlite
        .get_hypothesis_by_name(&req.name)
        .await?
        .is_some()
    {
        return Err(ApiError::Conflict(format!(
            "hypothesis with name '{}' already exists",
            req.name
        )));
    }

    let id = format!("hyp-{}", Uuid::now_v7());
    let state_machine_json = serde_json::to_string(&req.state_machine)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let tolerance_json = req
        .tolerance
        .as_ref()
        .map(|t| serde_json::to_string(t))
        .transpose()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let hypothesis = state
        .sqlite
        .create_hypothesis(
            &id,
            &req.name,
            &req.generator,
            &state_machine_json,
            tolerance_json.as_deref(),
        )
        .await?;

    // Create RocksDB CF and file directory
    state.rocks.create_cf(&id)?;
    state.files.create_hypothesis_dir(&id)?;

    // Create manager
    state
        .managers
        .create_from_hypothesis(&id, &req.generator, tolerance_json.clone())
        .await;

    let response = to_hypothesis_response(&hypothesis);
    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn list(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let hypotheses = state.sqlite.list_hypotheses().await?;
    let items: Vec<HypothesisResponse> = hypotheses.iter().map(to_hypothesis_response).collect();
    Ok(Json(HypothesisListResponse { items }))
}

pub async fn get_one(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hypothesis = state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    Ok(Json(to_hypothesis_response(&hypothesis)))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Check if running
    if let Some(manager) = state.managers.get(&id).await {
        let mgr = manager.lock().await;
        if mgr.is_running() {
            return Err(ApiError::Conflict("hypothesis is currently running".to_string()));
        }
    }

    // Remove manager
    state.managers.remove(&id).await;

    // Delete from SQLite, RocksDB, files
    state.sqlite.delete_hypothesis(&id).await?;
    let _ = state.rocks.drop_cf(&id);
    let _ = state.files.delete_hypothesis_dir(&id);

    Ok(StatusCode::NO_CONTENT)
}

// --- Origin Upload ---

pub async fn upload_origin(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    // Check hypothesis exists
    state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    // Check origin doesn't already exist
    if state.files.origin_exists(&id) {
        return Err(ApiError::Conflict("origin already exists".to_string()));
    }

    let (total_ops, total_fences) = store_origin(&id, &body, &state)?;

    Ok((
        StatusCode::CREATED,
        Json(OriginUploadResponse {
            hypothesis_id: id,
            total_ops,
            total_fences,
        }),
    ))
}

// --- Generate ---

pub async fn generate(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<GenerateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Check hypothesis exists
    state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    // Check origin doesn't already exist
    if state.files.origin_exists(&id) {
        return Err(ApiError::Conflict("origin already exists".to_string()));
    }

    // Validate
    if req.ops == 0 {
        return Err(ApiError::BadRequest("ops must be > 0".to_string()));
    }
    // Validate generator exists
    if state.sqlite.get_generator(&req.generator).await?.is_none() {
        return Err(ApiError::BadRequest(format!(
            "unknown generator: {}",
            req.generator
        )));
    }

    // Prepend hypothesis_id to key prefix for data isolation
    let mut params = req;
    params.key_space.prefix = format!("/{}/{}", id, params.key_space.prefix);

    // Generate to buffer
    let mut buf = Vec::new();
    let _stats = crate::generator::generate_to_writer(&params, &mut buf)
        .map_err(|e| ApiError::Internal(e.into()))?;

    // Store via shared helper
    let (total_ops, total_fences) = store_origin(&id, &buf, &state)?;

    Ok((
        StatusCode::CREATED,
        Json(OriginUploadResponse {
            hypothesis_id: id,
            total_ops,
            total_fences,
        }),
    ))
}

// --- Run Control ---

pub async fn start_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<StartRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let hypothesis = state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let manager = state
        .managers
        .get(&id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("manager for {id} not found")))?;

    // Parse duration string (e.g. "30m", "1h", "300s") to seconds
    let duration_secs = req.execution.duration.as_ref().and_then(|d| parse_duration(d));

    let config = ExecutionConfig {
        batch_size: req.execution.batch_size,
        checkpoint_every: req.execution.checkpoint_every,
        fail_fast: req.execution.fail_fast,
        max_ops: req.execution.max_ops,
        duration_secs,
    };

    // Look up adapter definition if specified
    let adapter = if let Some(adapter_name) = &req.adapter {
        Some(
            state
                .sqlite
                .get_adapter_by_name(adapter_name)
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("adapter '{}' not found", adapter_name)))?,
        )
    } else {
        None
    };

    let mut mgr = manager.lock().await;
    if mgr.is_running() {
        return Err(ApiError::Conflict("hypothesis is already running".to_string()));
    }

    // Build generate params from the hypothesis's generator config
    // Key prefix includes hypothesis_id for data isolation
    let mut gen_params = crate::generator::GenerateParams {
        generator: hypothesis.generator.clone(),
        ops: config.max_ops.unwrap_or(usize::MAX),
        ..crate::generator::GenerateParams::default()
    };
    gen_params.key_space.prefix = format!("/{}/", id);

    let (run_id, _progress) = mgr.start_run(config, gen_params, adapter, req.adapter_addr).await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(StartRunResponse {
            run_id,
            hypothesis_id: id,
            status: HypothesisStatus::Running.to_string(),
        }),
    ))
}

pub async fn stop_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let manager = state
        .managers
        .get(&id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let mut mgr = manager.lock().await;
    mgr.stop_run().await?;

    Ok(StatusCode::NO_CONTENT)
}

// --- Observability ---

pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hypothesis = state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let manager = state.managers.get(&id).await;
    let (run_id, progress) = if let Some(mgr_arc) = manager {
        let mgr = mgr_arc.lock().await;
        let run_id = mgr.run_id().map(|s| s.to_string());
        let progress = if let Some(p) = mgr.progress() {
            Some(p.read().await.clone())
        } else {
            None
        };
        (run_id, progress)
    } else {
        (None, None)
    };

    Ok(Json(StatusResponse {
        hypothesis_id: id,
        status: hypothesis.status,
        run_id,
        progress,
    }))
}

pub async fn events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Check exists
    state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let events = state.files.read_events(
        &id,
        query.run_id.as_deref(),
        query.event_type.as_deref(),
        query.since.as_deref(),
    )?;

    // Return as JSONL
    let mut body = String::new();
    for event in &events {
        body.push_str(&serde_json::to_string(event).unwrap_or_default());
        body.push('\n');
    }

    Ok((
        [(header::CONTENT_TYPE, "application/x-ndjson")],
        body,
    ))
}

pub async fn bundle(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<BundleQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Check exists
    state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let data = state
        .files
        .create_bundle(&id, query.run_id.as_deref())?;

    let disposition = format!("attachment; filename=\"hypothesis-{id}.zip\"");
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE.as_str(), "application/zip".to_string()),
            (header::CONTENT_DISPOSITION.as_str(), disposition),
        ],
        Body::from(data),
    ))
}

// --- Shared Helper ---

/// Validate JSONL, write origin.jsonl, index into RocksDB.
fn store_origin(
    hypothesis_id: &str,
    jsonl_bytes: &[u8],
    state: &AppState,
) -> Result<(usize, usize), ApiError> {
    let content = std::str::from_utf8(jsonl_bytes)
        .map_err(|_| ApiError::BadRequest("invalid UTF-8".to_string()))?;

    let mut total_ops = 0usize;
    let mut total_fences = 0usize;
    let mut seq = 0usize;

    for (line_num, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        // Validate JSON
        let _json: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            ApiError::BadRequest(format!("invalid JSON at line {}: {e}", line_num + 1))
        })?;

        // Check if fence
        if _json.get("type").and_then(|v| v.as_str()) == Some("fence") {
            total_fences += 1;
        } else {
            total_ops += 1;
        }

        // Index into RocksDB
        state
            .rocks
            .write_origin(hypothesis_id, seq, line.as_bytes())
            .map_err(|e| ApiError::Internal(e))?;
        seq += 1;
    }

    // Write file
    state
        .files
        .write_origin(hypothesis_id, jsonl_bytes)
        .map_err(|e| ApiError::Internal(e))?;

    Ok((total_ops, total_fences))
}

/// Parse duration strings like "30s", "5m", "1h" to seconds.
fn parse_duration(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.ends_with('s') {
        s[..s.len()-1].parse().ok()
    } else if s.ends_with('m') {
        s[..s.len()-1].parse::<u64>().ok().map(|m| m * 60)
    } else if s.ends_with('h') {
        s[..s.len()-1].parse::<u64>().ok().map(|h| h * 3600)
    } else {
        s.parse().ok()
    }
}

fn to_hypothesis_response(h: &crate::storage::sqlite::Hypothesis) -> HypothesisResponse {
    HypothesisResponse {
        id: h.id.clone(),
        name: h.name.clone(),
        generator: h.generator.clone(),
        state_machine: serde_json::from_str(&h.state_machine).unwrap_or_default(),
        tolerance: h
            .tolerance
            .as_ref()
            .and_then(|t| serde_json::from_str(t).ok()),
        status: h.status.clone(),
        created_at: h.created_at.clone(),
        last_run_at: h.last_run_at.clone(),
    }
}
