use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use uuid::Uuid;

use crate::adapter::grpc_client::GrpcAdapterClient;
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
    if state.sqlite.get_hypothesis_by_name(&req.name).await?.is_some() {
        return Err(ApiError::Conflict(format!("hypothesis '{}' already exists", req.name)));
    }
    if state.sqlite.get_generator(&req.generator).await?.is_none() {
        return Err(ApiError::BadRequest(format!("unknown generator: {}", req.generator)));
    }
    if let Some(adapter) = &req.adapter {
        if state.sqlite.get_adapter_by_name(adapter).await?.is_none() {
            return Err(ApiError::BadRequest(format!("unknown adapter: {adapter}")));
        }
    }

    let id = format!("hyp-{}", Uuid::now_v7());
    let tolerance_json = req.tolerance.as_ref().map(|t| serde_json::to_string(t)).transpose()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    let key_space_json = serde_json::to_string(&req.key_space.unwrap_or_default())
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let hypothesis = state.sqlite.create_hypothesis(
        &id, &req.name, &req.generator,
        req.adapter.as_deref(), req.adapter_addr.as_deref(), req.duration.as_deref(),
        tolerance_json.as_deref(), &req.checkpoint_every, &key_space_json, "{}",
    ).await?;

    state.files.create_hypothesis_dir(&id)?;
    state.managers.create_from_hypothesis(&id, &req.generator, tolerance_json).await;

    Ok((StatusCode::CREATED, Json(to_response(&hypothesis))))
}

pub async fn list(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let items: Vec<HypothesisResponse> = state.sqlite.list_hypotheses().await?.iter().map(to_response).collect();
    Ok(Json(HypothesisListResponse { items }))
}

pub async fn get_one(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let h = state.sqlite.get_hypothesis(&id).await?.ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    Ok(Json(to_response(&h)))
}

pub async fn update(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<CreateHypothesisRequest>,
) -> Result<impl IntoResponse, ApiError> {
    state.sqlite.get_hypothesis(&id).await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    if state.sqlite.get_generator(&req.generator).await?.is_none() {
        return Err(ApiError::BadRequest(format!("unknown generator: {}", req.generator)));
    }

    let tolerance_json = req.tolerance.as_ref().map(|t| serde_json::to_string(t)).transpose()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    let hypothesis = state.sqlite.update_hypothesis(
        &id, &req.name, &req.generator,
        req.adapter.as_deref(), req.adapter_addr.as_deref(),
        req.duration.as_deref(), tolerance_json.as_deref(),
    ).await?.ok_or_else(|| ApiError::Internal(anyhow::anyhow!("update failed")))?;

    Ok(Json(to_response(&hypothesis)))
}

pub async fn delete(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    if let Some(manager) = state.managers.get(&id).await {
        let mut mgr = manager.lock().await;
        if mgr.is_running() { mgr.stop_run().await?; }
    }
    state.managers.remove(&id).await;
    state.sqlite.delete_hypothesis(&id).await?;
    let _ = state.files.delete_hypothesis_dir(&id);

    // Cleanup adapter data in background (don't block the response)
    let adapters = state.sqlite.list_adapters_all().await.unwrap_or_default();
    let key_prefix = format!("/{id}/");
    tokio::spawn(async move {
        for adapter in &adapters {
            let addr = format!("http://adapter-{}:9090", adapter.name);
            let _ = GrpcAdapterClient::cleanup_prefix(&addr, &key_prefix).await;
        }
    });

    Ok(StatusCode::NO_CONTENT)
}

// --- Run Control ---

pub async fn start_run(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let hypothesis = state.sqlite.get_hypothesis(&id).await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let manager = state.managers.get(&id).await
        .ok_or_else(|| ApiError::NotFound(format!("manager for {id} not found")))?;

    let duration_secs = hypothesis.duration.as_ref().and_then(|d| parse_duration(d));

    let checkpoint_interval_secs = parse_duration(&hypothesis.checkpoint_every).unwrap_or(600);

    let config = ExecutionConfig {
        checkpoint_interval_secs,
        duration_secs,
        ..ExecutionConfig::default()
    };

    let adapter = if let Some(name) = &hypothesis.adapter {
        Some(state.sqlite.get_adapter_by_name(name).await?
            .ok_or_else(|| ApiError::NotFound(format!("adapter '{name}' not found")))?)
    } else { None };

    let mut gen_params = crate::generator::GenerateParams {
        generator: hypothesis.generator.clone(),
        ops: usize::MAX,
        ..crate::generator::GenerateParams::default()
    };
    // Apply stored key_space config (e.g. count)
    if let Ok(ks) = serde_json::from_str::<crate::generator::KeySpaceConfig>(&hypothesis.key_space) {
        gen_params.key_space = ks;
    }
    gen_params.key_space.prefix = format!("/{id}/");

    let mut mgr = manager.lock().await;
    if mgr.is_running() {
        return Err(ApiError::Conflict("hypothesis is already running".to_string()));
    }

    let (run_id, _) = mgr.start_run(config, gen_params, adapter, hypothesis.adapter_addr.clone()).await?;

    Ok((StatusCode::ACCEPTED, Json(StartRunResponse {
        run_id, hypothesis_id: id, status: HypothesisStatus::Running.to_string(),
    })))
}

pub async fn stop_run(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let manager = state.managers.get(&id).await
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    manager.lock().await.stop_run().await?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Observability ---

pub async fn status(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    let h = state.sqlite.get_hypothesis(&id).await?.ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    let (run_id, progress) = if let Some(mgr_arc) = state.managers.get(&id).await {
        let mgr = mgr_arc.lock().await;
        (mgr.run_id().map(|s| s.to_string()), if let Some(p) = mgr.progress() { Some(p.read().await.clone()) } else { None })
    } else { (None, None) };
    Ok(Json(StatusResponse { hypothesis_id: id, status: h.status, run_id, progress }))
}

pub async fn events(State(state): State<AppState>, Path(id): Path<String>, Query(query): Query<EventsQuery>) -> Result<impl IntoResponse, ApiError> {
    state.sqlite.get_hypothesis(&id).await?.ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    let events = state.files.read_merged_events(&id, query.run_id.as_deref(), query.event_type.as_deref(), query.since.as_deref())?;
    let tail = if let Some(last) = query.last {
        if events.len() > last { &events[events.len() - last..] } else { &events }
    } else {
        &events
    };
    let mut body = String::new();
    for event in tail { body.push_str(&serde_json::to_string(event).unwrap_or_default()); body.push('\n'); }
    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body))
}

pub async fn results(State(state): State<AppState>, Path(id): Path<String>) -> Result<impl IntoResponse, ApiError> {
    state.sqlite.get_hypothesis(&id).await?.ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    let results = state.sqlite.get_results(&id).await?;
    let items: Vec<serde_json::Value> = results.iter().map(|r| serde_json::json!({
        "id": r.id, "run_id": r.run_id, "total_batches": r.total_batches,
        "total_checkpoints": r.total_checkpoints, "passed_checkpoints": r.passed_checkpoints,
        "failed_checkpoints": r.failed_checkpoints, "total_response_ops": r.total_response_ops,
        "safety_violations": r.safety_violations, "stop_reason": r.stop_reason,
        "started_at": r.started_at, "finished_at": r.finished_at,
    })).collect();
    Ok(Json(serde_json::json!({ "items": items })))
}

pub async fn delete_result(
    State(state): State<AppState>,
    Path((id, result_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state.sqlite.get_hypothesis(&id).await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    let deleted = state.sqlite.delete_result(&result_id).await?;
    if !deleted {
        return Err(ApiError::NotFound(format!("result {result_id} not found")));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn bundle(State(state): State<AppState>, Path(id): Path<String>, Query(query): Query<BundleQuery>) -> Result<impl IntoResponse, ApiError> {
    state.sqlite.get_hypothesis(&id).await?.ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;
    let data = state.files.create_bundle(&id, query.run_id.as_deref())?;
    let disposition = format!("attachment; filename=\"hypothesis-{id}.zip\"");
    Ok((StatusCode::OK, [(header::CONTENT_TYPE.as_str(), "application/zip".to_string()), (header::CONTENT_DISPOSITION.as_str(), disposition)], Body::from(data)))
}

// --- Helpers ---

fn parse_duration(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.ends_with('s') { s[..s.len()-1].parse().ok() }
    else if s.ends_with('m') { s[..s.len()-1].parse::<u64>().ok().map(|m| m * 60) }
    else if s.ends_with('h') { s[..s.len()-1].parse::<u64>().ok().map(|h| h * 3600) }
    else { s.parse().ok() }
}

fn to_response(h: &crate::storage::sqlite::Hypothesis) -> HypothesisResponse {
    HypothesisResponse {
        id: h.id.clone(),
        name: h.name.clone(),
        generator: h.generator.clone(),
        adapter: h.adapter.clone(),
        adapter_addr: h.adapter_addr.clone(),
        duration: h.duration.clone(),
        checkpoint_every: h.checkpoint_every.clone(),
        tolerance: h.tolerance.as_ref().and_then(|t| serde_json::from_str(t).ok()),
        status: h.status.clone(),
        created_at: h.created_at.clone(),
        last_run_at: h.last_run_at.clone(),
    }
}
