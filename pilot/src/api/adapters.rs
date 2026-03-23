use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, Pod, PodSpec, Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, DeleteParams, PostParams};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tracing::{info, warn};
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
    if state.sqlite.get_adapter_by_name(&req.name).await?.is_some() {
        return Err(ApiError::Conflict(format!("adapter '{}' already exists", req.name)));
    }

    let id = format!("adp-{}", Uuid::now_v7());
    let env_json = serde_json::to_string(&req.env)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    // Step 1: Save to DB
    let adapter = state.sqlite.create_adapter(&id, &req.name, &req.image, &env_json).await?;

    // Step 2: Deploy Pod + Service to K8s
    let ns = &state.namespace;
    let pod_name = format!("adapter-{}", req.name);
    let labels: BTreeMap<String, String> = BTreeMap::from([
        ("app".to_string(), pod_name.clone()),
        ("regret.io/adapter".to_string(), req.name.clone()),
        ("regret.io/adapter-id".to_string(), id.clone()),
    ]);

    let env_vars: Vec<EnvVar> = req.env.iter().map(|(k, v)| EnvVar {
        name: k.clone(),
        value: Some(v.clone()),
        ..Default::default()
    }).collect();

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: Some(ns.clone()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                name: "adapter".to_string(),
                image: Some(req.image.clone()),
                image_pull_policy: Some("Always".to_string()),
                ports: Some(vec![ContainerPort {
                    container_port: 9090,
                    ..Default::default()
                }]),
                env: Some(env_vars),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            namespace: Some(ns.clone()),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([
                ("app".to_string(), pod_name.clone()),
            ])),
            ports: Some(vec![ServicePort {
                port: 9090,
                target_port: Some(IntOrString::Int(9090)),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let pods: Api<Pod> = Api::namespaced(state.kube.clone(), ns);
    let svcs: Api<Service> = Api::namespaced(state.kube.clone(), ns);
    let pp = PostParams::default();

    // Deploy pod
    if let Err(e) = pods.create(&pp, &pod).await {
        // Rollback DB record
        warn!(%e, "failed to create adapter pod, rolling back DB record");
        let _ = state.sqlite.delete_adapter(&id).await;
        return Err(ApiError::Internal(anyhow::anyhow!("failed to deploy adapter pod: {e}")));
    }

    // Deploy service
    if let Err(e) = svcs.create(&pp, &svc).await {
        // Rollback pod + DB record
        warn!(%e, "failed to create adapter service, rolling back");
        let _ = pods.delete(&pod_name, &DeleteParams::default()).await;
        let _ = state.sqlite.delete_adapter(&id).await;
        return Err(ApiError::Internal(anyhow::anyhow!("failed to deploy adapter service: {e}")));
    }

    info!(id = %id, name = %req.name, pod = %pod_name, "adapter deployed");
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
    let adapter = state.sqlite.get_adapter(&id).await?
        .ok_or_else(|| ApiError::NotFound(format!("adapter {id} not found")))?;
    Ok(Json(to_response(&adapter)))
}

pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let adapter = state.sqlite.get_adapter(&id).await?
        .ok_or_else(|| ApiError::NotFound(format!("adapter {id} not found")))?;

    let ns = &state.namespace;
    let pod_name = format!("adapter-{}", adapter.name);
    let dp = DeleteParams::default();

    // Delete K8s resources first (best-effort)
    let pods: Api<Pod> = Api::namespaced(state.kube.clone(), ns);
    let svcs: Api<Service> = Api::namespaced(state.kube.clone(), ns);

    if let Err(e) = svcs.delete(&pod_name, &dp).await {
        warn!(%e, "failed to delete adapter service (may not exist)");
    }
    if let Err(e) = pods.delete(&pod_name, &dp).await {
        warn!(%e, "failed to delete adapter pod (may not exist)");
    }

    // Delete DB record
    let deleted = state.sqlite.delete_adapter(&id).await?;
    if !deleted {
        return Err(ApiError::NotFound(format!("adapter {id} not found")));
    }

    info!(id = %id, name = %adapter.name, "adapter deleted");
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
