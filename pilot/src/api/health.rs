use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

pub async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

pub async fn metrics() -> impl IntoResponse {
    // TODO: integrate with prometheus registry
    (StatusCode::OK, "# regret-pilot metrics\n")
}
