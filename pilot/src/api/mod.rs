pub mod adapters;
pub mod error;
pub mod health;
pub mod hypothesis;
pub mod models;
pub mod profiles;

use axum::routing::{delete, get, post};
use axum::Router;

use crate::app_state::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        // Hypothesis CRUD
        .route("/api/hypothesis", post(hypothesis::create))
        .route("/api/hypothesis", get(hypothesis::list))
        .route("/api/hypothesis/{id}", get(hypothesis::get_one))
        .route("/api/hypothesis/{id}", delete(hypothesis::delete))
        // Origin
        .route("/api/hypothesis/{id}/origin", post(hypothesis::upload_origin))
        .route("/api/hypothesis/{id}/generate", post(hypothesis::generate))
        // Run control
        .route("/api/hypothesis/{id}/run", post(hypothesis::start_run))
        .route("/api/hypothesis/{id}/run", delete(hypothesis::stop_run))
        // Observability
        .route("/api/hypothesis/{id}/status", get(hypothesis::status))
        .route("/api/hypothesis/{id}/events", get(hypothesis::events))
        .route("/api/hypothesis/{id}/bundle", get(hypothesis::bundle))
        // Adapters (standalone)
        .route("/api/adapters", post(adapters::create))
        .route("/api/adapters", get(adapters::list))
        .route("/api/adapters/{id}", get(adapters::get_one))
        .route("/api/adapters/{id}", delete(adapters::delete))
        // Profiles
        .route("/api/profiles", get(profiles::list))
        .route("/api/profiles", post(profiles::create))
        .route("/api/profiles/{name}", get(profiles::get_one))
        .route("/api/profiles/{name}", delete(profiles::delete))
        // Health
        .route("/health", get(health::health))
        .route("/metrics", get(health::metrics))
        .with_state(state)
}
