pub mod adapters;
pub mod error;
pub mod generators;
pub mod health;
pub mod hypothesis;
pub mod models;

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
        // Run control
        .route("/api/hypothesis/{id}/run", post(hypothesis::start_run))
        .route("/api/hypothesis/{id}/run", delete(hypothesis::stop_run))
        // Observability
        .route("/api/hypothesis/{id}/status", get(hypothesis::status))
        .route("/api/hypothesis/{id}/events", get(hypothesis::events))
        .route("/api/hypothesis/{id}/results", get(hypothesis::results))
        .route("/api/hypothesis/{id}/bundle", get(hypothesis::bundle))
        // Adapters
        .route("/api/adapters", post(adapters::create))
        .route("/api/adapters", get(adapters::list))
        .route("/api/adapters/{id}", get(adapters::get_one))
        .route("/api/adapters/{id}", delete(adapters::delete))
        // Generators
        .route("/api/generators", get(generators::list))
        .route("/api/generators", post(generators::create))
        .route("/api/generators/{name}", get(generators::get_one))
        .route("/api/generators/{name}", delete(generators::delete))
        // Health
        .route("/health", get(health::health))
        .route("/metrics", get(health::metrics))
        .with_state(state)
}
