use axum::{extract::State, routing::get, Json, Router};

use crate::{
    health::{
        model::{HealthResponse, ReadinessResponse},
        service::{health_response, readiness_response},
    },
    ProductionServerState,
};

pub fn build_health_router(state: ProductionServerState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(readiness_handler))
        .with_state(state)
}

async fn health_handler() -> Json<HealthResponse> {
    Json(health_response())
}

async fn readiness_handler(State(state): State<ProductionServerState>) -> Json<ReadinessResponse> {
    Json(readiness_response(&state))
}
