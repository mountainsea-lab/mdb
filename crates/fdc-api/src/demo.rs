use axum::{extract::State, routing::get, Json, Router};

use crate::{
    build_live_runner_router, build_market_data_router, build_runner_control_router,
    build_runner_status_router, readiness_response_from_state, ApiAppState, ApiReadinessProjection,
    ApiResponse,
};

pub fn build_demo_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/ready", get(readiness_handler))
        .with_state(state.clone())
        .merge(build_runner_status_router(state.clone()))
        .merge(build_runner_control_router(state.clone()))
        .merge(build_live_runner_router(state.clone()))
        .merge(build_market_data_router(state))
}

async fn readiness_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiReadinessProjection>> {
    Json(readiness_response_from_state(&state))
}
