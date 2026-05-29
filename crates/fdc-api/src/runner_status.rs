use axum::{extract::State, routing::get, Json, Router};
use fdc_server::{BoundedMarketDataMvpResult, BoundedMarketDataRunnerHandle, BoundedRunnerState};
use serde::{Deserialize, Serialize};

use crate::{ApiAppState, ApiResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiRunnerLifecycleStatus {
    NotConfigured,
    Created,
    Running,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRunnerLastResultProjection {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRunnerStatusProjection {
    pub configured: bool,
    pub state: ApiRunnerLifecycleStatus,
    pub last_result: Option<ApiRunnerLastResultProjection>,
    pub failure_message: Option<String>,
}

pub fn runner_status_response_from_state(
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    ApiResponse::success(runner_status_projection_from_state(state))
}

pub async fn runner_status_response_from_state_async(
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    if let Some(runner) = state.market_data_runner_control() {
        let runner = runner.lock().await;
        return ApiResponse::success(runner_status_projection_from_runner(&runner));
    }

    runner_status_response_from_state(state)
}

pub fn build_runner_status_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/runner/status", get(runner_status_handler))
        .with_state(state)
}

async fn runner_status_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(runner_status_response_from_state_async(&state).await)
}

fn runner_status_projection_from_state(state: &ApiAppState) -> ApiRunnerStatusProjection {
    let Some(runner) = state.market_data_runner() else {
        return ApiRunnerStatusProjection {
            configured: false,
            state: ApiRunnerLifecycleStatus::NotConfigured,
            last_result: None,
            failure_message: None,
        };
    };

    runner_status_projection_from_runner(&runner)
}

pub fn runner_status_projection_from_runner(
    runner: &BoundedMarketDataRunnerHandle,
) -> ApiRunnerStatusProjection {
    ApiRunnerStatusProjection {
        configured: true,
        state: runner_lifecycle_status_label(runner.state()),
        last_result: runner.last_result().map(last_result_projection),
        failure_message: runner.failure().map(|failure| failure.message.clone()),
    }
}

fn runner_lifecycle_status_label(state: BoundedRunnerState) -> ApiRunnerLifecycleStatus {
    match state {
        BoundedRunnerState::Created => ApiRunnerLifecycleStatus::Created,
        BoundedRunnerState::Running => ApiRunnerLifecycleStatus::Running,
        BoundedRunnerState::Completed => ApiRunnerLifecycleStatus::Completed,
        BoundedRunnerState::Cancelled => ApiRunnerLifecycleStatus::Cancelled,
        BoundedRunnerState::Failed => ApiRunnerLifecycleStatus::Failed,
    }
}

fn last_result_projection(result: &BoundedMarketDataMvpResult) -> ApiRunnerLastResultProjection {
    ApiRunnerLastResultProjection {
        envelopes_received: result.envelopes_received,
        source_valid: result.source_valid,
        source_invalid: result.source_invalid,
        dto_mapped: result.dto_mapped,
        storage_records_written: result.storage_records_written,
        market_data_store_records: result.market_data_store_records,
    }
}
