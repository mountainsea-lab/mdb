use axum::{extract::State, routing::post, Json, Router};
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, DataQualityFlags, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    runner_status::runner_status_projection_from_runner, ApiAppState, ApiResponse,
    ApiRunnerStatusProjection,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerFixtureTradeInput {
    pub symbol: String,
    pub trade_id: String,
    pub sequence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerStartFixtureRequest {
    pub trades: Vec<RunnerFixtureTradeInput>,
}

pub async fn start_fixture_runner_from_state(
    state: &ApiAppState,
    request: RunnerStartFixtureRequest,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let Some(runner) = state.market_data_runner_control() else {
        return runner_control_error("runner control handle is not configured", state).await;
    };
    if request.trades.is_empty() {
        return runner_control_error("runner start requires at least one fixture trade", state)
            .await;
    }

    let envelopes = request
        .trades
        .into_iter()
        .enumerate()
        .map(|(index, trade)| fixture_trade_to_envelope(index, trade))
        .collect();

    let mut runner = runner.lock().await;
    match runner.start_once(envelopes).await {
        Ok(_) => ApiResponse::success(runner_status_projection_from_runner(&runner)),
        Err(error) => ApiResponse::error(
            runner_status_projection_from_runner(&runner),
            error.to_string(),
        ),
    }
}

pub async fn cancel_runner_from_state(
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let Some(runner) = state.market_data_runner_control() else {
        return runner_control_error("runner control handle is not configured", state).await;
    };

    let mut runner = runner.lock().await;
    match runner.cancel() {
        Ok(()) => ApiResponse::success(runner_status_projection_from_runner(&runner)),
        Err(error) => ApiResponse::error(
            runner_status_projection_from_runner(&runner),
            error.to_string(),
        ),
    }
}

pub fn build_runner_control_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/runner/start-fixture", post(start_fixture_handler))
        .route("/runner/cancel", post(cancel_handler))
        .with_state(state)
}

async fn start_fixture_handler(
    State(state): State<ApiAppState>,
    Json(request): Json<RunnerStartFixtureRequest>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(start_fixture_runner_from_state(&state, request).await)
}

async fn cancel_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(cancel_runner_from_state(&state).await)
}

async fn runner_control_error(
    message: impl Into<String>,
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let projection = if let Some(runner) = state.market_data_runner_control() {
        let runner = runner.lock().await;
        runner_status_projection_from_runner(&runner)
    } else {
        crate::runner_status_response_from_state(state).data
    };
    ApiResponse::error(projection, message.into())
}

fn fixture_trade_to_envelope(
    index: usize,
    input: RunnerFixtureTradeInput,
) -> BarterIngestionEnvelope {
    let sequence = input
        .sequence
        .unwrap_or_else(|| format!("seq-{}", index + 1));
    let timestamp = 1_700_000_000_000_000_000_i64 + index as i64;
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(input.symbol),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(timestamp),
        received_at: TimestampNs::from_nanos(timestamp + 100),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(input.trade_id.clone()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: Decimal::new(1, 0),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(sequence),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = format!("env-{}", input.trade_id);
    envelope.emitted_at = TimestampNs::from_nanos(timestamp + 200);
    envelope.quality = DataQualityFlags::default();
    envelope
}
