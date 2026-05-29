use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::{
    market_data::{
        model::{
            LiveMarketDataStatusResponse, MarketDataTradesResponse, StartLiveMarketDataRequest,
            StartLiveMarketDataResponse,
        },
        service::{live_status, query_trades, start_live_disabled},
    },
    ProductionServerState,
};

#[derive(Debug, Clone, Deserialize)]
pub struct TradeQueryParams {
    pub symbol: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ServerApiResponse<T> {
    pub data: T,
    pub status: String,
    pub message: Option<String>,
}

impl<T> ServerApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            data,
            status: "success".to_string(),
            message: None,
        }
    }

    pub fn error(data: T, message: impl Into<String>) -> Self {
        Self {
            data,
            status: "error".to_string(),
            message: Some(message.into()),
        }
    }
}

pub fn build_market_data_router(state: ProductionServerState) -> Router {
    Router::new()
        .route("/market-data/live/start", post(start_live_handler))
        .route("/market-data/live/status", get(live_status_handler))
        .route("/market-data/trades", get(query_trades_handler))
        .with_state(state)
}

async fn start_live_handler(
    State(state): State<ProductionServerState>,
    Json(_request): Json<StartLiveMarketDataRequest>,
) -> Json<ServerApiResponse<StartLiveMarketDataResponse>> {
    if !state.config().live_enabled {
        let (data, message) = start_live_disabled(&state);
        return Json(ServerApiResponse::error(data, message));
    }

    let (data, message) = start_live_disabled(&state);
    Json(ServerApiResponse::error(data, message))
}

async fn live_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<LiveMarketDataStatusResponse>> {
    Json(ServerApiResponse::success(live_status(&state)))
}

async fn query_trades_handler(
    State(state): State<ProductionServerState>,
    Query(params): Query<TradeQueryParams>,
) -> Json<ServerApiResponse<MarketDataTradesResponse>> {
    Json(ServerApiResponse::success(query_trades(
        &state,
        params.symbol,
        params.limit,
    )))
}
