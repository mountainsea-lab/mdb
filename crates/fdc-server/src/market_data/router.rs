use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;

use crate::{
    market_data::{
        model::{
            LiveMarketDataStatusResponse, MarketDataStorageHealthResponse,
            MarketDataStorageMaintenanceAuditResetRequest,
            MarketDataStorageMaintenanceAuditResetResponse,
            MarketDataStorageMaintenanceAuditResponse, MarketDataStorageMaintenanceRunRequest,
            MarketDataStorageMaintenanceRunResponse,
            MarketDataStorageMaintenanceSchedulerStatusResponse, MarketDataStorageStatusResponse,
            MarketDataTradesResponse, StartLiveMarketDataRequest, StartLiveMarketDataResponse,
            StopLiveMarketDataResponse,
        },
        service::{
            live_status, query_trades, reset_storage_maintenance_audit,
            run_storage_maintenance_once, start_live, start_live_disabled, stop_live,
            storage_health, storage_maintenance_audit, storage_maintenance_scheduler_status,
            storage_status, StorageMaintenanceHttpStatus,
        },
    },
    ProductionServerState,
};

#[derive(Debug, Clone, Deserialize)]
pub struct TradeQueryParams {
    pub symbol: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceAuditQueryParams {
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
        .route("/market-data/live/stop", post(stop_live_handler))
        .route("/market-data/live/status", get(live_status_handler))
        .route("/market-data/storage/status", get(storage_status_handler))
        .route("/market-data/storage/health", get(storage_health_handler))
        .route(
            "/market-data/storage/maintenance/run-once",
            post(storage_maintenance_run_once_handler),
        )
        .route(
            "/market-data/storage/maintenance/audit",
            get(storage_maintenance_audit_handler),
        )
        .route(
            "/market-data/storage/maintenance/audit/reset",
            post(storage_maintenance_audit_reset_handler),
        )
        .route(
            "/market-data/storage/maintenance/scheduler/status",
            get(storage_maintenance_scheduler_status_handler),
        )
        .route("/market-data/trades", get(query_trades_handler))
        .with_state(state)
}

async fn start_live_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<StartLiveMarketDataRequest>,
) -> Json<ServerApiResponse<StartLiveMarketDataResponse>> {
    if !state.config().live_enabled {
        let (data, message) = start_live_disabled(&state);
        return Json(ServerApiResponse::error(data, message));
    }

    match start_live(&state, request).await {
        Ok(data) => Json(ServerApiResponse::success(data)),
        Err(message) => {
            let status = state.market_data_supervisor().status();
            let data = StartLiveMarketDataResponse {
                state: status.state,
                task_id: status.task_id.clone(),
                envelopes_received: status
                    .last_result
                    .as_ref()
                    .map(|result| result.envelopes_received)
                    .unwrap_or(0),
                storage_records_written: status
                    .last_result
                    .as_ref()
                    .map(|result| result.storage_records_written)
                    .unwrap_or(0),
                market_data_store_records: state.market_data_store().record_count(),
            };
            Json(ServerApiResponse::error(data, message))
        }
    }
}

async fn stop_live_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<StopLiveMarketDataResponse>> {
    Json(ServerApiResponse::success(stop_live(&state)))
}

async fn live_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<LiveMarketDataStatusResponse>> {
    Json(ServerApiResponse::success(live_status(&state)))
}

async fn storage_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageStatusResponse>> {
    Json(ServerApiResponse::success(storage_status(&state)))
}

async fn storage_health_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageHealthResponse>> {
    Json(ServerApiResponse::success(storage_health(&state).await))
}

async fn storage_maintenance_run_once_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<MarketDataStorageMaintenanceRunRequest>,
) -> (
    StatusCode,
    Json<ServerApiResponse<MarketDataStorageMaintenanceRunResponse>>,
) {
    let result = run_storage_maintenance_once(&state, request).await;
    let status = storage_maintenance_status_code(result.http_status);
    let envelope = if result.http_status == StorageMaintenanceHttpStatus::Ok {
        ServerApiResponse::success(result.response)
    } else {
        ServerApiResponse::error(
            result.response,
            result
                .message
                .unwrap_or_else(|| "storage maintenance request failed".to_string()),
        )
    };
    (status, Json(envelope))
}

fn storage_maintenance_status_code(status: StorageMaintenanceHttpStatus) -> StatusCode {
    match status {
        StorageMaintenanceHttpStatus::Ok => StatusCode::OK,
        StorageMaintenanceHttpStatus::BadRequest => StatusCode::BAD_REQUEST,
        StorageMaintenanceHttpStatus::Forbidden => StatusCode::FORBIDDEN,
        StorageMaintenanceHttpStatus::Conflict => StatusCode::CONFLICT,
        StorageMaintenanceHttpStatus::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn storage_maintenance_audit_handler(
    State(state): State<ProductionServerState>,
    Query(params): Query<MaintenanceAuditQueryParams>,
) -> Json<ServerApiResponse<MarketDataStorageMaintenanceAuditResponse>> {
    Json(ServerApiResponse::success(
        storage_maintenance_audit(&state, params.limit).await,
    ))
}

async fn storage_maintenance_audit_reset_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<MarketDataStorageMaintenanceAuditResetRequest>,
) -> (
    StatusCode,
    Json<ServerApiResponse<MarketDataStorageMaintenanceAuditResetResponse>>,
) {
    let result = reset_storage_maintenance_audit(&state, request).await;
    let status = storage_maintenance_status_code(result.http_status);
    let envelope = if result.http_status == StorageMaintenanceHttpStatus::Ok {
        ServerApiResponse::success(result.response)
    } else {
        ServerApiResponse::error(
            result.response,
            result
                .message
                .unwrap_or_else(|| "storage maintenance audit reset request failed".to_string()),
        )
    };
    (status, Json(envelope))
}

async fn storage_maintenance_scheduler_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageMaintenanceSchedulerStatusResponse>> {
    Json(ServerApiResponse::success(
        storage_maintenance_scheduler_status(&state).await,
    ))
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
