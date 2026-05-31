use std::time::Duration;

use axum::{extract::State, routing::post, Json, Router};
use fdc_barter::{
    collect_live_market_data_envelopes, default_binance_spot_market_data_subscriptions,
    init_binance_spot_market_data,
};
use fdc_server::{run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig};
use fdc_storage::QueryableMarketDataStore;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{ApiAppState, ApiResponse};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRunnerStartRequest {
    pub timeout_secs: Option<u64>,
    pub max_envelopes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveRunnerStartResponse {
    pub started: bool,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

impl LiveRunnerStartResponse {
    fn not_started() -> Self {
        Self {
            started: false,
            envelopes_received: 0,
            storage_records_written: 0,
            market_data_store_records: 0,
        }
    }
}

pub fn build_live_runner_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/runner/start-live", post(start_live_runner_handler))
        .with_state(state)
}

async fn start_live_runner_handler(
    State(state): State<ApiAppState>,
    Json(request): Json<LiveRunnerStartRequest>,
) -> Json<ApiResponse<LiveRunnerStartResponse>> {
    Json(start_live_runner_from_state(&state, request).await)
}

pub async fn start_live_runner_from_state(
    state: &ApiAppState,
    request: LiveRunnerStartRequest,
) -> ApiResponse<LiveRunnerStartResponse> {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        return ApiResponse::error(
            LiveRunnerStartResponse::not_started(),
            "live runner requires explicit FDC_BARTER_LIVE_SMOKE=1".to_string(),
        );
    }

    let timeout_secs = request.timeout_secs.unwrap_or(30).max(1);
    let max_envelopes = request.max_envelopes.unwrap_or(100).max(1);
    let store = state.market_data_store();

    match tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to create live runner runtime: {error}"))?;

        runtime.block_on(run_live_collection_and_storage(
            store,
            timeout_secs,
            max_envelopes,
        ))
    })
    .await
    {
        Ok(Ok(response)) => ApiResponse::success(response),
        Ok(Err(message)) => ApiResponse::error(LiveRunnerStartResponse::not_started(), message),
        Err(error) => ApiResponse::error(
            LiveRunnerStartResponse::not_started(),
            format!("live runner task failed to join: {error}"),
        ),
    }
}

async fn run_live_collection_and_storage(
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> Result<LiveRunnerStartResponse, String> {
    eprintln!(
        "fdc live runner: starting Binance Spot market data timeout_secs={timeout_secs} max_envelopes={max_envelopes}"
    );

    let streams = init_binance_spot_market_data(default_binance_spot_market_data_subscriptions())
        .await
        .map_err(|error| {
            eprintln!("fdc live runner: failed to initialize stream: {error}");
            format!("failed to initialize Binance Spot live market-data stream: {error}")
        })?;

    let stream = streams.select_all();
    let envelopes = match tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        collect_live_market_data_envelopes(
            "barter-binance-spot-live-market-data",
            stream,
            max_envelopes,
        ),
    )
    .await
    {
        Ok(Ok(envelopes)) => envelopes,
        Ok(Err(error)) => {
            eprintln!("fdc live runner: stream item error: {error}");
            return Err(format!("failed while collecting live market data: {error}"));
        }
        Err(_) => {
            eprintln!("fdc live runner: timed out while collecting live market data");
            return Err(format!(
                "timed out after {timeout_secs}s while collecting live market data"
            ));
        }
    };

    eprintln!(
        "fdc live runner: collected {} live envelopes, writing to queryable store",
        envelopes.len()
    );

    if envelopes.is_empty() {
        return Err("live stream returned no market-data envelopes".to_string());
    }

    let summary = run_realtime_barter_envelope_stream(
        stream::iter(envelopes),
        store,
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(timeout_secs.max(1)),
            idle_timeout: Duration::from_millis(100),
            max_errors: 0,
        },
    )
    .await
    .map_err(|error| {
        eprintln!("fdc live runner: failed to write live envelopes: {error}");
        format!("failed to write live envelopes: {error}")
    })?;

    eprintln!(
        "fdc live runner: completed envelopes_received={} storage_records_written={} market_data_store_records={}",
        summary.envelopes_received,
        summary.storage_records_written,
        summary.market_data_store_records
    );

    Ok(LiveRunnerStartResponse {
        started: true,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}
