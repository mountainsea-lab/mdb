use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Mutex;
use tower::ServiceExt;

use crate::{
    build_demo_router, ApiAppState, ApiError, ApiReadinessProjection, ApiResponse,
    ApiRunnerStatusProjection, MarketDataTradesResponse, RunnerFixtureTradeInput,
    RunnerStartFixtureRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoFixtureTrade {
    pub symbol: String,
    pub trade_id: String,
    pub sequence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoFlowRequest {
    pub trades: Vec<DemoFixtureTrade>,
    pub query_symbol: String,
    pub query_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DemoFlowSummary {
    pub readiness: ApiReadinessProjection,
    pub start_status: ApiRunnerStatusProjection,
    pub final_status: ApiRunnerStatusProjection,
    pub market_data: MarketDataTradesResponse,
}

pub fn default_demo_flow_request() -> DemoFlowRequest {
    DemoFlowRequest {
        trades: vec![DemoFixtureTrade {
            symbol: "BTCUSDT".to_string(),
            trade_id: "btc-demo-1".to_string(),
            sequence: Some("seq-demo-1".to_string()),
        }],
        query_symbol: "BTCUSDT".to_string(),
        query_limit: 10,
    }
}

pub async fn run_demo_flow_once(request: DemoFlowRequest) -> Result<DemoFlowSummary, ApiError> {
    if request.trades.is_empty() {
        return Err(ApiError::validation(
            "demo flow requires at least one fixture trade",
        ));
    }

    let router = build_demo_router(build_demo_state()?);

    let readiness: ApiResponse<ApiReadinessProjection> =
        get_json(router.clone(), "/ready", "ready").await?;
    let start_status: ApiResponse<ApiRunnerStatusProjection> = post_json(
        router.clone(),
        "/runner/start-fixture",
        &RunnerStartFixtureRequest {
            trades: request.trades.into_iter().map(Into::into).collect(),
        },
        "runner start fixture",
    )
    .await?;
    ensure_api_success(&start_status, "runner start fixture")?;

    let final_status: ApiResponse<ApiRunnerStatusProjection> =
        get_json(router.clone(), "/runner/status", "runner status").await?;
    let market_data_path = format!(
        "/market-data/trades?symbol={}&limit={}",
        request.query_symbol, request.query_limit
    );
    let market_data: ApiResponse<MarketDataTradesResponse> =
        get_json(router, &market_data_path, "market data query").await?;

    Ok(DemoFlowSummary {
        readiness: readiness.data,
        start_status: start_status.data,
        final_status: final_status.data,
        market_data: market_data.data,
    })
}

fn build_demo_state() -> Result<ApiAppState, ApiError> {
    let mut app = FdcServerApp::with_defaults();
    app.initialize()
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));

    Ok(ApiAppState::new(app)
        .with_market_data_store(store)
        .with_market_data_runner_control(runner))
}

async fn get_json<T>(router: Router, path: &str, context: &str) -> Result<T, ApiError>
where
    T: DeserializeOwned,
{
    let response = router
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .map_err(|error| {
                    ApiError::internal(format!("{context} request build failed: {error}"))
                })?,
        )
        .await
        .map_err(|error| ApiError::internal(format!("{context} route failed: {error}")))?;

    decode_json_response(response.status(), response.into_body(), context).await
}

async fn post_json<T, B>(router: Router, path: &str, body: &B, context: &str) -> Result<T, ApiError>
where
    T: DeserializeOwned,
    B: Serialize,
{
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(body).map_err(
                    |error| ApiError::internal(format!("{context} json encode failed: {error}")),
                )?))
                .map_err(|error| {
                    ApiError::internal(format!("{context} request build failed: {error}"))
                })?,
        )
        .await
        .map_err(|error| ApiError::internal(format!("{context} route failed: {error}")))?;

    decode_json_response(response.status(), response.into_body(), context).await
}

async fn decode_json_response<T>(
    status: StatusCode,
    body: Body,
    context: &str,
) -> Result<T, ApiError>
where
    T: DeserializeOwned,
{
    if !status.is_success() {
        return Err(ApiError::internal(format!(
            "{context} route returned HTTP {status}"
        )));
    }

    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|error| ApiError::internal(format!("{context} body read failed: {error}")))?;

    serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::internal(format!("{context} json decode failed: {error}")))
}

fn ensure_api_success<T>(response: &ApiResponse<T>, context: &str) -> Result<(), ApiError> {
    if response.status == "success" {
        return Ok(());
    }

    Err(ApiError::validation(
        response
            .message
            .clone()
            .unwrap_or_else(|| format!("{context} returned API error")),
    ))
}

impl From<DemoFixtureTrade> for RunnerFixtureTradeInput {
    fn from(value: DemoFixtureTrade) -> Self {
        Self {
            symbol: value.symbol,
            trade_id: value.trade_id,
            sequence: value.sequence,
        }
    }
}
