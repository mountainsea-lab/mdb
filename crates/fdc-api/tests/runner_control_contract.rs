use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_market_data_router, build_runner_control_router, cancel_runner_from_state,
    start_fixture_runner_from_state, ApiAppState, MarketDataTradeQueryParams,
    RunnerFixtureTradeInput, RunnerStartFixtureRequest,
};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn state_with_control_runner() -> (ApiAppState, Arc<QueryableMarketDataStore>) {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));
    let state = ApiAppState::new(FdcServerApp::with_defaults())
        .with_market_data_store(Arc::clone(&store))
        .with_market_data_runner_control(runner);
    (state, store)
}

fn start_request() -> RunnerStartFixtureRequest {
    RunnerStartFixtureRequest {
        trades: vec![
            RunnerFixtureTradeInput {
                symbol: "BTCUSDT".to_string(),
                trade_id: "btc-1".to_string(),
                sequence: Some("seq-1".to_string()),
            },
            RunnerFixtureTradeInput {
                symbol: "ETHUSDT".to_string(),
                trade_id: "eth-1".to_string(),
                sequence: Some("seq-2".to_string()),
            },
        ],
    }
}

#[tokio::test]
async fn pure_start_helper_runs_fixture_and_returns_completed_status() {
    let (state, store) = state_with_control_runner();

    let response = start_fixture_runner_from_state(&state, start_request()).await;

    assert_eq!(response.status, "success");
    assert_eq!(response.data.configured, true);
    assert_eq!(
        response.data.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        response
            .data
            .last_result
            .as_ref()
            .expect("last result should project")
            .storage_records_written,
        2
    );
    assert_eq!(
        fdc_api::query_market_data_trades(
            &state,
            MarketDataTradeQueryParams {
                symbol: Some("BTCUSDT".to_string()),
                limit: Some(10),
            },
        )
        .data
        .returned_records,
        1
    );
    assert_eq!(
        store
            .query(&fdc_storage::MarketDataQuery::for_trades())
            .len(),
        2
    );
}

#[tokio::test]
async fn start_fixture_route_writes_records_readable_by_market_data_route() {
    let (state, _store) = state_with_control_runner();
    let control_router = build_runner_control_router(state.clone());

    let response = control_router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/start-fixture")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&start_request()).expect("request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be JSON");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["state"], "completed");

    let market_data_router = build_market_data_router(state);
    let query_response = market_data_router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("market-data router should respond");
    assert_eq!(query_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn cancel_helper_transitions_created_runner_to_cancelled() {
    let (state, _store) = state_with_control_runner();

    let response = cancel_runner_from_state(&state).await;

    assert_eq!(response.status, "success");
    assert_eq!(
        response.data.state,
        fdc_api::ApiRunnerLifecycleStatus::Cancelled
    );
    assert_eq!(response.data.last_result, None);
}

#[tokio::test]
async fn missing_control_handle_returns_error_response() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());

    let response = start_fixture_runner_from_state(&state, start_request()).await;

    assert_eq!(response.status, "error");
    assert!(response
        .message
        .expect("error response should include a message")
        .contains("runner control handle is not configured"));
    assert!(!response.data.configured);
}

#[tokio::test]
async fn empty_start_request_returns_error_response() {
    let (state, _store) = state_with_control_runner();

    let response =
        start_fixture_runner_from_state(&state, RunnerStartFixtureRequest { trades: Vec::new() })
            .await;

    assert_eq!(response.status, "error");
    assert!(response
        .message
        .expect("error response should include a message")
        .contains("at least one fixture trade"));
}
