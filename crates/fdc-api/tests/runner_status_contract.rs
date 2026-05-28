use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_runner_status_router, runner_status_response_from_state, ApiAppState,
    ApiRunnerLifecycleStatus,
};
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use rust_decimal::Decimal;
use tower::ServiceExt;

fn sample_trade_event(symbol: &str, trade_id: &str, sequence: &str) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(sequence.to_string()),
        checkpoint: None,
    }
}

fn sample_envelope(symbol: &str, trade_id: &str, sequence: &str) -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(
        "barter:binance_spot",
        sample_trade_event(symbol, trade_id, sequence),
    );
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[test]
fn default_state_projects_runner_not_configured() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());

    let response = runner_status_response_from_state(&state);

    assert_eq!(response.status, "success");
    assert!(!response.data.configured);
    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::NotConfigured);
    assert_eq!(response.data.last_result, None);
    assert_eq!(response.data.failure_message, None);
}

#[test]
fn created_runner_projects_created_state() {
    let runner = Arc::new(BoundedMarketDataRunnerHandle::new(Arc::new(
        QueryableMarketDataStore::new(),
    )));
    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_runner(runner);

    let response = runner_status_response_from_state(&state);

    assert!(response.data.configured);
    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Created);
    assert_eq!(response.data.last_result, None);
}

#[tokio::test]
async fn completed_runner_projects_last_result_counts() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("fixture run should complete");
    let state =
        ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_runner(Arc::new(runner));

    let response = runner_status_response_from_state(&state);
    let result = response
        .data
        .last_result
        .expect("last result should project");

    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Completed);
    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(response.data.failure_message, None);
}

#[test]
fn cancelled_runner_projects_cancelled_state() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner.cancel().expect("created runner should cancel");
    let state =
        ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_runner(Arc::new(runner));

    let response = runner_status_response_from_state(&state);

    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Cancelled);
    assert_eq!(response.data.last_result, None);
    assert_eq!(response.data.failure_message, None);
}

#[tokio::test]
async fn in_memory_runner_status_route_returns_completed_json() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("fixture run should complete");
    let state =
        ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_runner(Arc::new(runner));
    let router = build_runner_status_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/runner/status")
                .body(Body::empty())
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
    assert_eq!(json["data"]["configured"], true);
    assert_eq!(json["data"]["state"], "completed");
    assert_eq!(json["data"]["last_result"]["storage_records_written"], 1);
}
