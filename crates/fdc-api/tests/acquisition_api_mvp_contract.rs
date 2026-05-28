use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{build_market_data_router, ApiAppState};
use fdc_barter::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, public_trade_result_to_data_kind, BarterIngestionEnvelope,
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{run_barter_fixture_mvp_once, BoundedMarketDataMvpRunner, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use futures::StreamExt;
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

#[tokio::test]
async fn bounded_mvp_runner_populates_store_read_by_api_route() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = BoundedMarketDataMvpRunner::new(Arc::clone(&store));

    let result = runner
        .run_barter_fixtures_once(vec![
            sample_envelope("BTCUSDT", "btc-1", "seq-1"),
            sample_envelope("ETHUSDT", "eth-1", "seq-2"),
            sample_envelope("BTCUSDT", "btc-2", "seq-3"),
        ])
        .await
        .expect("bounded fixture runner should write to the queryable store");

    assert_eq!(result.envelopes_received, 3);
    assert_eq!(result.storage_records_written, 3);
    assert_eq!(result.market_data_store_records, 3);

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let router = build_market_data_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
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
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(
        json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"],
        "btc-1"
    );
    assert_eq!(
        json["data"]["records"][1]["payload"]["payload"]["Trade"]["trade_id"],
        "btc-2"
    );
}

#[tokio::test]
async fn convenience_helper_uses_the_injected_store() {
    let store = Arc::new(QueryableMarketDataStore::new());

    let result = run_barter_fixture_mvp_once(
        vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")],
        Arc::clone(&store),
    )
    .await
    .expect("convenience helper should write fixture data");

    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.source_valid, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(result.market_data_store_records, 1);

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let response = fdc_api::query_market_data_trades(
        &state,
        fdc_api::MarketDataTradeQueryParams {
            symbol: Some("BTCUSDT".to_string()),
            limit: Some(10),
        },
    );

    assert_eq!(response.data.returned_records, 1);
    assert_eq!(response.data.records[0].symbol.as_deref(), Some("BTCUSDT"));
}

#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .expect("live Binance Spot stream should initialize");
    let stream = streams.select_all().map(public_trade_result_to_data_kind);
    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        collect_live_trade_envelopes("barter-binance-spot-live-trades", stream, 1),
    )
    .await
    .expect("should receive one live trade within timeout")
    .expect("live collection should succeed");

    assert!(!envelopes.is_empty(), "expected at least one live envelope");

    let store = Arc::new(QueryableMarketDataStore::new());
    let result = run_barter_fixture_mvp_once(envelopes, Arc::clone(&store))
        .await
        .expect("live envelopes should write through the bounded MVP runner");

    assert!(
        result.storage_records_written >= 1,
        "expected the MVP runner to write at least one live trade record"
    );

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let router = build_market_data_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?limit=10")
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

    eprintln!("live acquisition API smoke response: {json:#}");

    let returned_records = json["data"]["returned_records"]
        .as_u64()
        .expect("returned_records should be numeric");
    assert_eq!(json["status"], "success");
    assert!(
        returned_records >= 1,
        "expected at least one API trade record"
    );
    assert_eq!(json["data"]["records"][0]["kind"], "trade");
    assert!(
        json["data"]["records"][0]["payload"]["symbol"].is_string(),
        "live trade payload should expose a symbol"
    );
}
