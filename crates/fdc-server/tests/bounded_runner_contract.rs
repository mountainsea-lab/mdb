use std::sync::Arc;

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{BoundedMarketDataRunnerHandle, BoundedRunnerState};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use rust_decimal::Decimal;

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
fn new_runner_starts_created_without_result_or_failure() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    assert_eq!(runner.state(), BoundedRunnerState::Created);
    assert_eq!(runner.last_result(), None);
    assert_eq!(runner.failure(), None);
    assert!(Arc::ptr_eq(&runner.market_data_store(), &store));
}

#[tokio::test]
async fn start_once_completes_and_records_queryable_market_data() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    let result = runner
        .start_once(vec![
            sample_envelope("BTCUSDT", "btc-1", "seq-1"),
            sample_envelope("ETHUSDT", "eth-1", "seq-2"),
        ])
        .await
        .expect("finite runner should complete");

    assert_eq!(runner.state(), BoundedRunnerState::Completed);
    assert_eq!(result.envelopes_received, 2);
    assert_eq!(result.storage_records_written, 2);
    assert_eq!(runner.last_result(), Some(&result));
    assert_eq!(runner.failure(), None);

    let btc_records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));
    assert_eq!(btc_records.len(), 1);
}

#[tokio::test]
async fn cancel_before_start_prevents_later_run_without_writes() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    runner.cancel().expect("created runner should cancel");

    assert_eq!(runner.state(), BoundedRunnerState::Cancelled);

    let error = runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect_err("cancelled runner must not start");

    assert!(error.to_string().contains("cancelled"));
    assert_eq!(runner.state(), BoundedRunnerState::Cancelled);
    assert_eq!(store.query(&MarketDataQuery::for_trades()).len(), 0);
}

#[tokio::test]
async fn completed_runner_rejects_second_run_and_preserves_first_result() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(store);

    let first = runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("first run should complete");

    let error = runner
        .start_once(vec![sample_envelope("ETHUSDT", "eth-1", "seq-2")])
        .await
        .expect_err("completed runner must not rerun");

    assert!(error.to_string().contains("completed"));
    assert_eq!(runner.state(), BoundedRunnerState::Completed);
    assert_eq!(runner.last_result(), Some(&first));
}
