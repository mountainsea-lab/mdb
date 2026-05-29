use std::sync::Arc;
use std::time::Duration;

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use futures::stream;
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
        "barter:binance_spot:live",
        sample_trade_event(symbol, trade_id, sequence),
    );
    envelope.envelope_id = format!("live-env-{trade_id}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags::default();
    envelope
}

#[tokio::test]
async fn realtime_runner_writes_all_available_stream_events_and_queries_them() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let stream = stream::iter(vec![
        sample_envelope("BTCUSDT", "btc-live-1", "seq-1"),
        sample_envelope("ETHUSDT", "eth-live-1", "seq-2"),
        sample_envelope("BTCUSDT", "btc-live-2", "seq-3"),
    ]);

    let summary = run_realtime_barter_envelope_stream(
        stream,
        Arc::clone(&store),
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(1),
            idle_timeout: Duration::from_millis(50),
            max_errors: 0,
        },
    )
    .await
    .expect("offline realtime stream should complete");

    assert_eq!(summary.envelopes_received, 3);
    assert_eq!(summary.storage_records_written, 3);
    assert_eq!(summary.market_data_store_records, 3);
    assert!(summary.started_at <= summary.stopped_at);

    let btc_records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));
    assert_eq!(btc_records.len(), 2);
}

#[tokio::test]
async fn realtime_runner_stops_on_idle_without_fixed_record_limit() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let stream = stream::iter(vec![
        sample_envelope("BTCUSDT", "btc-live-1", "seq-1"),
        sample_envelope("BTCUSDT", "btc-live-2", "seq-2"),
    ]);

    let summary = run_realtime_barter_envelope_stream(
        stream,
        Arc::clone(&store),
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(60),
            idle_timeout: Duration::from_millis(10),
            max_errors: 0,
        },
    )
    .await
    .expect("offline realtime stream should stop after source exhaustion/idle");

    assert_eq!(summary.envelopes_received, 2);
    assert_eq!(summary.storage_records_written, 2);
    assert_eq!(store.query(&MarketDataQuery::for_trades()).len(), 2);
}
