use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use rust_decimal::Decimal;

fn sample_trade_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("trade-1".to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_envelope() -> BarterIngestionEnvelope {
    let mut envelope =
        BarterIngestionEnvelope::from_event("barter:binance_spot", sample_trade_event());
    envelope.envelope_id = "env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: true,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[tokio::test]
async fn barter_fixture_flows_into_queryable_market_data_store() {
    let store = QueryableMarketDataStore::new();

    let result = run_barter_envelopes_to_storage_once(vec![sample_envelope()], &store)
        .await
        .expect("finite fixture should write to queryable store");

    assert_eq!(result.storage_records_written, 1);

    let records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].namespace, "market_data");
    assert_eq!(records[0].collection, "trades");
    assert_eq!(
        records[0].metadata.tags.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        records[0].metadata.tags.get("kind").map(String::as_str),
        Some("trade")
    );

    let json: serde_json::Value =
        serde_json::from_slice(&records[0].value).expect("record should contain DTO JSON");
    assert_eq!(json["event_id"], "env-1");
    assert_eq!(json["symbol"], "BTCUSDT");
    assert_eq!(json["payload"]["Trade"]["trade_id"], "trade-1");
}
