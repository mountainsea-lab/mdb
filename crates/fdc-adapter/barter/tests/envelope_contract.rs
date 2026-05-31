use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, DataQualityFlags, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;

#[test]
fn envelope_wraps_event_with_default_quality_flags() {
    let event = BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_000),
        received_at: TimestampNs::from_nanos(1_100),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("t1".to_string()),
            price: Price::from_f64(100.0).unwrap(),
            quantity: Decimal::new(25, 1),
            side: Some(TradeSide::Buy),
        }),
        sequence: None,
        checkpoint: None,
    };

    let envelope = BarterIngestionEnvelope::from_event("source-1", event);

    assert_eq!(envelope.source_id, "source-1");
    assert!(!envelope.envelope_id.is_empty());
    assert_eq!(envelope.quality, DataQualityFlags::default());
    assert_eq!(envelope.event.exchange, "binance_spot");
}
