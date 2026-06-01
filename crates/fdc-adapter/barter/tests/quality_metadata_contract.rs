use fdc_barter::{
    event_latency_ns, BarterKindCounters, BarterMarketDataKind, BarterMarketDataMode,
    BarterMarketEvent, BarterMarketPayload, BarterMarketType, BarterRuntimeObservation,
    TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;

fn trade_event(kind: BarterMarketDataKind) -> BarterMarketEvent {
    let payload = match kind {
        BarterMarketDataKind::Trade => BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("abc-123".to_string()),
            price: Price::from_f64(105.5).unwrap(),
            quantity: Decimal::new(225, 2),
            side: Some(TradeSide::Buy),
        }),
        _ => panic!("test helper only builds trade payloads"),
    };

    BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000),
        payload,
        sequence: None,
        checkpoint: None,
    }
}

#[test]
fn runtime_observation_can_represent_reconnect_without_envelope() {
    let observation = BarterRuntimeObservation::Reconnect {
        exchange: "binance_spot".to_string(),
    };

    assert_eq!(
        observation,
        BarterRuntimeObservation::Reconnect {
            exchange: "binance_spot".to_string()
        }
    );
}

#[test]
fn kind_counters_increment_by_market_data_kind() {
    let mut counters = BarterKindCounters::default();
    let event = trade_event(BarterMarketDataKind::Trade);

    counters.record_event(&event);
    counters.record_event(&event);

    assert_eq!(counters.count(BarterMarketDataKind::Trade), 2);
    assert_eq!(counters.count(BarterMarketDataKind::OrderBookL1), 0);
}

#[test]
fn event_latency_returns_received_minus_event_time() {
    let event = trade_event(BarterMarketDataKind::Trade);

    assert_eq!(event_latency_ns(&event), Some(1_000));
}
