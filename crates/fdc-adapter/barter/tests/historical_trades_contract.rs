use fdc_barter::{
    historical_trade_dedupe_key, validate_historical_backfill_request, BarterMarketDataKind,
    BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload, BarterMarketType,
    HistoricalBackfillRequest, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;

fn historical_trade_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(1_000),
        cursor: None,
    }
}

fn trade_event(trade_id: Option<&str>) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: trade_id.map(str::to_string),
            price: Price::from_f64(105.5).unwrap(),
            quantity: Decimal::new(225, 2),
            side: Some(TradeSide::Buy),
        }),
        sequence: None,
        checkpoint: None,
    }
}

#[test]
fn historical_trade_request_does_not_require_interval() {
    validate_historical_backfill_request(&historical_trade_request())
        .expect("historical trade request should not require interval");
}

#[test]
fn historical_trade_dedupe_key_uses_trade_id_when_available() {
    let key = historical_trade_dedupe_key(&trade_event(Some("abc-123")))
        .expect("trade event should produce dedupe key");

    assert_eq!(key, "binance_spot:BTCUSDT:abc-123");
}

#[test]
fn historical_trade_dedupe_key_falls_back_to_event_fields_without_trade_id() {
    let key = historical_trade_dedupe_key(&trade_event(None))
        .expect("trade event should produce fallback dedupe key");

    assert_eq!(key, "binance_spot:BTCUSDT:1700000000000000000:105.5:2.25");
}
