use barter_data::{
    event::{DataKind, MarketEvent},
    subscription::candle::Candle,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
};
use chrono::{TimeZone, Utc};
use fdc_barter::{BarterMarketDataKind, BarterMarketPayload, BarterMarketType};
use rust_decimal::Decimal;

fn candle_event(candle: Candle) -> MarketEvent<MarketDataInstrument, DataKind> {
    MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new("btc", "usdt", MarketDataInstrumentKind::Spot),
        kind: DataKind::Candle(candle),
    }
}

#[test]
fn candle_payload_exposes_research_fields() {
    let payload = fdc_barter::CandlePayload {
        interval: Some("1m".to_string()),
        open_time: fdc_core::types::TimestampNs::from_nanos(1_700_000_000_000_000_000),
        close_time: fdc_core::types::TimestampNs::from_nanos(1_700_000_060_000_000_000),
        open: fdc_core::types::Price::from_f64(100.0).unwrap(),
        high: fdc_core::types::Price::from_f64(110.0).unwrap(),
        low: fdc_core::types::Price::from_f64(90.0).unwrap(),
        close: fdc_core::types::Price::from_f64(105.0).unwrap(),
        volume: Decimal::new(12345, 2),
        trade_count: Some(42),
        quote_volume: Some(Decimal::new(129_622_500, 2)),
    };

    assert_eq!(payload.interval.as_deref(), Some("1m"));
    assert_eq!(payload.trade_count, Some(42));
    assert_eq!(payload.quote_volume.unwrap().to_string(), "1296225.00");
    assert_eq!(
        BarterMarketPayload::Candle(payload).kind(),
        BarterMarketDataKind::Candle
    );
}

#[test]
fn candle_event_maps_to_structured_payload() {
    let close_time = Utc.timestamp_nanos(1_700_000_060_000_000_000);
    let event = fdc_barter::map_market_event(candle_event(Candle {
        close_time,
        open: 100.0,
        high: 110.0,
        low: 90.0,
        close: 105.0,
        volume: 123.45,
        trade_count: 42,
    }))
    .expect("candle should map");

    assert_eq!(event.market_type, BarterMarketType::Spot);
    assert_eq!(event.kind, BarterMarketDataKind::Candle);
    match event.payload {
        BarterMarketPayload::Candle(candle) => {
            assert_eq!(candle.interval, None);
            assert_eq!(candle.open_time.as_nanos(), 1_700_000_000_000_000_000);
            assert_eq!(candle.close_time.as_nanos(), 1_700_000_060_000_000_000);
            assert_eq!(candle.open.to_f64(), 100.0);
            assert_eq!(candle.high.to_f64(), 110.0);
            assert_eq!(candle.low.to_f64(), 90.0);
            assert_eq!(candle.close.to_f64(), 105.0);
            assert_eq!(candle.volume.to_string(), "123.45");
            assert_eq!(candle.trade_count, Some(42));
            assert_eq!(candle.quote_volume, None);
        }
        other => panic!("expected candle payload, got {other:?}"),
    }
}
