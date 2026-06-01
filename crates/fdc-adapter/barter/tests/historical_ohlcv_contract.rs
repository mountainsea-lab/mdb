use async_trait::async_trait;
use barter_data::{
    event::{DataKind, MarketEvent},
    subscription::candle::Candle,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    validate_historical_backfill_request, BarterIngestionEnvelope, BarterMarketDataKind,
    BarterMarketType, HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalBackfillSource,
    HistoricalCursor,
};
use fdc_core::types::TimestampNs;

fn ohlcv_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(500),
        cursor: None,
    }
}

fn candle_envelope(source_id: &str) -> BarterIngestionEnvelope {
    let event = fdc_barter::map_market_event(MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_060_000_000_000),
        exchange: ExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new("btc", "usdt", MarketDataInstrumentKind::Spot),
        kind: DataKind::Candle(Candle {
            close_time: Utc.timestamp_nanos(1_700_000_060_000_000_000),
            open: 100.0,
            high: 110.0,
            low: 90.0,
            close: 105.0,
            volume: 123.45,
            trade_count: 42,
        }),
    })
    .expect("candle event should map");

    BarterIngestionEnvelope::from_backfill_event(source_id, event)
}

struct FakeOhlcvSource;

#[async_trait]
impl HistoricalBackfillSource for FakeOhlcvSource {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        let next_cursor = HistoricalCursor::next_start(
            request.exchange.clone(),
            request.symbol.clone(),
            request.kind,
            request.end,
        );
        Ok(HistoricalBackfillPage {
            envelopes: vec![candle_envelope(&request.source_id)],
            request,
            next_cursor: Some(next_cursor),
            complete: false,
        })
    }
}

#[test]
fn historical_ohlcv_request_requires_interval_and_valid_time_range() {
    let request = ohlcv_request();
    validate_historical_backfill_request(&request).expect("valid ohlcv request");

    let mut missing_interval = request.clone();
    missing_interval.interval = None;
    assert!(validate_historical_backfill_request(&missing_interval).is_err());

    let mut invalid_range = request;
    invalid_range.end = invalid_range.start;
    assert!(validate_historical_backfill_request(&invalid_range).is_err());
}

#[tokio::test]
async fn offline_historical_ohlcv_page_marks_backfill_quality_and_cursor() {
    let request = ohlcv_request();
    let page = FakeOhlcvSource
        .fetch_page(request.clone())
        .await
        .expect("fake source should return one page");

    assert_eq!(page.request, request);
    assert_eq!(page.envelopes.len(), 1);
    assert!(page.envelopes[0].quality.is_backfill);
    assert!(!page.envelopes[0].quality.is_replay);
    assert_eq!(page.next_cursor.as_ref().unwrap().symbol, "BTCUSDT");
    assert!(!page.complete);
}
