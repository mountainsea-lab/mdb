use fdc_barter::{
    binance_spot_ohlcv_provider_from_response, BarterMarketDataKind, BarterMarketPayload,
    BarterMarketType, HistoricalBackfillRequest, HistoricalExchangeProvider,
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
        end: TimestampNs::from_nanos(1_700_000_120_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

fn sample_binance_klines() -> &'static str {
    r#"[
        [1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"],
        [1700000060000,"105.40","120.00","100.00","119.99","10.00000000",1700000119999,"1100.00000000",7,"5.00000000","550.00000000","0"]
    ]"#
}

#[tokio::test]
async fn binance_spot_ohlcv_provider_maps_klines_into_backfill_candle_envelopes() {
    let provider = binance_spot_ohlcv_provider_from_response(sample_binance_klines())
        .expect("sample klines should parse");

    let page = provider
        .fetch_page(ohlcv_request())
        .await
        .expect("provider should return parsed klines");

    assert_eq!(page.envelopes.len(), 2);
    assert!(!page.complete);
    assert_eq!(
        page.next_cursor
            .as_ref()
            .unwrap()
            .next_start
            .unwrap()
            .as_nanos(),
        1_700_000_120_000_000_000
    );

    let first = &page.envelopes[0];
    assert!(first.quality.is_backfill);
    assert_eq!(first.source_id, "barter-binance-spot-history");
    assert_eq!(first.event.exchange, "binance_spot");
    assert_eq!(first.event.symbol.as_str(), "BTCUSDT");
    assert_eq!(first.event.timestamp.as_nanos(), 1_700_000_000_000_000_000);

    let BarterMarketPayload::Candle(candle) = &first.event.payload else {
        panic!("expected candle payload");
    };
    assert_eq!(candle.interval.as_deref(), Some("1m"));
    assert_eq!(candle.open.to_f64(), 100.10);
    assert_eq!(candle.high.to_f64(), 110.20);
    assert_eq!(candle.low.to_f64(), 90.30);
    assert_eq!(candle.close.to_f64(), 105.40);
    assert_eq!(candle.volume.to_string(), "123.45000000");
    assert_eq!(candle.quote_volume.unwrap().to_string(), "12999.99000000");
    assert_eq!(candle.trade_count, Some(42));
}

#[tokio::test]
async fn binance_spot_ohlcv_provider_marks_complete_when_response_has_fewer_rows_than_limit() {
    let provider = binance_spot_ohlcv_provider_from_response(sample_binance_klines())
        .expect("sample klines should parse");

    let mut request = ohlcv_request();
    request.limit = Some(3);
    let page = provider
        .fetch_page(request)
        .await
        .expect("provider should return parsed klines");

    assert!(page.complete);
    assert!(page.next_cursor.is_none());
}

#[test]
fn binance_spot_ohlcv_provider_rejects_invalid_numeric_payloads() {
    let invalid = r#"[[1700000000000,"not-a-number","110.20","90.30","105.40","123.45",1700000059999,"12999.99",42,"60","6300","0"]]"#;

    let error = binance_spot_ohlcv_provider_from_response(invalid)
        .expect_err("invalid numeric payload should be rejected while parsing fixture");

    assert!(error.to_string().contains("invalid numeric value"));
}
