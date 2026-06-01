use fdc_barter::{
    binance_spot_ohlcv_rest_request_descriptor, BarterMarketDataKind, BarterMarketType,
    HistoricalBackfillRequest,
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

#[test]
fn binance_spot_ohlcv_descriptor_matches_public_klines_request_shape() {
    let descriptor = binance_spot_ohlcv_rest_request_descriptor(&ohlcv_request())
        .expect("valid ohlcv request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_spot");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/api/v3/klines");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("interval".to_string(), "1m".to_string()),
            ("startTime".to_string(), "1700000000000".to_string()),
            ("endTime".to_string(), "1700000060000".to_string()),
            ("limit".to_string(), "500".to_string()),
        ]
    );
}

#[test]
fn binance_spot_ohlcv_descriptor_rejects_unsupported_kind_market_and_limit() {
    let mut trade = ohlcv_request();
    trade.kind = BarterMarketDataKind::Trade;
    assert!(binance_spot_ohlcv_rest_request_descriptor(&trade).is_err());

    let mut futures = ohlcv_request();
    futures.market_type = BarterMarketType::Perpetual;
    assert!(binance_spot_ohlcv_rest_request_descriptor(&futures).is_err());

    let mut high_limit = ohlcv_request();
    high_limit.limit = Some(1001);
    let error = binance_spot_ohlcv_rest_request_descriptor(&high_limit)
        .expect_err("Binance spot klines max limit is 1000");
    assert!(error.to_string().contains("limit"));
}
