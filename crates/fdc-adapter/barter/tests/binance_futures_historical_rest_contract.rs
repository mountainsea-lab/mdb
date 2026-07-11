use fdc_barter::{
    binance_futures_usd_funding_rate_rest_request_descriptor,
    binance_futures_usd_mark_price_rest_request_descriptor,
    binance_futures_usd_ohlcv_rest_request_descriptor,
    binance_futures_usd_open_interest_rest_request_descriptor, BarterMarketDataKind,
    BarterMarketType, HistoricalBackfillRequest,
};
use fdc_core::types::TimestampNs;

fn futures_request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-futures-usd-history".to_string(),
        exchange: "binance_futures_usd".to_string(),
        market_type: BarterMarketType::Perpetual,
        symbol: "BTCUSDT".to_string(),
        kind,
        interval: (kind == BarterMarketDataKind::Candle).then(|| "1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

#[test]
fn funding_rate_descriptor_matches_binance_futures_shape() {
    let descriptor = binance_futures_usd_funding_rate_rest_request_descriptor(&futures_request(
        BarterMarketDataKind::FundingRate,
    ))
    .expect("valid funding request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_futures_usd");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/fapi/v1/fundingRate");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("startTime".to_string(), "1700000000000".to_string()),
            ("endTime".to_string(), "1700000060000".to_string()),
            ("limit".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn open_interest_descriptor_matches_binance_futures_shape() {
    let descriptor = binance_futures_usd_open_interest_rest_request_descriptor(&futures_request(
        BarterMarketDataKind::OpenInterest,
    ))
    .expect("valid open interest request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_futures_usd");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/fapi/v1/openInterest");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![("symbol".to_string(), "BTCUSDT".to_string())]
    );
}

#[test]
fn mark_price_descriptor_matches_binance_futures_shape() {
    let descriptor = binance_futures_usd_mark_price_rest_request_descriptor(&futures_request(
        BarterMarketDataKind::MarkPrice,
    ))
    .expect("valid mark price request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_futures_usd");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/fapi/v1/premiumIndex");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![("symbol".to_string(), "BTCUSDT".to_string())]
    );
}

#[test]
fn futures_ohlcv_descriptor_matches_binance_futures_shape() {
    let descriptor = binance_futures_usd_ohlcv_rest_request_descriptor(&futures_request(
        BarterMarketDataKind::Candle,
    ))
    .expect("valid futures kline request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_futures_usd");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/fapi/v1/klines");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("interval".to_string(), "1m".to_string()),
            ("startTime".to_string(), "1700000000000".to_string()),
            ("endTime".to_string(), "1700000060000".to_string()),
            ("limit".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn descriptors_reject_wrong_market_kind_and_limit() {
    let mut spot = futures_request(BarterMarketDataKind::FundingRate);
    spot.market_type = BarterMarketType::Spot;
    assert!(binance_futures_usd_funding_rate_rest_request_descriptor(&spot).is_err());

    let wrong_kind = futures_request(BarterMarketDataKind::Trade);
    assert!(binance_futures_usd_funding_rate_rest_request_descriptor(&wrong_kind).is_err());

    let mut high_limit = futures_request(BarterMarketDataKind::FundingRate);
    high_limit.limit = Some(1001);
    let error = binance_futures_usd_funding_rate_rest_request_descriptor(&high_limit)
        .expect_err("Binance Futures funding max limit should be enforced");
    assert!(error.to_string().contains("limit"));
}
