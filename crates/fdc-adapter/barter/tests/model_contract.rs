use fdc_barter::{
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest, BarterMarketType,
    BarterSourceCapabilities, BarterSourceState, BarterSourceStatus, RateLimitRule,
};

#[test]
fn request_and_source_state_types_are_public() {
    let request = BarterMarketDataRequest::live(
        "binance_spot",
        vec!["BTCUSDT"],
        vec![BarterMarketDataKind::Trade],
    );

    assert_eq!(request.exchange, "binance_spot");
    assert_eq!(request.symbols, vec!["BTCUSDT".to_string()]);
    assert_eq!(request.mode, BarterMarketDataMode::Live);

    let state = BarterSourceState::new("barter-binance-live", BarterMarketDataMode::Live);
    assert_eq!(state.source_id, "barter-binance-live");
    assert_eq!(state.status, BarterSourceStatus::Created);
}

#[test]
fn capabilities_describe_live_and_historical_support() {
    let capabilities = BarterSourceCapabilities::crypto_exchange(
        "binance_spot",
        BarterMarketType::Spot,
        vec![BarterMarketDataKind::Trade, BarterMarketDataKind::Candle],
        vec![BarterMarketDataKind::Trade],
        vec![RateLimitRule::new(
            "binance_spot",
            "klines",
            1200,
            60_000,
            1,
        )],
    );

    assert!(capabilities.supports_live);
    assert!(capabilities.supports_historical);
    assert_eq!(capabilities.market_type, BarterMarketType::Spot);
    assert!(capabilities.kinds.contains(&BarterMarketDataKind::Trade));
}
