use fdc_barter::{
    supported_crypto_market_data_capabilities, BarterMarketDataKind, BarterMarketType,
};

#[test]
fn capability_matrix_contains_first_slice_realtime_targets() {
    let capabilities = supported_crypto_market_data_capabilities();

    let binance_spot = capabilities
        .iter()
        .find(|capability| {
            capability.exchange == "binance_spot"
                && capability.market_type == BarterMarketType::Spot
        })
        .expect("binance spot capability should exist");
    assert!(binance_spot.supports_live);
    assert!(binance_spot.supports_historical);
    assert_eq!(
        binance_spot.historical_kinds,
        vec![BarterMarketDataKind::Candle, BarterMarketDataKind::Trade]
    );
    assert_eq!(
        binance_spot.kinds,
        vec![
            BarterMarketDataKind::Trade,
            BarterMarketDataKind::OrderBookL1,
            BarterMarketDataKind::OrderBook,
        ]
    );

    let binance_futures = capabilities
        .iter()
        .find(|capability| {
            capability.exchange == "binance_futures_usd"
                && capability.market_type == BarterMarketType::Perpetual
        })
        .expect("binance futures usd capability should exist");
    assert_eq!(
        binance_futures.kinds,
        vec![
            BarterMarketDataKind::Trade,
            BarterMarketDataKind::OrderBookL1,
            BarterMarketDataKind::OrderBook,
            BarterMarketDataKind::Liquidation,
        ]
    );
    assert!(binance_futures.supports_historical);
    assert_eq!(
        binance_futures.historical_kinds,
        vec![BarterMarketDataKind::Candle]
    );
}

#[test]
fn capability_matrix_marks_binance_spot_and_futures_candles_as_historical_supported() {
    let capabilities = supported_crypto_market_data_capabilities();

    let binance_spot = capabilities
        .iter()
        .find(|capability| capability.exchange == "binance_spot")
        .expect("binance spot capability should exist");
    assert!(binance_spot.supports_historical);
    assert_eq!(
        binance_spot.historical_kinds,
        vec![BarterMarketDataKind::Candle, BarterMarketDataKind::Trade]
    );

    let binance_futures = capabilities
        .iter()
        .find(|capability| capability.exchange == "binance_futures_usd")
        .expect("binance futures usd capability should exist");
    assert!(binance_futures.supports_historical);
    assert_eq!(
        binance_futures.historical_kinds,
        vec![BarterMarketDataKind::Candle]
    );

    for capability in capabilities.iter().filter(|capability| {
        capability.exchange != "binance_spot" && capability.exchange != "binance_futures_usd"
    }) {
        assert!(
            !capability.supports_historical,
            "{} should not advertise historical support yet",
            capability.exchange
        );
        assert!(capability.historical_kinds.is_empty());
    }
}

#[test]
fn capability_matrix_includes_known_barter_trade_only_exchanges() {
    let capabilities = supported_crypto_market_data_capabilities();

    for exchange in ["coinbase", "bitfinex", "bitmex", "gateio_spot", "okx"] {
        let capability = capabilities
            .iter()
            .find(|capability| capability.exchange == exchange)
            .unwrap_or_else(|| panic!("{exchange} capability should exist"));
        assert!(capability.kinds.contains(&BarterMarketDataKind::Trade));
    }
}
