use fdc_barter::{
    supported_crypto_market_data_capabilities, BarterMarketDataKind, BarterMarketType,
};

#[test]
fn capability_matrix_contains_first_slice_realtime_targets() {
    let capabilities = supported_crypto_market_data_capabilities();

    let binance_spot = capabilities
        .iter()
        .find(|capability| {
            capability.exchange == "binance_spot" && capability.market_type == BarterMarketType::Spot
        })
        .expect("binance spot capability should exist");
    assert!(binance_spot.supports_live);
    assert!(!binance_spot.supports_historical);
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
}

#[test]
fn capability_matrix_marks_historical_as_future_work_for_now() {
    let capabilities = supported_crypto_market_data_capabilities();

    assert!(capabilities
        .iter()
        .all(|capability| !capability.supports_historical));
    assert!(capabilities
        .iter()
        .all(|capability| capability.historical_kinds.is_empty()));
}

#[test]
fn capability_matrix_includes_known_barter_trade_only_exchanges() {
    let capabilities = supported_crypto_market_data_capabilities();

    for exchange in [
        "coinbase",
        "bitfinex",
        "bitmex",
        "gateio_spot",
        "okx",
    ] {
        let capability = capabilities
            .iter()
            .find(|capability| capability.exchange == exchange)
            .unwrap_or_else(|| panic!("{exchange} capability should exist"));
        assert!(capability.kinds.contains(&BarterMarketDataKind::Trade));
    }
}
