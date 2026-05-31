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

#[test]
fn structured_order_book_payload_reports_order_book_kind() {
    let payload = fdc_barter::BarterMarketPayload::OrderBook(fdc_barter::OrderBookPayload {
        update_kind: fdc_barter::OrderBookUpdateKind::Snapshot,
        bids: vec![fdc_barter::OrderBookLevelPayload {
            price: fdc_core::types::Price::from_f64(100.0).unwrap(),
            quantity: rust_decimal::Decimal::new(15, 1),
        }],
        asks: vec![fdc_barter::OrderBookLevelPayload {
            price: fdc_core::types::Price::from_f64(101.0).unwrap(),
            quantity: rust_decimal::Decimal::new(20, 1),
        }],
        sequence: Some("42".to_string()),
    });

    assert_eq!(payload.kind(), BarterMarketDataKind::OrderBook);
}

#[test]
fn structured_liquidation_payload_reports_liquidation_kind() {
    let payload = fdc_barter::BarterMarketPayload::Liquidation(fdc_barter::LiquidationPayload {
        side: fdc_barter::TradeSide::Sell,
        price: fdc_core::types::Price::from_f64(64000.0).unwrap(),
        quantity: rust_decimal::Decimal::new(25, 1),
        liquidation_time: fdc_core::types::TimestampNs::from_nanos(1_700_000_000_000_000_000),
    });

    assert_eq!(payload.kind(), BarterMarketDataKind::Liquidation);
}
