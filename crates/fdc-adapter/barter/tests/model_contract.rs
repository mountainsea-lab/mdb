use fdc_barter::{
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest, BarterSourceState,
    BarterSourceStatus,
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
