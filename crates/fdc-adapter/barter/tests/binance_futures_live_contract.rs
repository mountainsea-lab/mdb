use fdc_barter::{
    default_binance_futures_usd_market_data_subscriptions, BarterMarketDataKind, LiveExchange,
};
use futures::StreamExt;

#[test]
fn default_binance_futures_usd_subscriptions_include_derivatives_targets() {
    let subscriptions = default_binance_futures_usd_market_data_subscriptions();

    assert!(subscriptions.iter().any(|sub| {
        sub.exchange == LiveExchange::BinanceFuturesUsd
            && sub.base == "btc"
            && sub.quote == "usdt"
            && sub.kind == BarterMarketDataKind::Trade
    }));
    assert!(subscriptions.iter().any(|sub| {
        sub.exchange == LiveExchange::BinanceFuturesUsd
            && sub.base == "eth"
            && sub.quote == "usdt"
            && sub.kind == BarterMarketDataKind::Trade
    }));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::OrderBookL1));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::OrderBook));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::Liquidation));
}

#[tokio::test]
#[ignore]
async fn ignored_live_smoke_can_initialize_binance_futures_usd_market_data() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = fdc_barter::init_binance_futures_usd_market_data(
        fdc_barter::default_binance_futures_usd_market_data_subscriptions(),
    )
    .await
    .expect("Binance Futures USD stream should initialize");

    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        fdc_barter::collect_live_market_data_envelopes(
            "barter-binance-futures-live",
            streams.select_all(),
            1,
        ),
    )
    .await
    .expect("should receive one live futures event within timeout")
    .expect("live collection should succeed");

    assert_eq!(envelopes.len(), 1);
}
