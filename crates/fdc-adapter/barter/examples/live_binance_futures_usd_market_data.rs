use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_futures_usd_market_data,
    BarterMarketDataKind, LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if std::env::var("FDC_BARTER_LIVE_EXAMPLE").as_deref() != Ok("1") {
        println!(
            "set FDC_BARTER_LIVE_EXAMPLE=1 to run the live Binance Futures USD market-data example"
        );
        return Ok(());
    }

    let streams = init_binance_futures_usd_market_data([
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Trade,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBookL1,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBook,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Liquidation,
        ),
    ])
    .await?;

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-futures-usd-market-data".to_string(),
            limit: 5,
        },
        streams.select_all(),
    )
    .await?;

    println!(
        "records_received={} complete={} skipped_reconnects={}",
        outcome.records_received, outcome.complete, outcome.skipped_reconnects
    );
    for envelope in outcome.envelopes {
        println!(
            "kind={:?} exchange={} symbol={} sequence={:?} ts={}",
            envelope.event.kind,
            envelope.event.exchange,
            envelope.event.symbol.as_str(),
            envelope.event.sequence,
            envelope.event.timestamp.as_nanos()
        );
    }

    Ok(())
}
