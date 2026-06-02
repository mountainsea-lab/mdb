use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_spot_market_data, BarterMarketDataKind,
    LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if std::env::var("FDC_BARTER_LIVE_EXAMPLE").as_deref() != Ok("1") {
        println!("set FDC_BARTER_LIVE_EXAMPLE=1 to run the live Binance Spot order books example");
        return Ok(());
    }

    println!("initializing Binance Spot order book streams...");
    let streams = init_binance_spot_market_data([
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceSpot,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            BarterMarketDataKind::OrderBookL1,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceSpot,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            BarterMarketDataKind::OrderBook,
        ),
    ])
    .await?;
    println!("streams initialized; collecting up to 5 records for 30s...");

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-spot-order-books".to_string(),
            limit: 5,
            timeout: Some(std::time::Duration::from_secs(30)),
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
