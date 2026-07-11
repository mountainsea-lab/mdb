use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketPayload, BarterMarketType, BinanceSpotOhlcvHistoricalPageFetcher,
    HistoricalBackfillRequest, HistoricalBackfillRunRequest,
};
use fdc_core::types::TimestampNs;

// Manual run commands for troubleshooting:
//
// Default real Binance Spot REST run, prints visible records:
//   cargo run --example historical_binance_spot_ohlcv
#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    println!("example=historical_binance_spot_ohlcv mode=real_network symbol=BTCUSDT");

    let end_ms = chrono::Utc::now().timestamp_millis() - 60_000;
    let start_ms = end_ms - 10 * 60_000;
    let executor = BarterIntegrationHistoricalRestExecutor::binance_spot();
    let fetcher = BinanceSpotOhlcvHistoricalPageFetcher::new(&executor);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: HistoricalBackfillRequest {
                source_id: "example-binance-spot-ohlcv".to_string(),
                exchange: "binance_spot".to_string(),
                market_type: BarterMarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                kind: BarterMarketDataKind::Candle,
                interval: Some("1m".to_string()),
                start: TimestampNs::from_nanos(start_ms * 1_000_000),
                end: TimestampNs::from_nanos(end_ms * 1_000_000),
                limit: Some(3),
                cursor: None,
            },
            max_pages: 2,
            max_records: Some(6),
        },
    )
    .await?;

    println!(
        "endpoint=/api/v3/klines exchange=binance_spot market_type=Spot symbol=BTCUSDT kind=Candle pages={} records={} complete={} stopped_reason={:?}",
        outcome.pages.len(),
        outcome.records_received,
        outcome.complete,
        outcome.stopped_reason
    );
    for envelope in outcome
        .pages
        .into_iter()
        .flat_map(|page| page.envelopes)
        .take(5)
    {
        if let BarterMarketPayload::Candle(candle) = envelope.event.payload {
            println!(
                "record exchange={} symbol={} kind=Candle event_time={} open={} close={} volume={}",
                envelope.event.exchange,
                envelope.event.symbol.as_str(),
                envelope.event.timestamp.as_nanos(),
                candle.open.to_f64(),
                candle.close.to_f64(),
                candle.volume
            );
        }
    }

    Ok(())
}
