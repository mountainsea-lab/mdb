use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketPayload, BarterMarketType, BinanceSpotOhlcvHistoricalPageFetcher,
    HistoricalBackfillRequest, HistoricalBackfillRunRequest,
};
use fdc_core::types::TimestampNs;

// Manual run commands for troubleshooting:
//
// Safe dry run, no network access:
//   cargo run --example historical_binance_spot_ohlcv
//
// Historical REST network run:
//   FDC_BARTER_HISTORICAL_EXAMPLE=1 cargo run --example historical_binance_spot_ohlcv
//
// IDE main run:
//   Add environment variable FDC_BARTER_HISTORICAL_EXAMPLE=1 to the run configuration.
#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if std::env::var("FDC_BARTER_HISTORICAL_EXAMPLE").as_deref() != Ok("1") {
        println!(
            "set FDC_BARTER_HISTORICAL_EXAMPLE=1 to run the historical Binance Spot OHLCV example"
        );
        return Ok(());
    }

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
        "pages={} records={} complete={} stopped_reason={:?}",
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
                "{} {} open={} close={} volume={}",
                envelope.event.exchange,
                envelope.event.symbol.as_str(),
                candle.open.to_f64(),
                candle.close.to_f64(),
                candle.volume
            );
        }
    }

    Ok(())
}
