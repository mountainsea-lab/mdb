use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketType, BinanceSpotOhlcvHistoricalPageFetcher, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{error::Error, types::TimestampNs, Result};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::StorageWriteSink;

use crate::runtime::config::MarketDataCandleAcquisitionRuntimeConfig;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CandleAcquisitionRunStatus {
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub final_cursors: Vec<HistoricalCursor>,
}

pub fn expand_candle_backfill_requests(
    config: &MarketDataCandleAcquisitionRuntimeConfig,
) -> Result<Vec<HistoricalBackfillRequest>> {
    let start = TimestampNs::from_nanos(config.start_ns.unwrap_or(0));
    let end = TimestampNs::from_nanos(config.end_ns.unwrap_or(i64::MAX));
    let source_id = format!("barter:{}:historical:candle", config.exchange);
    let mut requests = Vec::new();

    for symbol in &config.symbols {
        for interval in &config.base_intervals {
            requests.push(HistoricalBackfillRequest {
                source_id: source_id.clone(),
                exchange: config.exchange.clone(),
                market_type: BarterMarketType::Spot,
                symbol: symbol.clone(),
                kind: BarterMarketDataKind::Candle,
                interval: Some(interval.clone()),
                start,
                end,
                limit: Some(config.limit_per_page),
                cursor: None,
            });
        }
    }

    Ok(requests)
}

pub async fn run_candle_acquisition_once<S, W>(
    config: &MarketDataCandleAcquisitionRuntimeConfig,
    source: &S,
    storage_sink: &W,
) -> Result<CandleAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
    W: StorageWriteSink,
{
    let requests = expand_candle_backfill_requests(config)?;
    let mut status = CandleAcquisitionRunStatus::default();

    for first_request in requests {
        status.tasks_started += 1;
        let outcome = run_historical_backfill_pages(
            source,
            HistoricalBackfillRunRequest {
                first_request,
                max_pages: config.max_pages_per_run,
                max_records: None,
            },
        )
        .await
        .map_err(|error| Error::internal(error.to_string()))?;

        status.pages_fetched += outcome.pages.len();
        status.envelopes_received += outcome.records_received;
        if let Some(cursor) = outcome.final_cursor.clone() {
            status.final_cursors.push(cursor);
        }

        for page in outcome.pages {
            let pipeline = run_barter_envelopes_to_storage_once(page.envelopes, storage_sink).await?;
            status.storage_records_written += pipeline.storage_records_written;
        }

        if outcome.complete {
            status.tasks_completed += 1;
        }
    }

    Ok(status)
}

pub async fn run_binance_spot_candle_acquisition_once<W>(
    config: &MarketDataCandleAcquisitionRuntimeConfig,
    storage_sink: &W,
) -> Result<CandleAcquisitionRunStatus>
where
    W: StorageWriteSink,
{
    let executor = BarterIntegrationHistoricalRestExecutor::binance_spot();
    let fetcher = BinanceSpotOhlcvHistoricalPageFetcher::new(&executor);
    run_candle_acquisition_once(config, &fetcher, storage_sink).await
}
