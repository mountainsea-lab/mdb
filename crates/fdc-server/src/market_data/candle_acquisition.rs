use std::sync::Mutex;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CandleCheckpointKey {
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
}

impl CandleCheckpointKey {
    fn from_request(request: &HistoricalBackfillRequest) -> Option<Self> {
        Some(Self {
            exchange: request.exchange.clone(),
            symbol: request.symbol.clone(),
            interval: request.interval.clone()?,
        })
    }
}

pub trait CandleCheckpointStore: Send + Sync {
    fn load(&self, key: &CandleCheckpointKey) -> Result<Option<HistoricalCursor>>;
    fn save(&self, key: CandleCheckpointKey, cursor: HistoricalCursor) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct InMemoryCandleCheckpointStore {
    cursors: Mutex<Vec<(CandleCheckpointKey, HistoricalCursor)>>,
}

impl CandleCheckpointStore for InMemoryCandleCheckpointStore {
    fn load(&self, key: &CandleCheckpointKey) -> Result<Option<HistoricalCursor>> {
        Ok(self
            .cursors
            .lock()
            .expect("candle checkpoint lock")
            .iter()
            .rev()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, cursor)| cursor.clone()))
    }

    fn save(&self, key: CandleCheckpointKey, cursor: HistoricalCursor) -> Result<()> {
        let mut cursors = self.cursors.lock().expect("candle checkpoint lock");
        if let Some((_, existing)) = cursors.iter_mut().find(|(candidate, _)| candidate == &key) {
            *existing = cursor;
        } else {
            cursors.push((key, cursor));
        }
        Ok(())
    }
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

fn apply_checkpoint_to_request(
    request: &mut HistoricalBackfillRequest,
    store: &dyn CandleCheckpointStore,
) -> Result<()> {
    let Some(key) = CandleCheckpointKey::from_request(request) else {
        return Ok(());
    };
    let Some(cursor) = store.load(&key)? else {
        return Ok(());
    };
    if let Some(next_start) = cursor.next_start {
        request.start = next_start;
    }
    request.cursor = Some(cursor);
    Ok(())
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

pub async fn run_candle_acquisition_once_with_checkpoints<S, W, C>(
    config: &MarketDataCandleAcquisitionRuntimeConfig,
    source: &S,
    storage_sink: &W,
    checkpoint_store: &C,
) -> Result<CandleAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
    W: StorageWriteSink,
    C: CandleCheckpointStore,
{
    let mut requests = expand_candle_backfill_requests(config)?;
    let mut status = CandleAcquisitionRunStatus::default();

    for first_request in &mut requests {
        apply_checkpoint_to_request(first_request, checkpoint_store)?;
    }

    for first_request in requests {
        status.tasks_started += 1;
        let key = CandleCheckpointKey::from_request(&first_request);
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
            if let Some(key) = key {
                checkpoint_store.save(key, cursor.clone())?;
            }
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
