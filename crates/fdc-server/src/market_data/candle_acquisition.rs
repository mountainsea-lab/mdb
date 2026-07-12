use std::sync::Mutex;

use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterIngestionEnvelope,
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType,
    BinanceSpotOhlcvHistoricalPageFetcher, CandlePayload, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{error::Error, types::{Price, TimestampNs}, Result};
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
    pub verify_tasks_started: usize,
    pub verify_candles_checked: usize,
    pub verify_mismatches: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandleAcquisitionRequestRole {
    Base,
    Verify,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandleAcquisitionRequest {
    role: CandleAcquisitionRequestRole,
    request: HistoricalBackfillRequest,
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
    Ok(expand_candle_acquisition_requests(config)?
        .into_iter()
        .map(|request| request.request)
        .collect())
}

fn expand_candle_acquisition_requests(
    config: &MarketDataCandleAcquisitionRuntimeConfig,
) -> Result<Vec<CandleAcquisitionRequest>> {
    let start = TimestampNs::from_nanos(config.start_ns.unwrap_or(0));
    let end = TimestampNs::from_nanos(config.end_ns.unwrap_or(i64::MAX));
    let base_source_id = format!("barter:{}:historical:candle:base", config.exchange);
    let verify_source_id = format!("barter:{}:historical:candle:verify", config.exchange);
    let mut requests = Vec::new();

    for symbol in &config.symbols {
        for interval in &config.base_intervals {
            requests.push(CandleAcquisitionRequest {
                role: CandleAcquisitionRequestRole::Base,
                request: HistoricalBackfillRequest {
                    source_id: base_source_id.clone(),
                    exchange: config.exchange.clone(),
                    market_type: BarterMarketType::Spot,
                    symbol: symbol.clone(),
                    kind: BarterMarketDataKind::Candle,
                    interval: Some(interval.clone()),
                    start,
                    end,
                    limit: Some(config.limit_per_page),
                    cursor: None,
                },
            });
        }
        for interval in &config.verify_intervals {
            requests.push(CandleAcquisitionRequest {
                role: CandleAcquisitionRequestRole::Verify,
                request: HistoricalBackfillRequest {
                    source_id: verify_source_id.clone(),
                    exchange: config.exchange.clone(),
                    market_type: BarterMarketType::Spot,
                    symbol: symbol.clone(),
                    kind: BarterMarketDataKind::Candle,
                    interval: Some(interval.clone()),
                    start,
                    end,
                    limit: Some(config.limit_per_page),
                    cursor: None,
                },
            });
        }
    }

    Ok(requests)
}

fn candle_payloads_from_envelopes(envelopes: &[BarterIngestionEnvelope]) -> Vec<CandlePayload> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.event.payload {
            BarterMarketPayload::Candle(candle) => Some(candle.clone()),
            _ => None,
        })
        .collect()
}

pub fn aggregate_candle_payloads(
    candles: &[CandlePayload],
    interval: &str,
) -> Result<Option<CandlePayload>> {
    let Some(first) = candles.first() else {
        return Ok(None);
    };
    let last = candles.last().expect("first candle exists");
    let high = candles
        .iter()
        .map(|candle| candle.high)
        .max()
        .unwrap_or(first.high);
    let low = candles
        .iter()
        .map(|candle| candle.low)
        .min()
        .unwrap_or(first.low);
    let volume = candles
        .iter()
        .map(|candle| candle.volume)
        .sum();

    Ok(Some(CandlePayload {
        interval: Some(interval.to_string()),
        open_time: first.open_time,
        close_time: last.close_time,
        open: Price::new(first.open.as_decimal()),
        high,
        low,
        close: Price::new(last.close.as_decimal()),
        volume,
        trade_count: None,
        quote_volume: None,
    }))
}

fn count_candle_mismatches(expected: &[CandlePayload], official: &[CandlePayload]) -> usize {
    let common = expected.len().min(official.len());
    let mut mismatches = expected.len().max(official.len()) - common;
    for (left, right) in expected.iter().zip(official.iter()) {
        if !candle_payload_matches(left, right) {
            mismatches += 1;
        }
    }

    mismatches
}

fn expected_candles_for_verify(
    base_candles: &[CandlePayload],
    official: &[CandlePayload],
) -> Result<Vec<CandlePayload>> {
    let Some(official_interval) = official.first().and_then(|candle| candle.interval.as_deref()) else {
        return Ok(base_candles.to_vec());
    };
    let base_matches_official_interval = base_candles
        .first()
        .and_then(|candle| candle.interval.as_deref())
        == Some(official_interval);
    if base_matches_official_interval {
        return Ok(base_candles.to_vec());
    }

    Ok(aggregate_candle_payloads(base_candles, official_interval)?.into_iter().collect())
}

fn candle_payload_matches(left: &CandlePayload, right: &CandlePayload) -> bool {
    left.open_time == right.open_time
        && left.close_time == right.close_time
        && left.open == right.open
        && left.high == right.high
        && left.low == right.low
        && left.close == right.close
        && left.volume == right.volume
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
    let requests = expand_candle_acquisition_requests(config)?;
    let mut status = CandleAcquisitionRunStatus::default();
    let mut expected_verify_candles = Vec::new();

    for acquisition_request in requests {
        status.tasks_started += 1;
        if acquisition_request.role == CandleAcquisitionRequestRole::Verify {
            status.verify_tasks_started += 1;
        }
        let outcome = run_historical_backfill_pages(
            source,
            HistoricalBackfillRunRequest {
                first_request: acquisition_request.request,
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
            match acquisition_request.role {
                CandleAcquisitionRequestRole::Base => {
                    expected_verify_candles.extend(candle_payloads_from_envelopes(&page.envelopes));
                    let pipeline =
                        run_barter_envelopes_to_storage_once(page.envelopes, storage_sink).await?;
                    status.storage_records_written += pipeline.storage_records_written;
                }
                CandleAcquisitionRequestRole::Verify => {
                    let official = candle_payloads_from_envelopes(&page.envelopes);
                    let expected = expected_candles_for_verify(&expected_verify_candles, &official)?;
                    status.verify_candles_checked += official.len();
                    status.verify_mismatches +=
                        count_candle_mismatches(&expected, &official);
                }
            }
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
    let mut requests = expand_candle_acquisition_requests(config)?;
    let mut status = CandleAcquisitionRunStatus::default();
    let mut expected_verify_candles = Vec::new();

    for acquisition_request in &mut requests {
        if acquisition_request.role == CandleAcquisitionRequestRole::Base {
            apply_checkpoint_to_request(&mut acquisition_request.request, checkpoint_store)?;
        }
    }

    for acquisition_request in requests {
        status.tasks_started += 1;
        if acquisition_request.role == CandleAcquisitionRequestRole::Verify {
            status.verify_tasks_started += 1;
        }
        let key = if acquisition_request.role == CandleAcquisitionRequestRole::Base {
            CandleCheckpointKey::from_request(&acquisition_request.request)
        } else {
            None
        };
        let outcome = run_historical_backfill_pages(
            source,
            HistoricalBackfillRunRequest {
                first_request: acquisition_request.request,
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
            match acquisition_request.role {
                CandleAcquisitionRequestRole::Base => {
                    expected_verify_candles.extend(candle_payloads_from_envelopes(&page.envelopes));
                    let pipeline =
                        run_barter_envelopes_to_storage_once(page.envelopes, storage_sink).await?;
                    status.storage_records_written += pipeline.storage_records_written;
                }
                CandleAcquisitionRequestRole::Verify => {
                    let official = candle_payloads_from_envelopes(&page.envelopes);
                    let expected = expected_candles_for_verify(&expected_verify_candles, &official)?;
                    status.verify_candles_checked += official.len();
                    status.verify_mismatches +=
                        count_candle_mismatches(&expected, &official);
                }
            }
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
