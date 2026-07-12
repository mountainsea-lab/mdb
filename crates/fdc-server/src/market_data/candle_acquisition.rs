use std::sync::Mutex;

use chrono::Utc;
use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterIngestionEnvelope,
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType,
    BinanceSpotOhlcvHistoricalPageFetcher, CandlePayload, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{error::Error, types::{Price, TimestampNs}, Result};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{
    QueryableStorage, StorageQuery, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
    StorageWriteSink,
};
use serde::{Deserialize, Serialize};

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

const CANDLE_CHECKPOINT_COLLECTION: &str = "candle_checkpoints";
const CANDLE_VERIFY_AUDIT_COLLECTION: &str = "candle_verify_audits";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CandleCheckpointStorageValue {
    cursor: HistoricalCursor,
    updated_at_ns: i64,
}

pub struct StorageBackedCandleCheckpointStore<'a, S> {
    storage: &'a S,
}

impl<'a, S> StorageBackedCandleCheckpointStore<'a, S> {
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }
}

impl<S> CandleCheckpointStore for StorageBackedCandleCheckpointStore<'_, S>
where
    S: StorageWriteSink + QueryableStorage,
{
    fn load(&self, key: &CandleCheckpointKey) -> Result<Option<HistoricalCursor>> {
        let records = futures::executor::block_on(self.storage.query_storage(
            &StorageQuery::new("market_data")
                .with_collection(CANDLE_CHECKPOINT_COLLECTION)
                .with_key_prefix(candle_checkpoint_storage_key(key)),
        ))?;
        let Some(record) = records.last() else {
            return Ok(None);
        };
        let value: CandleCheckpointStorageValue = serde_json::from_slice(&record.value)
            .map_err(|error| Error::internal(error.to_string()))?;
        Ok(Some(value.cursor))
    }

    fn save(&self, key: CandleCheckpointKey, cursor: HistoricalCursor) -> Result<()> {
        let storage_key = candle_checkpoint_storage_key(&key);
        let value = CandleCheckpointStorageValue {
            cursor,
            updated_at_ns: TimestampNs::now().as_nanos(),
        };
        let mut metadata = StorageWriteMetadata::default();
        metadata.content_type = Some("application/json".to_string());
        metadata.schema = Some("candle_checkpoint".to_string());
        metadata.schema_version = Some("1".to_string());
        metadata.source = Some("fdc-server:candle_acquisition".to_string());
        metadata.tags.insert("kind".to_string(), "candle_checkpoint".to_string());
        metadata.tags.insert("exchange".to_string(), key.exchange);
        metadata.tags.insert("symbol".to_string(), key.symbol);
        metadata.tags.insert("interval".to_string(), key.interval);
        let record = StorageWriteRecord::new(
            "market_data",
            CANDLE_CHECKPOINT_COLLECTION,
            storage_key,
            serde_json::to_vec(&value).map_err(|error| Error::internal(error.to_string()))?,
        )
        .with_timestamp(Utc::now())
        .with_metadata(metadata);
        futures::executor::block_on(self.storage.write_batch(StorageWriteBatch::new(vec![record])))?;
        Ok(())
    }
}

fn candle_checkpoint_storage_key(key: &CandleCheckpointKey) -> Vec<u8> {
    format!("{}:{}:{}", key.exchange, key.symbol, key.interval).into_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CandleVerifyAuditStorageValue {
    run_id: String,
    exchange: String,
    symbol: String,
    interval: String,
    checked: usize,
    mismatches: usize,
    pages_fetched: usize,
    updated_at_ns: i64,
}

fn write_candle_verify_audit<W>(
    storage_sink: &W,
    run_id: &str,
    request: &HistoricalBackfillRequest,
    checked: usize,
    mismatches: usize,
    pages_fetched: usize,
) -> Result<()>
where
    W: StorageWriteSink,
{
    let interval = request.interval.clone().unwrap_or_else(|| "unknown".to_string());
    let updated_at_ns = TimestampNs::now().as_nanos();
    let value = CandleVerifyAuditStorageValue {
        run_id: run_id.to_string(),
        exchange: request.exchange.clone(),
        symbol: request.symbol.clone(),
        interval: interval.clone(),
        checked,
        mismatches,
        pages_fetched,
        updated_at_ns,
    };
    let mut metadata = StorageWriteMetadata::default();
    metadata.content_type = Some("application/json".to_string());
    metadata.schema = Some("candle_verify_audit".to_string());
    metadata.schema_version = Some("1".to_string());
    metadata.source = Some("fdc-server:candle_acquisition".to_string());
    metadata.tags.insert("kind".to_string(), "candle_verify_audit".to_string());
    metadata.tags.insert("run_id".to_string(), run_id.to_string());
    metadata.tags.insert("exchange".to_string(), request.exchange.clone());
    metadata.tags.insert("symbol".to_string(), request.symbol.clone());
    metadata.tags.insert("interval".to_string(), interval.clone());
    let key = format!("{}:{}:{}:{}", run_id, request.symbol, interval, updated_at_ns).into_bytes();
    let record = StorageWriteRecord::new(
        "market_data",
        CANDLE_VERIFY_AUDIT_COLLECTION,
        key,
        serde_json::to_vec(&value).map_err(|error| Error::internal(error.to_string()))?,
    )
    .with_timestamp(Utc::now())
    .with_metadata(metadata);
    futures::executor::block_on(storage_sink.write_batch(StorageWriteBatch::new(vec![record])))?;
    Ok(())
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
    let run_id = format!("candle-acquisition-{}", TimestampNs::now().as_nanos());
    let mut status = CandleAcquisitionRunStatus::default();
    let mut expected_verify_candles = Vec::new();

    for acquisition_request in requests {
        status.tasks_started += 1;
        if acquisition_request.role == CandleAcquisitionRequestRole::Verify {
            status.verify_tasks_started += 1;
        }
        let request_for_audit = acquisition_request.request.clone();
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

        let pages_fetched = outcome.pages.len();
        let mut verify_checked = 0usize;
        let mut verify_mismatches = 0usize;
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
                    let mismatches = count_candle_mismatches(&expected, &official);
                    status.verify_candles_checked += official.len();
                    status.verify_mismatches += mismatches;
                    verify_checked += official.len();
                    verify_mismatches += mismatches;
                }
            }
        }

        if acquisition_request.role == CandleAcquisitionRequestRole::Verify {
            write_candle_verify_audit(
                storage_sink,
                &run_id,
                &request_for_audit,
                verify_checked,
                verify_mismatches,
                pages_fetched,
            )?;
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
    let run_id = format!("candle-acquisition-{}", TimestampNs::now().as_nanos());
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
        let request_for_audit = acquisition_request.request.clone();
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

        let pages_fetched = outcome.pages.len();
        let mut verify_checked = 0usize;
        let mut verify_mismatches = 0usize;
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
                    let mismatches = count_candle_mismatches(&expected, &official);
                    status.verify_candles_checked += official.len();
                    status.verify_mismatches += mismatches;
                    verify_checked += official.len();
                    verify_mismatches += mismatches;
                }
            }
        }

        if acquisition_request.role == CandleAcquisitionRequestRole::Verify {
            write_candle_verify_audit(
                storage_sink,
                &run_id,
                &request_for_audit,
                verify_checked,
                verify_mismatches,
                pages_fetched,
            )?;
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
    W: StorageWriteSink + QueryableStorage,
{
    let executor = BarterIntegrationHistoricalRestExecutor::binance_spot();
    let fetcher = BinanceSpotOhlcvHistoricalPageFetcher::new(&executor);
    let checkpoint_store = StorageBackedCandleCheckpointStore::new(storage_sink);
    run_candle_acquisition_once_with_checkpoints(config, &fetcher, storage_sink, &checkpoint_store).await
}
