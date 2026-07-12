use chrono::Utc;
use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketType, BinanceFuturesUsdOhlcvHistoricalPageFetcher, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{error::Error, types::TimestampNs, Result};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{
    QueryableStorage, StorageQuery, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
    StorageWriteSink,
};
use serde::{Deserialize, Serialize};

use crate::runtime::config::MarketDataContractAcquisitionRuntimeConfig;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContractAcquisitionRunStatus {
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
    pub final_cursors: Vec<HistoricalCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContractCheckpointKey {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub interval: Option<String>,
}

impl ContractCheckpointKey {
    fn from_request(request: &HistoricalBackfillRequest) -> Self {
        Self {
            exchange: request.exchange.clone(),
            symbol: request.symbol.clone(),
            kind: match request.kind {
                BarterMarketDataKind::Candle => "candle".to_string(),
                BarterMarketDataKind::Trade => "trade".to_string(),
                BarterMarketDataKind::OrderBook => "order_book".to_string(),
                BarterMarketDataKind::OrderBookL1 => "order_book_l1".to_string(),
                BarterMarketDataKind::FundingRate => "funding_rate".to_string(),
                BarterMarketDataKind::OpenInterest => "open_interest".to_string(),
                BarterMarketDataKind::MarkPrice => "mark_price".to_string(),
                BarterMarketDataKind::IndexPrice => "index_price".to_string(),
                BarterMarketDataKind::Liquidation => "liquidation".to_string(),
            },
            interval: request.interval.clone(),
        }
    }
}

pub trait ContractCheckpointStore: Send + Sync {
    fn load(&self, key: &ContractCheckpointKey) -> Result<Option<HistoricalCursor>>;
    fn save(&self, key: ContractCheckpointKey, cursor: HistoricalCursor) -> Result<()>;
}

const CONTRACT_CHECKPOINT_COLLECTION: &str = "contract_checkpoints";
const CONTRACT_ACQUISITION_AUDIT_COLLECTION: &str = "contract_acquisition_audits";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContractCheckpointStorageValue {
    cursor: HistoricalCursor,
    updated_at_ns: i64,
}

pub struct StorageBackedContractCheckpointStore<'a, S> {
    storage: &'a S,
}

impl<'a, S> StorageBackedContractCheckpointStore<'a, S> {
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }
}

impl<S> ContractCheckpointStore for StorageBackedContractCheckpointStore<'_, S>
where
    S: StorageWriteSink + QueryableStorage,
{
    fn load(&self, key: &ContractCheckpointKey) -> Result<Option<HistoricalCursor>> {
        let records = futures::executor::block_on(self.storage.query_storage(
            &StorageQuery::new("market_data")
                .with_collection(CONTRACT_CHECKPOINT_COLLECTION)
                .with_key_prefix(contract_checkpoint_storage_key(key)),
        ))?;
        let Some(record) = records.last() else {
            return Ok(None);
        };
        let value: ContractCheckpointStorageValue = serde_json::from_slice(&record.value)
            .map_err(|error| Error::internal(error.to_string()))?;
        Ok(Some(value.cursor))
    }

    fn save(&self, key: ContractCheckpointKey, cursor: HistoricalCursor) -> Result<()> {
        let storage_key = contract_checkpoint_storage_key(&key);
        let value = ContractCheckpointStorageValue {
            cursor,
            updated_at_ns: TimestampNs::now().as_nanos(),
        };
        let mut metadata = StorageWriteMetadata::default();
        metadata.content_type = Some("application/json".to_string());
        metadata.schema = Some("contract_checkpoint".to_string());
        metadata.schema_version = Some("1".to_string());
        metadata.source = Some("fdc-server:contract_acquisition".to_string());
        metadata
            .tags
            .insert("kind".to_string(), "contract_checkpoint".to_string());
        metadata.tags.insert("exchange".to_string(), key.exchange);
        metadata.tags.insert("symbol".to_string(), key.symbol);
        metadata.tags.insert("data_kind".to_string(), key.kind);
        metadata.tags.insert(
            "interval".to_string(),
            key.interval.unwrap_or_else(|| "none".to_string()),
        );
        let record = StorageWriteRecord::new(
            "market_data",
            CONTRACT_CHECKPOINT_COLLECTION,
            storage_key,
            serde_json::to_vec(&value).map_err(|error| Error::internal(error.to_string()))?,
        )
        .with_timestamp(Utc::now())
        .with_metadata(metadata);
        futures::executor::block_on(self.storage.write_batch(StorageWriteBatch::new(vec![record])))?;
        Ok(())
    }
}

fn contract_checkpoint_storage_key(key: &ContractCheckpointKey) -> Vec<u8> {
    format!(
        "{}:{}:{}:{}",
        key.exchange,
        key.symbol,
        key.kind,
        key.interval.clone().unwrap_or_else(|| "none".to_string())
    )
    .into_bytes()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContractAcquisitionAuditStorageValue {
    run_id: String,
    exchange: String,
    symbol: String,
    kind: String,
    interval: Option<String>,
    pages_fetched: usize,
    envelopes_received: usize,
    storage_records_written: usize,
    final_cursor: Option<HistoricalCursor>,
    updated_at_ns: i64,
}

pub fn expand_contract_backfill_requests(
    config: &MarketDataContractAcquisitionRuntimeConfig,
) -> Result<Vec<HistoricalBackfillRequest>> {
    let start = TimestampNs::from_nanos(config.start_ns.unwrap_or(0));
    let end = TimestampNs::from_nanos(config.end_ns.unwrap_or(i64::MAX));
    let source_id = format!("barter:{}:historical:contract:candle", config.exchange);
    let mut requests = Vec::new();

    for symbol in &config.symbols {
        for interval in &config.intervals {
            requests.push(HistoricalBackfillRequest {
                source_id: source_id.clone(),
                exchange: config.exchange.clone(),
                market_type: BarterMarketType::Perpetual,
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
    store: &dyn ContractCheckpointStore,
) -> Result<ContractCheckpointKey> {
    let key = ContractCheckpointKey::from_request(request);
    if let Some(cursor) = store.load(&key)? {
        if let Some(next_start) = cursor.next_start {
            request.start = next_start;
        }
        request.cursor = Some(cursor);
    }
    Ok(key)
}

fn write_contract_acquisition_audit<W>(
    storage_sink: &W,
    run_id: &str,
    request: &HistoricalBackfillRequest,
    pages_fetched: usize,
    envelopes_received: usize,
    storage_records_written: usize,
    final_cursor: Option<HistoricalCursor>,
) -> Result<()>
where
    W: StorageWriteSink,
{
    let key = ContractCheckpointKey::from_request(request);
    let updated_at_ns = TimestampNs::now().as_nanos();
    let value = ContractAcquisitionAuditStorageValue {
        run_id: run_id.to_string(),
        exchange: key.exchange.clone(),
        symbol: key.symbol.clone(),
        kind: key.kind.clone(),
        interval: key.interval.clone(),
        pages_fetched,
        envelopes_received,
        storage_records_written,
        final_cursor,
        updated_at_ns,
    };
    let mut metadata = StorageWriteMetadata::default();
    metadata.content_type = Some("application/json".to_string());
    metadata.schema = Some("contract_acquisition_audit".to_string());
    metadata.schema_version = Some("1".to_string());
    metadata.source = Some("fdc-server:contract_acquisition".to_string());
    metadata
        .tags
        .insert("kind".to_string(), "contract_acquisition_audit".to_string());
    metadata.tags.insert("run_id".to_string(), run_id.to_string());
    metadata.tags.insert("exchange".to_string(), key.exchange);
    metadata.tags.insert("symbol".to_string(), key.symbol.clone());
    metadata.tags.insert("data_kind".to_string(), key.kind.clone());
    metadata.tags.insert(
        "interval".to_string(),
        key.interval.clone().unwrap_or_else(|| "none".to_string()),
    );
    let storage_key = format!(
        "{}:{}:{}:{}:{}",
        run_id,
        key.symbol,
        key.kind,
        key.interval.unwrap_or_else(|| "none".to_string()),
        updated_at_ns
    )
    .into_bytes();
    let record = StorageWriteRecord::new(
        "market_data",
        CONTRACT_ACQUISITION_AUDIT_COLLECTION,
        storage_key,
        serde_json::to_vec(&value).map_err(|error| Error::internal(error.to_string()))?,
    )
    .with_timestamp(Utc::now())
    .with_metadata(metadata);
    futures::executor::block_on(storage_sink.write_batch(StorageWriteBatch::new(vec![record])))?;
    Ok(())
}

pub async fn run_contract_acquisition_once_with_checkpoints<S, W, C>(
    config: &MarketDataContractAcquisitionRuntimeConfig,
    source: &S,
    storage_sink: &W,
    checkpoint_store: &C,
) -> Result<ContractAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
    W: StorageWriteSink,
    C: ContractCheckpointStore,
{
    let mut requests = expand_contract_backfill_requests(config)?;
    let run_id = format!("contract-acquisition-{}", TimestampNs::now().as_nanos());
    let mut status = ContractAcquisitionRunStatus::default();

    for request in &mut requests {
        apply_checkpoint_to_request(request, checkpoint_store)?;
    }

    for request in requests {
        status.tasks_started += 1;
        let key = ContractCheckpointKey::from_request(&request);
        let request_for_audit = request.clone();
        let outcome = run_historical_backfill_pages(
            source,
            HistoricalBackfillRunRequest {
                first_request: request,
                max_pages: config.max_pages_per_run,
                max_records: None,
            },
        )
        .await
        .map_err(|error| Error::internal(error.to_string()))?;

        status.pages_fetched += outcome.pages.len();
        status.envelopes_received += outcome.records_received;
        let final_cursor = outcome.final_cursor.clone();
        if let Some(cursor) = final_cursor.clone() {
            checkpoint_store.save(key, cursor.clone())?;
            status.final_cursors.push(cursor);
        }

        let pages_fetched = outcome.pages.len();
        let envelopes_received = outcome.records_received;
        let mut task_storage_records_written = 0usize;
        for page in outcome.pages {
            let pipeline = run_barter_envelopes_to_storage_once(page.envelopes, storage_sink).await?;
            task_storage_records_written += pipeline.storage_records_written;
        }
        status.storage_records_written += task_storage_records_written;
        write_contract_acquisition_audit(
            storage_sink,
            &run_id,
            &request_for_audit,
            pages_fetched,
            envelopes_received,
            task_storage_records_written,
            final_cursor,
        )?;
        status.audit_records_written += 1;

        if outcome.complete {
            status.tasks_completed += 1;
        }
    }

    Ok(status)
}

pub async fn run_contract_acquisition_once<S, W>(
    config: &MarketDataContractAcquisitionRuntimeConfig,
    source: &S,
    storage_sink: &W,
) -> Result<ContractAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
    W: StorageWriteSink + QueryableStorage,
{
    let checkpoint_store = StorageBackedContractCheckpointStore::new(storage_sink);
    run_contract_acquisition_once_with_checkpoints(config, source, storage_sink, &checkpoint_store)
        .await
}

pub async fn run_binance_futures_usd_contract_candle_acquisition_once<W>(
    config: &MarketDataContractAcquisitionRuntimeConfig,
    storage_sink: &W,
) -> Result<ContractAcquisitionRunStatus>
where
    W: StorageWriteSink + QueryableStorage,
{
    let executor = BarterIntegrationHistoricalRestExecutor::binance_futures_usd();
    let fetcher = BinanceFuturesUsdOhlcvHistoricalPageFetcher::new(&executor);
    let checkpoint_store = StorageBackedContractCheckpointStore::new(storage_sink);
    run_contract_acquisition_once_with_checkpoints(config, &fetcher, storage_sink, &checkpoint_store)
        .await
}
