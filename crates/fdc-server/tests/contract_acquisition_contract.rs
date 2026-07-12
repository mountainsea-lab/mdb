use std::sync::Mutex;

use async_trait::async_trait;
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, CandlePayload, DataQualityFlags, DecimalQuantity,
    HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{
    error::Error,
    types::{Price, Symbol, TimestampNs},
    Result,
};
use fdc_server::{
    market_data::contract_acquisition::{
        expand_contract_backfill_requests, run_contract_acquisition_once_with_checkpoints,
        ContractCheckpointKey, ContractCheckpointStore, StorageBackedContractCheckpointStore,
    },
    MarketDataContractAcquisitionRuntimeConfig, ProductionServerState, ServerRuntimeConfig,
};
use fdc_storage::{
    MarketDataQuery, QueryableMarketDataStore, StorageWriteBatch, StorageWriteOutcome,
    StorageWriteSink,
};
use rust_decimal::Decimal;

fn config() -> MarketDataContractAcquisitionRuntimeConfig {
    MarketDataContractAcquisitionRuntimeConfig {
        enabled: true,
        autostart: false,
        scheduler_enabled: false,
        exchange: "binance_futures_usd".to_string(),
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        kinds: vec!["candle".to_string()],
        intervals: vec!["1m".to_string(), "5m".to_string()],
        start_ns: Some(1_700_000_000_000_000_000),
        end_ns: Some(1_700_000_060_000_000_000),
        limit_per_page: 500,
        max_pages_per_run: 2,
        scheduler_interval_seconds: 3600,
        scheduler_jitter_seconds: 0,
        scheduler_max_consecutive_failures: 3,
    }
}

#[test]
fn contract_candle_requests_expand_for_symbols_and_intervals() {
    let requests = expand_contract_backfill_requests(&config()).expect("requests should expand");

    assert_eq!(requests.len(), 4);
    assert!(requests
        .iter()
        .all(|request| request.exchange == "binance_futures_usd"));
    assert!(requests
        .iter()
        .all(|request| request.market_type == BarterMarketType::Perpetual));
    assert!(requests
        .iter()
        .all(|request| request.kind == BarterMarketDataKind::Candle));
    assert!(requests
        .iter()
        .any(|request| request.symbol == "BTCUSDT" && request.interval.as_deref() == Some("1m")));
    assert!(requests
        .iter()
        .any(|request| request.symbol == "BTCUSDT" && request.interval.as_deref() == Some("5m")));
    assert!(requests
        .iter()
        .any(|request| request.symbol == "ETHUSDT" && request.interval.as_deref() == Some("1m")));
    assert!(requests
        .iter()
        .any(|request| request.symbol == "ETHUSDT" && request.interval.as_deref() == Some("5m")));
}

#[tokio::test]
async fn storage_backed_contract_checkpoint_store_round_trips_cursor() {
    let store = QueryableMarketDataStore::new();
    let checkpoint_store = StorageBackedContractCheckpointStore::new(&store);
    let key = ContractCheckpointKey {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: "candle".to_string(),
        interval: Some("1m".to_string()),
    };
    let cursor = HistoricalCursor {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
        page_token: Some("resume-token".to_string()),
        last_seen_exchange_id: Some("futures-kline-1".to_string()),
    };

    checkpoint_store
        .save(key.clone(), cursor.clone())
        .expect("save should work");
    let loaded = checkpoint_store
        .load(&key)
        .expect("load should work")
        .expect("cursor should exist");

    assert_eq!(loaded, cursor);
    let records = store.all_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].collection, "contract_checkpoints");
    assert_eq!(
        records[0].metadata.tags.get("kind").map(String::as_str),
        Some("contract_checkpoint")
    );
}

fn candle_envelope(symbol: &str, sequence: &str) -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_futures_usd".to_string(),
        symbol: Symbol::new(symbol),
        market_type: BarterMarketType::Perpetual,
        kind: BarterMarketDataKind::Candle,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Candle(CandlePayload {
            interval: Some("1m".to_string()),
            open_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            close_time: TimestampNs::from_nanos(1_700_000_060_000_000_000),
            open: Price::new(Decimal::new(42_000_00, 2)),
            high: Price::new(Decimal::new(42_100_00, 2)),
            low: Price::new(Decimal::new(41_900_00, 2)),
            close: Price::new(Decimal::new(42_050_00, 2)),
            volume: DecimalQuantity::new(25, 1),
            trade_count: Some(100),
            quote_volume: Some(DecimalQuantity::new(1_000_000, 2)),
        }),
        sequence: Some(sequence.to_string()),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_backfill_event(
        "barter:binance_futures_usd:historical:contract:candle",
        event,
    );
    envelope.envelope_id = format!("historical-contract-candle-env-{sequence}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[derive(Debug)]
struct ScriptedContractCandleSource {
    pages: Mutex<Vec<HistoricalBackfillPage>>,
    requests: Mutex<Vec<HistoricalBackfillRequest>>,
}

#[derive(Debug, Default)]
struct RecordingContractCheckpointStore {
    saved: Mutex<Vec<(ContractCheckpointKey, HistoricalCursor)>>,
}

impl RecordingContractCheckpointStore {
    fn saved(&self) -> Vec<(ContractCheckpointKey, HistoricalCursor)> {
        self.saved.lock().expect("checkpoint lock").clone()
    }
}

impl ContractCheckpointStore for RecordingContractCheckpointStore {
    fn load(&self, _key: &ContractCheckpointKey) -> Result<Option<HistoricalCursor>> {
        Ok(None)
    }

    fn save(&self, key: ContractCheckpointKey, cursor: HistoricalCursor) -> Result<()> {
        self.saved
            .lock()
            .expect("checkpoint lock")
            .push((key, cursor));
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FailingCanonicalCandleStorageSink;

#[async_trait]
impl StorageWriteSink for FailingCanonicalCandleStorageSink {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;
        if batch
            .records
            .iter()
            .any(|record| record.collection == "candles")
        {
            return Err(Error::storage("scripted canonical candle write failure"));
        }

        Ok(StorageWriteOutcome::accepted(
            batch.batch_id,
            batch.records.len(),
        ))
    }
}

impl ScriptedContractCandleSource {
    fn new(mut pages: Vec<HistoricalBackfillPage>) -> Self {
        pages.reverse();
        Self {
            pages: Mutex::new(pages),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<HistoricalBackfillRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

#[async_trait]
impl HistoricalPageFetcher for ScriptedContractCandleSource {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        self.requests.lock().expect("requests lock").push(request);
        self.pages
            .lock()
            .expect("scripted source lock")
            .pop()
            .ok_or_else(|| {
                fdc_barter::BarterAdapterError::InvalidHistoricalRequest(
                    "no scripted page".to_string(),
                )
            })
    }
}

#[tokio::test]
async fn contract_acquisition_runner_writes_candles_checkpoints_and_audit_to_storage() {
    let mut cfg = config();
    cfg.symbols = vec!["BTCUSDT".to_string()];
    cfg.intervals = vec!["1m".to_string()];
    cfg.max_pages_per_run = 1;

    let request = expand_contract_backfill_requests(&cfg)
        .expect("request should expand")
        .remove(0);
    let final_cursor = HistoricalCursor {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
        page_token: Some("resume-token".to_string()),
        last_seen_exchange_id: Some("futures-kline-1".to_string()),
    };
    let page = HistoricalBackfillPage {
        request,
        envelopes: vec![candle_envelope("BTCUSDT", "seq-1")],
        next_cursor: Some(final_cursor.clone()),
        complete: true,
    };
    let source = ScriptedContractCandleSource::new(vec![page]);
    let store = QueryableMarketDataStore::new();
    let checkpoint_store = StorageBackedContractCheckpointStore::new(&store);

    let status =
        run_contract_acquisition_once_with_checkpoints(&cfg, &source, &store, &checkpoint_store)
            .await
            .expect("runner should complete");

    assert_eq!(status.tasks_started, 1);
    assert_eq!(status.tasks_completed, 1);
    assert_eq!(status.pages_fetched, 1);
    assert_eq!(status.envelopes_received, 1);
    assert_eq!(status.storage_records_written, 1);
    assert_eq!(status.audit_records_written, 1);
    assert_eq!(status.final_cursors, vec![final_cursor.clone()]);
    assert_eq!(source.requests().len(), 1);
    let candles = store.query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"));
    assert_eq!(candles.len(), 1);
    let candle = &candles[0];
    assert_eq!(candle.collection, "candles");
    assert_eq!(
        candle.metadata.tags.get("kind").map(String::as_str),
        Some("candle")
    );
    assert_eq!(
        candle.metadata.tags.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        candle.metadata.tags.get("exchange").map(String::as_str),
        Some("binance_futures_usd")
    );
    assert_eq!(
        candle.metadata.tags.get("record.kind").map(String::as_str),
        Some("candle")
    );
    assert_eq!(
        candle.metadata.tags.get("mode").map(String::as_str),
        Some("backfill")
    );

    let all_records = store.all_records();
    assert!(all_records
        .iter()
        .any(|record| record.collection == "contract_checkpoints"));
    assert!(all_records
        .iter()
        .any(|record| record.collection == "contract_acquisition_audits"));
    assert!(all_records.iter().all(|record| {
        record.collection == "candles"
            || record.collection == "contract_checkpoints"
            || record.collection == "contract_acquisition_audits"
    }));
}

#[tokio::test]
async fn contract_acquisition_does_not_checkpoint_when_canonical_storage_write_fails() {
    let mut cfg = config();
    cfg.symbols = vec!["BTCUSDT".to_string()];
    cfg.intervals = vec!["1m".to_string()];
    cfg.max_pages_per_run = 1;

    let request = expand_contract_backfill_requests(&cfg)
        .expect("request should expand")
        .remove(0);
    let final_cursor = HistoricalCursor {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
        page_token: Some("resume-token".to_string()),
        last_seen_exchange_id: Some("futures-kline-1".to_string()),
    };
    let page = HistoricalBackfillPage {
        request,
        envelopes: vec![candle_envelope("BTCUSDT", "seq-fail")],
        next_cursor: Some(final_cursor),
        complete: true,
    };
    let source = ScriptedContractCandleSource::new(vec![page]);
    let storage_sink = FailingCanonicalCandleStorageSink;
    let checkpoint_store = RecordingContractCheckpointStore::default();

    let result = run_contract_acquisition_once_with_checkpoints(
        &cfg,
        &source,
        &storage_sink,
        &checkpoint_store,
    )
    .await;

    assert!(result.is_err());
    assert!(
        checkpoint_store.saved().is_empty(),
        "checkpoint must not advance before canonical candles are written"
    );
}

#[tokio::test]
async fn production_state_runs_configured_contract_acquisition_once_with_source() {
    let runtime = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
        ("FDC_MARKET_DATA_CONTRACTS_START_NS", "1700000000000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_END_NS", "1700000060000000000"),
    ])
    .expect("runtime config should parse");
    let state = ProductionServerState::new(runtime);
    let request =
        expand_contract_backfill_requests(&state.config().market_data_contract_acquisition)
            .expect("request should expand")
            .remove(0);
    let source = ScriptedContractCandleSource::new(vec![HistoricalBackfillPage {
        request,
        envelopes: vec![candle_envelope("BTCUSDT", "state-seq-1")],
        next_cursor: None,
        complete: true,
    }]);

    let status = state
        .run_contract_acquisition_once_with_source(&source)
        .await
        .expect("state runner should complete");

    assert_eq!(status.tasks_completed, 1);
    assert_eq!(status.storage_records_written, 1);
    assert_eq!(status.audit_records_written, 1);
    assert_eq!(
        state
            .market_data_store()
            .query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"))
            .len(),
        1
    );
    assert_eq!(
        state
            .market_data_contract_acquisition_last_run()
            .expect("last run should be recorded")
            .audit_records_written,
        1
    );
}
