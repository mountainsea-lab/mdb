use std::sync::Mutex;

use async_trait::async_trait;
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, CandlePayload, DataQualityFlags, DecimalQuantity,
    HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{
    market_data::candle_acquisition::{
        expand_candle_backfill_requests, run_candle_acquisition_once,
    },
    MarketDataCandleAcquisitionRuntimeConfig, ServerRuntimeConfig,
};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use rust_decimal::Decimal;

fn config() -> MarketDataCandleAcquisitionRuntimeConfig {
    MarketDataCandleAcquisitionRuntimeConfig {
        enabled: true,
        autostart: false,
        exchange: "binance_spot".to_string(),
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        base_intervals: vec!["1m".to_string(), "5m".to_string()],
        verify_intervals: vec!["1h".to_string()],
        start_ns: Some(1_700_000_000_000_000_000),
        end_ns: Some(1_700_000_060_000_000_000),
        limit_per_page: 500,
        max_pages_per_run: 2,
    }
}

#[test]
fn expands_candle_acquisition_requests_for_symbols_and_base_intervals() {
    let requests = expand_candle_backfill_requests(&config()).expect("requests should expand");

    assert_eq!(requests.len(), 6);
    assert_eq!(requests[0].source_id, "barter:binance_spot:historical:candle:base");
    assert_eq!(requests[0].exchange, "binance_spot");
    assert_eq!(requests[0].market_type, BarterMarketType::Spot);
    assert_eq!(requests[0].kind, BarterMarketDataKind::Candle);
    assert_eq!(requests[0].symbol, "BTCUSDT");
    assert_eq!(requests[0].interval.as_deref(), Some("1m"));
    assert_eq!(requests[0].start, TimestampNs::from_nanos(1_700_000_000_000_000_000));
    assert_eq!(requests[0].end, TimestampNs::from_nanos(1_700_000_060_000_000_000));
    assert_eq!(requests[0].limit, Some(500));
    assert!(requests[0].cursor.is_none());

    let pairs: Vec<(String, Option<String>)> = requests
        .iter()
        .map(|request| (request.symbol.clone(), request.interval.clone()))
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("BTCUSDT".to_string(), Some("1m".to_string())),
            ("BTCUSDT".to_string(), Some("5m".to_string())),
            ("BTCUSDT".to_string(), Some("1h".to_string())),
            ("ETHUSDT".to_string(), Some("1m".to_string())),
            ("ETHUSDT".to_string(), Some("5m".to_string())),
            ("ETHUSDT".to_string(), Some("1h".to_string())),
        ]
    );
}

fn candle_envelope(symbol: &str, sequence: &str) -> BarterIngestionEnvelope {
    candle_envelope_with_values(symbol, sequence, "1m", 42_000_00, 42_100_00, 41_900_00, 42_050_00, 25)
}

fn candle_envelope_with_values(
    symbol: &str,
    sequence: &str,
    interval: &str,
    open_cents: i64,
    high_cents: i64,
    low_cents: i64,
    close_cents: i64,
    volume_tenths: i64,
) -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Candle,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Candle(CandlePayload {
            interval: Some(interval.to_string()),
            open_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            close_time: TimestampNs::from_nanos(1_700_000_060_000_000_000),
            open: Price::new(Decimal::new(open_cents, 2)),
            high: Price::new(Decimal::new(high_cents, 2)),
            low: Price::new(Decimal::new(low_cents, 2)),
            close: Price::new(Decimal::new(close_cents, 2)),
            volume: DecimalQuantity::new(volume_tenths, 1),
            trade_count: Some(100),
            quote_volume: Some(DecimalQuantity::new(1_000_000, 2)),
        }),
        sequence: Some(sequence.to_string()),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_backfill_event(
        "barter:binance_spot:historical:candle",
        event,
    );
    envelope.envelope_id = format!("historical-candle-env-{sequence}");
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
struct ScriptedCandleSource {
    pages: Mutex<Vec<HistoricalBackfillPage>>,
    requests: Mutex<Vec<HistoricalBackfillRequest>>,
}

impl ScriptedCandleSource {
    fn new(pages: Vec<HistoricalBackfillPage>) -> Self {
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
impl HistoricalPageFetcher for ScriptedCandleSource {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        self.requests.lock().expect("requests lock").push(request);
        let mut pages = self.pages.lock().expect("pages lock");
        if pages.is_empty() {
            panic!("scripted candle source ran out of pages");
        }
        Ok(pages.remove(0))
    }
}

#[tokio::test]
async fn candle_acquisition_runner_writes_candles_to_storage() {
    let mut cfg = config();
    cfg.symbols = vec!["BTCUSDT".to_string()];
    cfg.base_intervals = vec!["1m".to_string()];
    cfg.verify_intervals = Vec::new();
    cfg.max_pages_per_run = 1;

    let request = expand_candle_backfill_requests(&cfg)
        .expect("request should expand")
        .remove(0);
    let page = HistoricalBackfillPage {
        request: request.clone(),
        envelopes: vec![candle_envelope("BTCUSDT", "seq-1")],
        next_cursor: Some(HistoricalCursor {
            exchange: "binance_spot".to_string(),
            symbol: "BTCUSDT".to_string(),
            kind: BarterMarketDataKind::Candle,
            next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
            page_token: None,
            last_seen_exchange_id: None,
        }),
        complete: true,
    };
    let source = ScriptedCandleSource::new(vec![page]);
    let store = QueryableMarketDataStore::new();

    let status = run_candle_acquisition_once(&cfg, &source, &store)
        .await
        .expect("runner should complete");

    assert_eq!(status.tasks_started, 1);
    assert_eq!(status.tasks_completed, 1);
    assert_eq!(status.pages_fetched, 1);
    assert_eq!(status.envelopes_received, 1);
    assert_eq!(status.storage_records_written, 1);
    assert_eq!(status.final_cursors.len(), 1);
    assert_eq!(source.requests().len(), 1);

    let records = store.query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].collection, "candles");
}

#[tokio::test]
async fn production_state_runs_configured_candle_acquisition_once_with_source() {
    let runtime = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_ENABLED", "1"),
        ("FDC_MARKET_DATA_CANDLES_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CANDLES_BASE_INTERVALS", "1m"),
        ("FDC_MARKET_DATA_CANDLES_START_NS", "1700000000000000000"),
        ("FDC_MARKET_DATA_CANDLES_END_NS", "1700000060000000000"),
    ])
    .expect("runtime config should parse");
    let state = fdc_server::ProductionServerState::new(runtime);
    let request = expand_candle_backfill_requests(&state.config().market_data_candle_acquisition)
        .expect("request should expand")
        .remove(0);
    let source = ScriptedCandleSource::new(vec![HistoricalBackfillPage {
        request,
        envelopes: vec![candle_envelope("BTCUSDT", "state-seq-1")],
        next_cursor: None,
        complete: true,
    }]);

    let status = state
        .run_candle_acquisition_once_with_source(&source)
        .await
        .expect("state runner should complete");

    assert_eq!(status.tasks_completed, 1);
    assert_eq!(status.storage_records_written, 1);
    let records = state
        .market_data_store()
        .query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"));
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn candle_acquisition_autostart_disabled_is_noop() {
    let state = fdc_server::ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap(),
    );

    let status = state
        .start_candle_acquisition_autostart_if_enabled()
        .await
        .expect("disabled autostart should be ok");

    assert_eq!(status.tasks_started, 0);
    assert_eq!(status.storage_records_written, 0);
}

#[tokio::test]
async fn candle_acquisition_persists_final_cursor_for_resume() {
    use fdc_server::market_data::candle_acquisition::{
        run_candle_acquisition_once_with_checkpoints, InMemoryCandleCheckpointStore,
    };

    let mut cfg = config();
    cfg.symbols = vec!["BTCUSDT".to_string()];
    cfg.base_intervals = vec!["1m".to_string()];
    cfg.verify_intervals = Vec::new();
    cfg.max_pages_per_run = 1;

    let first_request = expand_candle_backfill_requests(&cfg)
        .expect("request should expand")
        .remove(0);
    let final_cursor = HistoricalCursor {
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
        page_token: Some("resume-token".to_string()),
        last_seen_exchange_id: Some("kline-1".to_string()),
    };
    let checkpoint_store = InMemoryCandleCheckpointStore::default();
    let first_source = ScriptedCandleSource::new(vec![HistoricalBackfillPage {
        request: first_request,
        envelopes: vec![candle_envelope("BTCUSDT", "checkpoint-seq-1")],
        next_cursor: Some(final_cursor.clone()),
        complete: true,
    }]);
    let first_storage = QueryableMarketDataStore::new();

    let first_status = run_candle_acquisition_once_with_checkpoints(
        &cfg,
        &first_source,
        &first_storage,
        &checkpoint_store,
    )
    .await
    .expect("first run should complete");
    assert_eq!(first_status.final_cursors, vec![final_cursor.clone()]);

    let second_request = HistoricalBackfillRequest {
        source_id: "barter:binance_spot:historical:candle".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_060_000_000_001),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(500),
        cursor: Some(final_cursor.clone()),
    };
    let second_source = ScriptedCandleSource::new(vec![HistoricalBackfillPage {
        request: second_request,
        envelopes: vec![candle_envelope("BTCUSDT", "checkpoint-seq-2")],
        next_cursor: None,
        complete: true,
    }]);
    let second_storage = QueryableMarketDataStore::new();

    run_candle_acquisition_once_with_checkpoints(
        &cfg,
        &second_source,
        &second_storage,
        &checkpoint_store,
    )
    .await
    .expect("second run should complete from checkpoint");

    let second_requests = second_source.requests();
    assert_eq!(second_requests.len(), 1);
    assert_eq!(
        second_requests[0].start,
        TimestampNs::from_nanos(1_700_000_060_000_000_001)
    );
    assert_eq!(second_requests[0].cursor, Some(final_cursor));
}

#[tokio::test]
async fn candle_acquisition_verify_intervals_cross_checks_official_candles() {
    use fdc_server::market_data::candle_acquisition::run_candle_acquisition_once;

    let mut cfg = config();
    cfg.symbols = vec!["BTCUSDT".to_string()];
    cfg.base_intervals = vec!["1m".to_string()];
    cfg.verify_intervals = vec!["1m".to_string()];
    cfg.max_pages_per_run = 1;

    let requests = expand_candle_backfill_requests(&cfg).expect("requests should expand");
    let base_request = requests
        .iter()
        .find(|request| request.interval.as_deref() == Some("1m") && !request.source_id.contains(":verify"))
        .cloned()
        .expect("base request should exist");
    let verify_request = requests
        .iter()
        .find(|request| request.interval.as_deref() == Some("1m") && request.source_id.contains(":verify"))
        .cloned()
        .expect("verify request should exist");

    let source = ScriptedCandleSource::new(vec![
        HistoricalBackfillPage {
            request: base_request,
            envelopes: vec![candle_envelope_with_values(
                "BTCUSDT", "base-1", "1m", 42_000_00, 42_100_00, 41_900_00, 42_050_00, 25,
            )],
            next_cursor: None,
            complete: true,
        },
        HistoricalBackfillPage {
            request: verify_request,
            envelopes: vec![candle_envelope_with_values(
                "BTCUSDT", "verify-1", "1m", 42_000_00, 42_100_00, 41_900_00, 42_060_00, 25,
            )],
            next_cursor: None,
            complete: true,
        },
    ]);
    let store = QueryableMarketDataStore::new();

    let status = run_candle_acquisition_once(&cfg, &source, &store)
        .await
        .expect("runner should complete");

    assert_eq!(status.tasks_started, 2);
    assert_eq!(status.verify_tasks_started, 1);
    assert_eq!(status.verify_candles_checked, 1);
    assert_eq!(status.verify_mismatches, 1);
    let stored = store.query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"));
    assert_eq!(stored.len(), 1, "verify candles should not be stored as production candles");
}
