use std::{path::Path, sync::Arc, time::Duration};

use fdc_barter::{
    collect_live_market_data_envelopes, default_binance_spot_market_data_subscriptions,
    init_binance_spot_market_data, BarterIngestionEnvelope, BarterMarketDataKind,
    BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload, BarterMarketType,
    DataQualityFlags, LiveMarketDataSubscription, TradePayload, TradeSide,
};
use fdc_core::{
    types::{Price, Symbol, TimestampNs},
    Result,
};
use fdc_storage::{
    MarketDataQuery, QueryableMarketDataStore, StorageMaintenanceAuditEntry,
    StorageMaintenanceOptions, StorageMaintenanceReport, StorageTier, StorageTierHealthStatus,
    StorageWriteRecord,
};
use futures::stream;
use rust_decimal::Decimal;

use crate::{
    market_data::maintenance_audit::MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY,
    market_data::model::{
        MarketDataLiveState, MarketDataStorageHealthResponse,
        MarketDataStorageMaintenanceAuditEntryResponse,
        MarketDataStorageMaintenanceAuditResetRequest,
        MarketDataStorageMaintenanceAuditResetResponse, MarketDataStorageMaintenanceAuditResponse,
        MarketDataStorageMaintenanceRunRequest, MarketDataStorageMaintenanceRunResponse,
        MarketDataStorageStatusResponse, MarketDataStorageTierHealth, MarketDataStorageTierStatus,
        MarketDataTradeRecord, MarketDataTradesResponse, StartLiveMarketDataRequest,
        StartLiveMarketDataResponse, StopLiveMarketDataResponse,
    },
    run_realtime_barter_envelope_stream, MarketDataStorageBackendConfig,
    MarketDataStoragePolicyProfileConfig, ProductionServerState, RealtimeMarketDataMvpConfig,
};

pub fn live_status(
    state: &ProductionServerState,
) -> crate::market_data::model::LiveMarketDataStatusResponse {
    state.market_data_supervisor().status()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageMaintenanceHttpStatus {
    Ok,
    BadRequest,
    Forbidden,
    Conflict,
    InternalServerError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceServiceResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceRunResponse,
    pub message: Option<String>,
}

pub const STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION: &str = "reset_maintenance_audit";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceAuditResetResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceAuditResetResponse,
    pub message: Option<String>,
}

pub async fn run_storage_maintenance_once(
    state: &ProductionServerState,
    request: MarketDataStorageMaintenanceRunRequest,
) -> StorageMaintenanceServiceResult {
    if !state.config().market_data_storage_maintenance_enabled {
        return maintenance_error(
            StorageMaintenanceHttpStatus::Forbidden,
            "disabled",
            request.reason,
            "storage maintenance hook is disabled",
        );
    }

    if request.confirm != "run_maintenance_once" {
        return maintenance_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "confirmation_required",
            request.reason,
            "confirm must be run_maintenance_once",
        );
    }

    if matches!(request.timeout_ms, Some(0)) {
        return maintenance_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "confirmation_required",
            request.reason,
            "timeout_ms must be greater than zero",
        );
    }

    let mut options = StorageMaintenanceOptions::default()
        .with_audit_sink(state.market_data_storage_maintenance_audit());
    if let Some(timeout_ms) = request.timeout_ms {
        options = options.with_timeout(Duration::from_millis(timeout_ms));
    }

    match state
        .market_data_store()
        .run_maintenance_once_with_options(options)
        .await
    {
        Ok(Some(report)) => StorageMaintenanceServiceResult {
            http_status: StorageMaintenanceHttpStatus::Ok,
            response: maintenance_response_from_report(request.reason, report),
            message: None,
        },
        Ok(None) => maintenance_error(
            StorageMaintenanceHttpStatus::Conflict,
            "unsupported_backend",
            request.reason,
            "storage maintenance requires tiered storage backend",
        ),
        Err(error) => maintenance_error(
            StorageMaintenanceHttpStatus::InternalServerError,
            "failed",
            request.reason,
            format!("storage maintenance failed: {error}"),
        ),
    }
}

fn maintenance_error(
    http_status: StorageMaintenanceHttpStatus,
    status: &str,
    reason: Option<String>,
    message: impl Into<String>,
) -> StorageMaintenanceServiceResult {
    StorageMaintenanceServiceResult {
        http_status,
        response: empty_maintenance_response(false, status, reason),
        message: Some(message.into()),
    }
}

fn empty_maintenance_response(
    accepted: bool,
    status: &str,
    reason: Option<String>,
) -> MarketDataStorageMaintenanceRunResponse {
    MarketDataStorageMaintenanceRunResponse {
        accepted,
        status: status.to_string(),
        reason,
        duration_ms: None,
        scanned_entries: 0,
        ttl_deleted: 0,
        retention_demoted: 0,
        retention_deleted: 0,
        retained: 0,
        decode_errors: 0,
        compacted_tiers: 0,
        compaction_unsupported: 0,
        compaction_failed: 0,
        healthy_tiers: 0,
        degraded_tiers: 0,
    }
}

fn maintenance_response_from_report(
    reason: Option<String>,
    report: StorageMaintenanceReport,
) -> MarketDataStorageMaintenanceRunResponse {
    MarketDataStorageMaintenanceRunResponse {
        accepted: true,
        status: "completed".to_string(),
        reason,
        duration_ms: Some(
            report
                .finished_at
                .signed_duration_since(report.started_at)
                .num_milliseconds(),
        ),
        scanned_entries: report.lifecycle.scanned_entries,
        ttl_deleted: report.lifecycle.ttl_deleted,
        retention_demoted: report.lifecycle.retention_demoted,
        retention_deleted: report.lifecycle.retention_deleted,
        retained: report.lifecycle.retained,
        decode_errors: report.lifecycle.decode_errors,
        compacted_tiers: report.compacted_tiers.len(),
        compaction_unsupported: report.compaction_unsupported,
        compaction_failed: report.compaction_failed,
        healthy_tiers: report.healthy_tier_count(),
        degraded_tiers: report.degraded_tier_count(),
    }
}

pub fn storage_status(state: &ProductionServerState) -> MarketDataStorageStatusResponse {
    let storage = &state.config().market_data_storage;
    let tiered = storage.backend == MarketDataStorageBackendConfig::Tiered;

    MarketDataStorageStatusResponse {
        backend: backend_label(storage.backend).to_string(),
        policy_profile: policy_profile_label(storage.policy_profile).to_string(),
        tiers: vec![
            memory_tier_status("L1"),
            tier_status("L2", tiered, "redb", storage.tiers.l2_redb_path.as_deref()),
            tier_status(
                "L3",
                tiered,
                "duckdb",
                storage.tiers.l3_duckdb_path.as_deref(),
            ),
            tier_status(
                "L4",
                tiered,
                "rocksdb",
                storage.tiers.l4_rocksdb_path.as_deref(),
            ),
        ],
    }
}

pub async fn storage_maintenance_audit(
    state: &ProductionServerState,
    limit: Option<usize>,
) -> MarketDataStorageMaintenanceAuditResponse {
    let limit = limit
        .unwrap_or(10)
        .min(MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY);
    let snapshot = state
        .market_data_storage_maintenance_audit()
        .recent(limit)
        .await;
    let entries: Vec<_> = snapshot
        .entries
        .into_iter()
        .map(audit_entry_response)
        .collect();

    MarketDataStorageMaintenanceAuditResponse {
        total_entries: snapshot.total_entries,
        returned_entries: entries.len(),
        entries,
    }
}

pub async fn reset_storage_maintenance_audit(
    state: &ProductionServerState,
    request: MarketDataStorageMaintenanceAuditResetRequest,
) -> StorageMaintenanceAuditResetResult {
    if !state
        .config()
        .market_data_storage_maintenance_audit_reset_enabled
    {
        let remaining_entries = state
            .market_data_storage_maintenance_audit()
            .recent(0)
            .await
            .total_entries;
        return audit_reset_error(
            StorageMaintenanceHttpStatus::Forbidden,
            "disabled",
            request.reason,
            remaining_entries,
            "storage maintenance audit reset hook is disabled",
        );
    }

    if request.confirm != STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION {
        let remaining_entries = state
            .market_data_storage_maintenance_audit()
            .recent(0)
            .await
            .total_entries;
        return audit_reset_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "confirmation_required",
            request.reason,
            remaining_entries,
            format!("confirm must be {STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION}"),
        );
    }

    let audit = state.market_data_storage_maintenance_audit();
    let cleared_entries = audit.clear().await;
    let remaining_entries = audit.recent(0).await.total_entries;

    StorageMaintenanceAuditResetResult {
        http_status: StorageMaintenanceHttpStatus::Ok,
        response: MarketDataStorageMaintenanceAuditResetResponse {
            accepted: true,
            status: "reset".to_string(),
            reason: request.reason,
            cleared_entries,
            remaining_entries,
        },
        message: None,
    }
}

fn audit_reset_error(
    http_status: StorageMaintenanceHttpStatus,
    status: &str,
    reason: Option<String>,
    remaining_entries: usize,
    message: impl Into<String>,
) -> StorageMaintenanceAuditResetResult {
    StorageMaintenanceAuditResetResult {
        http_status,
        response: MarketDataStorageMaintenanceAuditResetResponse {
            accepted: false,
            status: status.to_string(),
            reason,
            cleared_entries: 0,
            remaining_entries,
        },
        message: Some(message.into()),
    }
}

fn audit_entry_response(
    entry: StorageMaintenanceAuditEntry,
) -> MarketDataStorageMaintenanceAuditEntryResponse {
    MarketDataStorageMaintenanceAuditEntryResponse {
        recorded_at: entry.recorded_at.to_rfc3339(),
        started_at: entry.started_at.to_rfc3339(),
        finished_at: entry.finished_at.to_rfc3339(),
        duration_ms: entry.duration_ms,
        scanned_entries: entry.scanned_entries,
        ttl_deleted: entry.ttl_deleted,
        retention_demoted: entry.retention_demoted,
        retention_deleted: entry.retention_deleted,
        retained: entry.retained,
        decode_errors: entry.decode_errors,
        compacted_tiers: entry.compacted_tiers,
        compaction_unsupported: entry.compaction_unsupported,
        compaction_failed: entry.compaction_failed,
        healthy_tiers: entry.healthy_tiers,
        degraded_tiers: entry.degraded_tiers,
    }
}

pub async fn storage_health(state: &ProductionServerState) -> MarketDataStorageHealthResponse {
    let backend = backend_label(state.config().market_data_storage.backend).to_string();

    match state.market_data_store().storage_health_snapshot().await {
        Ok(Some(snapshot)) => {
            let tiers: Vec<_> = snapshot
                .tiers
                .values()
                .map(|tier| MarketDataStorageTierHealth {
                    tier: storage_tier_label(&tier.tier).to_string(),
                    enabled: tier.enabled,
                    initialized: tier.initialized,
                    status: storage_tier_health_status_label(&tier.status).to_string(),
                    key_count: tier.stats.as_ref().map(|stats| stats.key_count),
                    total_size: tier.stats.as_ref().map(|stats| stats.total_size),
                })
                .collect();
            let status = if tiers.iter().all(|tier| tier.status == "healthy") {
                "healthy"
            } else {
                "degraded"
            };

            MarketDataStorageHealthResponse {
                backend,
                tiered: true,
                status: status.to_string(),
                tiers,
                access_patterns: snapshot.access_patterns,
                migration_queue_len: snapshot.migration_queue_len,
            }
        }
        Ok(None) => MarketDataStorageHealthResponse {
            backend,
            tiered: false,
            status: "healthy".to_string(),
            tiers: Vec::new(),
            access_patterns: 0,
            migration_queue_len: 0,
        },
        Err(_) => MarketDataStorageHealthResponse {
            backend,
            tiered: state.config().market_data_storage.backend
                == MarketDataStorageBackendConfig::Tiered,
            status: "unavailable".to_string(),
            tiers: Vec::new(),
            access_patterns: 0,
            migration_queue_len: 0,
        },
    }
}

fn storage_tier_label(tier: &StorageTier) -> &'static str {
    match tier {
        StorageTier::L1 => "L1",
        StorageTier::L2 => "L2",
        StorageTier::L3 => "L3",
        StorageTier::L4 => "L4",
    }
}

fn storage_tier_health_status_label(status: &StorageTierHealthStatus) -> &'static str {
    match status {
        StorageTierHealthStatus::Healthy => "healthy",
        StorageTierHealthStatus::MissingEngine => "missing_engine",
        StorageTierHealthStatus::StatsUnavailable => "stats_unavailable",
    }
}

fn backend_label(backend: MarketDataStorageBackendConfig) -> &'static str {
    match backend {
        MarketDataStorageBackendConfig::Memory => "memory",
        MarketDataStorageBackendConfig::Tiered => "tiered",
    }
}

fn policy_profile_label(profile: MarketDataStoragePolicyProfileConfig) -> &'static str {
    match profile {
        MarketDataStoragePolicyProfileConfig::Compatibility => "compatibility",
        MarketDataStoragePolicyProfileConfig::GenericRealtime => "generic_realtime",
    }
}

fn memory_tier_status(tier: &str) -> MarketDataStorageTierStatus {
    MarketDataStorageTierStatus {
        tier: tier.to_string(),
        engine: "memory".to_string(),
        durable_path_configured: false,
        path_hint: None,
    }
}

fn tier_status(
    tier: &str,
    tiered_backend: bool,
    durable_engine: &str,
    configured_path: Option<&Path>,
) -> MarketDataStorageTierStatus {
    match (tiered_backend, configured_path) {
        (true, Some(path)) => MarketDataStorageTierStatus {
            tier: tier.to_string(),
            engine: durable_engine.to_string(),
            durable_path_configured: true,
            path_hint: Some(safe_path_hint(path)),
        },
        _ => memory_tier_status(tier),
    }
}

fn safe_path_hint(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| "configured".to_string())
}

pub fn start_live_disabled(state: &ProductionServerState) -> (StartLiveMarketDataResponse, String) {
    let status = state.market_data_supervisor().status();
    (
        StartLiveMarketDataResponse {
            state: status.state,
            task_id: status.task_id,
            envelopes_received: 0,
            storage_records_written: 0,
            market_data_store_records: state.market_data_store().record_count(),
        },
        "live market-data acquisition requires FDC_LIVE_ENABLED=1".to_string(),
    )
}

pub async fn start_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    start_background_live(state, request).await
}

pub async fn start_background_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    if !state.config().live_enabled {
        let (_, message) = start_live_disabled(state);
        return Err(message);
    }

    let subscriptions = default_live_subscription_labels();
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(subscriptions)
        .map_err(|error| error.to_string())?;
    let timeout_secs = request
        .timeout_secs
        .unwrap_or(state.config().live_default_timeout_secs)
        .max(1);
    let max_envelopes = request
        .max_envelopes
        .unwrap_or(state.config().live_default_max_envelopes)
        .max(1);
    let store = state.market_data_store();
    let supervisor_for_task = state.market_data_supervisor();

    tokio::spawn(async move {
        if let Err(message) = run_background_live_collection(
            supervisor_for_task.clone(),
            store,
            timeout_secs,
            max_envelopes,
        )
        .await
        {
            supervisor_for_task.fail(message);
        }
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}

pub fn stop_live(state: &ProductionServerState) -> StopLiveMarketDataResponse {
    let supervisor = state.market_data_supervisor();
    supervisor.request_stop("requested");
    let status = supervisor.status();
    StopLiveMarketDataResponse {
        state: status.state,
        task_id: status.task_id,
        stopped_at_ns: status.stopped_at_ns,
        stop_reason: status.stop_reason,
    }
}

pub async fn start_fake_background_live_for_test(
    state: &ProductionServerState,
    ticks: usize,
    interval: Duration,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(vec!["test:fake:trades".to_string()])
        .map_err(|error| error.to_string())?;
    let supervisor_for_task = state.market_data_supervisor();
    let store = state.market_data_store();
    tokio::spawn(async move {
        for index in 0..ticks {
            if supervisor_for_task.stop_requested() {
                supervisor_for_task.stopped("requested");
                return;
            }
            tokio::time::sleep(interval).await;
            supervisor_for_task.record_progress(
                1,
                1,
                store.record_count() + index + 1,
                Some(TimestampNs::now().as_nanos().max(0) as u64),
            );
        }
        while !supervisor_for_task.stop_requested() {
            tokio::time::sleep(interval).await;
        }
        supervisor_for_task.stopped("requested");
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}

pub fn query_trades(
    state: &ProductionServerState,
    symbol: Option<String>,
    limit: Option<usize>,
) -> MarketDataTradesResponse {
    let mut query = MarketDataQuery::for_trades();
    if let Some(symbol) = symbol {
        query = query.with_symbol(symbol);
    }
    if let Some(limit) = limit {
        query = query.with_limit(limit);
    }

    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_trade_record)
        .collect();

    MarketDataTradesResponse {
        returned_records: records.len(),
        records,
    }
}

pub async fn ingest_test_trade(
    state: &ProductionServerState,
    symbol: &str,
    trade_id: &str,
) -> Result<StartLiveMarketDataResponse> {
    let envelope = test_trade_envelope(symbol, trade_id);
    let summary = run_realtime_barter_envelope_stream(
        futures::stream::iter(vec![envelope]),
        state.market_data_store(),
        RealtimeMarketDataMvpConfig::default(),
    )
    .await?;

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        task_id: None,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}

async fn run_background_live_collection(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<(), String> {
    while !supervisor.stop_requested() {
        let before = store.record_count();
        let result =
            run_live_collection_and_storage(store.clone(), timeout_secs, max_envelopes).await?;
        let after = store.record_count();
        supervisor.record_progress(
            result.envelopes_received,
            result.storage_records_written,
            after,
            Some(TimestampNs::now().as_nanos().max(0) as u64),
        );
        if after == before && supervisor.stop_requested() {
            break;
        }
    }
    supervisor.stopped("requested");
    Ok(())
}

async fn run_live_collection_and_storage(
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to create production live runner runtime: {error}"))?;

        runtime.block_on(run_live_collection_and_storage_on_current_thread(
            store,
            timeout_secs,
            max_envelopes,
        ))
    })
    .await
    .map_err(|error| format!("production live runner task failed to join: {error}"))?
}

async fn run_live_collection_and_storage_on_current_thread(
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    eprintln!(
        "fdc production live runner: starting Binance Spot market data timeout_secs={timeout_secs} max_envelopes={max_envelopes}"
    );

    let streams = init_binance_spot_market_data(default_binance_spot_market_data_subscriptions())
        .await
        .map_err(|error| {
            eprintln!("fdc production live runner: failed to initialize stream: {error}");
            format!("failed to initialize Binance Spot live market-data stream: {error}")
        })?;

    let stream = streams.select_all();
    let envelopes = match tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        collect_live_market_data_envelopes(
            "barter-binance-spot-live-market-data",
            stream,
            max_envelopes,
        ),
    )
    .await
    {
        Ok(Ok(envelopes)) => envelopes,
        Ok(Err(error)) => {
            eprintln!("fdc production live runner: stream item error: {error}");
            return Err(format!("failed while collecting live market data: {error}"));
        }
        Err(_) => {
            eprintln!("fdc production live runner: timed out while collecting live market data");
            return Err(format!(
                "timed out after {timeout_secs}s while collecting live market data"
            ));
        }
    };

    eprintln!(
        "fdc production live runner: collected {} live envelopes, writing to queryable store",
        envelopes.len()
    );

    if envelopes.is_empty() {
        return Err("live stream returned no market-data envelopes".to_string());
    }

    let summary = run_realtime_barter_envelope_stream(
        stream::iter(envelopes),
        store,
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(timeout_secs.max(1)),
            idle_timeout: Duration::from_millis(100),
            max_errors: 0,
        },
    )
    .await
    .map_err(|error| {
        eprintln!("fdc production live runner: failed to write live envelopes: {error}");
        format!("failed to write live envelopes: {error}")
    })?;

    eprintln!(
        "fdc production live runner: completed envelopes_received={} storage_records_written={} market_data_store_records={}",
        summary.envelopes_received,
        summary.storage_records_written,
        summary.market_data_store_records
    );

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        task_id: None,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}

fn default_live_subscription_labels() -> Vec<String> {
    default_binance_spot_market_data_subscriptions()
        .into_iter()
        .map(live_subscription_label)
        .collect()
}

fn live_subscription_label(subscription: LiveMarketDataSubscription) -> String {
    let exchange = match subscription.exchange {
        fdc_barter::LiveExchange::BinanceSpot => "binance_spot",
        fdc_barter::LiveExchange::BinanceFuturesUsd => "binance_futures_usd",
    };
    format!(
        "{}:{}{}:{}",
        exchange,
        subscription.base.to_ascii_uppercase(),
        subscription.quote.to_ascii_uppercase(),
        live_kind_label(subscription.kind)
    )
}

fn live_kind_label(kind: BarterMarketDataKind) -> &'static str {
    match kind {
        BarterMarketDataKind::Trade => "trade",
        BarterMarketDataKind::OrderBookL1 => "order_book_l1",
        BarterMarketDataKind::OrderBook => "order_book",
        BarterMarketDataKind::Candle => "candle",
        BarterMarketDataKind::Liquidation => "liquidation",
    }
}

fn record_to_trade_record(record: StorageWriteRecord) -> MarketDataTradeRecord {
    let payload = serde_json::from_slice(&record.value).unwrap_or_else(|_| serde_json::Value::Null);
    MarketDataTradeRecord {
        key: String::from_utf8_lossy(&record.key).to_string(),
        symbol: record.metadata.tags.get("symbol").cloned(),
        kind: record.metadata.tags.get("kind").cloned(),
        source: record.metadata.source.clone(),
        payload,
    }
}

fn test_trade_envelope(symbol: &str, trade_id: &str) -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::now(),
        received_at: TimestampNs::now(),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: Decimal::new(1, 0),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(format!("seq-{trade_id}")),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.quality = DataQualityFlags::default();
    envelope
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProductionServerState, ServerRuntimeConfig};

    #[test]
    fn live_subscription_label_formats_binance_futures_usd() {
        let subscription = fdc_barter::default_binance_futures_usd_market_data_subscriptions()
            .into_iter()
            .find(|subscription| {
                subscription.base == "btc" && subscription.kind == BarterMarketDataKind::Liquidation
            })
            .expect("default futures subscriptions include BTC/USDT liquidations");

        assert_eq!(
            live_subscription_label(subscription),
            "binance_futures_usd:BTCUSDT:liquidation"
        );
    }

    #[test]
    fn default_live_subscription_labels_cover_trades_l1_and_l2_for_default_symbols() {
        assert_eq!(
            default_live_subscription_labels(),
            vec![
                "binance_spot:BTCUSDT:trade".to_string(),
                "binance_spot:BTCUSDT:order_book_l1".to_string(),
                "binance_spot:BTCUSDT:order_book".to_string(),
                "binance_spot:ETHUSDT:trade".to_string(),
                "binance_spot:ETHUSDT:order_book_l1".to_string(),
                "binance_spot:ETHUSDT:order_book".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn successful_tiered_maintenance_records_audit_entry() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ])
        .unwrap();
        let state = ProductionServerState::try_new(config).await.unwrap();

        let result = run_storage_maintenance_once(
            &state,
            MarketDataStorageMaintenanceRunRequest {
                confirm: "run_maintenance_once".to_string(),
                timeout_ms: None,
                reason: Some("unit-test".to_string()),
            },
        )
        .await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Ok);
        let audit = state
            .market_data_storage_maintenance_audit()
            .recent(10)
            .await;
        assert_eq!(audit.entries.len(), 1);
        assert_eq!(audit.entries[0].healthy_tiers, 4);
    }
}
