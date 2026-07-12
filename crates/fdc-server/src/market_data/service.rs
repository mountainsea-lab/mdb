use std::{path::Path, sync::Arc, time::Duration};

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, CandlePayload, DataQualityFlags,
    LiveMarketDataSubscription, TradePayload, TradeSide, collect_live_market_data_envelopes,
    default_binance_spot_market_data_subscriptions, init_binance_spot_market_data,
};
use fdc_core::{
    Result,
    types::{Price, Symbol, TimestampNs},
};
use fdc_storage::{
    MarketDataQuery, QueryableMarketDataStore, StorageMaintenanceAuditEntry,
    StorageMaintenanceOptions, StorageMaintenanceReport, StorageTier, StorageTierHealthStatus,
    StorageWriteRecord,
};
use futures::stream;
use rust_decimal::Decimal;

use crate::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig, ProductionServerState,
    RealtimeMarketDataMvpConfig, ServerRuntimeConfig,
    market_data::maintenance_audit::MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY,
    market_data::maintenance_scheduler::{
        claim_storage_maintenance_scheduler_task, spawn_storage_maintenance_scheduler_into_handle,
    },
    market_data::model::{
        MarketDataCandleRecord, MarketDataCandlesResponse, MarketDataLiveState,
        MarketDataCandleAcquisitionRunStatusResponse,
        MarketDataCandleAcquisitionStatusResponse,
        MarketDataStorageHealthResponse, MarketDataStorageMaintenanceAuditEntryResponse,
        MarketDataStorageMaintenanceAuditResetRequest,
        MarketDataStorageMaintenanceAuditResetResponse, MarketDataStorageMaintenanceAuditResponse,
        MarketDataStorageMaintenanceRunRequest, MarketDataStorageMaintenanceRunResponse,
        MarketDataStorageMaintenanceSchedulerResetRequest,
        MarketDataStorageMaintenanceSchedulerResetResponse,
        MarketDataStorageMaintenanceSchedulerResumeRequest,
        MarketDataStorageMaintenanceSchedulerResumeResponse,
        MarketDataStorageMaintenanceSchedulerStatusResponse, MarketDataStorageStatusResponse,
        MarketDataStorageTierHealth, MarketDataStorageTierStatus, MarketDataTradeRecord,
        MarketDataTradesResponse, ResumeLiveMarketDataRequest, ResumeLiveMarketDataResponse,
        StartLiveMarketDataRequest, StartLiveMarketDataResponse, StopLiveMarketDataResponse,
    },
    run_realtime_barter_envelope_stream,
};

pub fn live_status(
    state: &ProductionServerState,
) -> crate::market_data::model::LiveMarketDataStatusResponse {
    let mut status = state.market_data_supervisor().status();
    status.resume_enabled = state.config().market_data_live_resume_enabled;
    status
}

pub fn candle_acquisition_status(
    state: &ProductionServerState,
) -> MarketDataCandleAcquisitionStatusResponse {
    let config = &state.config().market_data_candle_acquisition;
    let last_run = state
        .market_data_candle_acquisition_last_run()
        .map(|status| MarketDataCandleAcquisitionRunStatusResponse {
            tasks_started: status.tasks_started,
            tasks_completed: status.tasks_completed,
            pages_fetched: status.pages_fetched,
            envelopes_received: status.envelopes_received,
            storage_records_written: status.storage_records_written,
            final_cursors: status.final_cursors.len(),
        });

    MarketDataCandleAcquisitionStatusResponse {
        enabled: config.enabled,
        autostart: config.autostart,
        exchange: config.exchange.clone(),
        symbols: config.symbols.clone(),
        base_intervals: config.base_intervals.clone(),
        verify_intervals: config.verify_intervals.clone(),
        start_ns: config.start_ns,
        end_ns: config.end_ns,
        limit_per_page: config.limit_per_page,
        max_pages_per_run: config.max_pages_per_run,
        last_run,
        last_error: state.market_data_candle_acquisition_last_error(),
    }
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
pub const STORAGE_MAINTENANCE_SCHEDULER_RESET_CONFIRMATION: &str = "reset_scheduler_suppression";
pub const STORAGE_MAINTENANCE_SCHEDULER_RESUME_CONFIRMATION: &str = "resume_scheduler";
pub const LIVE_RESUME_CONFIRMATION: &str = "resume_live_collection";
pub const DEFAULT_TRADE_QUERY_LIMIT: usize = 100;
pub const MAX_TRADE_QUERY_LIMIT: usize = 1000;
pub const TRADE_QUERY_LIMIT_ERROR: &str = "limit must be between 1 and 1000";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveResumeResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: ResumeLiveMarketDataResponse,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct LiveRetryPolicy {
    enabled: bool,
    initial_delay: Duration,
    max_delay: Duration,
    max_consecutive_failures: u32,
}

impl LiveRetryPolicy {
    fn from_config(config: &ServerRuntimeConfig) -> Self {
        Self {
            enabled: config.live_retry_enabled,
            initial_delay: Duration::from_millis(config.live_retry_initial_delay_ms),
            max_delay: Duration::from_millis(config.live_retry_max_delay_ms),
            max_consecutive_failures: config.live_max_consecutive_failures,
        }
    }

    fn delay_for_failure(&self, consecutive_failures: u32) -> Duration {
        let multiplier = 1_u32
            .checked_shl(consecutive_failures.saturating_sub(1).min(16))
            .unwrap_or(u32::MAX);
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceAuditResetResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceAuditResetResponse,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResetResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceSchedulerResetResponse,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResumeResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceSchedulerResumeResponse,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTradesResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataTradesResponse,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryCandlesResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataCandlesResponse,
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
    let config = state.config();
    let storage = &config.market_data_storage;
    let tiered = storage.backend == MarketDataStorageBackendConfig::Tiered;
    let durable_tiers_configured = [
        storage.tiers.l2_redb_path.as_ref(),
        storage.tiers.l3_duckdb_path.as_ref(),
        storage.tiers.l4_rocksdb_path.as_ref(),
    ]
    .into_iter()
    .filter(|path| path.is_some())
    .count();

    MarketDataStorageStatusResponse {
        backend: backend_label(storage.backend).to_string(),
        policy_profile: policy_profile_label(storage.policy_profile).to_string(),
        tiered,
        durable_tiers_configured,
        maintenance_enabled: config.market_data_storage_maintenance_enabled,
        maintenance_audit_reset_enabled: config.market_data_storage_maintenance_audit_reset_enabled,
        maintenance_audit_capacity: config.market_data_storage_maintenance_audit_capacity,
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
        capacity: snapshot.capacity,
        total_entries: snapshot.total_entries,
        returned_entries: entries.len(),
        total_recorded_entries: snapshot.total_recorded_entries,
        reset_count: snapshot.reset_count,
        total_cleared_entries: snapshot.total_cleared_entries,
        last_recorded_at: snapshot
            .last_recorded_at
            .map(|timestamp| timestamp.to_rfc3339()),
        last_reset_at: snapshot
            .last_reset_at
            .map(|timestamp| timestamp.to_rfc3339()),
        entries,
    }
}

pub async fn storage_maintenance_scheduler_status(
    state: &ProductionServerState,
) -> MarketDataStorageMaintenanceSchedulerStatusResponse {
    let snapshot = state
        .market_data_storage_maintenance_scheduler()
        .snapshot()
        .await;

    MarketDataStorageMaintenanceSchedulerStatusResponse {
        enabled: snapshot.enabled,
        running: snapshot.running,
        backend: snapshot.backend,
        tiered: snapshot.tiered,
        interval_seconds: snapshot.interval_seconds,
        timeout_ms: snapshot.timeout_ms,
        jitter_seconds: snapshot.jitter_seconds,
        max_consecutive_failures: snapshot.max_consecutive_failures,
        consecutive_failures: snapshot.consecutive_failures,
        total_runs: snapshot.total_runs,
        successful_runs: snapshot.successful_runs,
        failed_runs: snapshot.failed_runs,
        skipped_runs: snapshot.skipped_runs,
        last_started_at: snapshot
            .last_started_at
            .map(|timestamp| timestamp.to_rfc3339()),
        last_finished_at: snapshot
            .last_finished_at
            .map(|timestamp| timestamp.to_rfc3339()),
        last_status: snapshot.last_status,
        last_error: snapshot.last_error,
        next_run_at: snapshot.next_run_at.map(|timestamp| timestamp.to_rfc3339()),
    }
}

pub async fn reset_storage_maintenance_scheduler(
    state: &ProductionServerState,
    request: MarketDataStorageMaintenanceSchedulerResetRequest,
) -> StorageMaintenanceSchedulerResetResult {
    if !state
        .config()
        .market_data_storage_maintenance_scheduler_reset_enabled
    {
        return scheduler_reset_error(
            StorageMaintenanceHttpStatus::Forbidden,
            "disabled",
            request.reason,
            0,
            0,
            "storage maintenance scheduler reset hook is disabled",
        );
    }

    if request.confirm != STORAGE_MAINTENANCE_SCHEDULER_RESET_CONFIRMATION {
        return scheduler_reset_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "confirmation_required",
            request.reason,
            0,
            0,
            format!("confirm must be {STORAGE_MAINTENANCE_SCHEDULER_RESET_CONFIRMATION}"),
        );
    }

    let outcome = state
        .market_data_storage_maintenance_scheduler()
        .reset_suppression()
        .await;
    if outcome.running {
        return scheduler_reset_error(
            StorageMaintenanceHttpStatus::Conflict,
            "running",
            request.reason,
            outcome.previous_consecutive_failures,
            outcome.consecutive_failures,
            "storage maintenance scheduler reset cannot run while scheduler attempt is running",
        );
    }

    StorageMaintenanceSchedulerResetResult {
        http_status: StorageMaintenanceHttpStatus::Ok,
        response: MarketDataStorageMaintenanceSchedulerResetResponse {
            accepted: true,
            status: "reset".to_string(),
            reason: request.reason,
            previous_consecutive_failures: outcome.previous_consecutive_failures,
            consecutive_failures: outcome.consecutive_failures,
        },
        message: None,
    }
}

pub async fn resume_storage_maintenance_scheduler(
    state: &ProductionServerState,
    request: MarketDataStorageMaintenanceSchedulerResumeRequest,
) -> StorageMaintenanceSchedulerResumeResult {
    if !state
        .config()
        .market_data_storage_maintenance_scheduler_resume_enabled
    {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Forbidden,
            "disabled",
            request.reason,
            0,
            0,
            "storage maintenance scheduler resume hook is disabled",
        );
    }

    if request.confirm != STORAGE_MAINTENANCE_SCHEDULER_RESUME_CONFIRMATION {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "confirmation_required",
            request.reason,
            0,
            0,
            format!("confirm must be {STORAGE_MAINTENANCE_SCHEDULER_RESUME_CONFIRMATION}"),
        );
    }

    if !state
        .config()
        .market_data_storage_maintenance_scheduler_enabled
    {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Forbidden,
            "scheduler_disabled",
            request.reason,
            0,
            0,
            "storage maintenance scheduler config is disabled",
        );
    }

    if state.config().market_data_storage.backend != MarketDataStorageBackendConfig::Tiered {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::BadRequest,
            "unsupported_backend",
            request.reason,
            0,
            0,
            "storage maintenance scheduler resume requires tiered storage backend",
        );
    }

    let task_handle = state.market_data_storage_maintenance_scheduler_task();
    if task_handle.is_active().await {
        let snapshot = state
            .market_data_storage_maintenance_scheduler()
            .snapshot()
            .await;
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "already_running",
            request.reason,
            snapshot.consecutive_failures,
            snapshot.consecutive_failures,
            "storage maintenance scheduler task is already running",
        );
    }

    let scheduler = state.market_data_storage_maintenance_scheduler();
    let Some(claimed_task) = claim_storage_maintenance_scheduler_task(
        state.config().clone(),
        state.market_data_store(),
        state.market_data_storage_maintenance_audit(),
        scheduler.clone(),
        task_handle,
    )
    .await
    else {
        let snapshot = scheduler.snapshot().await;
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "already_running",
            request.reason,
            snapshot.consecutive_failures,
            snapshot.consecutive_failures,
            "storage maintenance scheduler task could not be started",
        );
    };

    let outcome = scheduler.prepare_resume().await;
    if outcome.running {
        claimed_task.abort().await;
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "running",
            request.reason,
            outcome.previous_consecutive_failures,
            outcome.consecutive_failures,
            "storage maintenance scheduler resume cannot run while scheduler attempt is running",
        );
    }

    let task_started = true;
    claimed_task.start();

    StorageMaintenanceSchedulerResumeResult {
        http_status: StorageMaintenanceHttpStatus::Ok,
        response: MarketDataStorageMaintenanceSchedulerResumeResponse {
            accepted: true,
            status: "resumed".to_string(),
            reason: request.reason,
            previous_consecutive_failures: outcome.previous_consecutive_failures,
            consecutive_failures: outcome.consecutive_failures,
            task_started,
        },
        message: None,
    }
}

fn scheduler_reset_error(
    http_status: StorageMaintenanceHttpStatus,
    status: &str,
    reason: Option<String>,
    previous_consecutive_failures: u32,
    consecutive_failures: u32,
    message: impl Into<String>,
) -> StorageMaintenanceSchedulerResetResult {
    StorageMaintenanceSchedulerResetResult {
        http_status,
        response: MarketDataStorageMaintenanceSchedulerResetResponse {
            accepted: false,
            status: status.to_string(),
            reason,
            previous_consecutive_failures,
            consecutive_failures,
        },
        message: Some(message.into()),
    }
}

fn scheduler_resume_error(
    http_status: StorageMaintenanceHttpStatus,
    status: &str,
    reason: Option<String>,
    previous_consecutive_failures: u32,
    consecutive_failures: u32,
    message: impl Into<String>,
) -> StorageMaintenanceSchedulerResumeResult {
    StorageMaintenanceSchedulerResumeResult {
        http_status,
        response: MarketDataStorageMaintenanceSchedulerResumeResponse {
            accepted: false,
            status: status.to_string(),
            reason,
            previous_consecutive_failures,
            consecutive_failures,
            task_started: false,
        },
        message: Some(message.into()),
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
                .map(|tier| {
                    let readiness =
                        durable_path_readiness(configured_durable_path_for_tier(state, &tier.tier));
                    MarketDataStorageTierHealth {
                        tier: storage_tier_label(&tier.tier).to_string(),
                        enabled: tier.enabled,
                        initialized: tier.initialized,
                        status: storage_tier_health_status_label(&tier.status).to_string(),
                        key_count: tier.stats.as_ref().map(|stats| stats.key_count),
                        total_size: tier.stats.as_ref().map(|stats| stats.total_size),
                        durable_path_configured: readiness.durable_path_configured,
                        path_hint: readiness.path_hint,
                        path_exists: readiness.path_exists,
                        path_parent_exists: readiness.path_parent_exists,
                        path_parent_writable: readiness.path_parent_writable,
                    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DurablePathReadiness {
    durable_path_configured: bool,
    path_hint: Option<String>,
    path_exists: Option<bool>,
    path_parent_exists: Option<bool>,
    path_parent_writable: Option<bool>,
}

impl DurablePathReadiness {
    fn unconfigured() -> Self {
        Self {
            durable_path_configured: false,
            path_hint: None,
            path_exists: None,
            path_parent_exists: None,
            path_parent_writable: None,
        }
    }
}

fn durable_path_readiness(path: Option<&std::path::Path>) -> DurablePathReadiness {
    let Some(path) = path else {
        return DurablePathReadiness::unconfigured();
    };

    let path_parent = path.parent();
    let path_parent_exists = path_parent.map(std::path::Path::exists).unwrap_or(false);
    let path_parent_writable = path_parent
        .and_then(|parent| std::fs::metadata(parent).ok())
        .map(|metadata| !metadata.permissions().readonly())
        .unwrap_or(false);

    DurablePathReadiness {
        durable_path_configured: true,
        path_hint: path
            .file_name()
            .map(|name| name.to_string_lossy().to_string()),
        path_exists: Some(path.exists()),
        path_parent_exists: Some(path_parent_exists),
        path_parent_writable: Some(path_parent_exists && path_parent_writable),
    }
}

fn configured_durable_path_for_tier<'a>(
    state: &'a ProductionServerState,
    tier: &StorageTier,
) -> Option<&'a std::path::Path> {
    let tiers = &state.config().market_data_storage.tiers;
    match tier {
        StorageTier::L1 => None,
        StorageTier::L2 => tiers.l2_redb_path.as_deref(),
        StorageTier::L3 => tiers.l3_duckdb_path.as_deref(),
        StorageTier::L4 => tiers.l4_rocksdb_path.as_deref(),
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

pub async fn resume_live(
    state: &ProductionServerState,
    request: ResumeLiveMarketDataRequest,
) -> LiveResumeResult {
    if !state.config().live_enabled {
        return live_resume_error(
            StorageMaintenanceHttpStatus::Forbidden,
            state,
            request.reason,
            "live market-data acquisition requires FDC_LIVE_ENABLED=1",
        );
    }

    if !state.config().market_data_live_resume_enabled {
        return live_resume_error(
            StorageMaintenanceHttpStatus::Forbidden,
            state,
            request.reason,
            "live market-data resume hook is disabled",
        );
    }

    if request.confirm != LIVE_RESUME_CONFIRMATION {
        return live_resume_error(
            StorageMaintenanceHttpStatus::BadRequest,
            state,
            request.reason,
            format!("confirm must be {LIVE_RESUME_CONFIRMATION}"),
        );
    }

    let reason = request
        .reason
        .clone()
        .unwrap_or_else(|| "resume".to_string());
    let outcome = state.market_data_supervisor().prepare_resume(reason);
    if outcome.running {
        return LiveResumeResult {
            http_status: StorageMaintenanceHttpStatus::Conflict,
            response: live_resume_response(state, false, request.reason),
            message: Some("live market-data resume cannot run while runner is active".to_string()),
        };
    }

    match start_background_live(
        state,
        StartLiveMarketDataRequest {
            timeout_secs: None,
            max_envelopes: None,
        },
    )
    .await
    {
        Ok(_) => LiveResumeResult {
            http_status: StorageMaintenanceHttpStatus::Ok,
            response: live_resume_response(state, true, request.reason),
            message: None,
        },
        Err(message) => LiveResumeResult {
            http_status: StorageMaintenanceHttpStatus::Conflict,
            response: live_resume_response(state, false, request.reason),
            message: Some(message),
        },
    }
}

fn live_resume_error(
    http_status: StorageMaintenanceHttpStatus,
    state: &ProductionServerState,
    reason: Option<String>,
    message: impl Into<String>,
) -> LiveResumeResult {
    LiveResumeResult {
        http_status,
        response: live_resume_response(state, false, reason),
        message: Some(message.into()),
    }
}

fn live_resume_response(
    state: &ProductionServerState,
    resumed: bool,
    reason: Option<String>,
) -> ResumeLiveMarketDataResponse {
    let status = state.market_data_supervisor().status();
    ResumeLiveMarketDataResponse {
        state: status.state,
        task_id: status.task_id,
        resumed,
        reason,
        consecutive_failures: status.consecutive_failures,
        retry_count: status.retry_count,
        next_retry_at_ns: status.next_retry_at_ns,
    }
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
    let retry_policy = LiveRetryPolicy::from_config(state.config());

    tokio::spawn(async move {
        if let Err(message) = run_background_live_collection(
            supervisor_for_task.clone(),
            store,
            timeout_secs,
            max_envelopes,
            retry_policy,
        )
        .await
        {
            if supervisor_for_task.status().state != MarketDataLiveState::Suppressed {
                supervisor_for_task.fail(message);
            }
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
    limit: Option<String>,
) -> QueryTradesResult {
    let normalized_symbol = symbol.map(|symbol| symbol.trim().to_ascii_uppercase());
    let requested_limit = match parse_trade_query_limit(limit) {
        Ok(limit) => limit,
        Err(()) => {
            return QueryTradesResult {
                http_status: StorageMaintenanceHttpStatus::BadRequest,
                response: empty_trades_response(None, DEFAULT_TRADE_QUERY_LIMIT, normalized_symbol),
                message: Some(TRADE_QUERY_LIMIT_ERROR.to_string()),
            };
        }
    };
    let applied_limit = requested_limit.unwrap_or(DEFAULT_TRADE_QUERY_LIMIT);

    if !(1..=MAX_TRADE_QUERY_LIMIT).contains(&applied_limit) {
        return QueryTradesResult {
            http_status: StorageMaintenanceHttpStatus::BadRequest,
            response: empty_trades_response(
                requested_limit,
                DEFAULT_TRADE_QUERY_LIMIT,
                normalized_symbol,
            ),
            message: Some(TRADE_QUERY_LIMIT_ERROR.to_string()),
        };
    }

    let mut query = MarketDataQuery::for_trades().with_limit(applied_limit);
    if let Some(symbol) = normalized_symbol.clone() {
        query = query.with_symbol(symbol);
    }
    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_trade_record)
        .collect();

    QueryTradesResult {
        http_status: StorageMaintenanceHttpStatus::Ok,
        response: MarketDataTradesResponse {
            requested_limit,
            applied_limit,
            symbol: normalized_symbol,
            data_kind: "trade".to_string(),
            query_source: "market_data_store".to_string(),
            returned_records: records.len(),
            records,
        },
        message: None,
    }
}

fn parse_trade_query_limit(limit: Option<String>) -> std::result::Result<Option<usize>, ()> {
    match limit {
        Some(limit) => limit.parse::<usize>().map(Some).map_err(|_| ()),
        None => Ok(None),
    }
}

fn empty_trades_response(
    requested_limit: Option<usize>,
    applied_limit: usize,
    symbol: Option<String>,
) -> MarketDataTradesResponse {
    MarketDataTradesResponse {
        requested_limit,
        applied_limit,
        symbol,
        data_kind: "trade".to_string(),
        query_source: "market_data_store".to_string(),
        returned_records: 0,
        records: Vec::new(),
    }
}

pub fn query_candles(
    state: &ProductionServerState,
    symbol: Option<String>,
    limit: Option<String>,
) -> QueryCandlesResult {
    let normalized_symbol = symbol.map(|symbol| symbol.trim().to_ascii_uppercase());
    let requested_limit = match parse_trade_query_limit(limit) {
        Ok(limit) => limit,
        Err(()) => {
            return QueryCandlesResult {
                http_status: StorageMaintenanceHttpStatus::BadRequest,
                response: empty_candles_response(
                    None,
                    DEFAULT_TRADE_QUERY_LIMIT,
                    normalized_symbol,
                ),
                message: Some(TRADE_QUERY_LIMIT_ERROR.to_string()),
            };
        }
    };
    let applied_limit = requested_limit.unwrap_or(DEFAULT_TRADE_QUERY_LIMIT);

    if !(1..=MAX_TRADE_QUERY_LIMIT).contains(&applied_limit) {
        return QueryCandlesResult {
            http_status: StorageMaintenanceHttpStatus::BadRequest,
            response: empty_candles_response(
                requested_limit,
                DEFAULT_TRADE_QUERY_LIMIT,
                normalized_symbol,
            ),
            message: Some(TRADE_QUERY_LIMIT_ERROR.to_string()),
        };
    }

    let mut query = MarketDataQuery::for_candles().with_limit(applied_limit);
    if let Some(symbol) = normalized_symbol.clone() {
        query = query.with_symbol(symbol);
    }
    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_candle_record)
        .collect();

    QueryCandlesResult {
        http_status: StorageMaintenanceHttpStatus::Ok,
        response: MarketDataCandlesResponse {
            requested_limit,
            applied_limit,
            symbol: normalized_symbol,
            data_kind: "candle".to_string(),
            query_source: "market_data_store".to_string(),
            returned_records: records.len(),
            records,
        },
        message: None,
    }
}

fn empty_candles_response(
    requested_limit: Option<usize>,
    applied_limit: usize,
    symbol: Option<String>,
) -> MarketDataCandlesResponse {
    MarketDataCandlesResponse {
        requested_limit,
        applied_limit,
        symbol,
        data_kind: "candle".to_string(),
        query_source: "market_data_store".to_string(),
        returned_records: 0,
        records: Vec::new(),
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

pub async fn ingest_test_candle(
    state: &ProductionServerState,
    symbol: &str,
) -> Result<StartLiveMarketDataResponse> {
    let envelope = test_candle_envelope(symbol);
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
    retry_policy: LiveRetryPolicy,
) -> std::result::Result<(), String> {
    run_background_live_collection_with_runner(supervisor, store.clone(), retry_policy, move || {
        run_live_collection_and_storage(store.clone(), timeout_secs, max_envelopes)
    })
    .await
}

async fn run_background_live_collection_with_runner<F, Fut>(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    retry_policy: LiveRetryPolicy,
    mut runner: F,
) -> std::result::Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<StartLiveMarketDataResponse, String>>,
{
    while !supervisor.stop_requested() {
        let before = store.record_count();
        match runner().await {
            Ok(result) => {
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
            Err(message) => {
                if !retry_policy.enabled {
                    supervisor.fail(message.clone());
                    return Err(message);
                }

                let next_failure = supervisor.status().consecutive_failures.saturating_add(1);
                let delay = retry_policy.delay_for_failure(next_failure);
                let now_ns = TimestampNs::now().as_nanos().max(0) as u64;
                let delay_ns = delay.as_nanos().min(u128::from(u64::MAX)) as u64;
                let next_retry_at_ns = now_ns.saturating_add(delay_ns);
                supervisor.record_failure_for_retry(
                    message,
                    retry_policy.max_consecutive_failures,
                    Some(next_retry_at_ns),
                );

                if supervisor.status().state == MarketDataLiveState::Suppressed {
                    return Err("live collection suppressed after consecutive failures".to_string());
                }

                tokio::time::sleep(delay).await;
            }
        }
    }
    supervisor.stopped("requested");
    Ok(())
}

async fn run_background_live_collection_with_runner_for_test<F, Fut>(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    retry_policy: LiveRetryPolicy,
    runner: F,
) -> std::result::Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<StartLiveMarketDataResponse, String>>,
{
    run_background_live_collection_with_runner(supervisor, store, retry_policy, runner).await
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
        BarterMarketDataKind::FundingRate => "funding_rate",
        BarterMarketDataKind::OpenInterest => "open_interest",
        BarterMarketDataKind::MarkPrice => "mark_price",
        BarterMarketDataKind::IndexPrice => "index_price",
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

fn record_to_candle_record(record: StorageWriteRecord) -> MarketDataCandleRecord {
    let payload = serde_json::from_slice(&record.value).unwrap_or_else(|_| serde_json::Value::Null);
    MarketDataCandleRecord {
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

fn test_candle_envelope(symbol: &str) -> BarterIngestionEnvelope {
    let now = TimestampNs::now();
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Candle,
        timestamp: now,
        received_at: now,
        payload: BarterMarketPayload::Candle(CandlePayload {
            interval: Some("1m".to_string()),
            open_time: now,
            close_time: TimestampNs::from_nanos(now.as_nanos() + 60_000_000_000),
            open: Price::new(Decimal::new(42_000_00, 2)),
            high: Price::new(Decimal::new(42_100_00, 2)),
            low: Price::new(Decimal::new(41_900_00, 2)),
            close: Price::new(Decimal::new(42_050_00, 2)),
            volume: Decimal::new(25, 1),
            trade_count: Some(100),
            quote_volume: Some(Decimal::new(1_000_000, 2)),
        }),
        sequence: Some("seq-test-candle".to_string()),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = "env-test-candle".to_string();
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProductionServerState, ServerRuntimeConfig};

    fn scheduler_resume_request() -> MarketDataStorageMaintenanceSchedulerResumeRequest {
        MarketDataStorageMaintenanceSchedulerResumeRequest {
            confirm: STORAGE_MAINTENANCE_SCHEDULER_RESUME_CONFIRMATION.to_string(),
            reason: Some("unit-test".to_string()),
        }
    }

    fn scheduler_resume_config(
        extra: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ServerRuntimeConfig {
        ServerRuntimeConfig::from_env_pairs(extra).expect("config parses")
    }

    async fn suppress_scheduler(state: &ProductionServerState) {
        let scheduler = state.market_data_storage_maintenance_scheduler();
        assert!(scheduler.mark_started(chrono::Utc::now()).await);
        scheduler
            .mark_failed(
                chrono::Utc::now(),
                chrono::Utc::now() + chrono::Duration::seconds(60),
                "simulated failure",
            )
            .await;
        assert!(scheduler.is_suppressed().await);
    }

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

    #[test]
    fn durable_path_readiness_reports_missing_parent_without_creating_paths() {
        let path = std::env::temp_dir()
            .join(format!(
                "fdc-server-missing-parent-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .join("l2.redb");

        let readiness = durable_path_readiness(Some(path.as_path()));

        assert_eq!(readiness.durable_path_configured, true);
        assert_eq!(readiness.path_hint.as_deref(), Some("l2.redb"));
        assert_eq!(readiness.path_exists, Some(false));
        assert_eq!(readiness.path_parent_exists, Some(false));
        assert_eq!(readiness.path_parent_writable, Some(false));
        assert!(!path.exists());
        assert!(!path.parent().unwrap().exists());
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

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_is_disabled_by_default() {
        let state = ProductionServerState::new(scheduler_resume_config([]));

        let result = resume_storage_maintenance_scheduler(&state, scheduler_resume_request()).await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Forbidden);
        assert!(!result.response.accepted);
        assert_eq!(result.response.status, "disabled");
        assert!(!result.response.task_started);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_requires_confirmation() {
        let state = ProductionServerState::new(scheduler_resume_config([(
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
            "1",
        )]));

        let result = resume_storage_maintenance_scheduler(
            &state,
            MarketDataStorageMaintenanceSchedulerResumeRequest {
                confirm: "wrong".to_string(),
                reason: Some("unit-test".to_string()),
            },
        )
        .await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::BadRequest);
        assert!(!result.response.accepted);
        assert_eq!(result.response.status, "confirmation_required");
        assert!(!result.response.task_started);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_requires_scheduler_enabled() {
        let state = ProductionServerState::new(scheduler_resume_config([(
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
            "1",
        )]));

        let result = resume_storage_maintenance_scheduler(&state, scheduler_resume_request()).await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Forbidden);
        assert!(!result.response.accepted);
        assert_eq!(result.response.status, "scheduler_disabled");
        assert!(!result.response.task_started);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_rejects_memory_backend() {
        let state = ProductionServerState::new(scheduler_resume_config([
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
                "1",
            ),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ]));

        let result = resume_storage_maintenance_scheduler(&state, scheduler_resume_request()).await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::BadRequest);
        assert!(!result.response.accepted);
        assert_eq!(result.response.status, "unsupported_backend");
        assert!(!result.response.task_started);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_rejects_already_running_task() {
        let state = ProductionServerState::new(scheduler_resume_config([
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
                "1",
            ),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "1",
            ),
        ]));
        suppress_scheduler(&state).await;
        assert!(
            state
                .market_data_storage_maintenance_scheduler_task()
                .try_store(tokio::spawn(async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }))
                .await
        );

        let result = resume_storage_maintenance_scheduler(&state, scheduler_resume_request()).await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Conflict);
        assert!(!result.response.accepted);
        assert_eq!(result.response.status, "already_running");
        assert_eq!(result.response.previous_consecutive_failures, 1);
        assert_eq!(result.response.consecutive_failures, 1);
        assert!(!result.response.task_started);
        assert!(
            state
                .market_data_storage_maintenance_scheduler()
                .is_suppressed()
                .await
        );
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_starts_task_after_suppression() {
        let state = ProductionServerState::new(scheduler_resume_config([
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
                "1",
            ),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
                "7200",
            ),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
                "3600",
            ),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "1",
            ),
        ]));
        suppress_scheduler(&state).await;

        let result = resume_storage_maintenance_scheduler(&state, scheduler_resume_request()).await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Ok);
        assert!(result.response.accepted);
        assert_eq!(result.response.status, "resumed");
        assert_eq!(result.response.previous_consecutive_failures, 1);
        assert_eq!(result.response.consecutive_failures, 0);
        assert!(result.response.task_started);
        assert!(
            state
                .market_data_storage_maintenance_scheduler_task()
                .is_active()
                .await
        );
        assert!(
            !state
                .market_data_storage_maintenance_scheduler()
                .is_suppressed()
                .await
        );
    }
}

#[cfg(test)]
mod live_retry_tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn live_retry_loop_suppresses_after_configured_failures() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_LIVE_ENABLED", "1"),
            ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "100"),
            ("FDC_LIVE_RETRY_MAX_DELAY_MS", "100"),
            ("FDC_LIVE_MAX_CONSECUTIVE_FAILURES", "2"),
        ])
        .expect("config should parse");
        let state = ProductionServerState::new(config);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_runner = attempts.clone();

        let result = run_background_live_collection_with_runner_for_test(
            state.market_data_supervisor(),
            state.market_data_store(),
            LiveRetryPolicy::from_config(state.config()),
            move || {
                attempts_for_runner.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err("network down".to_string()) })
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let status = state.market_data_supervisor().status();
        assert_eq!(status.state, MarketDataLiveState::Suppressed);
        assert_eq!(status.consecutive_failures, 2);
    }
}
