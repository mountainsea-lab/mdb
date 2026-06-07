use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use fdc_storage::{QueryableMarketDataStore, StorageMaintenanceOptions};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::{
    market_data::maintenance_audit::MarketDataStorageMaintenanceAuditLog,
    MarketDataStorageBackendConfig, ServerRuntimeConfig,
};

const SCHEDULER_STATUS_FAILED: &str = "failed";
const SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES: &str = "suppressed_after_failures";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerSnapshot {
    pub enabled: bool,
    pub running: bool,
    pub backend: String,
    pub tiered: bool,
    pub interval_seconds: u64,
    pub timeout_ms: u64,
    pub jitter_seconds: u64,
    pub max_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub total_runs: u64,
    pub successful_runs: u64,
    pub failed_runs: u64,
    pub skipped_runs: u64,
    pub last_started_at: Option<DateTime<Utc>>,
    pub last_finished_at: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub next_run_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct StorageMaintenanceSchedulerStateInner {
    snapshot: StorageMaintenanceSchedulerSnapshot,
}

#[derive(Debug, Clone)]
pub struct StorageMaintenanceSchedulerState {
    inner: Arc<Mutex<StorageMaintenanceSchedulerStateInner>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMaintenanceSchedulerAttemptResult {
    Completed,
    Unsupported,
    Failed(String),
}

impl StorageMaintenanceSchedulerState {
    pub fn from_config(config: &ServerRuntimeConfig) -> Self {
        let backend = config.market_data_storage.backend;
        let tiered = backend == MarketDataStorageBackendConfig::Tiered;
        let mut snapshot = StorageMaintenanceSchedulerSnapshot {
            enabled: config.market_data_storage_maintenance_scheduler_enabled,
            running: false,
            backend: scheduler_backend_label(backend).to_string(),
            tiered,
            interval_seconds: config.market_data_storage_maintenance_scheduler_interval_seconds,
            timeout_ms: config.market_data_storage_maintenance_scheduler_timeout_ms,
            jitter_seconds: config.market_data_storage_maintenance_scheduler_jitter_seconds,
            max_consecutive_failures: config
                .market_data_storage_maintenance_scheduler_max_consecutive_failures,
            consecutive_failures: 0,
            total_runs: 0,
            successful_runs: 0,
            failed_runs: 0,
            skipped_runs: 0,
            last_started_at: None,
            last_finished_at: None,
            last_status: None,
            last_error: None,
            next_run_at: None,
        };

        if snapshot.enabled && !tiered {
            snapshot.skipped_runs = 1;
            snapshot.last_status = Some("unsupported_backend".to_string());
        }

        Self {
            inner: Arc::new(Mutex::new(StorageMaintenanceSchedulerStateInner {
                snapshot,
            })),
        }
    }

    pub async fn snapshot(&self) -> StorageMaintenanceSchedulerSnapshot {
        self.inner.lock().await.snapshot.clone()
    }

    pub async fn is_suppressed(&self) -> bool {
        let guard = self.inner.lock().await;
        scheduler_snapshot_is_suppressed(&guard.snapshot)
    }

    pub async fn mark_suppressed(&self) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.last_status = Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
        guard.snapshot.next_run_at = None;
    }

    pub async fn mark_next_run_at(&self, next_run_at: DateTime<Utc>) {
        let mut guard = self.inner.lock().await;
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.next_run_at = None;
            guard.snapshot.last_status =
                Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            return;
        }
        guard.snapshot.next_run_at = Some(next_run_at);
    }

    pub async fn mark_started(&self, started_at: DateTime<Utc>) -> bool {
        let mut guard = self.inner.lock().await;
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.running = false;
            guard.snapshot.last_status =
                Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            guard.snapshot.next_run_at = None;
            return false;
        }
        if guard.snapshot.running {
            guard.snapshot.skipped_runs += 1;
            guard.snapshot.last_status = Some("skipped_overlap".to_string());
            return false;
        }
        guard.snapshot.running = true;
        guard.snapshot.total_runs += 1;
        guard.snapshot.last_started_at = Some(started_at);
        guard.snapshot.last_status = Some("running".to_string());
        guard.snapshot.last_error = None;
        true
    }

    pub async fn mark_completed(&self, finished_at: DateTime<Utc>, next_run_at: DateTime<Utc>) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.successful_runs += 1;
        guard.snapshot.consecutive_failures = 0;
        guard.snapshot.last_finished_at = Some(finished_at);
        guard.snapshot.last_status = Some("completed".to_string());
        guard.snapshot.last_error = None;
        guard.snapshot.next_run_at = Some(next_run_at);
    }

    pub async fn mark_failed(
        &self,
        finished_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
        error: impl Into<String>,
    ) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.failed_runs += 1;
        guard.snapshot.consecutive_failures += 1;
        guard.snapshot.last_finished_at = Some(finished_at);
        guard.snapshot.last_error = Some(sanitize_scheduler_error(error));
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.last_status =
                Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            guard.snapshot.next_run_at = None;
        } else {
            guard.snapshot.last_status = Some(SCHEDULER_STATUS_FAILED.to_string());
            guard.snapshot.next_run_at = Some(next_run_at);
        }
    }

    pub async fn mark_unsupported(&self, finished_at: DateTime<Utc>, next_run_at: DateTime<Utc>) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.skipped_runs += 1;
        guard.snapshot.last_finished_at = Some(finished_at);
        guard.snapshot.last_status = Some("unsupported_backend".to_string());
        guard.snapshot.next_run_at = Some(next_run_at);
    }

    pub fn should_spawn(&self, config: &ServerRuntimeConfig) -> bool {
        config.market_data_storage_maintenance_scheduler_enabled
            && config.market_data_storage.backend == MarketDataStorageBackendConfig::Tiered
    }
}

fn scheduler_snapshot_is_suppressed(snapshot: &StorageMaintenanceSchedulerSnapshot) -> bool {
    snapshot.enabled
        && snapshot.tiered
        && snapshot.consecutive_failures >= snapshot.max_consecutive_failures
}

pub fn scheduler_interval(config: &ServerRuntimeConfig) -> Duration {
    Duration::from_secs(config.market_data_storage_maintenance_scheduler_interval_seconds)
}

pub fn scheduler_timeout(config: &ServerRuntimeConfig) -> Duration {
    Duration::from_millis(config.market_data_storage_maintenance_scheduler_timeout_ms)
}

pub fn spawn_storage_maintenance_scheduler(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) -> Option<JoinHandle<()>> {
    if !state.should_spawn(&config) {
        return None;
    }

    Some(tokio::spawn(async move {
        run_scheduler_loop(config, store, audit, state).await;
    }))
}

async fn run_scheduler_loop(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) {
    let first_delay = if config.market_data_storage_maintenance_scheduler_jitter_seconds == 0 {
        Duration::from_millis(10)
    } else {
        Duration::from_secs(config.market_data_storage_maintenance_scheduler_jitter_seconds)
    };
    let interval = scheduler_interval(&config);
    let mut next_delay = first_delay;
    loop {
        if state.is_suppressed().await {
            state.mark_suppressed().await;
            break;
        }
        let next_run_at = Utc::now()
            + chrono::Duration::from_std(next_delay)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
        state.mark_next_run_at(next_run_at).await;
        tokio::time::sleep(next_delay).await;
        run_scheduler_attempt(
            &config,
            Arc::clone(&store),
            Arc::clone(&audit),
            state.clone(),
        )
        .await;
        if state.is_suppressed().await {
            state.mark_suppressed().await;
            break;
        }
        next_delay = interval;
    }
}

pub async fn run_scheduler_attempt(
    config: &ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) {
    run_scheduler_attempt_with_executor(config, audit, state, move |options| {
        let store = Arc::clone(&store);
        async move {
            match store.run_maintenance_once_with_options(options).await {
                Ok(Some(_report)) => StorageMaintenanceSchedulerAttemptResult::Completed,
                Ok(None) => StorageMaintenanceSchedulerAttemptResult::Unsupported,
                Err(error) => StorageMaintenanceSchedulerAttemptResult::Failed(format!("{error}")),
            }
        }
    })
    .await;
}

pub async fn run_scheduler_attempt_with_executor<F, Fut>(
    config: &ServerRuntimeConfig,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
    executor: F,
) where
    F: FnOnce(StorageMaintenanceOptions) -> Fut,
    Fut: std::future::Future<Output = StorageMaintenanceSchedulerAttemptResult>,
{
    let started_at = Utc::now();
    if !state.mark_started(started_at).await {
        return;
    }

    let next_run_at = Utc::now()
        + chrono::Duration::from_std(scheduler_interval(config))
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
    let options = StorageMaintenanceOptions::default()
        .with_timeout(scheduler_timeout(config))
        .with_audit_sink(audit);

    match executor(options).await {
        StorageMaintenanceSchedulerAttemptResult::Completed => {
            state.mark_completed(Utc::now(), next_run_at).await;
        }
        StorageMaintenanceSchedulerAttemptResult::Unsupported => {
            state.mark_unsupported(Utc::now(), next_run_at).await;
        }
        StorageMaintenanceSchedulerAttemptResult::Failed(error) => {
            state.mark_failed(Utc::now(), next_run_at, error).await;
        }
    }
}

pub fn scheduler_backend_label(backend: MarketDataStorageBackendConfig) -> &'static str {
    match backend {
        MarketDataStorageBackendConfig::Memory => "memory",
        MarketDataStorageBackendConfig::Tiered => "tiered",
    }
}

fn sanitize_scheduler_error(error: impl Into<String>) -> String {
    let sanitized: String = error
        .into()
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    const MAX_LEN: usize = 240;
    if sanitized.len() <= MAX_LEN {
        return sanitized;
    }
    format!("{}...", &sanitized[..MAX_LEN])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn storage_maintenance_scheduler_state_defaults_disabled() {
        let config =
            ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let snapshot = state.snapshot().await;
        assert!(!snapshot.enabled);
        assert!(!snapshot.running);
        assert_eq!(snapshot.backend, "memory");
        assert!(!snapshot.tiered);
        assert_eq!(snapshot.interval_seconds, 3600);
        assert_eq!(snapshot.timeout_ms, 30000);
        assert_eq!(snapshot.total_runs, 0);
        assert_eq!(snapshot.skipped_runs, 0);
        assert!(snapshot.next_run_at.is_none());
        assert!(!state.should_spawn(&config));
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_state_marks_memory_enabled_unsupported() {
        let config = ServerRuntimeConfig::from_env_pairs([(
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED",
            "1",
        )])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let snapshot = state.snapshot().await;
        assert!(snapshot.enabled);
        assert_eq!(snapshot.backend, "memory");
        assert_eq!(snapshot.skipped_runs, 1);
        assert_eq!(snapshot.last_status.as_deref(), Some("unsupported_backend"));
        assert!(!state.should_spawn(&config));
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_state_tracks_started_completed_and_overlap() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        assert!(state.should_spawn(&config));

        let started = Utc::now();
        assert!(state.mark_started(started).await);
        assert!(!state.mark_started(started).await);
        let overlap_snapshot = state.snapshot().await;
        assert_eq!(overlap_snapshot.skipped_runs, 1);
        assert_eq!(
            overlap_snapshot.last_status.as_deref(),
            Some("skipped_overlap")
        );
        let next_run_at = Utc::now();
        state.mark_completed(Utc::now(), next_run_at).await;

        let snapshot = state.snapshot().await;
        assert!(!snapshot.running);
        assert_eq!(snapshot.total_runs, 1);
        assert_eq!(snapshot.successful_runs, 1);
        assert_eq!(snapshot.skipped_runs, 1);
        assert_eq!(snapshot.last_status.as_deref(), Some("completed"));
        assert!(snapshot.next_run_at.is_some());
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_state_suppresses_after_failure_threshold() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "2",
            ),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let first_started_at = Utc::now();
        let first_finished_at = first_started_at + chrono::Duration::milliseconds(5);
        let first_next_run_at = first_finished_at + chrono::Duration::seconds(60);

        assert!(state.mark_started(first_started_at).await);
        state
            .mark_failed(first_finished_at, first_next_run_at, "first failure")
            .await;
        let first = state.snapshot().await;
        assert!(!first.running);
        assert_eq!(first.total_runs, 1);
        assert_eq!(first.failed_runs, 1);
        assert_eq!(first.consecutive_failures, 1);
        assert_eq!(first.last_status.as_deref(), Some("failed"));
        assert_eq!(first.last_error.as_deref(), Some("first failure"));
        assert_eq!(first.next_run_at, Some(first_next_run_at));
        assert!(!state.is_suppressed().await);

        let second_started_at = first_next_run_at;
        let second_finished_at = second_started_at + chrono::Duration::milliseconds(5);
        let second_next_run_at = second_finished_at + chrono::Duration::seconds(60);
        assert!(state.mark_started(second_started_at).await);
        state
            .mark_failed(
                second_finished_at,
                second_next_run_at,
                "second failure with\ncontrol\tcharacters",
            )
            .await;
        let second = state.snapshot().await;
        assert!(!second.running);
        assert_eq!(second.total_runs, 2);
        assert_eq!(second.failed_runs, 2);
        assert_eq!(second.consecutive_failures, 2);
        assert_eq!(
            second.last_status.as_deref(),
            Some("suppressed_after_failures")
        );
        assert_eq!(
            second.last_error.as_deref(),
            Some("second failure with control characters")
        );
        assert!(second.next_run_at.is_none());
        assert!(state.is_suppressed().await);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_error_sanitization_bounds_status_text() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let started_at = Utc::now();
        let finished_at = started_at + chrono::Duration::milliseconds(1);
        let next_run_at = finished_at + chrono::Duration::seconds(60);
        let long_error = format!("{}\n{}", "x".repeat(260), "secret line");

        assert!(state.mark_started(started_at).await);
        state
            .mark_failed(finished_at, next_run_at, long_error)
            .await;

        let snapshot = state.snapshot().await;
        let error = snapshot.last_error.expect("last error should be recorded");
        assert!(error.len() <= 243, "error was not bounded: {error}");
        assert!(
            error.ends_with("..."),
            "error should include truncation marker"
        );
        assert!(!error.contains('\n'));
        assert!(!error.contains('\t'));
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_attempts_stop_after_suppression() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "1",
            ),
        ])
        .expect("config parses");
        let audit = Arc::new(MarketDataStorageMaintenanceAuditLog::new(10));
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let calls = Arc::new(AtomicUsize::new(0));

        let first_calls = Arc::clone(&calls);
        run_scheduler_attempt_with_executor(
            &config,
            Arc::clone(&audit),
            state.clone(),
            move |_options| {
                let first_calls = Arc::clone(&first_calls);
                async move {
                    first_calls.fetch_add(1, Ordering::SeqCst);
                    StorageMaintenanceSchedulerAttemptResult::Failed(
                        "simulated failure".to_string(),
                    )
                }
            },
        )
        .await;

        let second_calls = Arc::clone(&calls);
        run_scheduler_attempt_with_executor(
            &config,
            Arc::clone(&audit),
            state.clone(),
            move |_options| {
                let second_calls = Arc::clone(&second_calls);
                async move {
                    second_calls.fetch_add(1, Ordering::SeqCst);
                    StorageMaintenanceSchedulerAttemptResult::Completed
                }
            },
        )
        .await;

        let snapshot = state.snapshot().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.total_runs, 1);
        assert_eq!(snapshot.failed_runs, 1);
        assert_eq!(snapshot.successful_runs, 0);
        assert_eq!(snapshot.consecutive_failures, 1);
        assert_eq!(
            snapshot.last_status.as_deref(),
            Some("suppressed_after_failures")
        );
        assert!(snapshot.next_run_at.is_none());
        assert!(audit.recent(10).await.entries.is_empty());
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_attempt_executor_success_resets_failures() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "2",
            ),
        ])
        .expect("config parses");
        let audit = Arc::new(MarketDataStorageMaintenanceAuditLog::new(10));
        let state = StorageMaintenanceSchedulerState::from_config(&config);

        run_scheduler_attempt_with_executor(
            &config,
            Arc::clone(&audit),
            state.clone(),
            |_options| async {
                StorageMaintenanceSchedulerAttemptResult::Failed("temporary failure".to_string())
            },
        )
        .await;
        run_scheduler_attempt_with_executor(
            &config,
            Arc::clone(&audit),
            state.clone(),
            |_options| async { StorageMaintenanceSchedulerAttemptResult::Completed },
        )
        .await;

        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.total_runs, 2);
        assert_eq!(snapshot.failed_runs, 1);
        assert_eq!(snapshot.successful_runs, 1);
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.last_status.as_deref(), Some("completed"));
        assert!(snapshot.last_error.is_none());
        assert!(snapshot.next_run_at.is_some());
        assert!(!state.is_suppressed().await);
    }
}
