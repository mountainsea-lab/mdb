use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use crate::{MarketDataStorageBackendConfig, ServerRuntimeConfig};

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

    pub async fn mark_next_run_at(&self, next_run_at: DateTime<Utc>) {
        self.inner.lock().await.snapshot.next_run_at = Some(next_run_at);
    }

    pub async fn mark_started(&self, started_at: DateTime<Utc>) -> bool {
        let mut guard = self.inner.lock().await;
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
        guard.snapshot.last_status = Some("failed".to_string());
        guard.snapshot.last_error = Some(sanitize_scheduler_error(error));
        guard.snapshot.next_run_at = Some(next_run_at);
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

pub fn scheduler_interval(config: &ServerRuntimeConfig) -> Duration {
    Duration::from_secs(config.market_data_storage_maintenance_scheduler_interval_seconds)
}

pub fn scheduler_timeout(config: &ServerRuntimeConfig) -> Duration {
    Duration::from_millis(config.market_data_storage_maintenance_scheduler_timeout_ms)
}

pub fn scheduler_backend_label(backend: MarketDataStorageBackendConfig) -> &'static str {
    match backend {
        MarketDataStorageBackendConfig::Memory => "memory",
        MarketDataStorageBackendConfig::Tiered => "tiered",
    }
}

fn sanitize_scheduler_error(error: impl Into<String>) -> String {
    let error = error.into();
    const MAX_LEN: usize = 240;
    if error.len() <= MAX_LEN {
        return error;
    }
    format!("{}...", &error[..MAX_LEN])
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
}
