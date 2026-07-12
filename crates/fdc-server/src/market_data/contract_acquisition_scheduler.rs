use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use fdc_storage::QueryableMarketDataStore;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    market_data::contract_acquisition::{
        run_binance_futures_usd_contract_candle_acquisition_once, ContractAcquisitionRunStatus,
    },
    runtime::config::ServerRuntimeConfig,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractAcquisitionSchedulerSnapshot {
    pub enabled: bool,
    pub running: bool,
    pub suppressed: bool,
    pub interval_seconds: u64,
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
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
}

#[derive(Debug, Clone)]
pub struct ContractAcquisitionSchedulerState {
    inner: Arc<Mutex<ContractAcquisitionSchedulerInner>>,
}

#[derive(Debug, Clone)]
struct ContractAcquisitionSchedulerInner {
    enabled: bool,
    running: bool,
    suppressed: bool,
    interval_seconds: u64,
    jitter_seconds: u64,
    max_consecutive_failures: u32,
    consecutive_failures: u32,
    total_runs: u64,
    successful_runs: u64,
    failed_runs: u64,
    skipped_runs: u64,
    last_started_at: Option<DateTime<Utc>>,
    last_finished_at: Option<DateTime<Utc>>,
    last_status: Option<String>,
    last_error: Option<String>,
    next_run_at: Option<DateTime<Utc>>,
    last_run: ContractAcquisitionRunStatus,
}

impl ContractAcquisitionSchedulerState {
    pub fn from_config(config: &ServerRuntimeConfig) -> Self {
        let contract_config = &config.market_data_contract_acquisition;
        Self {
            inner: Arc::new(Mutex::new(ContractAcquisitionSchedulerInner {
                enabled: contract_config.enabled && contract_config.scheduler_enabled,
                running: false,
                suppressed: false,
                interval_seconds: contract_config.scheduler_interval_seconds,
                jitter_seconds: contract_config.scheduler_jitter_seconds,
                max_consecutive_failures: contract_config.scheduler_max_consecutive_failures,
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
                last_run: ContractAcquisitionRunStatus::default(),
            })),
        }
    }

    pub fn should_spawn(&self, config: &ServerRuntimeConfig) -> bool {
        config.market_data_contract_acquisition.enabled
            && config.market_data_contract_acquisition.scheduler_enabled
    }

    pub async fn snapshot(&self) -> ContractAcquisitionSchedulerSnapshot {
        let guard = self.inner.lock().await;
        ContractAcquisitionSchedulerSnapshot {
            enabled: guard.enabled,
            running: guard.running,
            suppressed: guard.suppressed,
            interval_seconds: guard.interval_seconds,
            jitter_seconds: guard.jitter_seconds,
            max_consecutive_failures: guard.max_consecutive_failures,
            consecutive_failures: guard.consecutive_failures,
            total_runs: guard.total_runs,
            successful_runs: guard.successful_runs,
            failed_runs: guard.failed_runs,
            skipped_runs: guard.skipped_runs,
            last_started_at: guard.last_started_at,
            last_finished_at: guard.last_finished_at,
            last_status: guard.last_status.clone(),
            last_error: guard.last_error.clone(),
            next_run_at: guard.next_run_at,
            tasks_started: guard.last_run.tasks_started,
            tasks_completed: guard.last_run.tasks_completed,
            pages_fetched: guard.last_run.pages_fetched,
            envelopes_received: guard.last_run.envelopes_received,
            storage_records_written: guard.last_run.storage_records_written,
            audit_records_written: guard.last_run.audit_records_written,
        }
    }

    pub async fn mark_next_run_at(&self, next_run_at: DateTime<Utc>) {
        self.inner.lock().await.next_run_at = Some(next_run_at);
    }

    pub async fn mark_started(&self, started_at: DateTime<Utc>) -> bool {
        let mut guard = self.inner.lock().await;
        if guard.running {
            guard.skipped_runs += 1;
            guard.last_status = Some("skipped_overlap".to_string());
            return false;
        }
        if guard.suppressed {
            guard.last_status = Some("suppressed".to_string());
            return false;
        }
        guard.running = true;
        guard.total_runs += 1;
        guard.last_started_at = Some(started_at);
        guard.last_status = Some("running".to_string());
        true
    }

    pub async fn mark_completed(
        &self,
        finished_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
        status: ContractAcquisitionRunStatus,
    ) {
        let mut guard = self.inner.lock().await;
        guard.running = false;
        guard.consecutive_failures = 0;
        guard.successful_runs += 1;
        guard.last_finished_at = Some(finished_at);
        guard.next_run_at = Some(next_run_at);
        guard.last_status = Some("completed".to_string());
        guard.last_error = None;
        guard.last_run = status;
    }

    pub async fn mark_failed(
        &self,
        finished_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
        error: impl Into<String>,
    ) {
        let mut guard = self.inner.lock().await;
        guard.running = false;
        guard.failed_runs += 1;
        guard.consecutive_failures += 1;
        guard.last_finished_at = Some(finished_at);
        guard.next_run_at = Some(next_run_at);
        guard.last_error = Some(sanitize_scheduler_error(error));
        if guard.consecutive_failures >= guard.max_consecutive_failures {
            guard.suppressed = true;
            guard.last_status = Some("suppressed".to_string());
        } else {
            guard.last_status = Some("failed".to_string());
        }
    }

    pub async fn mark_suppressed(&self) {
        let mut guard = self.inner.lock().await;
        guard.running = false;
        guard.suppressed = true;
        guard.last_status = Some("suppressed".to_string());
        guard.next_run_at = None;
    }

    pub async fn is_suppressed(&self) -> bool {
        self.inner.lock().await.suppressed
    }
}

#[derive(Debug, Clone, Default)]
pub struct ContractAcquisitionSchedulerTaskHandle {
    inner: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl ContractAcquisitionSchedulerTaskHandle {
    pub async fn try_store(&self, handle: JoinHandle<()>) -> bool {
        let mut guard = self.inner.lock().await;
        if guard.is_some() {
            handle.abort();
            return false;
        }
        *guard = Some(handle);
        true
    }

    pub async fn abort_active(&self) {
        let mut guard = self.inner.lock().await;
        if let Some(handle) = guard.take() {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractAcquisitionSchedulerAttemptResult {
    Completed(ContractAcquisitionRunStatus),
    Failed(String),
}

pub fn scheduler_interval(config: &ServerRuntimeConfig) -> Duration {
    Duration::from_secs(
        config
            .market_data_contract_acquisition
            .scheduler_interval_seconds,
    )
}

pub fn scheduler_initial_delay(config: &ServerRuntimeConfig) -> Duration {
    if config
        .market_data_contract_acquisition
        .scheduler_jitter_seconds
        == 0
    {
        Duration::from_millis(10)
    } else {
        Duration::from_secs(
            config
                .market_data_contract_acquisition
                .scheduler_jitter_seconds,
        )
    }
}

pub async fn spawn_contract_acquisition_scheduler_into_handle(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    state: ContractAcquisitionSchedulerState,
    task_handle: ContractAcquisitionSchedulerTaskHandle,
) -> bool {
    if !state.should_spawn(&config) {
        return false;
    }

    let handle = tokio::spawn(async move {
        run_scheduler_loop(config, store, state).await;
    });
    task_handle.try_store(handle).await
}

async fn run_scheduler_loop(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    state: ContractAcquisitionSchedulerState,
) {
    let mut next_delay = scheduler_initial_delay(&config);
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
        run_scheduler_attempt(&config, Arc::clone(&store), state.clone()).await;
        if state.is_suppressed().await {
            state.mark_suppressed().await;
            break;
        }
        next_delay = scheduler_interval(&config);
    }
}

pub async fn run_scheduler_attempt(
    config: &ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    state: ContractAcquisitionSchedulerState,
) {
    let acquisition_config = config.market_data_contract_acquisition.clone();
    run_scheduler_attempt_with_executor(config, state, move || async move {
        match run_binance_futures_usd_contract_candle_acquisition_once(
            &acquisition_config,
            store.as_ref(),
        )
        .await
        {
            Ok(status) => ContractAcquisitionSchedulerAttemptResult::Completed(status),
            Err(error) => ContractAcquisitionSchedulerAttemptResult::Failed(error.to_string()),
        }
    })
    .await;
}

pub async fn run_scheduler_attempt_with_executor<F, Fut>(
    config: &ServerRuntimeConfig,
    state: ContractAcquisitionSchedulerState,
    executor: F,
) where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ContractAcquisitionSchedulerAttemptResult>,
{
    let started_at = Utc::now();
    if !state.mark_started(started_at).await {
        return;
    }

    let next_run_at = Utc::now()
        + chrono::Duration::from_std(scheduler_interval(config))
            .unwrap_or_else(|_| chrono::Duration::seconds(0));

    match executor().await {
        ContractAcquisitionSchedulerAttemptResult::Completed(status) => {
            state.mark_completed(Utc::now(), next_run_at, status).await;
        }
        ContractAcquisitionSchedulerAttemptResult::Failed(error) => {
            state.mark_failed(Utc::now(), next_run_at, error).await;
        }
    }
}

fn sanitize_scheduler_error(error: impl Into<String>) -> String {
    let sanitized = error
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
    use crate::runtime::config::ServerRuntimeConfig;

    #[tokio::test]
    async fn contract_scheduler_state_defaults_disabled() {
        let config =
            ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config parses");
        let state = ContractAcquisitionSchedulerState::from_config(&config);
        let snapshot = state.snapshot().await;

        assert!(!snapshot.enabled);
        assert!(!snapshot.running);
        assert!(!snapshot.suppressed);
        assert_eq!(snapshot.interval_seconds, 3600);
        assert_eq!(snapshot.max_consecutive_failures, 3);
        assert!(!state.should_spawn(&config));
    }

    #[tokio::test]
    async fn contract_scheduler_state_spawns_only_when_contracts_and_scheduler_enabled() {
        let scheduler_only = ServerRuntimeConfig::from_env_pairs([(
            "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED",
            "1",
        )])
        .expect("config parses");
        let scheduler_only_state = ContractAcquisitionSchedulerState::from_config(&scheduler_only);
        assert!(!scheduler_only_state.should_spawn(&scheduler_only));

        let enabled = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
            ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
        ])
        .expect("config parses");
        let enabled_state = ContractAcquisitionSchedulerState::from_config(&enabled);
        assert!(enabled_state.should_spawn(&enabled));
    }

    #[tokio::test]
    async fn contract_scheduler_attempt_tracks_success_failure_suppression_and_overlap() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
            ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
            (
                "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "2",
            ),
        ])
        .expect("config parses");
        let state = ContractAcquisitionSchedulerState::from_config(&config);

        assert!(state.mark_started(chrono::Utc::now()).await);
        assert!(!state.mark_started(chrono::Utc::now()).await);
        state
            .mark_completed(
                chrono::Utc::now(),
                chrono::Utc::now(),
                ContractAcquisitionRunStatus {
                    tasks_started: 1,
                    tasks_completed: 1,
                    pages_fetched: 1,
                    envelopes_received: 2,
                    storage_records_written: 2,
                    audit_records_written: 1,
                    final_cursors: Vec::new(),
                },
            )
            .await;

        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.total_runs, 1);
        assert_eq!(snapshot.successful_runs, 1);
        assert_eq!(snapshot.skipped_runs, 1);
        assert_eq!(snapshot.last_status.as_deref(), Some("completed"));
        assert_eq!(snapshot.storage_records_written, 2);

        assert!(state.mark_started(chrono::Utc::now()).await);
        state
            .mark_failed(chrono::Utc::now(), chrono::Utc::now(), "network down")
            .await;
        assert!(state.mark_started(chrono::Utc::now()).await);
        state
            .mark_failed(chrono::Utc::now(), chrono::Utc::now(), "network down again")
            .await;

        let snapshot = state.snapshot().await;
        assert!(snapshot.suppressed);
        assert_eq!(snapshot.failed_runs, 2);
        assert_eq!(snapshot.consecutive_failures, 2);
        assert_eq!(snapshot.last_status.as_deref(), Some("suppressed"));
    }
}
