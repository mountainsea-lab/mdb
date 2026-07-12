# Contract Candle Long-Term Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-process long-term scheduler that repeatedly maintains Binance Futures USD perpetual OHLCV/candle acquisition using the existing canonical storage path.

**Architecture:** Add contract scheduler config to `ServerRuntimeConfig`, implement a focused `contract_acquisition_scheduler` module that reuses existing contract acquisition run-once logic, wire scheduler state into `ProductionServerState`, expose HTTP scheduler status, and update runbook docs. Canonical candle data continues to flow through `fdc-barter -> fdc-orchestrator -> fdc-storage -> candles`; scheduler state remains runtime-only.

**Tech Stack:** Rust, Tokio, Axum, serde, existing `fdc-server`, `fdc-barter`, `fdc-storage`, targeted cargo contract tests.

---

## File Structure

- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add contract scheduler runtime fields and env parsing.
- Create `crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs`
  - Own scheduler state, snapshot DTO-internal type, task handle, loop and attempt helpers.
- Modify `crates/fdc-server/src/market_data/mod.rs`
  - Export the new scheduler module.
- Modify `crates/fdc-server/src/runtime/app.rs`
  - Store scheduler state/task handle, spawn scheduler in `try_new`, expose accessors.
- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add `MarketDataContractAcquisitionSchedulerStatusResponse`.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Add `contract_acquisition_scheduler_status` mapper.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add `GET /market-data/contracts/acquisition/scheduler/status`.
- Modify tests:
  - `crates/fdc-server/tests/runtime_config_contract.rs`
  - `crates/fdc-server/tests/contract_acquisition_contract.rs`
  - `crates/fdc-server/tests/production_server_router_contract.rs`
- Modify `docs/runbooks/market-data-production-runbook.md`
  - Document scheduler operator flow.

---

## Task 1: Add Runtime Config for Contract Scheduler

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Test: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Write failing config tests**

Add tests near existing `contract_acquisition_config_*` tests in `runtime_config_contract.rs`:

```rust
#[test]
fn contract_acquisition_scheduler_config_defaults_disabled() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("defaults should parse");

    assert!(!config.market_data_contract_acquisition.scheduler_enabled);
    assert_eq!(config.market_data_contract_acquisition.scheduler_interval_seconds, 3600);
    assert_eq!(config.market_data_contract_acquisition.scheduler_jitter_seconds, 0);
    assert_eq!(
        config
            .market_data_contract_acquisition
            .scheduler_max_consecutive_failures,
        3
    );
}

#[test]
fn contract_acquisition_scheduler_config_accepts_valid_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS", "120"),
        ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS", "15"),
        (
            "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "5",
        ),
    ])
    .expect("contract scheduler config should parse");

    assert!(config.market_data_contract_acquisition.scheduler_enabled);
    assert_eq!(config.market_data_contract_acquisition.scheduler_interval_seconds, 120);
    assert_eq!(config.market_data_contract_acquisition.scheduler_jitter_seconds, 15);
    assert_eq!(
        config
            .market_data_contract_acquisition
            .scheduler_max_consecutive_failures,
        5
    );
}

#[test]
fn contract_acquisition_scheduler_config_rejects_invalid_values() {
    let interval_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS",
        "0",
    )])
    .expect_err("zero interval should be rejected");
    assert!(interval_error
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS"));

    let jitter_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS",
        "86401",
    )])
    .expect_err("large jitter should be rejected");
    assert!(jitter_error
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS"));

    let failures_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
        "0",
    )])
    .expect_err("zero max failures should be rejected");
    assert!(failures_error
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES"));
}
```

- [ ] **Step 2: Run failing test**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/contract-candle-long-term-maintenance
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_scheduler_config -- --nocapture
```

Expected: compile fails because scheduler fields do not exist.

- [ ] **Step 3: Implement config fields and parsing**

In `MarketDataContractAcquisitionRuntimeConfig`, add:

```rust
pub scheduler_enabled: bool,
pub scheduler_interval_seconds: u64,
pub scheduler_jitter_seconds: u64,
pub scheduler_max_consecutive_failures: u32,
```

In its `Default`, add:

```rust
scheduler_enabled: false,
scheduler_interval_seconds: 3600,
scheduler_jitter_seconds: 0,
scheduler_max_consecutive_failures: 3,
```

In `ServerRuntimeConfig::from_env_pairs`, add env match arms:

```rust
"FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED" => {
    market_data_contract_acquisition.scheduler_enabled =
        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
}
"FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS" => {
    market_data_contract_acquisition.scheduler_interval_seconds = parse_u64_range(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS",
        value.as_ref(),
        1,
        86_400,
    )?;
}
"FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS" => {
    market_data_contract_acquisition.scheduler_jitter_seconds = parse_u64_range(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS",
        value.as_ref(),
        0,
        86_400,
    )?;
}
"FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES" => {
    market_data_contract_acquisition.scheduler_max_consecutive_failures = parse_u32_range(
        "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
        value.as_ref(),
        1,
        100,
    )?;
}
```

- [ ] **Step 4: Format and verify**

```bash
rustfmt --edition 2021 crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_scheduler_config -- --nocapture
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat: add contract candle scheduler config"
```

---

## Task 2: Implement Contract Acquisition Scheduler State and Attempt Helpers

**Files:**
- Create: `crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs`
- Modify: `crates/fdc-server/src/market_data/mod.rs`
- Test: module tests in new file

- [ ] **Step 1: Create failing scheduler module tests**

Create `contract_acquisition_scheduler.rs` with tests first. Include the production types in the same file so tests compile after implementation:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::config::ServerRuntimeConfig;

    #[tokio::test]
    async fn contract_scheduler_state_defaults_disabled() {
        let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
            .expect("config parses");
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
```

- [ ] **Step 2: Run failing module tests**

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server contract_acquisition_scheduler -- --nocapture
```

Expected: compile fails until the module and types are implemented/exported.

- [ ] **Step 3: Implement scheduler state types**

In `contract_acquisition_scheduler.rs`, define:

```rust
use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use fdc_core::Result;
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
```

Implement `from_config`, `should_spawn`, `snapshot`, `mark_next_run_at`, `mark_started`, `mark_completed`, `mark_failed`, `mark_suppressed`, `is_suppressed`.

Use statuses: `completed`, `failed`, `skipped_overlap`, `suppressed`.

- [ ] **Step 4: Implement task handle and attempt helpers**

Add:

```rust
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
```

- [ ] **Step 5: Export module**

In `crates/fdc-server/src/market_data/mod.rs`, add:

```rust
pub mod contract_acquisition_scheduler;
```

- [ ] **Step 6: Format and verify**

```bash
rustfmt --edition 2021 crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs crates/fdc-server/src/market_data/mod.rs
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server contract_acquisition_scheduler -- --nocapture
```

Expected: new scheduler tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs crates/fdc-server/src/market_data/mod.rs
git commit -m "feat: add contract candle scheduler state"
```

---

## Task 3: Wire Scheduler Into Production State and Startup

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Test: `crates/fdc-server/tests/contract_acquisition_contract.rs`

- [ ] **Step 1: Add state-level tests**

Add tests to `contract_acquisition_contract.rs`:

```rust
#[tokio::test]
async fn production_state_contract_scheduler_defaults_disabled() {
    let runtime = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("runtime config should parse");
    let state = ProductionServerState::new(runtime);

    let snapshot = state.market_data_contract_acquisition_scheduler().snapshot().await;

    assert!(!snapshot.enabled);
    assert!(!snapshot.running);
    assert!(!snapshot.suppressed);
}

#[tokio::test]
async fn production_state_contract_scheduler_is_enabled_when_configured() {
    let runtime = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
        ("FDC_MARKET_DATA_CONTRACTS_START_NS", "1700000000000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_END_NS", "1700000060000000000"),
    ])
    .expect("runtime config should parse");
    let state = ProductionServerState::new(runtime);

    let snapshot = state.market_data_contract_acquisition_scheduler().snapshot().await;

    assert!(snapshot.enabled);
    assert!(!snapshot.running);
    assert_eq!(snapshot.interval_seconds, 3600);
}
```

- [ ] **Step 2: Run failing tests**

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract production_state_contract_scheduler -- --nocapture
```

Expected: compile fails because state accessor does not exist.

- [ ] **Step 3: Modify `ProductionServerState`**

In `app.rs` imports, add:

```rust
contract_acquisition_scheduler::{
    spawn_contract_acquisition_scheduler_into_handle, ContractAcquisitionSchedulerState,
    ContractAcquisitionSchedulerTaskHandle,
},
```

Add fields:

```rust
market_data_contract_acquisition_scheduler: ContractAcquisitionSchedulerState,
market_data_contract_acquisition_scheduler_task: ContractAcquisitionSchedulerTaskHandle,
```

Initialize them in `new`, `try_new`, and `with_market_data_store` using:

```rust
let market_data_contract_acquisition_scheduler =
    ContractAcquisitionSchedulerState::from_config(&config);
```

In `try_new`, after storage scheduler spawn, call:

```rust
let market_data_contract_acquisition_scheduler_task =
    ContractAcquisitionSchedulerTaskHandle::default();
spawn_contract_acquisition_scheduler_into_handle(
    config.clone(),
    Arc::clone(&market_data_store),
    market_data_contract_acquisition_scheduler.clone(),
    market_data_contract_acquisition_scheduler_task.clone(),
)
.await;
```

Add accessors:

```rust
pub fn market_data_contract_acquisition_scheduler(&self) -> ContractAcquisitionSchedulerState {
    self.market_data_contract_acquisition_scheduler.clone()
}

pub fn market_data_contract_acquisition_scheduler_task(
    &self,
) -> ContractAcquisitionSchedulerTaskHandle {
    self.market_data_contract_acquisition_scheduler_task.clone()
}
```

- [ ] **Step 4: Implement scheduler loop spawn**

In `contract_acquisition_scheduler.rs`, add:

```rust
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
    run_scheduler_attempt_with_executor(config, state, move || async move {
        match run_binance_futures_usd_contract_candle_acquisition_once(
            &config.market_data_contract_acquisition,
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
```

- [ ] **Step 5: Format and verify**

```bash
rustfmt --edition 2021 crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs crates/fdc-server/tests/contract_acquisition_contract.rs
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract production_state_contract_scheduler -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server contract_acquisition_scheduler -- --nocapture
```

Expected: all focused tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/contract_acquisition_scheduler.rs crates/fdc-server/tests/contract_acquisition_contract.rs
git commit -m "feat: wire contract candle scheduler startup"
```

---

## Task 4: Expose Contract Scheduler Status API

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add route contract tests**

Add tests near existing contract acquisition route tests:

```rust
#[tokio::test]
async fn market_data_contract_acquisition_scheduler_status_reports_disabled_defaults() {
    let app = build_market_data_router(ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap(),
    ));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/market-data/contracts/acquisition/scheduler/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["enabled"], false);
    assert_eq!(json["data"]["running"], false);
    assert_eq!(json["data"]["suppressed"], false);
    assert_eq!(json["data"]["total_runs"], 0);
    assert_eq!(json["data"]["storage_records_written"], 0);
}

#[tokio::test]
async fn market_data_contract_acquisition_scheduler_status_reports_enabled_config() {
    let app = build_market_data_router(ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED", "1"),
            ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS", "120"),
            ("FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS", "5"),
            (
                "FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "4",
            ),
        ])
        .unwrap(),
    ));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/market-data/contracts/acquisition/scheduler/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["data"]["enabled"], true);
    assert_eq!(json["data"]["interval_seconds"], 120);
    assert_eq!(json["data"]["jitter_seconds"], 5);
    assert_eq!(json["data"]["max_consecutive_failures"], 4);
}
```

- [ ] **Step 2: Run failing route tests**

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract contract_acquisition_scheduler_status -- --nocapture
```

Expected: route returns 404 or compile fails because DTO/service are missing.

- [ ] **Step 3: Add response DTO**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataContractAcquisitionSchedulerStatusResponse {
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
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub next_run_at: Option<String>,
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
}
```

- [ ] **Step 4: Add service mapper**

In `service.rs`, add:

```rust
pub async fn contract_acquisition_scheduler_status(
    state: &ProductionServerState,
) -> MarketDataContractAcquisitionSchedulerStatusResponse {
    let snapshot = state.market_data_contract_acquisition_scheduler().snapshot().await;
    MarketDataContractAcquisitionSchedulerStatusResponse {
        enabled: snapshot.enabled,
        running: snapshot.running,
        suppressed: snapshot.suppressed,
        interval_seconds: snapshot.interval_seconds,
        jitter_seconds: snapshot.jitter_seconds,
        max_consecutive_failures: snapshot.max_consecutive_failures,
        consecutive_failures: snapshot.consecutive_failures,
        total_runs: snapshot.total_runs,
        successful_runs: snapshot.successful_runs,
        failed_runs: snapshot.failed_runs,
        skipped_runs: snapshot.skipped_runs,
        last_started_at: snapshot.last_started_at.map(|ts| ts.to_rfc3339()),
        last_finished_at: snapshot.last_finished_at.map(|ts| ts.to_rfc3339()),
        last_status: snapshot.last_status,
        last_error: snapshot.last_error,
        next_run_at: snapshot.next_run_at.map(|ts| ts.to_rfc3339()),
        tasks_started: snapshot.tasks_started,
        tasks_completed: snapshot.tasks_completed,
        pages_fetched: snapshot.pages_fetched,
        envelopes_received: snapshot.envelopes_received,
        storage_records_written: snapshot.storage_records_written,
        audit_records_written: snapshot.audit_records_written,
    }
}
```

- [ ] **Step 5: Add router handler and route**

In `router.rs`, import DTO and service function, add route:

```rust
.route(
    "/market-data/contracts/acquisition/scheduler/status",
    get(contract_acquisition_scheduler_status_handler),
)
```

Add handler:

```rust
async fn contract_acquisition_scheduler_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataContractAcquisitionSchedulerStatusResponse>> {
    Json(ServerApiResponse::success(
        contract_acquisition_scheduler_status(&state).await,
    ))
}
```

- [ ] **Step 6: Format and verify**

```bash
rustfmt --edition 2021 crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract contract_acquisition_scheduler_status -- --nocapture
```

Expected: 2 route tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat: expose contract candle scheduler status"
```

---

## Task 5: Runbook and Full Validation

**Files:**
- Modify: `docs/runbooks/market-data-production-runbook.md`

- [ ] **Step 1: Update runbook**

In the contract candle acquisition section, add scheduler config:

```bash
export FDC_MARKET_DATA_CONTRACTS_ENABLED=1
export FDC_MARKET_DATA_CONTRACTS_AUTOSTART=0
export FDC_MARKET_DATA_CONTRACTS_SCHEDULER_ENABLED=1
export FDC_MARKET_DATA_CONTRACTS_SCHEDULER_INTERVAL_SECONDS=300
export FDC_MARKET_DATA_CONTRACTS_SCHEDULER_JITTER_SECONDS=5
export FDC_MARKET_DATA_CONTRACTS_SCHEDULER_MAX_CONSECUTIVE_FAILURES=3
```

Add status check:

```bash
curl --noproxy '*' -sS \
  'http://127.0.0.1:18080/market-data/contracts/acquisition/scheduler/status'
```

Document expected behavior:

- scheduler starts only when contracts enabled and scheduler enabled
- run-once remains available for manual recovery
- scheduler uses checkpoints to continue bounded windows
- suppression after repeated failures requires operator inspection/restart or future explicit resume feature
- canonical query remains `/market-data/candles`

- [ ] **Step 2: Format docs not required, inspect diff**

```bash
git diff -- docs/runbooks/market-data-production-runbook.md
```

Expected: runbook documents scheduler config and operator flow.

- [ ] **Step 3: Run full validation matrix**

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server contract_acquisition_scheduler -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo check -p fdc-server
```

Expected: all tests pass and `cargo check -p fdc-server` has 0 errors. Existing unrelated warnings are acceptable.

- [ ] **Step 4: Commit runbook**

```bash
git add docs/runbooks/market-data-production-runbook.md
git commit -m "docs: document contract candle scheduler operations"
```

---

## Task 6: Merge Back and Cleanup

**Files:**
- No source files unless validation reveals merge issues.

- [ ] **Step 1: Check feature branch status**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/contract-candle-long-term-maintenance
rtk git status
rtk git log --oneline --max-count=8
```

Expected: clean working tree with implementation commits on `contract-candle-long-term-maintenance`.

- [ ] **Step 2: Merge into `mdb-mqdev`**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb
git merge --no-ff contract-candle-long-term-maintenance -m "merge: contract candle long-term maintenance"
```

Expected: merge commit created without conflicts.

- [ ] **Step 3: Validate merged result**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo check -p fdc-server
```

Expected: same pass result as feature branch.

- [ ] **Step 4: Cleanup worktree and branch**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb
git worktree remove .worktrees/contract-candle-long-term-maintenance
git branch -d contract-candle-long-term-maintenance
git worktree list
```

Expected: feature worktree removed and branch deleted.
