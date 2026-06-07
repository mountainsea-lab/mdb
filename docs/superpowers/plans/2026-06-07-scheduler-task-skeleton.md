# Scheduler Task Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-disabled, server-owned storage maintenance scheduler skeleton that can run scheduled maintenance only when the dedicated scheduler gate is enabled and the market-data storage backend is tiered.

**Architecture:** Add a focused `market_data::maintenance_scheduler` module that owns scheduler snapshots, mutable counters, and the background loop. `ProductionServerState` owns the scheduler state plus an optional task handle, and the existing scheduler status route maps that shared state. Maintenance execution reuses existing generic storage maintenance options and audit sink, preserving the `fdc-storage` boundary.

**Tech Stack:** Rust, Tokio, Axum, Serde, `fdc-server`, `fdc-storage` generic maintenance API, existing router/runtime contract tests.

---

## File map

- Create: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
  - Defines scheduler snapshot/state/handle.
  - Defines scheduler spawn decision and background loop.
  - Defines one-attempt execution helper for TDD and non-overlap tests.
- Modify: `crates/fdc-server/src/market_data/mod.rs`
  - Exports the new maintenance scheduler module.
- Modify: `crates/fdc-server/src/runtime/app.rs`
  - Adds scheduler state and optional task handle to `ProductionServerState`.
  - Initializes scheduler state in constructors.
  - Starts the scheduler only in `try_new` when config enables it and backend is tiered.
  - Exposes a scheduler snapshot/state accessor.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Changes `storage_maintenance_scheduler_status()` to read scheduler state snapshots instead of returning static zero counters.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Adds RED route-level tests for default no-spawn, manual-gate-only no-spawn, enabled memory unsupported, enabled tiered scheduled run, audit visibility, and non-overlap-visible skip behavior where practical.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Records P32 completion and verification evidence after implementation.

---

### Task 1: Add RED scheduler runtime behavior route tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add a bounded JSON polling helper near `response_body_json`**

Add this helper after `response_body_json`:

```rust
async fn wait_for_scheduler_status_field_at_least(
    router: axum::Router,
    field: &str,
    minimum: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/market-data/storage/maintenance/scheduler/status")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("scheduler status should respond");
        assert_eq!(response.status(), StatusCode::OK);
        let json = response_body_json(response).await;
        if json["data"][field].as_u64().unwrap_or(0) >= minimum {
            return json;
        }
        if std::time::Instant::now() >= deadline {
            panic!("scheduler field {field} did not reach {minimum}: {json}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
```

- [ ] **Step 2: Add default disabled no-spawn/no-audit test**

Add near existing scheduler status tests:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_disabled_default_does_not_spawn_or_audit() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["data"]["enabled"], false);
    assert_eq!(status_json["data"]["running"], false);
    assert_eq!(status_json["data"]["total_runs"], 0);
    assert_eq!(status_json["data"]["successful_runs"], 0);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["skipped_runs"], 0);
    assert!(status_json["data"]["next_run_at"].is_null());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
    assert_eq!(audit_json["data"]["total_recorded_entries"], 0);
}
```

- [ ] **Step 3: Add manual maintenance gate does not enable scheduler test**

Add:

```rust
#[tokio::test]
async fn storage_maintenance_manual_gate_does_not_enable_scheduler() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["enabled"], false);
    assert_eq!(json["data"]["total_runs"], 0);
    assert_eq!(json["data"]["skipped_runs"], 0);
    assert!(json["data"]["next_run_at"].is_null());
}
```

- [ ] **Step 4: Add enabled memory backend unsupported/no-audit test**

Add:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_enabled_memory_backend_reports_unsupported_without_audit() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["data"]["enabled"], true);
    assert_eq!(status_json["data"]["backend"], "memory");
    assert_eq!(status_json["data"]["tiered"], false);
    assert_eq!(status_json["data"]["running"], false);
    assert_eq!(status_json["data"]["total_runs"], 0);
    assert_eq!(status_json["data"]["successful_runs"], 0);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["skipped_runs"], 1);
    assert_eq!(status_json["data"]["last_status"], "unsupported_backend");
    assert!(status_json["data"]["next_run_at"].is_null());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
}
```

- [ ] **Step 5: Add enabled tiered scheduled run/audit test**

Add:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_enabled_tiered_backend_runs_and_records_audit() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "60",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "5000",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    let status_json = wait_for_scheduler_status_field_at_least(
        router.clone(),
        "successful_runs",
        1,
    )
    .await;
    assert_eq!(status_json["data"]["enabled"], true);
    assert_eq!(status_json["data"]["backend"], "tiered");
    assert_eq!(status_json["data"]["tiered"], true);
    assert_eq!(status_json["data"]["total_runs"], 1);
    assert_eq!(status_json["data"]["successful_runs"], 1);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["consecutive_failures"], 0);
    assert_eq!(status_json["data"]["last_status"], "completed");
    assert!(status_json["data"]["last_started_at"].is_string());
    assert!(status_json["data"]["last_finished_at"].is_string());
    assert!(status_json["data"]["next_run_at"].is_string());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
    assert_eq!(audit_json["data"]["total_recorded_entries"], 1);
    assert_eq!(audit_json["data"]["entries"][0]["healthy_tiers"], 4);
}
```

- [ ] **Step 6: Verify RED**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
```

Expected:

- New default/manual/memory tests may fail because status is still static and memory unsupported state is not recorded.
- Enabled tiered test must fail because no background scheduler increments counters or audit.

---

### Task 2: Add scheduler state module with unit tests

**Files:**
- Create: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
- Modify: `crates/fdc-server/src/market_data/mod.rs`

- [ ] **Step 1: Create scheduler module with state snapshot, shared state, and tests**

Create `crates/fdc-server/src/market_data/maintenance_scheduler.rs` with:

```rust
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
            inner: Arc::new(Mutex::new(StorageMaintenanceSchedulerStateInner { snapshot })),
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
```

- [ ] **Step 2: Export module**

In `crates/fdc-server/src/market_data/mod.rs`, add:

```rust
pub mod maintenance_scheduler;
```

Keep existing module declarations unchanged.

- [ ] **Step 3: Run module tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_state
```

Expected: scheduler state tests pass.

- [ ] **Step 4: Commit scheduler state module**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs crates/fdc-server/src/market_data/mod.rs
git commit -m "feat(server): add storage maintenance scheduler state"
```

---

### Task 3: Integrate scheduler state into production state and status route

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Add scheduler fields/accessor to `ProductionServerState`**

In `crates/fdc-server/src/runtime/app.rs`, update imports:

```rust
use tokio::task::JoinHandle;
```

and extend the market_data import:

```rust
maintenance_scheduler::StorageMaintenanceSchedulerState,
```

Add fields to `ProductionServerState`:

```rust
market_data_storage_maintenance_scheduler: StorageMaintenanceSchedulerState,
_market_data_storage_maintenance_scheduler_task: Option<Arc<JoinHandle<()>>>,
```

Add this accessor in `impl ProductionServerState`:

```rust
pub fn market_data_storage_maintenance_scheduler(&self) -> StorageMaintenanceSchedulerState {
    self.market_data_storage_maintenance_scheduler.clone()
}
```

- [ ] **Step 2: Initialize scheduler state without spawning in `new` and `with_market_data_store`**

In `new(config)`, before `Self { ... }`, add:

```rust
let market_data_storage_maintenance_scheduler =
    StorageMaintenanceSchedulerState::from_config(&config);
```

Then add fields to `Self`:

```rust
market_data_storage_maintenance_scheduler,
_market_data_storage_maintenance_scheduler_task: None,
```

Repeat the same initialization and fields in `with_market_data_store`.

- [ ] **Step 3: Initialize scheduler state in `try_new` without spawning yet**

In `try_new(config)`, before `Ok(Self { ... })`, add:

```rust
let market_data_storage_maintenance_scheduler =
    StorageMaintenanceSchedulerState::from_config(&config);
```

Add fields to `Self`:

```rust
market_data_storage_maintenance_scheduler,
_market_data_storage_maintenance_scheduler_task: None,
```

This step intentionally does not start the background task yet.

- [ ] **Step 4: Map status route from scheduler snapshot**

Change `storage_maintenance_scheduler_status` in `crates/fdc-server/src/market_data/service.rs` from a sync function to async:

```rust
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
```

Remove any now-unused `MarketDataStorageBackendConfig` logic from this function only if the compiler reports it as unused elsewhere.

- [ ] **Step 5: Await status mapper in router**

In `crates/fdc-server/src/market_data/router.rs`, update handler body:

```rust
async fn storage_maintenance_scheduler_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageMaintenanceSchedulerStatusResponse>> {
    Json(ServerApiResponse::success(
        storage_maintenance_scheduler_status(&state).await,
    ))
}
```

- [ ] **Step 6: Run route tests for static state behavior**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status_reports_disabled_defaults
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status_reports_configured_values_without_running
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_enabled_memory_backend_reports_unsupported_without_audit
```

Expected:

- Disabled defaults pass.
- Configured values created with `ProductionServerState::new` pass with zero counters.
- Memory backend unsupported test passes because `from_config` marks unsupported without spawning.

- [ ] **Step 7: Commit production state/status integration**

Run:

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose storage maintenance scheduler state"
```

---

### Task 4: Implement scheduler background loop for enabled tiered runtime

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
- Modify: `crates/fdc-server/src/runtime/app.rs`

- [ ] **Step 1: Add scheduler task spawning and attempt functions**

Append these imports near the top of `maintenance_scheduler.rs`:

```rust
use fdc_storage::{QueryableMarketDataStore, StorageMaintenanceOptions};
use tokio::task::JoinHandle;

use crate::market_data::maintenance_audit::MarketDataStorageMaintenanceAuditLog;
```

Add below `scheduler_timeout`:

```rust
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
        let next_run_at = Utc::now()
            + chrono::Duration::from_std(next_delay)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
        state.mark_next_run_at(next_run_at).await;
        tokio::time::sleep(next_delay).await;
        run_scheduler_attempt(&config, Arc::clone(&store), Arc::clone(&audit), state.clone()).await;
        next_delay = interval;
    }
}

pub async fn run_scheduler_attempt(
    config: &ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) {
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

    match store.run_maintenance_once_with_options(options).await {
        Ok(Some(_report)) => {
            state.mark_completed(Utc::now(), next_run_at).await;
        }
        Ok(None) => {
            state.mark_unsupported(Utc::now(), next_run_at).await;
        }
        Err(error) => {
            state
                .mark_failed(Utc::now(), next_run_at, format!("{error}"))
                .await;
        }
    }
}
```

This uses a 10ms first delay when jitter is zero so contract tests do not wait 60 seconds. When jitter is configured, the first delay uses the configured jitter seconds. Subsequent delays use the configured interval.

- [ ] **Step 2: Spawn scheduler in `ProductionServerState::try_new`**

In `crates/fdc-server/src/runtime/app.rs`, import:

```rust
maintenance_scheduler::{
    spawn_storage_maintenance_scheduler, StorageMaintenanceSchedulerState,
},
```

In `try_new`, after creating `market_data_store`, `market_data_storage_maintenance_audit`, and `market_data_storage_maintenance_scheduler`, add:

```rust
let market_data_store = Arc::new(market_data_store);
let market_data_storage_maintenance_scheduler_task = spawn_storage_maintenance_scheduler(
    config.clone(),
    Arc::clone(&market_data_store),
    Arc::clone(&market_data_storage_maintenance_audit),
    market_data_storage_maintenance_scheduler.clone(),
)
.map(Arc::new);
```

Then in `Ok(Self { ... })`, use the existing `market_data_store` Arc and set:

```rust
_market_data_storage_maintenance_scheduler_task: market_data_storage_maintenance_scheduler_task,
```

Do not spawn in `new` or `with_market_data_store`.

- [ ] **Step 3: Run enabled tiered scheduler route test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_enabled_tiered_backend_runs_and_records_audit
```

Expected: test passes with one scheduled run and one audit entry.

- [ ] **Step 4: Run scheduler status route group**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
```

Expected: scheduler status and scheduler behavior route tests pass.

- [ ] **Step 5: Commit scheduler loop**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs crates/fdc-server/src/runtime/app.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): run gated storage maintenance scheduler"
```

---

### Task 5: Add direct non-overlap unit coverage

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Add state-level non-overlap test if not already sufficient**

If Task 2 test `storage_maintenance_scheduler_state_tracks_started_completed_and_overlap` already exists, extend it with explicit assertions for `last_status="skipped_overlap"` before completion:

```rust
let overlap_snapshot = state.snapshot().await;
assert_eq!(overlap_snapshot.skipped_runs, 1);
assert_eq!(overlap_snapshot.last_status.as_deref(), Some("skipped_overlap"));
```

Place these immediately after:

```rust
assert!(!state.mark_started(started).await);
```

- [ ] **Step 2: Run non-overlap unit test**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_state_tracks_started_completed_and_overlap
```

Expected: test passes.

- [ ] **Step 3: Commit non-overlap coverage if changed**

If Step 1 changed the file, run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs
git commit -m "test(server): cover scheduler non-overlap accounting"
```

If Step 1 required no change because equivalent assertions are already present, skip this commit and note that Task 2 covered non-overlap.

---

### Task 6: Focused regression verification

**Files:**
- No source changes expected.

- [ ] **Step 1: Run scheduler route/status tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
```

Expected: scheduler route tests pass.

- [ ] **Step 2: Run manual maintenance regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
```

Expected: manual run-once route tests pass.

- [ ] **Step 3: Run audit route regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: audit route tests pass.

- [ ] **Step 4: Run scheduler unit tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler
```

Expected: scheduler unit and route-filtered tests pass.

- [ ] **Step 5: Run storage dependency guard**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: dependency guard passes.

- [ ] **Step 6: Run package-scoped fmt check**

Run:

```bash
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: exit 0.

---

### Task 7: Record P32 completion status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Insert P32 status entry above P31**

Insert this template above the P31 entry and replace commit hashes with `git log --oneline -10` output:

```markdown
## 2026-06-07 P32 Scheduler Task Skeleton

Completed:

- Added server-owned storage maintenance scheduler state and counters.
- Wired scheduler state into `ProductionServerState` and the existing read-only scheduler status route.
- Added gated background scheduler execution for tiered backend only:
  - scheduler gate must be enabled
  - default runtime spawns no scheduler
  - memory backend reports unsupported without running maintenance
- Scheduler uses existing generic storage maintenance options with configured timeout and audit sink.
- Scheduled tiered runs record audit entries through the existing audit route.
- Added non-overlap accounting through scheduler state.
- Preserved manual run-once gate separation.
- Preserved the `fdc-storage` boundary: no server/runtime/admin semantics were added to storage.

Design and plan:

- `docs/superpowers/specs/2026-06-07-scheduler-task-skeleton-design.md`
- `docs/superpowers/plans/2026-06-07-scheduler-task-skeleton.md`

Commits:

- `db0339f docs(server): design scheduler task skeleton`
- plan commit hash from `git log --oneline -10` after committing this plan
- scheduler state commit hash from `git log --oneline -10`
- scheduler status integration commit hash from `git log --oneline -10`
- scheduler loop commit hash from `git log --oneline -10`
- non-overlap test commit hash from `git log --oneline -10` if a separate non-overlap commit was needed

Verification:

- RED route tests failed before implementation because scheduler status remained static and no background task existed.
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route` - passed
- `rtk cargo test -p fdc-server storage_maintenance_scheduler` - passed
- `rtk cargo test -p fdc-storage --test dependency_guard` - 1 passed
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- **P33 scheduler failure handling and suppression**: add failure suppression after max consecutive failures, more explicit sanitized error reporting, and regression tests that prove the server does not panic on scheduler failure.
```

- [ ] **Step 2: Update top checkpoint commit line**

Change the top line to:

```markdown
Latest checkpoint commit when this file was written: this document update commit (`docs: record scheduler task skeleton status`)
```

- [ ] **Step 3: Commit status docs**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record scheduler task skeleton status"
```

- [ ] **Step 4: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected: `ok`.

---

## Plan self-review

- Spec coverage: default-disabled behavior, separate scheduler gate, tiered-only execution, memory unsupported visibility, audit visibility, non-overlap accounting, status route mapping, and `fdc-storage` boundary all map to tasks above.
- TDD coverage: Task 1 adds RED route tests before implementation; Task 2 adds focused state unit tests before integration; later tasks verify GREEN before commits.
- Placeholder scan: no incomplete implementation placeholders remain. The final status-doc template tells the implementer exactly how to fill commit hashes from `git log --oneline -10`.
- Type consistency: scheduler state names, status DTO fields, and `ProductionServerState` accessor names are consistent across tasks.
- Scope check: P33 suppression is explicitly excluded; P32 only adds skeleton execution and counters.
