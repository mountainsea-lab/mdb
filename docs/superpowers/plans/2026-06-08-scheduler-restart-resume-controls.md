# P35 Scheduler Restart/Resume Controls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-disabled, confirmation-protected scheduler resume route that can spawn a new scheduler loop after suppression/reset without directly running maintenance.

**Architecture:** Keep all lifecycle/admin semantics in `fdc-server`. Add a resume runtime gate, scheduler state resume helpers, a server-owned scheduler task handle in `ProductionServerState`, DTO/service/route wiring, and contract tests that prove resume is gated, safe, and distinct from P34 reset.

**Tech Stack:** Rust, Tokio, Axum, serde DTOs, existing `fdc-server` runtime config/service/router patterns, Cargo package tests.

---

## File Structure

- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Add `market_data_storage_maintenance_scheduler_resume_enabled`.
  - Parse `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED`.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add default safety assertion and env override test for resume gate.
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
  - Add resume outcome type.
  - Add state method to clear retry state and mark `last_status="resumed"`.
  - Add lifecycle wrapper for `JoinHandle<()>` ownership.
  - Add spawn helper that records active/completed task handle state.
- Modify: `crates/fdc-server/src/runtime/app.rs`
  - Replace immutable optional scheduler task with lifecycle handle.
  - Expose store, audit, scheduler, and lifecycle accessors needed by service logic.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add scheduler resume request/response DTOs.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add resume confirmation constant, result type, error helper, and service function.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add POST `/market-data/storage/maintenance/scheduler/resume` and handler.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route contract tests for disabled, wrong confirmation, scheduler disabled, unsupported backend, success, and P34 reset non-spawn regression.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P35 completion after implementation.

No `fdc-storage` source files should be modified.

---

## Task 1: Resume config gate TDD

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing config tests**

In `crates/fdc-server/tests/runtime_config_contract.rs`, add this assertion after the existing reset gate assertion in `runtime_config_defaults_are_safe_for_local_production_server()`:

```rust
    assert!(!config.market_data_storage_maintenance_scheduler_resume_enabled);
```

Add this test after `storage_maintenance_scheduler_reset_gate_can_be_enabled_by_env()`:

```rust
#[test]
fn storage_maintenance_scheduler_resume_gate_can_be_enabled_by_env() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("scheduler resume config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_resume_enabled);
}
```

- [ ] **Step 2: Run RED config test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_resume
```

Expected: FAIL to compile because `market_data_storage_maintenance_scheduler_resume_enabled` does not exist.

- [ ] **Step 3: Implement resume config field and parser**

In `crates/fdc-server/src/runtime/config.rs`, add the field after `market_data_storage_maintenance_scheduler_reset_enabled`:

```rust
    pub market_data_storage_maintenance_scheduler_resume_enabled: bool,
```

Add the local default after `market_data_storage_maintenance_scheduler_reset_enabled`:

```rust
        let mut market_data_storage_maintenance_scheduler_resume_enabled = false;
```

Add this match arm after the reset gate arm:

```rust
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED" => {
                    market_data_storage_maintenance_scheduler_resume_enabled =
                        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
```

Add the struct assignment after `market_data_storage_maintenance_scheduler_reset_enabled`:

```rust
            market_data_storage_maintenance_scheduler_resume_enabled,
```

- [ ] **Step 4: Run GREEN config test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_resume
```

Expected: PASS, with 1 matching test passed.

- [ ] **Step 5: Commit config gate**

Run:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): add scheduler resume config gate"
```

---

## Task 2: Scheduler state resume and lifecycle handle TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Write failing state resume tests**

Append these tests inside the existing `#[cfg(test)] mod tests` in `maintenance_scheduler.rs`:

```rust
    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_clears_retry_state_and_marks_resumed() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "1",
            ),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let started_at = Utc::now();
        let finished_at = started_at + chrono::Duration::milliseconds(1);
        let next_run_at = finished_at + chrono::Duration::seconds(60);

        assert!(state.mark_started(started_at).await);
        state
            .mark_failed(finished_at, next_run_at, "simulated failure")
            .await;
        assert!(state.is_suppressed().await);

        let outcome = state.prepare_resume().await;
        assert!(outcome.resumed);
        assert!(!outcome.running);
        assert_eq!(outcome.previous_consecutive_failures, 1);
        assert_eq!(outcome.consecutive_failures, 0);

        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.last_status.as_deref(), Some("resumed"));
        assert!(snapshot.last_error.is_none());
        assert!(snapshot.next_run_at.is_none());
        assert!(!state.is_suppressed().await);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_resume_rejects_running_attempt() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        assert!(state.mark_started(Utc::now()).await);

        let outcome = state.prepare_resume().await;
        assert!(!outcome.resumed);
        assert!(outcome.running);

        let snapshot = state.snapshot().await;
        assert!(snapshot.running);
        assert_eq!(snapshot.last_status.as_deref(), Some("running"));
    }
```

- [ ] **Step 2: Write failing lifecycle handle tests**

Append these tests inside the same test module:

```rust
    #[tokio::test]
    async fn storage_maintenance_scheduler_task_handle_tracks_active_and_completed_task() {
        let handle = StorageMaintenanceSchedulerTaskHandle::default();
        assert!(!handle.is_active().await);

        assert!(handle
            .try_store(tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }))
            .await);
        assert!(handle.is_active().await);
        assert!(!handle
            .try_store(tokio::spawn(async {}))
            .await);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_active().await);
    }
```

- [ ] **Step 3: Run RED state/lifecycle tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_resume
```

Expected: FAIL to compile because `prepare_resume()` and `StorageMaintenanceSchedulerTaskHandle` do not exist.

- [ ] **Step 4: Implement resume outcome and state method**

In `maintenance_scheduler.rs`, add this type after `StorageMaintenanceSchedulerResetOutcome`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResumeOutcome {
    pub resumed: bool,
    pub running: bool,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
}
```

Add this method in `impl StorageMaintenanceSchedulerState` after `reset_suppression()`:

```rust
    pub async fn prepare_resume(&self) -> StorageMaintenanceSchedulerResumeOutcome {
        let mut guard = self.inner.lock().await;
        let previous_consecutive_failures = guard.snapshot.consecutive_failures;
        if guard.snapshot.running {
            return StorageMaintenanceSchedulerResumeOutcome {
                resumed: false,
                running: true,
                previous_consecutive_failures,
                consecutive_failures: guard.snapshot.consecutive_failures,
            };
        }

        guard.snapshot.consecutive_failures = 0;
        guard.snapshot.last_error = None;
        guard.snapshot.last_status = Some("resumed".to_string());
        guard.snapshot.next_run_at = None;

        StorageMaintenanceSchedulerResumeOutcome {
            resumed: true,
            running: false,
            previous_consecutive_failures,
            consecutive_failures: guard.snapshot.consecutive_failures,
        }
    }
```

- [ ] **Step 5: Implement lifecycle handle**

At the top of `maintenance_scheduler.rs`, keep the existing `JoinHandle` import and add this type after `StorageMaintenanceSchedulerState`:

```rust
#[derive(Debug, Clone, Default)]
pub struct StorageMaintenanceSchedulerTaskHandle {
    inner: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl StorageMaintenanceSchedulerTaskHandle {
    pub async fn try_store(&self, handle: JoinHandle<()>) -> bool {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.as_ref() {
            if !existing.is_finished() {
                return false;
            }
        }
        *guard = Some(handle);
        true
    }

    pub async fn is_active(&self) -> bool {
        let mut guard = self.inner.lock().await;
        if guard.as_ref().is_some_and(|handle| handle.is_finished()) {
            *guard = None;
            return false;
        }
        guard.is_some()
    }
}
```

- [ ] **Step 6: Run GREEN state/lifecycle tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_resume
```

Expected: PASS for the new resume-related unit tests.

- [ ] **Step 7: Commit state/lifecycle primitives**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs
git commit -m "feat(server): add scheduler resume state primitives"
```

---

## Task 3: Runtime task ownership and resumable spawn helper

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
- Modify: `crates/fdc-server/src/runtime/app.rs`

- [ ] **Step 1: Add resumable spawn helper**

In `maintenance_scheduler.rs`, add this function after `spawn_storage_maintenance_scheduler()`:

```rust
pub async fn spawn_storage_maintenance_scheduler_into_handle(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
    task_handle: StorageMaintenanceSchedulerTaskHandle,
) -> bool {
    if !state.should_spawn(&config) {
        return false;
    }

    let handle = tokio::spawn(async move {
        run_scheduler_loop(config, store, audit, state).await;
    });
    task_handle.try_store(handle).await
}
```

Leave existing `spawn_storage_maintenance_scheduler()` in place for now or refactor it only after tests are green.

- [ ] **Step 2: Replace immutable task field in runtime state**

In `crates/fdc-server/src/runtime/app.rs`, update imports:

```rust
        maintenance_scheduler::{
            spawn_storage_maintenance_scheduler_into_handle, StorageMaintenanceSchedulerState,
            StorageMaintenanceSchedulerTaskHandle,
        },
```

Remove `use tokio::task::JoinHandle;`.

Change the field:

```rust
    market_data_storage_maintenance_scheduler_task: StorageMaintenanceSchedulerTaskHandle,
```

In `new()` and `with_market_data_store()`, initialize:

```rust
            market_data_storage_maintenance_scheduler_task: StorageMaintenanceSchedulerTaskHandle::default(),
```

In `try_new()`, create the handle before spawning:

```rust
        let market_data_storage_maintenance_scheduler_task =
            StorageMaintenanceSchedulerTaskHandle::default();
        spawn_storage_maintenance_scheduler_into_handle(
            config.clone(),
            Arc::clone(&market_data_store),
            Arc::clone(&market_data_storage_maintenance_audit),
            market_data_storage_maintenance_scheduler.clone(),
            market_data_storage_maintenance_scheduler_task.clone(),
        )
        .await;
```

Then assign the handle in the struct:

```rust
            market_data_storage_maintenance_scheduler_task,
```

- [ ] **Step 3: Add runtime accessors needed by resume service**

In `impl ProductionServerState`, add this accessor after `market_data_storage_maintenance_scheduler()`:

```rust
    pub fn market_data_storage_maintenance_scheduler_task(
        &self,
    ) -> StorageMaintenanceSchedulerTaskHandle {
        self.market_data_storage_maintenance_scheduler_task.clone()
    }
```

Existing `market_data_store()` and `market_data_storage_maintenance_audit()` already provide the other resume inputs.

- [ ] **Step 4: Run runtime scheduler regression tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
```

Expected: PASS. Existing scheduler startup/status behavior remains green.

- [ ] **Step 5: Commit runtime lifecycle ownership**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs crates/fdc-server/src/runtime/app.rs
git commit -m "feat(server): track scheduler task lifecycle"
```

---

## Task 4: Resume DTO and service TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Add DTOs**

In `model.rs`, add these structs after the scheduler reset response:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResumeRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResumeResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub task_started: bool,
}
```

- [ ] **Step 2: Wire service imports and types**

In `service.rs`, add these imports beside reset DTO imports:

```rust
        MarketDataStorageMaintenanceSchedulerResumeRequest,
        MarketDataStorageMaintenanceSchedulerResumeResponse,
```

Add this import in the crate import list:

```rust
    market_data::maintenance_scheduler::spawn_storage_maintenance_scheduler_into_handle,
```

Add the confirmation constant after the reset confirmation constant:

```rust
pub const STORAGE_MAINTENANCE_SCHEDULER_RESUME_CONFIRMATION: &str = "resume_scheduler";
```

Add this result type after `StorageMaintenanceSchedulerResetResult`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResumeResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceSchedulerResumeResponse,
    pub message: Option<String>,
}
```

- [ ] **Step 3: Add resume service function**

In `service.rs`, add this function after `reset_storage_maintenance_scheduler()`:

```rust
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

    if !state.config().market_data_storage_maintenance_scheduler_enabled {
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
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "already_running",
            request.reason,
            0,
            0,
            "storage maintenance scheduler task is already running",
        );
    }

    let scheduler = state.market_data_storage_maintenance_scheduler();
    let outcome = scheduler.prepare_resume().await;
    if outcome.running {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "running",
            request.reason,
            outcome.previous_consecutive_failures,
            outcome.consecutive_failures,
            "storage maintenance scheduler resume cannot run while scheduler attempt is running",
        );
    }

    let task_started = spawn_storage_maintenance_scheduler_into_handle(
        state.config().clone(),
        state.market_data_store(),
        state.market_data_storage_maintenance_audit(),
        scheduler,
        task_handle,
    )
    .await;

    if !task_started {
        return scheduler_resume_error(
            StorageMaintenanceHttpStatus::Conflict,
            "already_running",
            request.reason,
            outcome.previous_consecutive_failures,
            outcome.consecutive_failures,
            "storage maintenance scheduler task could not be started",
        );
    }

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
```

- [ ] **Step 4: Add resume error helper**

Add this helper after `scheduler_reset_error()`:

```rust
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
```

- [ ] **Step 5: Run focused compile/tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_resume
```

Expected: PASS or compile clean if only route tests remain absent.

- [ ] **Step 6: Commit DTO/service**

Run:

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs
git commit -m "feat(server): resume storage maintenance scheduler"
```

---

## Task 5: Resume route contract TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add test request helper**

In `production_server_router_contract.rs`, add this helper after `scheduler_reset_request()`:

```rust
fn scheduler_resume_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}
```

- [ ] **Step 2: Add route contract tests**

Append these tests after existing scheduler reset route tests:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_resume_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
    assert_eq!(json["data"]["task_started"], false);
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_requires_scheduler_enabled() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["status"], "scheduler_disabled");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_rejects_memory_backend() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["status"], "unsupported_backend");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_starts_new_loop_after_suppression() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::with_market_data_store(
        config,
        Arc::new(QueryableMarketDataStore::new()),
    );
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let started_at = chrono::Utc::now();
    let finished_at = started_at + chrono::Duration::milliseconds(1);
    let next_run_at = finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(started_at).await);
    scheduler
        .mark_failed(finished_at, next_run_at, "simulated resume route failure")
        .await;
    assert!(scheduler.is_suppressed().await);
    let router = build_production_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "resumed");
    assert_eq!(json["data"]["previous_consecutive_failures"], 1);
    assert_eq!(json["data"]["consecutive_failures"], 0);
    assert_eq!(json["data"]["task_started"], true);

    let status = wait_for_scheduler_status_field_at_least(router, "total_runs", 2).await;
    assert_eq!(status["data"]["consecutive_failures"], 0);
    assert!(status["data"]["next_run_at"].is_string());
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_does_not_start_scheduler_task() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::with_market_data_store(
        config,
        Arc::new(QueryableMarketDataStore::new()),
    );
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let started_at = chrono::Utc::now();
    let finished_at = started_at + chrono::Duration::milliseconds(1);
    let next_run_at = finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(started_at).await);
    scheduler
        .mark_failed(finished_at, next_run_at, "simulated reset route failure")
        .await;
    let router = build_production_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("reset_scheduler_suppression"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");
    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["total_runs"], 1);
    assert!(json["data"]["next_run_at"].is_null());
}
```

- [ ] **Step 3: Run RED route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
```

Expected: FAIL because route `/market-data/storage/maintenance/scheduler/resume` is not wired yet.

- [ ] **Step 4: Wire router imports, route, and handler**

In `router.rs`, add DTO imports:

```rust
            MarketDataStorageMaintenanceSchedulerResumeRequest,
            MarketDataStorageMaintenanceSchedulerResumeResponse,
```

Add service import:

```rust
            resume_storage_maintenance_scheduler,
```

Add route after scheduler reset:

```rust
        .route(
            "/market-data/storage/maintenance/scheduler/resume",
            post(storage_maintenance_scheduler_resume_handler),
        )
```

Add handler after `storage_maintenance_scheduler_reset_handler`:

```rust
async fn storage_maintenance_scheduler_resume_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<MarketDataStorageMaintenanceSchedulerResumeRequest>,
) -> (
    StatusCode,
    Json<ServerApiResponse<MarketDataStorageMaintenanceSchedulerResumeResponse>>,
) {
    let result = resume_storage_maintenance_scheduler(&state, request).await;
    let status = storage_maintenance_status_code(result.http_status);
    let envelope = if result.http_status == StorageMaintenanceHttpStatus::Ok {
        ServerApiResponse::success(result.response)
    } else {
        ServerApiResponse::error(
            result.response,
            result.message.unwrap_or_else(|| {
                "storage maintenance scheduler resume request failed".to_string()
            }),
        )
    };
    (status, Json(envelope))
}
```

- [ ] **Step 5: Run GREEN route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
```

Expected: PASS. Resume route tests pass, and P34 reset route remains accounting-only.

- [ ] **Step 6: Commit route contracts**

Run:

```bash
git add crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): add scheduler resume route"
```

---

## Task 6: Documentation, final verification, and completion commit

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run full focused verification**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all tests pass and fmt exits 0.

- [ ] **Step 2: Update development status**

Prepend this section after the file header in `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-08 P35 Scheduler Restart/Resume Controls

Completed:

- Added default-disabled scheduler resume runtime gate:
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED`
- Added explicit confirmation-protected scheduler resume route:
  - `POST /market-data/storage/maintenance/scheduler/resume`
  - confirmation string: `resume_scheduler`
- Resume starts a new scheduler loop only when scheduler config is enabled, backend is tiered, no scheduler loop is already active, and no scheduler attempt is currently running.
- Resume clears retry/suppression state, sets `last_status="resumed"`, and allows the normal scheduler loop to publish `next_run_at`.
- Resume does not directly run storage maintenance, clear audit entries, reset storage data, or alter storage tier paths.
- Preserved P34 reset semantics: reset remains accounting-only and does not spawn scheduler tasks.
- Preserved the `fdc-storage` boundary: no server/runtime/admin semantics were added to storage.

Design and plan:

- `docs/superpowers/specs/2026-06-08-scheduler-restart-resume-controls-design.md`
- `docs/superpowers/plans/2026-06-08-scheduler-restart-resume-controls.md`

Commits:

Insert the actual P35 implementation commit list by running:

```bash
git log --oneline --grep "scheduler resume" --grep "scheduler task lifecycle" -6
```

The section should include every P35 implementation commit produced by this plan, including config gate, state primitives, lifecycle tracking, service, and route commits.

Verification:

- `rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_resume`
- `rtk cargo test -p fdc-server storage_maintenance_scheduler_resume`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset`
- `rtk cargo test -p fdc-storage --test dependency_guard`
- `cargo fmt -p fdc-server -p fdc-storage -- --check`

Recommended next slice:

- Live collection hardening, unless operators explicitly request additional scheduler lifecycle controls beyond resume.
```

- [ ] **Step 3: Commit documentation**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record scheduler restart resume controls status"
```

- [ ] **Step 4: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.
