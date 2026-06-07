# P34 Scheduler Recovery Controls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-disabled, confirmation-protected scheduler reset route that clears server-owned scheduler retry/suppression state without running maintenance or restarting scheduler tasks.

**Architecture:** Add one runtime gate to `ServerRuntimeConfig`, a state reset method in `market_data::maintenance_scheduler`, server-owned request/response DTOs, a service function mirroring existing audit reset patterns, and one POST route. Reset only mutates scheduler counters/status fields and remains entirely in `fdc-server`.

**Tech Stack:** Rust, Axum, Tokio, serde DTOs, existing `StorageMaintenanceHttpStatus`, Cargo package tests.

---

## File Structure

- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Add `market_data_storage_maintenance_scheduler_reset_enabled` bool.
  - Parse `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED`.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add RED/GREEN config tests for reset gate default and env override.
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
  - Add `StorageMaintenanceSchedulerResetOutcome`.
  - Add `reset_suppression()` state method.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add scheduler reset request/response DTOs.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add `STORAGE_MAINTENANCE_SCHEDULER_RESET_CONFIRMATION`.
  - Add `StorageMaintenanceSchedulerResetResult`.
  - Add `reset_storage_maintenance_scheduler()` service.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add POST route `/market-data/storage/maintenance/scheduler/reset`.
  - Add handler using existing status envelope pattern.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route tests for disabled gate, wrong confirmation, success, and status follow-up.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P34 completion and verification evidence.

No `fdc-storage` source files should be modified.

---

## Task 1: Config gate TDD

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing config tests**

In `crates/fdc-server/tests/runtime_config_contract.rs`, add this assertion to `runtime_config_defaults_are_safe_for_local_production_server()` after the existing scheduler enabled assertion:

```rust
    assert!(!config.market_data_storage_maintenance_scheduler_reset_enabled);
```

Add this new test after `storage_maintenance_scheduler_config_accepts_valid_overrides()`:

```rust
#[test]
fn storage_maintenance_scheduler_reset_gate_can_be_enabled_by_env() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
        "1",
    )])
    .expect("scheduler reset config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_reset_enabled);
}
```

- [ ] **Step 2: Run RED config test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_reset
```

Expected: FAIL to compile because `market_data_storage_maintenance_scheduler_reset_enabled` does not exist.

- [ ] **Step 3: Implement config field and parser**

In `crates/fdc-server/src/runtime/config.rs`:

1. Add field to `ServerRuntimeConfig` after `market_data_storage_maintenance_scheduler_enabled`:

```rust
    pub market_data_storage_maintenance_scheduler_reset_enabled: bool,
```

2. Add local default after `market_data_storage_maintenance_scheduler_enabled`:

```rust
        let mut market_data_storage_maintenance_scheduler_reset_enabled = false;
```

3. Add match arm after scheduler enabled arm:

```rust
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED" => {
                    market_data_storage_maintenance_scheduler_reset_enabled =
                        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
```

4. Add struct assignment after scheduler enabled assignment:

```rust
            market_data_storage_maintenance_scheduler_reset_enabled,
```

- [ ] **Step 4: Run GREEN config test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_reset
```

Expected: PASS.

- [ ] **Step 5: Commit config gate**

Run:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): add scheduler reset config gate"
```

---

## Task 2: Scheduler state reset TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Write failing state tests**

Append these tests inside the scheduler module test block:

```rust
    #[tokio::test]
    async fn storage_maintenance_scheduler_reset_clears_retry_state_only() {
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

        let outcome = state.reset_suppression().await;
        assert_eq!(outcome.previous_consecutive_failures, 1);
        assert_eq!(outcome.consecutive_failures, 0);
        assert!(outcome.reset);

        let snapshot = state.snapshot().await;
        assert!(!snapshot.running);
        assert_eq!(snapshot.total_runs, 1);
        assert_eq!(snapshot.failed_runs, 1);
        assert_eq!(snapshot.successful_runs, 0);
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.last_status.as_deref(), Some("reset"));
        assert!(snapshot.last_error.is_none());
        assert!(snapshot.next_run_at.is_none());
        assert!(!state.is_suppressed().await);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_reset_rejects_running_attempt() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        assert!(state.mark_started(Utc::now()).await);

        let outcome = state.reset_suppression().await;
        assert!(!outcome.reset);
        assert!(outcome.running);

        let snapshot = state.snapshot().await;
        assert!(snapshot.running);
        assert_eq!(snapshot.last_status.as_deref(), Some("running"));
    }
```

- [ ] **Step 2: Run RED state tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_reset_clears_retry_state_only
rtk cargo test -p fdc-server storage_maintenance_scheduler_reset_rejects_running_attempt
```

Expected: FAIL to compile because `reset_suppression()` does not exist.

- [ ] **Step 3: Implement state reset outcome and method**

In `maintenance_scheduler.rs`, add this struct after `StorageMaintenanceSchedulerAttemptResult`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResetOutcome {
    pub reset: bool,
    pub running: bool,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
}
```

Add this method inside `impl StorageMaintenanceSchedulerState` after `mark_suppressed()`:

```rust
    pub async fn reset_suppression(&self) -> StorageMaintenanceSchedulerResetOutcome {
        let mut guard = self.inner.lock().await;
        let previous_consecutive_failures = guard.snapshot.consecutive_failures;
        if guard.snapshot.running {
            return StorageMaintenanceSchedulerResetOutcome {
                reset: false,
                running: true,
                previous_consecutive_failures,
                consecutive_failures: guard.snapshot.consecutive_failures,
            };
        }

        guard.snapshot.consecutive_failures = 0;
        guard.snapshot.last_error = None;
        guard.snapshot.last_status = Some("reset".to_string());
        guard.snapshot.next_run_at = None;

        StorageMaintenanceSchedulerResetOutcome {
            reset: true,
            running: false,
            previous_consecutive_failures,
            consecutive_failures: guard.snapshot.consecutive_failures,
        }
    }
```

- [ ] **Step 4: Run GREEN state tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_reset_clears_retry_state_only
rtk cargo test -p fdc-server storage_maintenance_scheduler_reset_rejects_running_attempt
```

Expected: PASS.

- [ ] **Step 5: Commit state reset**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs
git commit -m "feat(server): reset scheduler suppression state"
```

---

## Task 3: Scheduler reset route TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add request helper and failing route tests**

In `production_server_router_contract.rs`, add helper near `audit_reset_request()`:

```rust
fn scheduler_reset_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}
```

Insert these tests after `storage_maintenance_scheduler_status_reports_suppressed_failures()`:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_reset_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
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

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_clears_suppressed_status_without_running_work() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
            "1",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
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
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["reason"], "contract-test");
    assert_eq!(json["data"]["previous_consecutive_failures"], 1);
    assert_eq!(json["data"]["consecutive_failures"], 0);

    let status_response = router
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
    assert_eq!(status_json["data"]["last_status"], "reset");
    assert_eq!(status_json["data"]["consecutive_failures"], 0);
    assert!(status_json["data"]["last_error"].is_null());
    assert!(status_json["data"]["next_run_at"].is_null());
    assert_eq!(status_json["data"]["total_runs"], 1);
    assert_eq!(status_json["data"]["failed_runs"], 1);
}
```

- [ ] **Step 2: Run RED route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
```

Expected: FAIL because route returns 404 and config field/DTO/service are not wired yet.

- [ ] **Step 3: Add DTOs**

In `model.rs`, after `MarketDataStorageMaintenanceSchedulerStatusResponse`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
}
```

- [ ] **Step 4: Add service reset function**

In `service.rs`, update model imports to include the two DTOs. Add constant and result after audit reset definitions:

```rust
pub const STORAGE_MAINTENANCE_SCHEDULER_RESET_CONFIRMATION: &str = "reset_scheduler_suppression";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMaintenanceSchedulerResetResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceSchedulerResetResponse,
    pub message: Option<String>,
}
```

Add this function after `storage_maintenance_scheduler_status()`:

```rust
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
```

- [ ] **Step 5: Wire router**

In `router.rs`:

1. Add DTO imports:

```rust
MarketDataStorageMaintenanceSchedulerResetRequest,
MarketDataStorageMaintenanceSchedulerResetResponse,
```

2. Add service import:

```rust
reset_storage_maintenance_scheduler,
```

3. Add route after scheduler status route:

```rust
        .route(
            "/market-data/storage/maintenance/scheduler/reset",
            post(storage_maintenance_scheduler_reset_handler),
        )
```

4. Add handler after scheduler status handler:

```rust
async fn storage_maintenance_scheduler_reset_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<MarketDataStorageMaintenanceSchedulerResetRequest>,
) -> (
    StatusCode,
    Json<ServerApiResponse<MarketDataStorageMaintenanceSchedulerResetResponse>>,
) {
    let result = reset_storage_maintenance_scheduler(&state, request).await;
    let status = storage_maintenance_status_code(result.http_status);
    let envelope = if result.http_status == StorageMaintenanceHttpStatus::Ok {
        ServerApiResponse::success(result.response)
    } else {
        ServerApiResponse::error(
            result.response,
            result
                .message
                .unwrap_or_else(|| "storage maintenance scheduler reset request failed".to_string()),
        )
    };
    (status, Json(envelope))
}
```

- [ ] **Step 6: Run GREEN route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
```

Expected: PASS.

- [ ] **Step 7: Commit route**

Run:

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): add scheduler reset route"
```

---

## Task 4: Final verification and docs

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run final verification**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Update development status**

Add a new P34 section above P33 in `docs/DEVELOPMENT_STATUS.md` recording:

```markdown
## 2026-06-07 P34 Scheduler Recovery Controls

Completed:

- Added default-disabled scheduler reset runtime gate.
- Added explicit confirmation-protected scheduler reset route.
- Reset clears scheduler retry/suppression state without running maintenance or restarting scheduler tasks.
- Reset preserves scheduler run counters and audit entries.
- Preserved manual maintenance, audit reset, and scheduler gates as separate controls.
- Preserved the `fdc-storage` boundary.

Design and plan:

- `docs/superpowers/specs/2026-06-07-scheduler-recovery-controls-design.md`
- `docs/superpowers/plans/2026-06-07-scheduler-recovery-controls.md`

Commits:

- `27e3017 docs(server): design scheduler recovery controls`
- Include the concrete P34 implementation commit hashes created by Tasks 1, 2, and 3.

Verification:

- Include the concrete RED/GREEN and final verification pass counts from Task 4 Step 1.

Recommended next slice:

- **P35 scheduler restart/resume controls**, if runtime recovery without process restart is required; otherwise move to live collection hardening.
```

Replace the concrete-output instruction lines with exact commit hashes and pass counts before committing `docs/DEVELOPMENT_STATUS.md`.

- [ ] **Step 3: Commit docs**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record scheduler recovery controls status"
```

- [ ] **Step 4: Check final status**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.

---

## Self-Review

- Spec coverage:
  - Config gate: Task 1.
  - State reset without maintenance/restart: Task 2.
  - Confirmation-protected route: Task 3.
  - Disabled, wrong confirmation, and success route contracts: Task 3.
  - Final docs and boundary verification: Task 4.
- Placeholder scan:
  - No `TBD`, unexplained placeholders, or incomplete code snippets remain. The development status section instructs the implementer to insert exact commit hashes and pass counts after verification.
- Type consistency:
  - DTO names, service names, route path, confirmation string, and config field names match the P34 design spec.
