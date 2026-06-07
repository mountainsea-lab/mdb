# Scheduler Config and Read-Only Status Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement P31 from the P30 scheduler design: scheduler runtime config parsing and a read-only scheduler status route, without spawning any background task or running maintenance automatically.

**Architecture:** Add server runtime config fields for future scheduler behavior, add a server-owned scheduler status DTO/service mapper, and expose `GET /market-data/storage/maintenance/scheduler/status`. The status response is derived from config/state only and reports zero runtime counters because P31 does not start a scheduler task.

**Tech Stack:** Rust, Axum, Tokio, Serde, `fdc-server`, existing runtime config and production router contract tests.

---

## File map

- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Add scheduler config fields to `ServerRuntimeConfig`.
  - Parse/validate scheduler env vars.
  - Add helper validation functions if needed.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add public contract tests for scheduler defaults, overrides, and invalid values.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add `MarketDataStorageMaintenanceSchedulerStatusResponse` DTO.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add `storage_maintenance_scheduler_status(&ProductionServerState)` read-only mapper.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add `GET /market-data/storage/maintenance/scheduler/status` route.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route tests for default disabled status, configured enabled status, and non-mutating behavior.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P31 completion and verification evidence.

---

### Task 1: Add RED runtime config tests

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Add defaults test assertions**

In `runtime_config_defaults_are_safe_for_local_production_server`, after existing maintenance assertions, add:

```rust
assert!(!config.market_data_storage_maintenance_scheduler_enabled);
assert_eq!(config.market_data_storage_maintenance_scheduler_interval_seconds, 3600);
assert_eq!(config.market_data_storage_maintenance_scheduler_timeout_ms, 30000);
assert_eq!(config.market_data_storage_maintenance_scheduler_jitter_seconds, 0);
assert_eq!(
    config.market_data_storage_maintenance_scheduler_max_consecutive_failures,
    3
);
```

- [ ] **Step 2: Add scheduler override test**

Add:

```rust
#[test]
fn storage_maintenance_scheduler_config_accepts_valid_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "120",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "45000",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
            "30",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "5",
        ),
    ])
    .expect("scheduler config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_enabled);
    assert_eq!(config.market_data_storage_maintenance_scheduler_interval_seconds, 120);
    assert_eq!(config.market_data_storage_maintenance_scheduler_timeout_ms, 45000);
    assert_eq!(config.market_data_storage_maintenance_scheduler_jitter_seconds, 30);
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_max_consecutive_failures,
        5
    );
}
```

- [ ] **Step 3: Add scheduler validation test**

Add:

```rust
#[test]
fn storage_maintenance_scheduler_config_rejects_invalid_values() {
    let interval_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
        "59",
    )])
    .expect_err("short interval should be rejected");
    assert!(interval_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS"));

    let timeout_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
        "999",
    )])
    .expect_err("short timeout should be rejected");
    assert!(timeout_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS"));

    let failures_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
        "0",
    )])
    .expect_err("zero max failures should be rejected");
    assert!(failures_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES"));
}
```

- [ ] **Step 4: Verify RED**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler
```

Expected:

- Compile/test failure because scheduler config fields do not exist.

---

### Task 2: Implement scheduler runtime config parsing

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Add fields to `ServerRuntimeConfig`**

Add fields after `market_data_storage_maintenance_audit_reset_enabled`:

```rust
pub market_data_storage_maintenance_scheduler_enabled: bool,
pub market_data_storage_maintenance_scheduler_interval_seconds: u64,
pub market_data_storage_maintenance_scheduler_timeout_ms: u64,
pub market_data_storage_maintenance_scheduler_jitter_seconds: u64,
pub market_data_storage_maintenance_scheduler_max_consecutive_failures: u32,
```

- [ ] **Step 2: Add defaults in parser**

In `from_env_pairs`, initialize:

```rust
let mut market_data_storage_maintenance_scheduler_enabled = false;
let mut market_data_storage_maintenance_scheduler_interval_seconds = 3600_u64;
let mut market_data_storage_maintenance_scheduler_timeout_ms = 30000_u64;
let mut market_data_storage_maintenance_scheduler_jitter_seconds = 0_u64;
let mut market_data_storage_maintenance_scheduler_max_consecutive_failures = 3_u32;
```

- [ ] **Step 3: Parse env vars**

Add match arms:

```rust
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED" => {
    market_data_storage_maintenance_scheduler_enabled =
        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
}
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS" => {
    market_data_storage_maintenance_scheduler_interval_seconds = parse_u64_range(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
        value.as_ref(),
        60,
        86400,
    )?;
}
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS" => {
    market_data_storage_maintenance_scheduler_timeout_ms = parse_u64_range(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
        value.as_ref(),
        1000,
        600000,
    )?;
}
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS" => {
    market_data_storage_maintenance_scheduler_jitter_seconds = parse_u64_range(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
        value.as_ref(),
        0,
        3600,
    )?;
}
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES" => {
    market_data_storage_maintenance_scheduler_max_consecutive_failures = parse_u32_range(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
        value.as_ref(),
        1,
        100,
    )?;
}
```

- [ ] **Step 4: Add cross-field jitter validation**

Before constructing `Self`, add:

```rust
let max_jitter = (market_data_storage_maintenance_scheduler_interval_seconds / 2).min(3600);
if market_data_storage_maintenance_scheduler_jitter_seconds > max_jitter {
    return Err(Error::config(format!(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS must be <= min(interval/2, 3600), got {} with interval {}",
        market_data_storage_maintenance_scheduler_jitter_seconds,
        market_data_storage_maintenance_scheduler_interval_seconds
    )));
}
```

- [ ] **Step 5: Add fields to `Ok(Self { ... })`**

Add all five scheduler fields to the returned config.

- [ ] **Step 6: Add parsing helpers**

Add below `parse_positive_usize`:

```rust
fn parse_u64_range(name: &str, value: &str, min: u64, max: u64) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| Error::config(format!("{name} must be between {min} and {max}: {error}")))?;
    if !(min..=max).contains(&parsed) {
        return Err(Error::config(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(parsed)
}

fn parse_u32_range(name: &str, value: &str, min: u32, max: u32) -> Result<u32> {
    let parsed = value
        .parse::<u32>()
        .map_err(|error| Error::config(format!("{name} must be between {min} and {max}: {error}")))?;
    if !(min..=max).contains(&parsed) {
        return Err(Error::config(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(parsed)
}
```

- [ ] **Step 7: Verify GREEN**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler
```

Expected:

- Scheduler config tests pass.

- [ ] **Step 8: Commit runtime config slice**

Run:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): parse storage maintenance scheduler config"
```

---

### Task 3: Add RED scheduler status route tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add default scheduler status route test**

Add near other storage maintenance route tests:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_status_reports_disabled_defaults() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

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
    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["enabled"], false);
    assert_eq!(data["running"], false);
    assert_eq!(data["backend"], "memory");
    assert_eq!(data["tiered"], false);
    assert_eq!(data["interval_seconds"], 3600);
    assert_eq!(data["timeout_ms"], 30000);
    assert_eq!(data["jitter_seconds"], 0);
    assert_eq!(data["max_consecutive_failures"], 3);
    assert_eq!(data["consecutive_failures"], 0);
    assert_eq!(data["total_runs"], 0);
    assert_eq!(data["successful_runs"], 0);
    assert_eq!(data["failed_runs"], 0);
    assert_eq!(data["skipped_runs"], 0);
    assert!(data["last_started_at"].is_null());
    assert!(data["last_finished_at"].is_null());
    assert!(data["last_status"].is_null());
    assert!(data["last_error"].is_null());
    assert!(data["next_run_at"].is_null());
}
```

- [ ] **Step 2: Add configured scheduler status route test**

Add:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_status_reports_configured_values_without_running() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "120",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "45000",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
            "30",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "5",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

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
    let data = &json["data"];
    assert_eq!(data["enabled"], true);
    assert_eq!(data["running"], false);
    assert_eq!(data["backend"], "tiered");
    assert_eq!(data["tiered"], true);
    assert_eq!(data["interval_seconds"], 120);
    assert_eq!(data["timeout_ms"], 45000);
    assert_eq!(data["jitter_seconds"], 30);
    assert_eq!(data["max_consecutive_failures"], 5);
    assert_eq!(data["total_runs"], 0);
    assert!(data["next_run_at"].is_null());
}
```

- [ ] **Step 3: Verify RED**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status
```

Expected:

- Tests fail because the route is missing or response fields are missing.

---

### Task 4: Implement read-only scheduler status route

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`

- [ ] **Step 1: Add DTO**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerStatusResponse {
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
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub next_run_at: Option<String>,
}
```

- [ ] **Step 2: Add service mapper**

In `service.rs`, add:

```rust
pub fn storage_maintenance_scheduler_status(
    state: &ProductionServerState,
) -> MarketDataStorageMaintenanceSchedulerStatusResponse {
    let config = state.config();
    let backend = config.market_data_storage.backend;
    let tiered = backend == MarketDataStorageBackendConfig::Tiered;

    MarketDataStorageMaintenanceSchedulerStatusResponse {
        enabled: config.market_data_storage_maintenance_scheduler_enabled,
        running: false,
        backend: backend_label(backend).to_string(),
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
    }
}
```

- [ ] **Step 3: Wire router**

In `router.rs` imports, include `MarketDataStorageMaintenanceSchedulerStatusResponse` and `storage_maintenance_scheduler_status`.

Add route:

```rust
.route(
    "/market-data/storage/maintenance/scheduler/status",
    get(storage_maintenance_scheduler_status_handler),
)
```

Add handler:

```rust
async fn storage_maintenance_scheduler_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageMaintenanceSchedulerStatusResponse>> {
    Json(ServerApiResponse::success(
        storage_maintenance_scheduler_status(&state),
    ))
}
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status
```

Expected:

- Scheduler status route tests pass.

- [ ] **Step 5: Commit status route slice**

Run:

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose storage maintenance scheduler status"
```

---

### Task 5: Focused regression verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run scheduler runtime config tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler
```

Expected: scheduler runtime config tests pass.

- [ ] **Step 2: Run scheduler status route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status
```

Expected: scheduler status route tests pass.

- [ ] **Step 3: Run maintenance run-once regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
```

Expected: manual maintenance route tests still pass.

- [ ] **Step 4: Run audit route regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: audit route tests still pass.

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

### Task 6: Record P31 completion status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add P31 status entry**

Insert above P30:

```markdown
## 2026-06-07 P31 Scheduler Config and Read-Only Status Surface

Completed:

- Added default-disabled storage maintenance scheduler runtime config parsing:
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED`
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS`
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS`
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS`
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES`
- Added read-only scheduler status DTO and route:
  - `GET /market-data/storage/maintenance/scheduler/status`
- P31 does not spawn a scheduler task and does not run maintenance automatically.
- Status reports configured values plus zero runtime counters.
- Preserved separation between manual run-once gate and scheduler gate.
- Preserved the `fdc-storage` boundary: no server/runtime dependency and no market-data DTO dependency were introduced.

Design and plan:

- `docs/superpowers/specs/2026-06-07-maintenance-scheduler-design.md`
- `docs/superpowers/plans/2026-06-07-scheduler-config-status-surface.md`

Commits:

- `<plan commit> docs(server): plan scheduler config status surface`
- `<config commit> feat(server): parse storage maintenance scheduler config`
- `<route commit> feat(server): expose storage maintenance scheduler status`

Verification:

- `rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route` - passed
- `rtk cargo test -p fdc-storage --test dependency_guard` - 1 passed
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- **P32 scheduler task skeleton, disabled by default**: add background task wiring that activates only under the scheduler gate, with non-overlap protection and state counters.
```

Replace commit placeholders using:

```bash
rtk git log --oneline -8
```

- [ ] **Step 2: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record scheduler config status surface"
```

- [ ] **Step 3: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected: `ok`.

---

## Plan self-review

- P30 coverage: P31 implements only config parsing and read-only status, no scheduler task.
- TDD coverage: runtime config tests RED before parser changes, route tests RED before route/DTO/service changes.
- Safety: no automatic maintenance, no background spawn, no mutating route.
- Boundary: no `fdc-storage` changes except dependency guard verification.
- Placeholder scan: commit hash placeholders appear only in final status template with explicit replacement instructions.
