# Explicit Storage Maintenance Hook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-disabled, confirmation-required `POST /market-data/storage/maintenance/run-once` hook that runs one tiered storage maintenance pass only when explicitly enabled.

**Architecture:** `fdc-storage` exposes a generic optional maintenance facade on `QueryableMarketDataStore`. `fdc-server` owns runtime gating, request confirmation, HTTP status mapping, and response DTOs.

**Tech Stack:** Rust, Tokio, Axum, Serde, `fdc-storage`, `fdc-server`, package-scoped cargo tests.

---

## File map

- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add `market_data_storage_maintenance_enabled` and env parsing.
- Modify `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add default-disabled and truthy-enabled config tests.
- Modify `crates/fdc-storage/src/queryable.rs`
  - Add `run_maintenance_once_with_options` facade and tests.
- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add maintenance request/response DTOs.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Add gated maintenance service function and report mapping.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add POST route and map service result to envelope/status.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route contract tests for disabled, wrong confirmation, memory unsupported, and tiered completed.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P23 completion and verification.

---

### Task 1: Runtime config gate

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Test: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Add failing config tests**

Add tests to `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
#[test]
fn storage_maintenance_hook_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap();

    assert!(!config.market_data_storage_maintenance_enabled);
}

#[test]
fn storage_maintenance_hook_can_be_enabled_by_env() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
    ])
    .unwrap();

    assert!(config.market_data_storage_maintenance_enabled);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_hook
```

Expected: compile failure because `market_data_storage_maintenance_enabled` does not exist.

- [ ] **Step 3: Implement config field and parsing**

In `ServerRuntimeConfig`, add:

```rust
pub market_data_storage_maintenance_enabled: bool,
```

In `from_env_pairs`, initialize:

```rust
let mut market_data_storage_maintenance_enabled = false;
```

Add match arm:

```rust
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED" => {
    market_data_storage_maintenance_enabled = matches!(value.as_ref(), "1" | "true" | "yes" | "on");
}
```

Add field to returned `Self`.

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_hook
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
rtk git commit -m "feat(server): gate storage maintenance hook by runtime config"
```

---

### Task 2: Storage maintenance facade

**Files:**
- Modify: `crates/fdc-storage/src/queryable.rs`

- [ ] **Step 1: Add failing facade tests**

Add imports in the existing test module:

```rust
use crate::StorageMaintenanceOptions;
```

Add tests:

```rust
#[tokio::test]
async fn in_memory_market_data_store_has_no_maintenance_pass() {
    let store = QueryableMarketDataStore::in_memory();

    let report = store
        .run_maintenance_once_with_options(StorageMaintenanceOptions::default())
        .await
        .unwrap();

    assert!(report.is_none());
}

#[tokio::test]
async fn tiered_market_data_store_runs_maintenance_pass() {
    let store = QueryableMarketDataStore::memory_tiered().await.unwrap();

    let report = store
        .run_maintenance_once_with_options(StorageMaintenanceOptions::default())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(report.lifecycle.scanned_entries, 0);
    assert_eq!(report.compaction_failed, 0);
    assert_eq!(report.health.tiers.len(), 4);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-storage maintenance_pass
```

Expected: compile failure because facade method does not exist.

- [ ] **Step 3: Implement facade method**

Import `StorageMaintenanceOptions` and `StorageMaintenanceReport`, then add to `impl QueryableMarketDataStore`:

```rust
pub async fn run_maintenance_once_with_options(
    &self,
    options: StorageMaintenanceOptions,
) -> Result<Option<StorageMaintenanceReport>> {
    match &self.backend {
        QueryableMarketDataBackend::InMemory(_) => Ok(None),
        QueryableMarketDataBackend::Tiered(store) => Ok(Some(
            store.run_maintenance_once_with_options(options).await?,
        )),
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-storage maintenance_pass
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/fdc-storage/src/queryable.rs
rtk git commit -m "feat(storage): expose market data maintenance facade"
```

---

### Task 3: Server maintenance route contracts

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing route tests**

Add helper:

```rust
fn maintenance_request(confirm: &str) -> Body {
    Body::from(format!(r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#))
}
```

Add tests:

```rust
#[tokio::test]
async fn storage_maintenance_run_once_is_disabled_by_default() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
}

#[tokio::test]
async fn storage_maintenance_run_once_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_run_once_rejects_memory_backend() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["status"], "unsupported_backend");
}

#[tokio::test]
async fn storage_maintenance_run_once_completes_for_enabled_tiered_backend() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "completed");
    assert!(json["data"]["duration_ms"].as_i64().unwrap() >= 0);
    assert_eq!(json["data"]["scanned_entries"], 0);
    assert_eq!(json["data"]["compaction_failed"], 0);
    assert_eq!(json["data"]["healthy_tiers"], 4);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
```

Expected: route returns 404 or compile failure because route/models do not exist.

---

### Task 4: Implement server DTOs, service, and route

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`

- [ ] **Step 1: Add DTOs**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceRunRequest {
    pub confirm: String,
    pub timeout_ms: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceRunResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub duration_ms: Option<i64>,
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub compacted_tiers: usize,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
    pub healthy_tiers: usize,
    pub degraded_tiers: usize,
}
```

- [ ] **Step 2: Add service result and mapping**

In `service.rs`, import `StorageMaintenanceOptions` and `Duration`. Add:

```rust
pub enum StorageMaintenanceHttpStatus {
    Ok,
    BadRequest,
    Forbidden,
    Conflict,
    InternalServerError,
}

pub struct StorageMaintenanceServiceResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceRunResponse,
    pub message: Option<String>,
}
```

Add `run_storage_maintenance_once(...)` that:

- returns `disabled` if config flag is false
- returns `confirmation_required` if confirm is not `run_maintenance_once`
- returns `confirmation_required` if `timeout_ms == Some(0)`
- builds `StorageMaintenanceOptions` with timeout when provided
- calls `state.market_data_store().run_maintenance_once_with_options(options).await`
- maps `None` to `unsupported_backend`
- maps `Some(report)` to `completed`
- maps `Err(error)` to `failed`

- [ ] **Step 3: Add route**

In `router.rs`, add route:

```rust
.route("/market-data/storage/maintenance/run-once", post(storage_maintenance_run_once_handler))
```

Add handler that converts `StorageMaintenanceHttpStatus` to Axum `StatusCode` and returns `(StatusCode, Json<ServerApiResponse<_>>)`.

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat(server): expose gated storage maintenance hook"
```

---

### Task 5: Verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_hook
rtk cargo test -p fdc-storage maintenance_pass
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all tests pass and package-scoped fmt exits 0.

- [ ] **Step 2: Update development status**

Add P23 entry to `docs/DEVELOPMENT_STATUS.md` with:

- design and plan paths
- env flag name and default-disabled behavior
- route path
- confirmation token requirement
- tiered-only behavior
- verification commands and pass counts
- next recommended slice

- [ ] **Step 3: Commit status update**

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record explicit storage maintenance hook status"
```

- [ ] **Step 4: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected: no output or `ok`.
