# Maintenance Audit Reset Hook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a disabled-by-default, confirmation-protected HTTP hook that clears the server-owned in-memory storage maintenance audit log.

**Architecture:** Keep reset semantics entirely in `fdc-server`: runtime config owns the gate, service owns the admin response semantics, router owns HTTP status mapping, and `MarketDataStorageMaintenanceAuditLog` owns clearing its entries. `fdc-storage` remains unchanged because the audit log is server-owned memory.

**Tech Stack:** Rust, axum, serde, tokio, fdc-server runtime config, existing market-data router/service/model patterns.

---

## File map

- `crates/fdc-server/src/runtime/config.rs`
  - Add `market_data_storage_maintenance_audit_reset_enabled` bool.
  - Parse `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED` truthy values.
  - Add config contract tests.
- `crates/fdc-server/src/market_data/maintenance_audit.rs`
  - Add `clear().await -> usize`.
  - Add unit tests for clearing populated and empty logs.
- `crates/fdc-server/src/market_data/model.rs`
  - Add `MarketDataStorageMaintenanceAuditResetRequest`.
  - Add `MarketDataStorageMaintenanceAuditResetResponse`.
- `crates/fdc-server/src/market_data/service.rs`
  - Add `reset_storage_maintenance_audit` service function.
  - Reuse `StorageMaintenanceHttpStatus` for 200/400/403.
- `crates/fdc-server/src/market_data/router.rs`
  - Add `POST /market-data/storage/maintenance/audit/reset`.
  - Map service result through existing maintenance status-code helper.
- `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route contract tests for disabled, wrong confirmation, successful reset, and empty reset.
- `docs/DEVELOPMENT_STATUS.md`
  - Record P26 after final verification.

---

### Task 1: Runtime config gate

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing config tests**

Add tests in the existing `#[cfg(test)] mod tests` in `runtime/config.rs`:

```rust
#[test]
fn runtime_config_storage_maintenance_audit_reset_defaults_disabled() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("runtime config should parse");

    assert!(!config.market_data_storage_maintenance_audit_reset_enabled);
}

#[test]
fn runtime_config_storage_maintenance_audit_reset_accepts_truthy_override() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED",
        "yes",
    )])
    .expect("runtime config should parse");

    assert!(config.market_data_storage_maintenance_audit_reset_enabled);
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_reset
```

Expected: FAIL because `market_data_storage_maintenance_audit_reset_enabled` does not exist.

- [ ] **Step 3: Implement config field and parsing**

In `ServerRuntimeConfig`, add:

```rust
pub market_data_storage_maintenance_audit_reset_enabled: bool,
```

In `from_env_pairs`, initialize:

```rust
let mut market_data_storage_maintenance_audit_reset_enabled = false;
```

In the env match, add:

```rust
"FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED" => {
    market_data_storage_maintenance_audit_reset_enabled =
        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
}
```

Include the field in the returned config:

```rust
market_data_storage_maintenance_audit_reset_enabled,
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_reset
```

Expected: PASS for both reset-gate config tests.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/runtime/config.rs
git commit -m "feat(server): gate storage audit reset hook"
```

---

### Task 2: Audit log clear primitive

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_audit.rs`

- [ ] **Step 1: Write failing audit-log tests**

Add tests in `maintenance_audit.rs`:

```rust
#[tokio::test]
async fn audit_log_clear_removes_entries_and_returns_count() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);
    log.record_maintenance(audit_entry(1)).await.unwrap();
    log.record_maintenance(audit_entry(2)).await.unwrap();

    let cleared = log.clear().await;
    let snapshot = log.recent(10).await;

    assert_eq!(cleared, 2);
    assert_eq!(snapshot.total_entries, 0);
    assert!(snapshot.entries.is_empty());
}

#[tokio::test]
async fn audit_log_clear_empty_log_returns_zero() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);

    let cleared = log.clear().await;
    let snapshot = log.recent(10).await;

    assert_eq!(cleared, 0);
    assert_eq!(snapshot.total_entries, 0);
    assert!(snapshot.entries.is_empty());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server audit_log_clear
```

Expected: FAIL because `MarketDataStorageMaintenanceAuditLog::clear` does not exist.

- [ ] **Step 3: Implement `clear`**

Add to `impl MarketDataStorageMaintenanceAuditLog`:

```rust
pub async fn clear(&self) -> usize {
    let mut entries = self.entries.lock().await;
    let cleared = entries.len();
    entries.clear();
    cleared
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server audit_log_clear
```

Expected: PASS for both clear tests.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/market_data/maintenance_audit.rs
git commit -m "feat(server): clear storage maintenance audit log"
```

---

### Task 3: Reset route service and HTTP contract

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing disabled route test**

Add helper in `production_server_router_contract.rs`:

```rust
fn audit_reset_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}
```

Add test:

```rust
#[tokio::test]
async fn storage_maintenance_audit_reset_route_is_disabled_by_default() {
    let state = tiered_storage_state_with_audit_capacity(3).await;
    let router = build_production_router(state.clone());

    state.ingest_test_trade("BTCUSDT", "audit-reset-disabled").await.unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("reset_maintenance_audit"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
    assert_eq!(json["data"]["cleared_entries"], 0);

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset_route_is_disabled_by_default
```

Expected: FAIL because the route does not exist and returns 404, or because reset DTO/service does not exist once wired.

- [ ] **Step 3: Write failing success and confirmation tests**

Add helper:

```rust
async fn tiered_storage_state_with_audit_capacity_and_reset(
    capacity: usize,
    reset_enabled: bool,
) -> ProductionServerState {
    let capacity = capacity.to_string();
    let reset_enabled = if reset_enabled { "1" } else { "0" };
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
            capacity.as_str(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED",
            reset_enabled,
        ),
    ])
    .expect("tiered config should parse");

    ProductionServerState::try_new(config)
        .await
        .expect("tiered production state should build")
}
```

Add tests:

```rust
#[tokio::test]
async fn storage_maintenance_audit_reset_route_requires_confirmation() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state.ingest_test_trade("BTCUSDT", "audit-reset-confirm").await.unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_clears_entries_when_enabled_and_confirmed() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state.ingest_test_trade("BTCUSDT", "audit-reset-1").await.unwrap();
    run_successful_storage_maintenance(router.clone()).await;
    state.ingest_test_trade("BTCUSDT", "audit-reset-2").await.unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("reset_maintenance_audit"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["cleared_entries"], 2);
    assert_eq!(json["data"]["remaining_entries"], 0);

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
    assert_eq!(audit_json["data"]["returned_entries"], 0);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_succeeds_on_empty_log() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("reset_maintenance_audit"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["cleared_entries"], 0);
    assert_eq!(json["data"]["remaining_entries"], 0);
}
```

- [ ] **Step 4: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset_route
```

Expected: FAIL because route/service/model are not implemented.

- [ ] **Step 5: Implement model DTOs**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub cleared_entries: usize,
    pub remaining_entries: usize,
}
```

- [ ] **Step 6: Implement service function**

In `service.rs`, add imports for the new DTOs and add:

```rust
pub const STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION: &str = "reset_maintenance_audit";

pub struct StorageMaintenanceAuditResetResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: MarketDataStorageMaintenanceAuditResetResponse,
    pub message: Option<String>,
}

pub async fn reset_storage_maintenance_audit(
    state: &ProductionServerState,
    request: MarketDataStorageMaintenanceAuditResetRequest,
) -> StorageMaintenanceAuditResetResult {
    if !state
        .config()
        .market_data_storage_maintenance_audit_reset_enabled
    {
        return StorageMaintenanceAuditResetResult {
            http_status: StorageMaintenanceHttpStatus::Forbidden,
            response: MarketDataStorageMaintenanceAuditResetResponse {
                accepted: false,
                status: "disabled".to_string(),
                reason: request.reason,
                cleared_entries: 0,
                remaining_entries: state
                    .market_data_storage_maintenance_audit()
                    .recent(0)
                    .await
                    .total_entries,
            },
            message: Some(
                "storage maintenance audit reset is disabled; set FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=1".to_string(),
            ),
        };
    }

    if request.confirm != STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION {
        return StorageMaintenanceAuditResetResult {
            http_status: StorageMaintenanceHttpStatus::BadRequest,
            response: MarketDataStorageMaintenanceAuditResetResponse {
                accepted: false,
                status: "confirmation_required".to_string(),
                reason: request.reason,
                cleared_entries: 0,
                remaining_entries: state
                    .market_data_storage_maintenance_audit()
                    .recent(0)
                    .await
                    .total_entries,
            },
            message: Some(format!(
                "confirm must be '{STORAGE_MAINTENANCE_AUDIT_RESET_CONFIRMATION}'"
            )),
        };
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
```

- [ ] **Step 7: Implement router route**

In `router.rs`, import the new request/response DTOs and `reset_storage_maintenance_audit`, then add route:

```rust
.route(
    "/market-data/storage/maintenance/audit/reset",
    post(storage_maintenance_audit_reset_handler),
)
```

Add handler:

```rust
async fn storage_maintenance_audit_reset_handler(
    State(state): State<ProductionServerState>,
    Json(request): Json<MarketDataStorageMaintenanceAuditResetRequest>,
) -> (
    StatusCode,
    Json<ServerApiResponse<MarketDataStorageMaintenanceAuditResetResponse>>,
) {
    let result = reset_storage_maintenance_audit(&state, request).await;
    let status = storage_maintenance_status_code(result.http_status);
    let envelope = if result.http_status == StorageMaintenanceHttpStatus::Ok {
        ServerApiResponse::success(result.response)
    } else {
        ServerApiResponse::error(
            result.response,
            result
                .message
                .unwrap_or_else(|| "storage maintenance audit reset request failed".to_string()),
        )
    };
    (status, Json(envelope))
}
```

- [ ] **Step 8: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset_route
```

Expected: PASS for all reset route tests.

- [ ] **Step 9: Commit**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose gated storage audit reset hook"
```

---

### Task 4: Final verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused regression suite**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_reset
rtk cargo test -p fdc-server audit_log_clear
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
rtk git status --short
```

Expected: all tests and package-scoped fmt pass; status shows only expected status-doc changes before final commit.

- [ ] **Step 2: Update development status**

Add a P26 section above P25 in `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-07 P26 Maintenance Audit Admin Reset Hook

Completed:

- Added separate default-disabled runtime gate:
  - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=1`
- Added server-owned audit reset DTOs.
- Added `MarketDataStorageMaintenanceAuditLog::clear().await`.
- Added `POST /market-data/storage/maintenance/audit/reset`.
- Reset requires exact confirmation `reset_maintenance_audit`.
- HTTP/status behavior:
  - `403 disabled`
  - `400 confirmation_required`
  - `200 reset`
- Reset clears only server-owned in-memory audit entries and does not call storage or run maintenance.
- Preserved the `fdc-storage` boundary.
```

- [ ] **Step 3: Commit status update**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record maintenance audit reset hook status"
```

- [ ] **Step 4: Final clean verification**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
