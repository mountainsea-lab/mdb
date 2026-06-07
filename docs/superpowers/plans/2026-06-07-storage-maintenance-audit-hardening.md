# Storage Maintenance Audit Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make maintenance audit retention configurable and harden audit route behavior for newest-first ordering, limit handling, and capacity eviction.

**Architecture:** Keep audit ownership in `fdc-server`. Add one bounded server runtime config field, pass it into `ProductionServerState`, and keep `fdc-storage` unchanged. Strengthen tests at the config, audit-log, and production router contract layers.

**Tech Stack:** Rust, axum, tokio, serde, chrono, fdc-server runtime config, generic `fdc_storage::StorageMaintenanceAuditSink`.

---

## File map

- `crates/fdc-server/src/runtime/config.rs`
  - Add `market_data_storage_maintenance_audit_capacity` to `ServerRuntimeConfig`.
  - Parse `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY`.
  - Reject zero and values above 1024.
- `crates/fdc-server/src/runtime/app.rs`
  - Construct `MarketDataStorageMaintenanceAuditLog` with configured capacity.
- `crates/fdc-server/src/market_data/maintenance_audit.rs`
  - Add focused tests for `recent(0)` and capacity eviction if not already complete.
  - No storage/server API dependency changes.
- `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add contract tests for configured capacity, newest-first route ordering, and `limit=1`.
- `docs/DEVELOPMENT_STATUS.md`
  - Record P25 completion after implementation verification.

---

### Task 1: Runtime config audit capacity

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing config tests**

Add tests near existing `ServerRuntimeConfig` env parsing tests:

```rust
#[test]
fn runtime_config_defaults_storage_maintenance_audit_capacity() {
    let _guard = EnvVarGuard::new(&[
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
    ]);

    let config = ServerRuntimeConfig::from_env().expect("runtime config should parse");

    assert_eq!(config.market_data_storage_maintenance_audit_capacity, 32);
}

#[test]
fn runtime_config_accepts_storage_maintenance_audit_capacity_override() {
    let _guard = EnvVarGuard::new(&[
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
    ]);
    std::env::set_var("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY", "7");

    let config = ServerRuntimeConfig::from_env().expect("runtime config should parse");

    assert_eq!(config.market_data_storage_maintenance_audit_capacity, 7);
}

#[test]
fn runtime_config_rejects_zero_storage_maintenance_audit_capacity() {
    let _guard = EnvVarGuard::new(&[
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
    ]);
    std::env::set_var("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY", "0");

    let error = ServerRuntimeConfig::from_env().expect_err("zero capacity should be rejected");

    assert!(error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY must be between 1 and 1024"));
}

#[test]
fn runtime_config_rejects_oversized_storage_maintenance_audit_capacity() {
    let _guard = EnvVarGuard::new(&[
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
    ]);
    std::env::set_var("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY", "1025");

    let error = ServerRuntimeConfig::from_env().expect_err("oversized capacity should be rejected");

    assert!(error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY must be between 1 and 1024"));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_capacity
```

Expected: FAIL because `market_data_storage_maintenance_audit_capacity` does not exist and the env var is not parsed.

- [ ] **Step 3: Implement config field and parsing**

In `ServerRuntimeConfig`, add:

```rust
pub market_data_storage_maintenance_audit_capacity: usize,
```

In `Default`, set:

```rust
market_data_storage_maintenance_audit_capacity: 32,
```

In `from_env`, parse after `market_data_storage_maintenance_enabled`:

```rust
let market_data_storage_maintenance_audit_capacity = parse_env_usize(
    "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
    default.market_data_storage_maintenance_audit_capacity,
)?;
if !(1..=1024).contains(&market_data_storage_maintenance_audit_capacity) {
    return Err(anyhow::anyhow!(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY must be between 1 and 1024"
    ));
}
```

Include the field in the returned config:

```rust
market_data_storage_maintenance_audit_capacity,
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_capacity
```

Expected: PASS for the four capacity tests.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/runtime/config.rs
git commit -m "feat(server): configure storage maintenance audit capacity"
```

---

### Task 2: Wire configured capacity into production state

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing route contract test for configured capacity**

Add a helper if needed to build a tiered test state with `runtime_config.market_data_storage_maintenance_audit_capacity = 2` and maintenance enabled. Then add a test:

```rust
#[tokio::test]
async fn storage_maintenance_audit_route_clamps_to_configured_capacity_newest_first() {
    let state = tiered_storage_state_with_audit_capacity(2).await;
    let app = build_market_data_router(state.clone());

    insert_trade_for_audit(&state, "BTC-USD", 1.0).await;
    run_successful_storage_maintenance(&app).await;
    insert_trade_for_audit(&state, "BTC-USD", 2.0).await;
    run_successful_storage_maintenance(&app).await;
    insert_trade_for_audit(&state, "BTC-USD", 3.0).await;
    run_successful_storage_maintenance(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=10")
                .method(Method::GET)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit request should complete");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["total_entries"], 2);
    assert_eq!(body["data"]["returned_entries"], 2);
    assert_eq!(body["data"]["entries"][0]["lifecycle"]["scanned_entries"], 3);
    assert_eq!(body["data"]["entries"][1]["lifecycle"]["scanned_entries"], 2);
}
```

Use existing response JSON helpers and maintenance run helpers in the test file. If their names differ, keep the same behavior and adapt only to existing helper names.

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route_clamps_to_configured_capacity_newest_first
```

Expected: FAIL because production state still uses the default audit capacity of 32.

- [ ] **Step 3: Implement production state wiring**

In `ProductionServerState` constructors, replace default construction:

```rust
Arc::new(MarketDataStorageMaintenanceAuditLog::default())
```

with:

```rust
Arc::new(MarketDataStorageMaintenanceAuditLog::new(
    config.market_data_storage_maintenance_audit_capacity,
))
```

Ensure all constructors that allocate the audit log use the runtime config value.

- [ ] **Step 4: Run test to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route_clamps_to_configured_capacity_newest_first
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): apply configured storage audit capacity"
```

---

### Task 3: Audit route limit hardening tests

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_audit.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing or strengthening unit test for `recent(0)`**

Add in `maintenance_audit.rs` tests:

```rust
#[tokio::test]
async fn recent_zero_returns_no_entries() {
    let log = MarketDataStorageMaintenanceAuditLog::new(2);
    log.record_maintenance(StorageMaintenanceReport::default())
        .await
        .expect("record should succeed");

    let snapshot = log.recent(0);

    assert_eq!(snapshot.total_entries, 1);
    assert!(snapshot.entries.is_empty());
}
```

- [ ] **Step 2: Write route test for `limit=1` newest entry**

Add in `production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn storage_maintenance_audit_route_limit_one_returns_newest_entry() {
    let state = tiered_storage_state_with_audit_capacity(3).await;
    let app = build_market_data_router(state.clone());

    insert_trade_for_audit(&state, "BTC-USD", 1.0).await;
    run_successful_storage_maintenance(&app).await;
    insert_trade_for_audit(&state, "BTC-USD", 2.0).await;
    run_successful_storage_maintenance(&app).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=1")
                .method(Method::GET)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit request should complete");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_json(response).await;
    assert_eq!(body["data"]["total_entries"], 2);
    assert_eq!(body["data"]["returned_entries"], 1);
    assert_eq!(body["data"]["entries"][0]["lifecycle"]["scanned_entries"], 2);
}
```

- [ ] **Step 3: Run tests**

Run:

```bash
rtk cargo test -p fdc-server maintenance_audit
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route_limit_one_returns_newest_entry
```

Expected: PASS if P24 already had correct `recent` semantics; otherwise FAIL and expose the behavior to fix.

- [ ] **Step 4: Minimal implementation if needed**

If `recent(0)` fails, update `recent` to preserve `total_entries` but return no entries when limit is zero:

```rust
pub fn recent(&self, limit: usize) -> MarketDataStorageMaintenanceAuditSnapshot {
    let entries = self.entries.lock().expect("audit log mutex should not be poisoned");
    let total_entries = entries.len();
    let entries = entries.iter().take(limit).cloned().collect();
    MarketDataStorageMaintenanceAuditSnapshot {
        total_entries,
        entries,
    }
}
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server maintenance_audit
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route_limit_one_returns_newest_entry
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-server/src/market_data/maintenance_audit.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): harden storage audit route limits"
```

---

### Task 4: Final verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused regression suite**

Run:

```bash
rtk cargo test -p fdc-server runtime_config_storage_maintenance_audit_capacity
rtk cargo test -p fdc-server maintenance_audit
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
rtk git status --short
```

Expected: all tests and package-scoped fmt pass; git status shows only expected status-doc changes before final commit.

- [ ] **Step 2: Update development status**

Add a P25 section to `docs/DEVELOPMENT_STATUS.md` recording:

```markdown
### P25 Storage Maintenance Audit Hardening - Completed 2026-06-07

- Made server-owned maintenance audit capacity configurable with `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY`.
- Default capacity remains 32; invalid capacities outside 1..=1024 are rejected during runtime config parsing.
- Production server state now constructs the audit log with configured capacity.
- Added route coverage for capacity clamping, newest-first ordering across multiple successful maintenance runs, and `limit=1` behavior.
- Added audit-log coverage for `recent(0)`.
- `fdc-storage` remains generic and unchanged.
```

- [ ] **Step 3: Commit status update**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record storage audit hardening status"
```

- [ ] **Step 4: Final clean verification**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
