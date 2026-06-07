# Storage Runtime Status Surface Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `GET /market-data/storage/status` with safe server-owned runtime/admin metadata while preserving `fdc-storage` as a generic crate.

**Architecture:** Add fields to the server-owned status DTO and compute them from `ProductionServerState::config()`. The status route remains read-only and does not call storage health, maintenance, compaction, or audit reset.

**Tech Stack:** Rust, Axum, Tokio, Serde, `fdc-server`, existing production router contract tests.

---

## File map

- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Extend existing status route tests with new metadata assertions.
  - Add one new test for maintenance gate and audit capacity metadata.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add fields to `MarketDataStorageStatusResponse`.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Populate new fields in `storage_status`.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record completed P28 work and verification evidence after implementation.

---

### Task 1: Add RED route contract coverage for status metadata

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:219-298`

- [ ] **Step 1: Extend memory default status test**

In `production_storage_status_reports_memory_defaults`, after existing assertions for `backend` and `policy_profile`, add:

```rust
assert_eq!(json["data"]["tiered"], false);
assert_eq!(json["data"]["durable_tiers_configured"], 0);
assert_eq!(json["data"]["maintenance_enabled"], false);
assert_eq!(json["data"]["maintenance_audit_reset_enabled"], false);
assert_eq!(json["data"]["maintenance_audit_capacity"], 32);
```

- [ ] **Step 2: Extend durable tiered status test**

In `production_storage_status_reports_durable_tiered_config_without_full_paths`, after existing assertions for `backend` and `policy_profile`, add:

```rust
assert_eq!(json["data"]["tiered"], true);
assert_eq!(json["data"]["durable_tiers_configured"], 3);
assert_eq!(json["data"]["maintenance_enabled"], false);
assert_eq!(json["data"]["maintenance_audit_reset_enabled"], false);
assert_eq!(json["data"]["maintenance_audit_capacity"], 32);
```

- [ ] **Step 3: Add route test for admin gate metadata**

Add this new test after `production_storage_status_reports_durable_tiered_config_without_full_paths`:

```rust
#[tokio::test]
async fn production_storage_status_reports_maintenance_gate_metadata() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED",
            "1",
        ),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY", "7"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["backend"], "memory");
    assert_eq!(json["data"]["tiered"], false);
    assert_eq!(json["data"]["durable_tiers_configured"], 0);
    assert_eq!(json["data"]["maintenance_enabled"], true);
    assert_eq!(json["data"]["maintenance_audit_reset_enabled"], true);
    assert_eq!(json["data"]["maintenance_audit_capacity"], 7);
}
```

- [ ] **Step 4: Verify RED**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected result:

- Test command exits non-zero.
- Existing/new tests fail because response fields are `Null` instead of expected booleans/numbers.

---

### Task 2: Add status DTO fields and service mapping

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs:47-52`
- Modify: `crates/fdc-server/src/market_data/service.rs:195-218`

- [ ] **Step 1: Extend status DTO**

Change `MarketDataStorageStatusResponse` to include the new fields:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageStatusResponse {
    pub backend: String,
    pub policy_profile: String,
    pub tiered: bool,
    pub durable_tiers_configured: usize,
    pub maintenance_enabled: bool,
    pub maintenance_audit_reset_enabled: bool,
    pub maintenance_audit_capacity: usize,
    pub tiers: Vec<MarketDataStorageTierStatus>,
}
```

- [ ] **Step 2: Map fields in `storage_status`**

In `crates/fdc-server/src/market_data/service.rs`, update `storage_status` to compute durable tier count and populate all fields:

```rust
pub fn storage_status(state: &ProductionServerState) -> MarketDataStorageStatusResponse {
    let config = state.config();
    let storage = &config.market_data_storage;
    let tiered = storage.backend == MarketDataStorageBackendConfig::Tiered;
    let durable_tiers_configured = [
        storage.tiers.l2_redb_path.as_ref(),
        storage.tiers.l3_duckdb_path.as_ref(),
        storage.tiers.l4_rocksdb_path.as_ref(),
    ]
    .into_iter()
    .filter(|path| path.is_some())
    .count();

    MarketDataStorageStatusResponse {
        backend: backend_label(storage.backend).to_string(),
        policy_profile: policy_profile_label(storage.policy_profile).to_string(),
        tiered,
        durable_tiers_configured,
        maintenance_enabled: config.market_data_storage_maintenance_enabled,
        maintenance_audit_reset_enabled: config
            .market_data_storage_maintenance_audit_reset_enabled,
        maintenance_audit_capacity: config.market_data_storage_maintenance_audit_capacity,
        tiers: vec![
            memory_tier_status("L1"),
            tier_status("L2", tiered, "redb", storage.tiers.l2_redb_path.as_deref()),
            tier_status(
                "L3",
                tiered,
                "duckdb",
                storage.tiers.l3_duckdb_path.as_deref(),
            ),
            tier_status(
                "L4",
                tiered,
                "rocksdb",
                storage.tiers.l4_rocksdb_path.as_deref(),
            ),
        ],
    }
}
```

- [ ] **Step 3: Verify GREEN**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected result:

- All `production_storage_status` tests pass.

- [ ] **Step 4: Commit implementation**

Run:

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): harden storage runtime status metadata"
```

---

### Task 3: Focused regression verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run status route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected result:

- `3 passed` for status route tests.

- [ ] **Step 2: Run runtime config maintenance tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance
```

Expected result:

- Runtime config maintenance-related tests pass.

- [ ] **Step 3: Run health route regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected result:

- Health route tests pass.

- [ ] **Step 4: Run storage dependency guard**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected result:

- Dependency guard passes.

- [ ] **Step 5: Run package-scoped fmt check**

Run:

```bash
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected result:

- Command exits 0.

---

### Task 4: Record P28 completion status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add P28 status entry**

Insert a new section above P27:

```markdown
## 2026-06-07 P28 Storage Runtime Status Surface Hardening

Completed:

- Extended read-only `GET /market-data/storage/status` with safe runtime/admin metadata:
  - `tiered`
  - `durable_tiers_configured`
  - `maintenance_enabled`
  - `maintenance_audit_reset_enabled`
  - `maintenance_audit_capacity`
- Kept existing status fields for `backend`, `policy_profile`, and L1-L4 tier summaries.
- Preserved path safety: response still exposes only basename `path_hint` values, never full configured paths.
- Preserved read-only behavior: status does not trigger maintenance, compaction, lifecycle deletion, demotion, audit clear, or audit reset.
- Preserved the `fdc-storage` boundary: no server/runtime dependency and no market-data DTO dependency were introduced.

Design and plan:

- `docs/superpowers/specs/2026-06-07-storage-runtime-status-surface-hardening-design.md`
- `docs/superpowers/plans/2026-06-07-storage-runtime-status-surface-hardening.md`

Commits:

- `<design commit> docs(server): design storage runtime status hardening`
- `<plan commit> docs(server): plan storage runtime status hardening`
- `<implementation commit> feat(server): harden storage runtime status metadata`

Verification:

- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status` - 3 passed
- `rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health` - passed
- `rtk cargo test -p fdc-storage --test dependency_guard` - 1 passed
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- **P29 durable path/disk health hardening**: add safe read-only durable path existence/writability/free-space signals to health/status without exposing full paths and without triggering maintenance.
```

Replace placeholder commit hashes with actual short hashes from:

```bash
rtk git log --oneline -5
```

- [ ] **Step 2: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record storage runtime status hardening status"
```

- [ ] **Step 3: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected result:

- `ok`

---

## Plan self-review

- Spec coverage: every requested field and safety constraint maps to Tasks 1-4.
- Placeholder scan: only commit hash placeholders appear in Task 4, with an explicit command and replacement step before commit.
- Type consistency: field names match the design spec exactly.
- Boundary check: no task modifies `fdc-storage` except running dependency/fmt verification.
- TDD check: production status tests are written and verified RED before DTO/service implementation.
