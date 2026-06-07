# Durable Path/Disk Health Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe durable path readiness fields to `GET /market-data/storage/health` tier rows without changing `fdc-storage` or triggering any mutating storage behavior.

**Architecture:** Extend the server-owned health DTO and merge generic storage health snapshot rows with server runtime path metadata. Path checks are read-only and expose only booleans plus basename hints.

**Tech Stack:** Rust, Axum, Tokio, Serde, `fdc-server`, existing production router contract tests.

---

## File map

- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add RED route assertions for memory-backed tiered health rows.
  - Add RED route test for durable L2/L3/L4 path readiness and full-path redaction.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Extend `MarketDataStorageTierHealth` with durable path readiness fields.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add server-owned path readiness helpers.
  - Merge path readiness into `storage_health` tier rows.
  - Add unit tests for missing-parent path readiness if route-level construction cannot safely cover it.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P29 completion and verification evidence.

---

### Task 1: Add RED route tests for path readiness fields

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:383-432`

- [ ] **Step 1: Extend memory-backed tiered health test**

In `production_storage_health_tiered_backend_reports_initialized_tiers`, inside the loop that checks each tier, add these assertions:

```rust
assert_eq!(tier["durable_path_configured"], false);
assert!(tier["path_hint"].is_null());
assert!(tier["path_exists"].is_null());
assert!(tier["path_parent_exists"].is_null());
assert!(tier["path_parent_writable"].is_null());
```

- [ ] **Step 2: Add durable path readiness route test**

Add this test after `production_storage_health_tiered_backend_reports_initialized_tiers`:

```rust
#[tokio::test]
async fn production_storage_health_reports_durable_path_readiness_without_full_paths() {
    let root = unique_test_path("storage-health-paths");
    std::fs::create_dir_all(&root).expect("durable root should be created");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage health should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    let tiers = json["data"]["tiers"].as_array().unwrap();

    let l1 = tiers.iter().find(|tier| tier["tier"] == "L1").unwrap();
    assert_eq!(l1["durable_path_configured"], false);
    assert!(l1["path_hint"].is_null());
    assert!(l1["path_exists"].is_null());
    assert!(l1["path_parent_exists"].is_null());
    assert!(l1["path_parent_writable"].is_null());

    for (tier_name, hint) in [("L2", "l2.redb"), ("L3", "l3.duckdb"), ("L4", "l4-rocksdb")] {
        let tier = tiers
            .iter()
            .find(|tier| tier["tier"] == tier_name)
            .unwrap_or_else(|| panic!("missing tier {tier_name}"));
        assert_eq!(tier["durable_path_configured"], true);
        assert_eq!(tier["path_hint"], hint);
        assert_eq!(tier["path_exists"], true);
        assert_eq!(tier["path_parent_exists"], true);
        assert_eq!(tier["path_parent_writable"], true);
    }

    let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
    assert!(
        !body_text.contains(root.to_string_lossy().as_ref()),
        "storage health must not leak full configured paths: {body_text}"
    );
}
```

- [ ] **Step 3: Verify RED**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected result:

- Command exits non-zero.
- Tests fail because new JSON fields are `Null` instead of expected booleans/strings.

---

### Task 2: Add DTO fields and route mapping

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs:77-85`
- Modify: `crates/fdc-server/src/market_data/service.rs:365-415`

- [ ] **Step 1: Extend health tier DTO**

Change `MarketDataStorageTierHealth` to:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageTierHealth {
    pub tier: String,
    pub enabled: bool,
    pub initialized: bool,
    pub status: String,
    pub key_count: Option<u64>,
    pub total_size: Option<u64>,
    pub durable_path_configured: bool,
    pub path_hint: Option<String>,
    pub path_exists: Option<bool>,
    pub path_parent_exists: Option<bool>,
    pub path_parent_writable: Option<bool>,
}
```

- [ ] **Step 2: Add path readiness struct and helper**

In `crates/fdc-server/src/market_data/service.rs`, near the status/health helpers, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
struct DurablePathReadiness {
    durable_path_configured: bool,
    path_hint: Option<String>,
    path_exists: Option<bool>,
    path_parent_exists: Option<bool>,
    path_parent_writable: Option<bool>,
}

impl DurablePathReadiness {
    fn unconfigured() -> Self {
        Self {
            durable_path_configured: false,
            path_hint: None,
            path_exists: None,
            path_parent_exists: None,
            path_parent_writable: None,
        }
    }
}

fn durable_path_readiness(path: Option<&std::path::Path>) -> DurablePathReadiness {
    let Some(path) = path else {
        return DurablePathReadiness::unconfigured();
    };

    let path_parent = path.parent();
    let path_parent_exists = path_parent.map(std::path::Path::exists).unwrap_or(false);
    let path_parent_writable = path_parent
        .and_then(|parent| std::fs::metadata(parent).ok())
        .map(|metadata| !metadata.permissions().readonly())
        .unwrap_or(false);

    DurablePathReadiness {
        durable_path_configured: true,
        path_hint: path.file_name().map(|name| name.to_string_lossy().to_string()),
        path_exists: Some(path.exists()),
        path_parent_exists: Some(path_parent_exists),
        path_parent_writable: Some(path_parent_exists && path_parent_writable),
    }
}
```

- [ ] **Step 3: Add tier path mapping helper**

Add this helper near `storage_tier_label`:

```rust
fn configured_durable_path_for_tier<'a>(
    state: &'a ProductionServerState,
    tier: &StorageTier,
) -> Option<&'a std::path::Path> {
    let tiers = &state.config().market_data_storage.tiers;
    match tier {
        StorageTier::L1 => None,
        StorageTier::L2 => tiers.l2_redb_path.as_deref(),
        StorageTier::L3 => tiers.l3_duckdb_path.as_deref(),
        StorageTier::L4 => tiers.l4_rocksdb_path.as_deref(),
    }
}
```

- [ ] **Step 4: Merge readiness into health rows**

Replace the `.map(|tier| MarketDataStorageTierHealth { ... })` block in `storage_health` with:

```rust
.map(|tier| {
    let readiness = durable_path_readiness(configured_durable_path_for_tier(
        state,
        &tier.tier,
    ));
    MarketDataStorageTierHealth {
        tier: storage_tier_label(&tier.tier).to_string(),
        enabled: tier.enabled,
        initialized: tier.initialized,
        status: storage_tier_health_status_label(&tier.status).to_string(),
        key_count: tier.stats.as_ref().map(|stats| stats.key_count),
        total_size: tier.stats.as_ref().map(|stats| stats.total_size),
        durable_path_configured: readiness.durable_path_configured,
        path_hint: readiness.path_hint,
        path_exists: readiness.path_exists,
        path_parent_exists: readiness.path_parent_exists,
        path_parent_writable: readiness.path_parent_writable,
    }
})
```

- [ ] **Step 5: Verify GREEN for route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected result:

- Health route tests pass.

- [ ] **Step 6: Commit route implementation**

Run:

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): report durable path health metadata"
```

---

### Task 3: Add helper coverage for missing parents

**Files:**
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Write failing unit test for missing parent**

Inside the existing `#[cfg(test)] mod tests` in `service.rs`, add:

```rust
#[test]
fn durable_path_readiness_reports_missing_parent_without_creating_paths() {
    let path = std::env::temp_dir()
        .join(format!(
            "fdc-server-missing-parent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .join("l2.redb");

    let readiness = durable_path_readiness(Some(path.as_path()));

    assert_eq!(readiness.durable_path_configured, true);
    assert_eq!(readiness.path_hint.as_deref(), Some("l2.redb"));
    assert_eq!(readiness.path_exists, Some(false));
    assert_eq!(readiness.path_parent_exists, Some(false));
    assert_eq!(readiness.path_parent_writable, Some(false));
    assert!(!path.exists());
    assert!(!path.parent().unwrap().exists());
}
```

- [ ] **Step 2: Verify test passes**

Run:

```bash
rtk cargo test -p fdc-server durable_path_readiness_reports_missing_parent_without_creating_paths
```

Expected result:

- Test passes. If it fails because the helper is not visible, keep the test in the same module as the helper or make the helper private within the tested module.

- [ ] **Step 3: Commit helper coverage**

Run:

```bash
git add crates/fdc-server/src/market_data/service.rs
git commit -m "test(server): cover missing durable path parents"
```

---

### Task 4: Focused regression verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run health route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected result:

- Health route tests pass.

- [ ] **Step 2: Run status route regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected result:

- Status route tests pass.

- [ ] **Step 3: Run maintenance route regression**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
```

Expected result:

- Maintenance route tests pass.

- [ ] **Step 4: Run missing-parent helper test**

Run:

```bash
rtk cargo test -p fdc-server durable_path_readiness_reports_missing_parent_without_creating_paths
```

Expected result:

- Helper test passes.

- [ ] **Step 5: Run storage dependency guard**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected result:

- Dependency guard passes.

- [ ] **Step 6: Run package-scoped fmt check**

Run:

```bash
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected result:

- Command exits 0.

---

### Task 5: Record P29 completion status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add P29 status entry**

Insert above P28:

```markdown
## 2026-06-07 P29 Durable Path/Disk Health Hardening

Completed:

- Extended read-only `GET /market-data/storage/health` tier rows with safe durable path readiness metadata:
  - `durable_path_configured`
  - `path_hint`
  - `path_exists`
  - `path_parent_exists`
  - `path_parent_writable`
- Preserved path safety: only basename hints are exposed, never full configured paths.
- Kept path checks read-only: no probe files or directories are created.
- Added missing-parent helper coverage to verify readiness reports false without creating paths.
- Preserved read-only health behavior: no maintenance, compaction, lifecycle deletion, demotion, audit clear, or audit reset is triggered.
- Preserved the `fdc-storage` boundary: no server/runtime dependency and no market-data DTO dependency were introduced.

Design and plan:

- `docs/superpowers/specs/2026-06-07-durable-path-disk-health-hardening-design.md`
- `docs/superpowers/plans/2026-06-07-durable-path-disk-health-hardening.md`

Commits:

- `<design commit> docs(server): design durable path health hardening`
- `<plan commit> docs(server): plan durable path health hardening`
- `<implementation commit> feat(server): report durable path health metadata`
- `<test commit> test(server): cover missing durable path parents`

Verification:

- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once` - passed
- `rtk cargo test -p fdc-server durable_path_readiness_reports_missing_parent_without_creating_paths` - passed
- `rtk cargo test -p fdc-storage --test dependency_guard` - 1 passed
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- **P30 maintenance scheduler design**: design a default-disabled scheduler/status surface for periodic storage maintenance, keeping destructive/costly behavior explicitly gated and observable before implementation.
```

Replace commit placeholders using:

```bash
rtk git log --oneline -8
```

- [ ] **Step 2: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record durable path health hardening status"
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

- Spec coverage: Tasks 1-5 cover API fields, read-only path checks, full-path redaction, missing-parent behavior, and status documentation.
- Placeholder scan: commit placeholders appear only in the final status template with explicit replacement instructions.
- Type consistency: DTO field names match the spec exactly.
- Boundary check: no task modifies `fdc-storage`; dependency guard verifies crate boundary.
- TDD check: route tests are RED before DTO/service implementation; missing-parent helper coverage is added around the helper behavior.
