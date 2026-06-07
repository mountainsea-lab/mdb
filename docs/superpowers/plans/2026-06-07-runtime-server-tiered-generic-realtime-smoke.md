# P18 Runtime Server Tiered Generic Realtime Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production server contract test proving `tiered + generic_realtime` runtime assembly writes a realistic live fixture into hot tier storage and serves it through the HTTP trade route.

**Architecture:** Use existing `ProductionServerState::try_new`, `ProductionServerState::ingest_test_trade`, `build_production_router`, and `QueryableStorage` APIs. This is a narrow test-only smoke slice with no new production endpoint and no storage behavior changes.

**Tech Stack:** Rust, Tokio, Axum/Tower tests, `fdc-server`, `fdc-storage`, TDD with `rtk cargo test`.

---

## Reference design

- `docs/superpowers/specs/2026-06-07-runtime-server-tiered-generic-realtime-smoke-design.md`
- `crates/fdc-server/tests/production_server_router_contract.rs`
- `crates/fdc-server/src/runtime/app.rs`
- `crates/fdc-server/src/runtime/storage.rs`
- `crates/fdc-server/src/market_data/service.rs`

## File structure

- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add imports for `QueryableStorage`, `StorageQuery`, `StorageTier`, and `StorageTierScope`.
  - Add one focused async test that assembles runtime config, ingests a live fixture, checks L2 tier placement, and queries the HTTP route.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Add P18 completion checkpoint after verification.

---

## Task 1: Add runtime server smoke contract test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add generic query imports**

In `crates/fdc-server/tests/production_server_router_contract.rs`, update the `fdc_storage` import from:

```rust
use fdc_storage::{
    QueryableMarketDataStore, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
    StorageWriteSink,
};
```

to:

```rust
use fdc_storage::{
    QueryableMarketDataStore, QueryableStorage, StorageQuery, StorageTier, StorageTierScope,
    StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord, StorageWriteSink,
};
```

- [ ] **Step 2: Add the smoke test**

Append this test after `production_state_try_new_uses_tiered_runtime_storage_config()`:

```rust
#[tokio::test]
async fn runtime_server_path_routes_live_fixture_with_tiered_generic_realtime() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        (
            "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE",
            "generic_realtime",
        ),
    ])
    .expect("tiered generic realtime runtime config should parse");

    let state = ProductionServerState::try_new(config)
        .await
        .expect("state should assemble tiered generic realtime storage");
    state
        .ingest_test_trade("BTCUSDT", "generic-runtime-live-1")
        .await
        .expect("fixture ingest should write through configured store");

    let store = state.market_data_store();
    let hot_records = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("hot tier query should succeed");

    assert_eq!(hot_records.len(), 1);
    assert_eq!(
        hot_records[0].metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(
        hot_records[0]
            .metadata
            .tags
            .get("record.kind")
            .map(String::as_str),
        Some("trade")
    );

    let router = build_production_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 1);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(
        json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"],
        "generic-runtime-live-1"
    );
}
```

- [ ] **Step 3: Run targeted test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_path_routes_live_fixture_with_tiered_generic_realtime
```

Expected: PASS if P14-P17 already implemented all required runtime behavior. If it fails, inspect the failure before modifying production code.

- [ ] **Step 4: Commit smoke test**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): smoke tiered generic realtime runtime path"
```

---

## Task 2: Final verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run final verification**

Run:

```bash
rtk cargo fmt --package fdc-server --package fdc-storage --check
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_path_routes_live_fixture_with_tiered_generic_realtime
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server --test production_server_router_contract
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected:

- All commands exit 0.
- Full `production_server_router_contract` remains `12+` passed with any existing ignored tests unchanged.
- `fdc-storage` dependency guard remains green.

- [ ] **Step 2: Add P18 status checkpoint**

Prepend this section to `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-07 P18 Runtime Server Tiered Generic Realtime Smoke

Completed:

- Added a production server contract smoke test for `FDC_MARKET_DATA_STORAGE_BACKEND=tiered` with `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime`.
- Verified `ProductionServerState::try_new(config)` assembles the configured tiered market-data store.
- Verified existing server fixture ingestion writes through the configured store and lands live trade data in generic hot tier L2.
- Verified `GET /market-data/trades` reads the same shared store through the production router.
- Preserved `fdc-storage` as a generic storage layer.

Verification:

- `rtk cargo fmt --package fdc-server --package fdc-storage --check`
- `rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_path_routes_live_fixture_with_tiered_generic_realtime`
- `rtk cargo test -p fdc-server --test runtime_config_contract`
- `rtk cargo test -p fdc-server --test production_server_router_contract`
- `rtk cargo test -p fdc-storage --test dependency_guard`
```

- [ ] **Step 3: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record tiered generic realtime runtime smoke"
```

- [ ] **Step 4: Confirm clean working tree**

Run:

```bash
rtk git status --short
```

Expected: clean status.

---

## Self-review

- Spec coverage: the plan verifies runtime config, state assembly, fixture ingestion, tier placement, HTTP route query, and dependency guard.
- Placeholder scan: no placeholders or vague implementation instructions remain.
- Type consistency: all referenced APIs exist in P14-P17 code paths.
- Boundary check: this is test-only server coverage and does not add storage business knowledge.
