# P37 Four-Tier End-to-End Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic P37 acceptance coverage proving fixture/live-like acquisition writes into tiered durable storage, survives reopen, serves HTTP trade queries, and remains safe across maintenance and P36 live recovery controls.

**Architecture:** Keep P37 at the server contract layer by extending `production_server_router_contract.rs` with reusable acceptance helpers and three named `p37_` tests. The tests exercise existing runtime config, `ProductionServerState::try_new`, fixture ingestion, tiered L2 queries, Axum routes, maintenance audit, and live resume safety controls without adding public-network dependencies.

**Tech Stack:** Rust, Tokio async tests, Axum router contract tests, `fdc-server`, `fdc-storage`, RTK-wrapped Cargo verification.

---

## File Structure

- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add small helper functions near existing helpers at the top of the file:
    - `p37_durable_config()` builds durable tiered runtime configs with optional extra env flags.
    - `p37_query_trades()` calls `/market-data/trades` and returns parsed JSON.
    - `p37_trade_ids()` extracts trade IDs from query JSON as a sorted set.
    - `p37_assert_trade_ids()` asserts query counts and trade-ID membership without depending on record order.
    - `p37_l2_records()` queries only L2 hot storage records.
    - `p37_ingest_fixture_trades()` writes deterministic BTC/ETH fixture trades.
  - Add three P37 tests after the existing durable/runtime query coverage and before live supervisor unit tests:
    - `p37_tiered_acquisition_query_acceptance_survives_reopen`
    - `p37_maintenance_after_ingestion_records_audit_without_hiding_query_data`
    - `p37_live_recovery_controls_do_not_mutate_persisted_market_data`
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Mark P37 as completed after implementation and record commits plus verification commands.

No `fdc-storage` source files should be modified for P37 unless a focused failing test proves a storage contract defect.

---

### Task 1: Add P37 Acceptance Helper Functions

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:147-181`

- [ ] **Step 1: Write one temporary failing helper compile check**

Insert this temporary test after `response_body_json()` so helper names are required by the compiler before implementation:

```rust
#[tokio::test]
async fn p37_helper_compile_contract() {
    let root = unique_test_path("p37-helper-compile");
    let config = p37_durable_config(&root, &[]);
    let state = ProductionServerState::try_new(config)
        .await
        .expect("p37 helper config should build");
    p37_ingest_fixture_trades(&state)
        .await
        .expect("p37 fixture ingestion should write");

    let l2 = p37_l2_records(&state).await;
    assert_eq!(l2.len(), 3);

    let router = build_production_router(state);
    let json = p37_query_trades(router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&json, &["p37-btc-1", "p37-btc-2"]);

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Run the temporary compile check and verify it fails**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_helper_compile_contract
```

Expected: FAIL to compile with missing functions such as `p37_durable_config`, `p37_ingest_fixture_trades`, `p37_l2_records`, `p37_query_trades`, and `p37_assert_trade_ids`.

- [ ] **Step 3: Add imports needed by the helpers**

Change the first import in `crates/fdc-server/tests/production_server_router_contract.rs` from:

```rust
use std::sync::Arc;
```

to:

```rust
use std::{collections::BTreeSet, sync::Arc};
```

- [ ] **Step 4: Add the P37 helper implementations**

Insert this block immediately after `response_body_json()`:

```rust
fn p37_durable_config(root: &std::path::Path, extra_env: &[(&str, &str)]) -> ServerRuntimeConfig {
    let mut env = durable_tier_env(root).to_vec();
    env.extend(
        extra_env
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
    );

    ServerRuntimeConfig::from_env_pairs(env.iter().map(|(key, value)| (key.as_str(), value.as_str())))
        .expect("p37 durable runtime config should parse")
}

async fn p37_query_trades(router: axum::Router, symbol: &str, limit: usize) -> serde_json::Value {
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/market-data/trades?symbol={symbol}&limit={limit}"))
                .body(Body::empty())
                .expect("trade query request should build"),
        )
        .await
        .expect("trade query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    response_body_json(response).await
}

fn p37_trade_ids(json: &serde_json::Value) -> BTreeSet<String> {
    json["data"]["records"]
        .as_array()
        .expect("records should be an array")
        .iter()
        .map(|record| {
            record["payload"]["payload"]["Trade"]["trade_id"]
                .as_str()
                .expect("trade id should be a string")
                .to_string()
        })
        .collect()
}

fn p37_assert_trade_ids(json: &serde_json::Value, expected: &[&str]) {
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], expected.len());

    let actual = p37_trade_ids(json);
    let expected: BTreeSet<String> = expected.iter().map(|trade_id| (*trade_id).to_string()).collect();
    assert_eq!(actual, expected);
}

async fn p37_l2_records(state: &ProductionServerState) -> Vec<fdc_storage::StorageRecord> {
    state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("p37 L2 query should succeed")
}

async fn p37_ingest_fixture_trades(
    state: &ProductionServerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    state.ingest_test_trade("BTCUSDT", "p37-btc-1").await?;
    state.ingest_test_trade("ETHUSDT", "p37-eth-1").await?;
    state.ingest_test_trade("BTCUSDT", "p37-btc-2").await?;
    Ok(())
}
```

- [ ] **Step 5: Run the temporary compile check and verify it passes**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_helper_compile_contract
```

Expected: PASS with one test executed.

- [ ] **Step 6: Remove the temporary helper compile check**

Delete only the temporary `p37_helper_compile_contract` test added in Step 1. Keep all helper functions.

- [ ] **Step 7: Commit the helpers**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): add p37 acceptance helpers"
```

Expected: commit succeeds.

---

### Task 2: Add Reopen-Survives-Acquisition Query Acceptance Test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:2070-2153`

- [ ] **Step 1: Write the failing P37 reopen acceptance test**

Insert this test after `runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade()`:

```rust
#[tokio::test]
async fn p37_tiered_acquisition_query_acceptance_survives_reopen() {
    let root = unique_test_path("p37-acquisition-query-reopen");
    let config = p37_durable_config(&root, &[]);

    let first_state = ProductionServerState::try_new(config.clone())
        .await
        .expect("first p37 durable state should build");
    p37_ingest_fixture_trades(&first_state)
        .await
        .expect("p37 fixture ingestion should write");

    let before_reopen = p37_l2_records(&first_state).await;
    assert_eq!(before_reopen.len(), 3);
    assert!(before_reopen.iter().all(|record| {
        record.metadata.tags.get("mode").map(String::as_str) == Some("live")
            && record
                .metadata
                .tags
                .get("record.kind")
                .map(String::as_str)
                == Some("trade")
    }));

    let before_router = build_production_router(first_state.clone());
    let before_json = p37_query_trades(before_router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&before_json, &["p37-btc-1", "p37-btc-2"]);
    assert!(before_json["data"]["records"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| record["symbol"] == "BTCUSDT"));

    drop(first_state);

    let reopened_state = ProductionServerState::try_new(config)
        .await
        .expect("reopened p37 durable state should build");
    let persisted_l2 = p37_l2_records(&reopened_state).await;
    assert_eq!(persisted_l2.len(), 3);
    assert!(persisted_l2.iter().all(|record| {
        record.metadata.tags.get("mode").map(String::as_str) == Some("live")
            && record
                .metadata
                .tags
                .get("record.kind")
                .map(String::as_str)
                == Some("trade")
    }));

    let reopened_router = build_production_router(reopened_state);
    let reopened_json = p37_query_trades(reopened_router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&reopened_json, &["p37-btc-1", "p37-btc-2"]);
    assert!(reopened_json["data"]["records"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| record["symbol"] == "BTCUSDT"));

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Run only the new P37 test and verify behavior**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_tiered_acquisition_query_acceptance_survives_reopen
```

Expected before Task 1 helpers exist: FAIL to compile. Expected after Task 1 helpers exist: PASS. If it fails at runtime, keep the failure output and fix only the assertion that contradicts existing route/storage contracts.

- [ ] **Step 3: Commit the reopen acceptance test**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): accept p37 durable acquisition query reopen"
```

Expected: commit succeeds.

---

### Task 3: Add Maintenance-After-Ingestion Acceptance Test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:1430-1455` or after the P37 reopen test block

- [ ] **Step 1: Write the failing P37 maintenance acceptance test**

Insert this test after `p37_tiered_acquisition_query_acceptance_survives_reopen()`:

```rust
#[tokio::test]
async fn p37_maintenance_after_ingestion_records_audit_without_hiding_query_data() {
    let root = unique_test_path("p37-maintenance-after-ingestion");
    let config = p37_durable_config(
        &root,
        &[("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")],
    );
    let state = ProductionServerState::try_new(config)
        .await
        .expect("p37 maintenance state should build");
    p37_ingest_fixture_trades(&state)
        .await
        .expect("p37 fixture ingestion should write");

    let router = build_production_router(state);
    let before_json = p37_query_trades(router.clone(), "BTCUSDT", 10).await;
    p37_assert_trade_ids(&before_json, &["p37-btc-1", "p37-btc-2"]);

    let maintenance_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("maintenance request should build"),
        )
        .await
        .expect("maintenance route should respond");
    assert_eq!(maintenance_response.status(), StatusCode::OK);
    let maintenance_json = response_body_json(maintenance_response).await;
    assert_eq!(maintenance_json["status"], "success");
    assert_eq!(maintenance_json["data"]["accepted"], true);
    assert_eq!(maintenance_json["data"]["status"], "completed");
    assert_eq!(maintenance_json["data"]["healthy_tiers"], 4);

    let audit_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=10")
                .body(Body::empty())
                .expect("audit request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["status"], "success");
    assert_eq!(audit_json["data"]["total_entries"], 1);
    assert_eq!(audit_json["data"]["total_recorded_entries"], 1);
    assert_eq!(audit_json["data"]["entries"][0]["healthy_tiers"], 4);

    let after_json = p37_query_trades(router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&after_json, &["p37-btc-1", "p37-btc-2"]);

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Run only the new maintenance acceptance test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_maintenance_after_ingestion_records_audit_without_hiding_query_data
```

Expected: PASS after Task 1 helpers exist. If maintenance scanned counts differ, keep assertions on `status`, `accepted`, `completed`, `healthy_tiers`, audit totals, and query data visibility because those are the P37 contract.

- [ ] **Step 3: Commit the maintenance acceptance test**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): accept p37 maintenance query safety"
```

Expected: commit succeeds.

---

### Task 4: Add Live-Recovery-Controls Data Safety Acceptance Test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs:1837-1893` or after the P37 maintenance test block

- [ ] **Step 1: Write the failing P37 live recovery safety test**

Insert this test after `p37_maintenance_after_ingestion_records_audit_without_hiding_query_data()`:

```rust
#[tokio::test]
async fn p37_live_recovery_controls_do_not_mutate_persisted_market_data() {
    use fdc_server::market_data::service::start_fake_background_live_for_test;

    let root = unique_test_path("p37-live-recovery-data-safety");
    let config = p37_durable_config(
        &root,
        &[
            ("FDC_LIVE_ENABLED", "1"),
            ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
        ],
    );
    let state = ProductionServerState::try_new(config)
        .await
        .expect("p37 live recovery state should build");
    p37_ingest_fixture_trades(&state)
        .await
        .expect("p37 fixture ingestion should write");

    let initial_count = state.market_data_store().record_count();
    assert_eq!(initial_count, 3);

    let router = build_production_router(state.clone());
    let before_json = p37_query_trades(router.clone(), "BTCUSDT", 10).await;
    let before_ids = p37_trade_ids(&before_json);
    p37_assert_trade_ids(&before_json, &["p37-btc-1", "p37-btc-2"]);

    let wrong_confirmation = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("wrong"))
                .expect("wrong confirmation resume request should build"),
        )
        .await
        .expect("wrong confirmation resume should respond");
    assert_eq!(wrong_confirmation.status(), StatusCode::BAD_REQUEST);
    let wrong_json = response_body_json(wrong_confirmation).await;
    assert_eq!(wrong_json["status"], "error");
    assert_eq!(wrong_json["data"]["resumed"], false);

    start_fake_background_live_for_test(&state, 10, std::time::Duration::from_millis(25))
        .await
        .expect("fake live should start");
    let running_conflict = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("resume_live_collection"))
                .expect("running conflict resume request should build"),
        )
        .await
        .expect("running conflict resume should respond");
    assert_eq!(running_conflict.status(), StatusCode::CONFLICT);

    let after_json = p37_query_trades(router.clone(), "BTCUSDT", 10).await;
    p37_assert_trade_ids(&after_json, &["p37-btc-1", "p37-btc-2"]);
    assert_eq!(p37_trade_ids(&after_json), before_ids);
    assert_eq!(state.market_data_store().record_count(), initial_count);

    let stop = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .expect("stop request should build"),
        )
        .await
        .expect("stop should respond");
    assert_eq!(stop.status(), StatusCode::OK);

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Run only the new live recovery data safety test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_live_recovery_controls_do_not_mutate_persisted_market_data
```

Expected: PASS after Task 1 helpers exist. If fake live increments supervisor counters, that is acceptable; the test must keep proving stored record count and BTC trade query results remain unchanged.

- [ ] **Step 3: Commit the live recovery safety test**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): accept p37 live recovery data safety"
```

Expected: commit succeeds.

---

### Task 5: Documentation, Focused Verification, and Completion Commit

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run all P37 acceptance tests together**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_
```

Expected: PASS with exactly the three intended P37 tests. If the removed temporary helper test still runs, delete it before continuing.

- [ ] **Step 2: Run regression tests named in the P37 spec**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all commands PASS.

- [ ] **Step 3: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, find the post-P36 roadmap section containing P37. Replace the P37 entry with text equivalent to this, preserving the surrounding document style:

```markdown
- **P37 Acquisition → Four-Tier Storage → Query End-to-End Acceptance:** completed. Added deterministic server contract acceptance coverage for fixture/live-like acquisition into durable tiered storage, L2 hot-tier verification, durable reopen readback, `/market-data/trades` query validation, maintenance/audit safety after ingestion, and P36 live recovery controls preserving persisted market data. Default P37 tests avoid public internet access; real-network live smoke remains ignored/manual.
```

Add a verification note near the same status area:

```markdown
P37 verification:

- `rtk cargo test -p fdc-server --test production_server_router_contract p37_`
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once`
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume`
- `rtk cargo test -p fdc-storage --test dependency_guard`
- `rtk cargo fmt -p fdc-server -p fdc-storage -- --check`
```

- [ ] **Step 4: Commit documentation and final status**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record p37 acceptance completion"
```

Expected: commit succeeds.

- [ ] **Step 5: Final working tree check**

Run:

```bash
rtk git status --short --branch
```

Expected: current branch is clean and ahead of origin. If execution used an isolated worktree, follow the finishing-development-branch skill to merge back to `mdb-mqdev`, re-run the focused verification commands on `mdb-mqdev`, and remove the feature worktree only after merge verification passes.

---

## Self-Review Against Spec

Spec coverage mapping:

- Deterministic fixture/live-like ingestion: Task 1 helper `p37_ingest_fixture_trades()` and all three P37 tests.
- Tiered runtime config with durable tier paths: Task 1 helper `p37_durable_config()` and Tasks 2-4.
- Hot L2 write verification: Task 2 uses `p37_l2_records()` before and after reopen.
- Durable reopen verification: Task 2 drops and rebuilds `ProductionServerState` from the same config.
- HTTP `/market-data/trades` verification: Tasks 2-4 use `p37_query_trades()` and `p37_assert_trade_ids()`.
- Manual maintenance plus audit observation: Task 3 posts run-once and checks audit totals.
- P36 live recovery safety: Task 4 checks wrong confirmation and running conflict while stored records/query IDs stay unchanged.
- Public internet excluded: no task calls `/market-data/live/start`; fake background live is local test-only support.
- Documentation and verification: Task 5 updates `docs/DEVELOPMENT_STATUS.md` and runs the spec verification commands.

Type and name consistency checks:

- Helper names are identical across Tasks 1-4.
- Existing project helpers are used with their known names: `unique_test_path`, `durable_tier_env`, `maintenance_request`, `live_resume_request`, `response_body_json`, `ingest_test_trade`, `build_production_router`.
- Existing route paths match current tests: `/market-data/trades`, `/market-data/storage/maintenance/run-once`, `/market-data/storage/maintenance/audit?limit=10`, `/market-data/live/resume`, `/market-data/live/stop`.
- Test names exactly match the approved P37 spec.
