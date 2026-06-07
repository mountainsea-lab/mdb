# Durable Runtime Persistence Reopen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a server runtime configured with durable tier paths can write market-data records, rebuild with the same paths, and read the persisted records back.

**Architecture:** Add a server contract smoke test that uses `ProductionServerState::try_new` with tiered durable runtime config, writes through the existing test trade helper, drops the state, rebuilds with the same paths, and verifies persisted data through both `QueryableStorage` and the production route. If this exposes engine reopen data loss, add the minimal generic storage engine fix and a focused regression test.

**Tech Stack:** Rust, Tokio tests, Axum router contract tests, `fdc-storage` tier engines (`redb`, DuckDB, RocksDB), generic `TierConfig` path config.

---

## File Structure

- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add the durable runtime reopen acceptance smoke.
  - Use temp paths and existing production router/query assertions.
- Modify `crates/fdc-storage/src/engines/redb.rs` only if the smoke exposes existing redb path reopen failure.
  - Use open-or-create behavior for `db_path` instead of always creating/truncating/failing.
- Optionally modify `crates/fdc-storage/src/engines/redb.rs` tests if redb needs a focused regression.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P20 result and next recommended slice.

## Task 1: Add server durable reopen smoke test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add helper functions if missing**

Near existing test helpers, add:

```rust
fn unique_test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fdc-server-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn durable_tier_env(root: &std::path::Path) -> [(String, String); 5] {
    [
        (
            "FDC_MARKET_DATA_STORAGE_BACKEND".to_string(),
            "tiered".to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE".to_string(),
            "generic_realtime".to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L2_REDB_PATH".to_string(),
            root.join("l2.redb").display().to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH".to_string(),
            root.join("l3.duckdb").display().to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH".to_string(),
            root.join("l4-rocksdb").display().to_string(),
        ),
    ]
}
```

If the file already has equivalent helpers, reuse them and avoid duplicate definitions.

- [ ] **Step 2: Add failing acceptance test**

Add this test:

```rust
#[tokio::test]
async fn runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade() {
    let root = unique_test_path("durable-reopen");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .expect("durable runtime config should parse");

    let first_state = ProductionServerState::try_new(config.clone())
        .await
        .expect("first durable state should build");
    first_state
        .ingest_test_trade("BTCUSDT", "durable-reopen-live-1")
        .await
        .expect("fixture write should succeed");

    let before_reopen = first_state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("L2 query before reopen should succeed");
    assert_eq!(before_reopen.len(), 1);
    assert_eq!(
        before_reopen[0].metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );

    drop(first_state);

    let reopened_state = ProductionServerState::try_new(config)
        .await
        .expect("reopened durable state should build");
    let persisted_l2 = reopened_state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("L2 query after reopen should succeed");
    assert_eq!(persisted_l2.len(), 1);
    assert_eq!(
        persisted_l2[0].metadata.tags.get("record.kind").map(String::as_str),
        Some("trade")
    );

    let router = build_production_router(reopened_state);
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
    assert_eq!(
        json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"],
        "durable-reopen-live-1"
    );

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 3: Run test red**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade
```

Expected: fail before implementation if an engine reopen bug exists. The likely failure is redb creation/opening an existing configured file or returning no persisted L2 record after rebuild.

## Task 2: Fix generic durable engine reopen behavior if needed

**Files:**
- Modify: `crates/fdc-storage/src/engines/redb.rs` only if Task 1 fails due to redb reopen behavior.

- [ ] **Step 1: Add focused redb reopen regression if the server smoke fails in redb**

If the failure points to redb reopen, add this test in `crates/fdc-storage/src/engines/redb.rs`:

```rust
#[tokio::test]
async fn redb_engine_reopens_existing_database_without_losing_data() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("reopen.redb");
    let config = HashMap::from([(
        "db_path".to_string(),
        db_path.display().to_string(),
    )]);

    {
        let mut engine = RedbEngine::new(config.clone()).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"key", b"value").await.unwrap();
    }

    let mut reopened = RedbEngine::new(config).await.unwrap();
    reopened.initialize().await.unwrap();
    assert_eq!(reopened.get(b"key").await.unwrap(), Some(b"value".to_vec()));
}
```

Run:

```bash
rtk cargo test -p fdc-storage redb_engine_reopens_existing_database_without_losing_data
```

Expected: fails before the fix if `Database::create` cannot reopen or truncates an existing DB.

- [ ] **Step 2: Implement open-or-create in redb**

In `RedbEngine::new`, replace unconditional `Database::create(&db_path)` with:

```rust
let db = if db_path.exists() {
    Database::open(&db_path).map_err(redb_error)?
} else {
    Database::create(&db_path).map_err(redb_error)?
};
```

Do not delete existing files and do not change `engine_config` keys.

- [ ] **Step 3: Verify redb regression if added**

Run:

```bash
rtk cargo test -p fdc-storage redb_engine_reopens_existing_database_without_losing_data
```

Expected: pass.

- [ ] **Step 4: Commit engine fix if any code changed**

```bash
git add crates/fdc-storage/src/engines/redb.rs
git commit -m "fix(storage): reopen existing redb tier databases"
```

If Task 1 passes without engine changes, skip this commit.

## Task 3: Verify server reopen smoke and commit test

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Run server reopen smoke**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade
```

Expected: pass.

- [ ] **Step 2: Run related P19 smoke**

Run:

```bash
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
```

Expected: pass.

- [ ] **Step 3: Commit server smoke test**

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): verify durable runtime tier reopen"
```

## Task 4: Final verification and status docs

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Update development status**

Add a P20 entry above P19 that records:

- Durable runtime reopen scenario completed.
- Data was verified through `QueryableStorage` after rebuild.
- Data was verified through `/market-data/trades` after rebuild.
- Any generic engine reopen fix, if made.
- Verification commands and results.
- Next recommended slice.

- [ ] **Step 3: Commit status docs**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record durable runtime persistence reopen status"
```

- [ ] **Step 4: Final cleanliness check**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
