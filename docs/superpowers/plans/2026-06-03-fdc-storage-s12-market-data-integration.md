# fdc-storage S12 Market Data Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Back `QueryableMarketDataStore` with S11 `TieredStorageStore` and prove market-data write/query flows work through storage-backed server/API integration.

**Architecture:** Keep `QueryableMarketDataStore` as the compatibility facade used by `fdc-server` and `fdc-api`. Add an internal backend enum with the existing in-memory implementation and a new tiered-storage implementation, then inject the tiered-backed facade in integration tests and explicit server state construction.

**Tech Stack:** Rust, Tokio, `async_trait`, Axum test routers, `fdc-storage` generic `StorageWriteSink`/`QueryableStorage`, `TieredStorageStore::memory_only()`.

---

## File structure

- Modify `crates/fdc-storage/src/queryable.rs`
  - Add `QueryableMarketDataBackend`.
  - Add constructors `in_memory()`, `from_tiered_store()`, and `memory_tiered()`.
  - Delegate write/query/count/all-record behavior by backend.
- Modify `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`
  - Add tiered-backed parity tests for market-data query behavior and invalid batch rejection.
- Modify `crates/fdc-server/src/runtime/app.rs`
  - Add `ProductionServerState::with_market_data_store()` to inject a prebuilt store.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add tiered-backed server write/query route contract.
- Modify `crates/fdc-api/tests/market_data_route_contract.rs`
  - Add tiered-backed API query helper/router contract.
- Modify `crates/fdc-storage/README.md`
  - Record S12 integration baseline once complete.
- Modify `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
  - Record market-data facade integration validation.
- Create `docs/superpowers/progress/2026-06-03-mdb-storage-s12-progress.md`
  - Handoff summary and verification results.

## Task 1: Add storage-backed `QueryableMarketDataStore` backend

**Files:**
- Modify: `crates/fdc-storage/src/queryable.rs`
- Test: `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`

- [ ] **Step 1: Add failing tiered-backed query test**

Append this test to `crates/fdc-storage/tests/queryable_market_data_store_contract.rs` after `queryable_store_returns_records_by_symbol`:

```rust
#[tokio::test]
async fn tiered_queryable_store_returns_records_by_symbol() {
    let store = QueryableMarketDataStore::memory_tiered()
        .await
        .expect("memory tiered store should initialize");
    let record = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"symbol":"BTCUSDT"}"#);

    store
        .write_batch(StorageWriteBatch::new(vec![record.clone()]))
        .await
        .expect("valid market data record should write to tiered store");

    let records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));

    assert_eq!(records, vec![record]);
    assert_eq!(store.record_count(), 1);
    assert_eq!(store.all_records().len(), 1);
}
```

- [ ] **Step 2: Run the failing storage test**

Run:

```bash
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract tiered_queryable_store_returns_records_by_symbol
```

Expected: FAIL because `QueryableMarketDataStore::memory_tiered` does not exist.

- [ ] **Step 3: Implement backend enum and constructors**

In `crates/fdc-storage/src/queryable.rs`, replace the current `QueryableMarketDataStore` struct and impl with this shape. Keep the existing imports and add `std::sync::Arc`, `StorageTierScope`, and `TieredStorageStore` to the imports.

```rust
use std::sync::Arc;

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageQuery,
    StorageTierScope, StorageWriteBatch, StorageWriteOutcome, StorageWriteRecord, StorageWriteSink,
    TieredStorageStore,
};
```

Replace:

```rust
#[derive(Debug, Default)]
pub struct QueryableMarketDataStore {
    inner: InMemoryQueryableStorage,
}
```

with:

```rust
enum QueryableMarketDataBackend {
    InMemory(InMemoryQueryableStorage),
    Tiered(Arc<TieredStorageStore>),
}

pub struct QueryableMarketDataStore {
    backend: QueryableMarketDataBackend,
}
```

Replace the impl with:

```rust
impl Default for QueryableMarketDataStore {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl QueryableMarketDataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_memory() -> Self {
        Self {
            backend: QueryableMarketDataBackend::InMemory(InMemoryQueryableStorage::new()),
        }
    }

    pub fn from_tiered_store(store: Arc<TieredStorageStore>) -> Self {
        Self {
            backend: QueryableMarketDataBackend::Tiered(store),
        }
    }

    pub async fn memory_tiered() -> Result<Self> {
        let store = TieredStorageStore::memory_only().await?;
        Ok(Self::from_tiered_store(Arc::new(store)))
    }

    pub fn query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => {
                futures::executor::block_on(inner.query_storage(&query.to_storage_query()))
                    .expect("market data query should be valid")
            }
            QueryableMarketDataBackend::Tiered(store) => {
                futures::executor::block_on(store.query_storage(&query.to_storage_query()))
                    .expect("market data query should be valid")
            }
        }
    }

    pub fn all_records(&self) -> Vec<StorageWriteRecord> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => inner.all_records(),
            QueryableMarketDataBackend::Tiered(store) => futures::executor::block_on(
                store.query_storage(
                    &StorageQuery::new("market_data")
                        .with_tier_scope(StorageTierScope::All),
                ),
            )
            .expect("market data record scan should be valid"),
        }
    }

    pub fn record_count(&self) -> usize {
        self.all_records().len()
    }
}
```

Update `impl StorageWriteSink for QueryableMarketDataStore` to:

```rust
#[async_trait]
impl StorageWriteSink for QueryableMarketDataStore {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => inner.write_batch(batch).await,
            QueryableMarketDataBackend::Tiered(store) => store.write_batch(batch).await,
        }
    }
}
```

- [ ] **Step 4: Run the storage test again**

Run:

```bash
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract tiered_queryable_store_returns_records_by_symbol
```

Expected: PASS.

- [ ] **Step 5: Run existing queryable unit tests**

Run:

```bash
rtk cargo test -p fdc-storage queryable
```

Expected: PASS. This proves `QueryableMarketDataStore::new()` still uses the in-memory backend and existing compatibility is preserved.

- [ ] **Step 6: Commit Task 1**

```bash
git add crates/fdc-storage/src/queryable.rs crates/fdc-storage/tests/queryable_market_data_store_contract.rs
git commit -m "feat(storage): add tiered market data store backend"
```

## Task 2: Add backend parity tests for limit, count, and invalid batch behavior

**Files:**
- Modify: `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`

- [ ] **Step 1: Add tiered collection/limit and invalid batch tests**

Append these tests to `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`:

```rust
#[tokio::test]
async fn tiered_queryable_store_applies_trade_collection_and_limit() {
    let store = QueryableMarketDataStore::memory_tiered()
        .await
        .expect("memory tiered store should initialize");
    let first = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"trade_id":"1"}"#);
    let second = market_data_record("BTCUSDT", "trade", b"btc-2", br#"{"trade_id":"2"}"#);
    let book = StorageWriteRecord::new(
        "market_data",
        "order_book_l1",
        b"book-1".to_vec(),
        br#"{"symbol":"BTCUSDT"}"#.to_vec(),
    );

    store
        .write_batch(StorageWriteBatch::new(vec![first.clone(), second, book]))
        .await
        .expect("valid mixed market data records should write");

    let records = store.query(
        &MarketDataQuery::for_trades()
            .with_symbol("BTCUSDT")
            .with_limit(1),
    );

    assert_eq!(records, vec![first]);
    assert_eq!(store.record_count(), 3);
    assert_eq!(store.all_records().len(), 3);
}

#[tokio::test]
async fn tiered_queryable_store_rejects_invalid_batch_atomically() {
    let store = QueryableMarketDataStore::memory_tiered()
        .await
        .expect("memory tiered store should initialize");
    let seed = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"symbol":"BTCUSDT"}"#);
    store
        .write_batch(StorageWriteBatch::new(vec![seed.clone()]))
        .await
        .expect("seed record should write");

    let invalid = StorageWriteRecord::new("market_data", "trades", Vec::new(), b"value".to_vec());
    let error = store
        .write_batch(StorageWriteBatch::new(vec![invalid]))
        .await
        .expect_err("invalid record should be rejected");

    assert!(error
        .to_string()
        .contains("storage write record key must not be empty"));
    assert_eq!(store.all_records(), vec![seed]);
}
```

- [ ] **Step 2: Run the new parity tests**

Run:

```bash
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract tiered_queryable_store
```

Expected: PASS.

- [ ] **Step 3: Run the full storage test package**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: PASS.

- [ ] **Step 4: Commit Task 2**

```bash
git add crates/fdc-storage/tests/queryable_market_data_store_contract.rs
git commit -m "test(storage): cover tiered market data store parity"
```

## Task 3: Add server state injection and tiered-backed server route contract

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing server integration test**

In `crates/fdc-server/tests/production_server_router_contract.rs`, update the imports:

```rust
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use fdc_storage::QueryableMarketDataStore;
use tower::ServiceExt;
```

Add this test after `production_trade_query_reads_shared_store_after_fixture_ingest_helper`:

```rust
#[tokio::test]
async fn production_trade_query_reads_tiered_backed_market_data_store() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("config should parse");
    let store = Arc::new(
        QueryableMarketDataStore::memory_tiered()
            .await
            .expect("memory tiered store should initialize"),
    );
    let state = ProductionServerState::with_market_data_store(config, store);

    state
        .ingest_test_trade("BTCUSDT", "tiered-prod-btc-1")
        .await
        .expect("fixture ingest should write to tiered-backed store");

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
}
```

- [ ] **Step 2: Run the failing server test**

Run:

```bash
rtk cargo test -p fdc-server production_trade_query_reads_tiered_backed_market_data_store
```

Expected: FAIL because `ProductionServerState::with_market_data_store` does not exist.

- [ ] **Step 3: Implement `ProductionServerState::with_market_data_store`**

In `crates/fdc-server/src/runtime/app.rs`, add this method inside `impl ProductionServerState` after `new(config)`:

```rust
    pub fn with_market_data_store(
        config: ServerRuntimeConfig,
        market_data_store: Arc<QueryableMarketDataStore>,
    ) -> Self {
        Self {
            config,
            market_data_store,
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
        }
    }
```

- [ ] **Step 4: Run the server test again**

Run:

```bash
rtk cargo test -p fdc-server production_trade_query_reads_tiered_backed_market_data_store
```

Expected: PASS.

- [ ] **Step 5: Run fdc-server tests**

Run:

```bash
rtk cargo test -p fdc-server
```

Expected: PASS. Ignored live smoke tests remain ignored unless explicitly enabled.

- [ ] **Step 6: Commit Task 3**

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): support injected market data storage backend"
```

## Task 4: Add tiered-backed API market-data contract

**Files:**
- Modify: `crates/fdc-api/tests/market_data_route_contract.rs`

- [ ] **Step 1: Add tiered-backed seeded state helper**

In `crates/fdc-api/tests/market_data_route_contract.rs`, add this helper after `seeded_state()`:

```rust
async fn tiered_seeded_state() -> ApiAppState {
    let store = Arc::new(
        QueryableMarketDataStore::memory_tiered()
            .await
            .expect("memory tiered store should initialize"),
    );
    store
        .write_batch(StorageWriteBatch::new(vec![
            trade_record("BTCUSDT", b"btc-1", "btc-1"),
            trade_record("ETHUSDT", b"eth-1", "eth-1"),
            trade_record("BTCUSDT", b"btc-2", "btc-2"),
        ]))
        .await
        .expect("seed records should write");

    ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store)
}
```

- [ ] **Step 2: Add failing API helper/router tests**

Append these tests after `in_memory_router_returns_seeded_trade_json`:

```rust
#[tokio::test]
async fn tiered_pure_helper_filters_trades_by_symbol() {
    let state = tiered_seeded_state().await;

    let response = query_market_data_trades(
        &state,
        MarketDataTradeQueryParams {
            symbol: Some("BTCUSDT".to_string()),
            limit: Some(10),
        },
    );

    assert_eq!(response.status, "success");
    assert_eq!(response.data.returned_records, 2);
    assert!(response.data.records.iter().all(|record| {
        record.symbol.as_deref() == Some("BTCUSDT") && record.kind.as_deref() == Some("trade")
    }));
}

#[tokio::test]
async fn tiered_router_returns_seeded_trade_json() {
    let state = tiered_seeded_state().await;
    let router = build_market_data_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be JSON");

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(json["data"]["records"][0]["payload"]["symbol"], "BTCUSDT");
}
```

These tests should pass if Tasks 1-3 are complete because they use the public `QueryableMarketDataStore::memory_tiered()` constructor.

- [ ] **Step 3: Run API market-data tests**

Run:

```bash
rtk cargo test -p fdc-api --test market_data_route_contract tiered
```

Expected: PASS.

- [ ] **Step 4: Run fdc-api tests**

Run:

```bash
rtk cargo test -p fdc-api
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

```bash
git add crates/fdc-api/tests/market_data_route_contract.rs
git commit -m "test(api): cover tiered market data query route"
```

## Task 5: Update S12 documentation and progress handoff

**Files:**
- Modify: `crates/fdc-storage/README.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Create: `docs/superpowers/progress/2026-06-03-mdb-storage-s12-progress.md`

- [ ] **Step 1: Update `crates/fdc-storage/README.md`**

In the `Current status` section, change:

```markdown
Status as of S11: **P1-closed, pre-integration-ready**.
```

to:

```markdown
Status as of S12: **P1-closed, market-data integration baseline validated**.
```

After the S11 bullet list, add:

```markdown
## Completed in S12

- `QueryableMarketDataStore` can use either the original in-memory backend or a `TieredStorageStore` backend.
- Market-data write/query contracts pass against a memory-tiered S11 storage backend.
- `fdc-server` can inject a storage-backed market-data facade for production router tests.
- `fdc-api` market-data query contracts pass with a storage-backed facade.
```

- [ ] **Step 2: Update `storage-boundary-acceptance-report.md`**

In `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`, change the status line to:

```markdown
Status: S12 market-data integration baseline validated
```

Add a capability matrix row after `P1 closure`:

```markdown
| Market-data integration baseline | Done | `QueryableMarketDataStore` can delegate to `TieredStorageStore`; server/API contracts validate write/query flow through storage-backed facade |
```

Add a section after `S11 P1 Closure Notes`:

```markdown
## S12 Market-data Integration Notes

- `QueryableMarketDataStore` remains the market-data facade used by `fdc-server` and `fdc-api`.
- The facade can now be backed by S11 `TieredStorageStore` while preserving the original in-memory constructor behavior.
- Server and API contract tests validate that market-data writes and `/market-data/trades` queries work through a storage-backed facade.
```

- [ ] **Step 3: Create progress handoff**

Create `docs/superpowers/progress/2026-06-03-mdb-storage-s12-progress.md`:

```markdown
# MDB fdc-storage S12 Progress Handoff

Date: 2026-06-03
Branch: `fdc-storage-s12-market-data-integration`
Module scope: `fdc-storage`, `fdc-server`, `fdc-api`

## Completed

S12 market-data integration baseline is complete.

- `QueryableMarketDataStore` supports the original in-memory backend and a `TieredStorageStore` backend.
- Market-data store contract tests cover tiered-backed symbol filtering, trade collection filtering, limits, counts, all-record scans, and invalid batch rejection.
- `ProductionServerState` can be built with an injected market-data store.
- `fdc-server` production router tests validate fixture ingest and `/market-data/trades` query through a tiered-backed facade.
- `fdc-api` market-data route tests validate helper and router query behavior through a tiered-backed facade.

## Verification

```bash
rtk cargo test -p fdc-storage
rtk cargo test -p fdc-server
rtk cargo test -p fdc-api
```

Expected result: all three package test suites pass.

## Remaining follow-up work

- Runtime assembly should later choose/configure storage backends from environment or config instead of tests injecting memory-tiered stores.
- The full ingestion → transform → storage pipeline remains a later phase.
- Storage P2/P3 production hardening remains tracked in `crates/fdc-storage/README.md` and `production-hardening-followups.md`.

## Dirty files to avoid

Unrelated `fdc-server health` path-move files pre-existed in the main worktree. Do not modify them during S12.
```

- [ ] **Step 4: Commit Task 5**

```bash
git add crates/fdc-storage/README.md crates/fdc-storage/docs/storage-boundary-acceptance-report.md docs/superpowers/progress/2026-06-03-mdb-storage-s12-progress.md
git commit -m "docs(storage): record s12 market data integration"
```

## Task 6: Final verification and cleanup

**Files:**
- No source files expected unless formatting changes are produced.

- [ ] **Step 1: Format**

Run:

```bash
rtk cargo fmt -p fdc-storage -p fdc-server -p fdc-api
```

Expected: command exits 0.

- [ ] **Step 2: Run final package tests**

Run:

```bash
rtk cargo test -p fdc-storage
rtk cargo test -p fdc-server
rtk cargo test -p fdc-api
```

Expected: all pass.

- [ ] **Step 3: Clean generated storage test artifacts**

If tests create untracked storage data, remove only generated storage artifacts:

```bash
rm -rf crates/fdc-storage/data/duckdb.db crates/fdc-storage/data/redb crates/fdc-storage/data/rocksdb
```

Do not remove unrelated `crates/fdc-server/src/health/*` or `crates/fdc-server/src/bin/health/*` files.

- [ ] **Step 4: Commit formatting changes if any**

Run:

```bash
rtk git status --short --branch
```

If only S12 source/doc formatting changes exist, commit them:

```bash
git add crates/fdc-storage crates/fdc-server crates/fdc-api docs/superpowers/progress/2026-06-03-mdb-storage-s12-progress.md
git commit -m "style(storage): format s12 market data integration"
```

If there are no S12 formatting changes, do not create an empty commit.

- [ ] **Step 5: Final status**

Run:

```bash
rtk git status --short --branch
```

Expected: S12 worktree is clean except for any pre-existing unrelated `fdc-server health` path-move files if working in the main worktree. Prefer an isolated worktree to avoid those files.

## Execution notes

- Execute S12 in an isolated worktree from `mdb-mqdev`, for example branch `fdc-storage-s12-market-data-integration`.
- Keep `fdc-storage` generic. Do not add dependencies from `fdc-storage` to `fdc-server`, `fdc-api`, `fdc-barter`, `fdc-ingestion`, `fdc-transform`, or `fdc-orchestrator`.
- Commit after each task.
- If `rtk cargo test` creates `crates/fdc-storage/data/*`, clean only those generated artifacts.
