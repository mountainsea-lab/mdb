# B10 Bounded API Market Data Query Route Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the B9 queryable market-data store through one bounded in-memory API route for MVP storage -> query validation.

**Architecture:** `fdc-api` consumes `fdc-storage::QueryableMarketDataStore` through `ApiAppState`. A focused `market_data` module translates query params to `MarketDataQuery`, converts storage records into API DTOs, and builds an Axum router for `GET /market-data/trades`. No real listener, acquisition runner, persistence, SQL engine, or orchestrator mapping logic is added to `fdc-api`.

**Tech Stack:** Rust 1.95, `fdc-api`, `fdc-storage`, `axum`, `tower::ServiceExt`, `serde`, `serde_json`, `Arc`.

---

## File Structure

- Modify: `crates/fdc-api/Cargo.toml`  
  Add dependency on `fdc-storage`. Enable `tower` util feature for route tests.

- Modify: `crates/fdc-api/src/state.rs`  
  Add a shared `QueryableMarketDataStore` handle to `ApiAppState`.

- Create: `crates/fdc-api/src/market_data.rs`  
  Owns market-data query params, response DTOs, pure query helper, and router factory.

- Modify: `crates/fdc-api/src/lib.rs`  
  Export `market_data` module and B10 public helpers/types.

- Create: `crates/fdc-api/tests/market_data_route_contract.rs`  
  Contract tests for pure helper, limit behavior, in-memory Axum route, and dependency guard.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Record B10 completion and next B11 bounded acquisition runner slice.

---

## Task 1: Add failing B10 API market-data route contract tests

**Files:**
- Modify: `crates/fdc-api/Cargo.toml`
- Create: `crates/fdc-api/tests/market_data_route_contract.rs`

- [ ] **Step 1: Add B10 dependencies**

Update `crates/fdc-api/Cargo.toml`:

```toml
fdc-storage = { path = "../fdc-storage" }
tower = { version = "0.4", features = ["util"] }
```

- [ ] **Step 2: Write failing contract tests**

Create tests covering:

- pure helper filters `BTCUSDT` from a seeded store
- pure helper applies `limit=1`
- in-memory router returns HTTP 200 and response JSON for `/market-data/trades?symbol=BTCUSDT&limit=10`
- dependency guard lower-level crates do not reference `fdc-api`

- [ ] **Step 3: Run RED**

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
```

Expected: FAIL because B10 exports/functions do not exist.

---

## Task 2: Implement B10 state extension and market-data module

**Files:**
- Modify: `crates/fdc-api/src/state.rs`
- Create: `crates/fdc-api/src/market_data.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Extend `ApiAppState`**

Add `Arc<QueryableMarketDataStore>` to `ApiAppState`, default it to an empty store, and expose `with_market_data_store` plus `market_data_store`.

- [ ] **Step 2: Implement market-data query DTOs/helper/router**

Create `market_data.rs` with:

- `MarketDataTradeQueryParams`
- `MarketDataTradeRecord`
- `MarketDataTradesResponse`
- `query_market_data_trades`
- `build_market_data_router`

Route handler should clone state from Axum `State<ApiAppState>` and return `Json<ApiResponse<MarketDataTradesResponse>>`.

- [ ] **Step 3: Export B10 module and types**

Update `lib.rs` to export `market_data` and public B10 functions/types.

- [ ] **Step 4: Run GREEN contract test**

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
```

Expected: PASS.

---

## Task 3: Verify and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run final B10 verification**

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-storage
```

Expected: all pass.

- [ ] **Step 2: Update development status**

Add B10 completion status and make B11 the next recommended slice: bounded acquisition runner that writes N live or fixture Barter trades into the shared queryable store.

- [ ] **Step 3: Commit**

```bash
git add crates/fdc-api docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-27-fdc-api-market-data-query-route.md
git commit -m "feat: add api market data query route"
```
