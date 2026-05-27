# B9 Queryable Market Data Storage Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-memory `StorageWriteSink` that stores market-data records and lets the MVP query them by symbol/kind/collection/limit.

**Architecture:** Implement a focused `queryable` module in `fdc-storage`. The store remains storage-owned and generic: it filters `StorageWriteRecord` values by namespace, collection, and metadata tags, while upstream DTO decoding stays outside storage. `fdc-orchestrator` gets one integration contract proving existing Barter fixture writes can be queried back.

**Tech Stack:** Rust 1.95, `fdc-storage`, `fdc-orchestrator`, `parking_lot::RwLock`, `async_trait`, `StorageWriteSink`, `StorageWriteRecord`.

---

## File Structure

- Create: `crates/fdc-storage/src/queryable.rs`  
  Owns `MarketDataQuery` and `QueryableMarketDataStore`.

- Modify: `crates/fdc-storage/src/lib.rs`  
  Exports the `queryable` module and public queryable store types.

- Create: `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`  
  Storage-level contract tests for query filtering and atomic validation.

- Create: `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`  
  Cross-layer fixture test from Barter envelope to queryable storage.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Records B9 completion and next B10 route integration slice.

---

## Task 1: Add failing storage-level queryable store contract tests

**Files:**
- Create: `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`

- [ ] **Step 1: Write failing storage contract tests**

Create `crates/fdc-storage/tests/queryable_market_data_store_contract.rs` with complete tests for:

- `queryable_store_returns_records_by_symbol`
- `queryable_store_filters_symbols_independently`
- `queryable_store_applies_trade_collection_and_limit`
- `queryable_store_rejects_invalid_batch_atomically`
- `dependency_guard_queryable_storage_stays_decoupled_from_upstream_crates`

- [ ] **Step 2: Run RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage --test queryable_market_data_store_contract
```

Expected: FAIL because `QueryableMarketDataStore` and `MarketDataQuery` are not exported.

---

## Task 2: Implement queryable store in `fdc-storage`

**Files:**
- Create: `crates/fdc-storage/src/queryable.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Implement `MarketDataQuery` and `QueryableMarketDataStore`**

Create `queryable.rs` with:

- `MarketDataQuery` builder methods.
- `QueryableMarketDataStore` with `records: RwLock<Vec<StorageWriteRecord>>`.
- `StorageWriteSink` impl that validates batch then appends records.
- `query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord>`.
- `record_count()` and `all_records()` helpers.

- [ ] **Step 2: Export module and types**

Modify `crates/fdc-storage/src/lib.rs` to include:

```rust
pub mod queryable;
pub use queryable::{MarketDataQuery, QueryableMarketDataStore};
```

- [ ] **Step 3: Run GREEN storage contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage --test queryable_market_data_store_contract
```

Expected: PASS.

---

## Task 3: Add orchestrator fixture query contract

**Files:**
- Create: `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`

- [ ] **Step 1: Write failing/passing integration contract**

Create an orchestrator test that builds a sample `BarterIngestionEnvelope`, writes through `run_barter_envelopes_to_storage_once`, queries `QueryableMarketDataStore` with `MarketDataQuery::for_trades().with_symbol("BTCUSDT")`, and checks one JSON payload is returned.

- [ ] **Step 2: Run orchestrator contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract
```

Expected: PASS after Task 2.

---

## Task 4: Verify and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run final B9 verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-storage --package fdc-orchestrator --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage --test queryable_market_data_store_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage -p fdc-orchestrator
```

Expected: all pass.

- [ ] **Step 2: Update development status**

Add B9 completion notes and set B10 as next recommended slice: bounded API router/query integration.

- [ ] **Step 3: Commit**

Run:

```bash
git add crates/fdc-storage crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-27-fdc-queryable-market-data-storage-boundary.md
git commit -m "feat: add queryable market data storage boundary"
```
