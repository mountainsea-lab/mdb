# fdc-storage S12 Market Data Integration Design

Date: 2026-06-03
Status: proposed
Scope: `fdc-storage`, `fdc-server`, `fdc-api`

## Goal

S12 connects the S11 generic/tiered storage boundary to the existing market-data write/query path without making `fdc-storage` depend on business modules.

The first integration target is the existing `QueryableMarketDataStore` facade used by `fdc-server` and `fdc-api`. After S12, market-data writes and `/market-data/trades` queries should exercise the same generic storage APIs that S11 hardened.

## Current state

`fdc-storage` is S11 P1-closed and pre-integration-ready. It provides:

- generic `StorageWriteSink` and `QueryableStorage` boundaries;
- `StorageWriteRecord`, `StorageWriteBatch`, `StorageQuery`, and metadata/tag filters;
- `TieredStorageStore` backed by initialized tiers;
- memory, redb, DuckDB, and RocksDB engines;
- lifecycle, maintenance, health, metrics, tracing, typed feature errors, and public API docs.

The upper modules already have a market-data facade:

- `fdc-server::ProductionServerState` owns `Arc<QueryableMarketDataStore>`;
- `fdc-server` live runner writes `StorageWriteBatch` through that facade;
- `fdc-server` `/market-data/trades` queries `MarketDataQuery::for_trades()` through that facade;
- `fdc-api::ApiAppState` owns its own `Arc<QueryableMarketDataStore>` and exposes a market-data query route.

The gap: `QueryableMarketDataStore` currently wraps `InMemoryQueryableStorage`, a Vec-backed test/MVP implementation. It does not yet delegate to `TieredStorageStore`.

## Chosen approach

Use a compatibility-preserving backend upgrade.

`QueryableMarketDataStore` remains the business-facing facade, but gains a storage-backed backend that delegates to a `TieredStorageStore`. Existing callers keep using:

```rust
state.market_data_store().write_batch(batch).await
state.market_data_store().query(&MarketDataQuery::for_trades())
state.market_data_store().record_count()
```

This keeps S12 focused on integration. It avoids a broader server/API runtime rewrite and avoids leaking storage internals into market-data handlers.

## Non-goals

S12 does not implement storage P2/P3 production-hardening items:

- no maintenance scheduler loop;
- no runtime metrics exporter wiring;
- no durable audit persistence;
- no disk health/degraded-state expansion;
- no query indexes/cursor pagination;
- no backup/restore orchestration;
- no physical shard routing/rebalancing.

S12 also does not rewrite the full ingestion/transform pipeline. It validates the storage integration point that the pipeline will use later.

## Component design

### `fdc-storage::QueryableMarketDataStore`

Add an internal backend enum:

```rust
enum QueryableMarketDataBackend {
    InMemory(InMemoryQueryableStorage),
    Tiered(Arc<TieredStorageStore>),
}
```

Expose constructors:

```rust
impl QueryableMarketDataStore {
    pub fn new() -> Self;
    pub fn in_memory() -> Self;
    pub fn from_tiered_store(store: Arc<TieredStorageStore>) -> Self;
    pub async fn memory_tiered() -> Result<Self>;
}
```

Expected behavior:

- `new()` remains backward compatible and returns the in-memory backend.
- `in_memory()` is an explicit alias for tests and callers that want MVP behavior.
- `from_tiered_store()` wraps an initialized `TieredStorageStore`.
- `memory_tiered()` builds a minimal tiered store using memory engines for L1/L2, initializes tiers, and returns a facade backed by S11 storage.

The facade keeps these methods:

```rust
pub fn query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord>;
pub fn record_count(&self) -> usize;
pub fn all_records(&self) -> Vec<StorageWriteRecord>;
```

For the tiered backend:

- `write_batch()` delegates to `TieredStorageStore::write_batch()`.
- `query()` delegates to `TieredStorageStore::query_storage()` using `MarketDataQuery::to_storage_query()`.
- `record_count()` and `all_records()` use a generic query over namespace `market_data`, with no collection filter, so existing status projections keep working.

### `fdc-server::ProductionServerState`

Keep the default market-data store construction backward compatible, and add an explicit path for storage-backed construction.

Because `ProductionServerState::new(config)` is synchronous today, S12 should avoid forcing a broad async runtime-state rewrite. Add a constructor that accepts a prebuilt store:

```rust
impl ProductionServerState {
    pub fn with_market_data_store(
        config: ServerRuntimeConfig,
        market_data_store: Arc<QueryableMarketDataStore>,
    ) -> Self;
}
```

Then choose one of these internally safe defaults:

- keep `new(config)` synchronous and in-memory for compatibility;
- use `with_market_data_store()` in tests and API/server assembly paths that can build the tiered store asynchronously.

This minimizes risk while making the new integration path explicit.

### `fdc-api::ApiAppState`

`ApiAppState` already has:

```rust
pub fn with_market_data_store(
    mut self,
    market_data_store: Arc<QueryableMarketDataStore>,
) -> Self;
```

S12 should use this existing hook in API integration tests to prove `/market-data/trades` reads from a storage-backed facade.

No API response DTO changes are required.

## Data flow

```mermaid
flowchart LR
    LiveRunner[fdc-server live/test runner]
    Batch[StorageWriteBatch]
    Facade[QueryableMarketDataStore]
    Tiered[TieredStorageStore]
    Engine[Storage engines]
    Handler[/market-data/trades]
    Query[MarketDataQuery]

    LiveRunner --> Batch --> Facade --> Tiered --> Engine
    Handler --> Query --> Facade --> Tiered --> Engine
```

Market-data records continue to use storage-owned generic metadata:

- namespace: `market_data`
- collection: `trades`
- tags: `symbol`, `kind`, and source/adapter/exchange tags where available
- value: encoded payload bytes

## Error handling

Existing `StorageWriteSink::write_batch()` returns `fdc_core::Result<StorageWriteOutcome>` and should preserve write errors.

`QueryableMarketDataStore::query()` currently returns `Vec<StorageWriteRecord>` and panics on invalid internal query. S12 keeps that public behavior for compatibility because `MarketDataQuery::to_storage_query()` creates valid queries. Tests must cover valid symbol/limit queries on both in-memory and tiered backends.

If a future caller needs fallible query behavior, add a separate `try_query()` method later. Do not change `query()` in S12.

## Testing strategy

### Storage crate tests

Add backend parity tests in `crates/fdc-storage/tests/queryable_market_data_store_contract.rs` or `src/queryable.rs` tests:

1. In-memory backend returns trades by symbol.
2. Tiered memory backend returns trades by symbol.
3. Tiered backend applies collection and limit.
4. Tiered backend `record_count()` and `all_records()` reflect generic storage contents.
5. Invalid batch remains atomically rejected for both backends.

### Server tests

Add or update `fdc-server` tests to construct `ProductionServerState` with `QueryableMarketDataStore::memory_tiered().await` and verify:

1. `ingest_test_trade()` writes one record.
2. `query_trades()` returns the written record.
3. `StartLiveMarketDataResponse.market_data_store_records` reflects tiered-backed count.

### API tests

Add or update `fdc-api` market-data tests to inject a tiered-backed `QueryableMarketDataStore` with `ApiAppState::with_market_data_store()` and verify `query_market_data_trades()` reads from storage-backed data.

### Dependency guard

Keep existing dependency guard coverage and add no business crate dependency to `fdc-storage`.

Run:

```bash
rtk cargo test -p fdc-storage
rtk cargo test -p fdc-server
rtk cargo test -p fdc-api
```

## Documentation updates

Update:

- `crates/fdc-storage/README.md` to mark S12 integration baseline as complete after implementation.
- `crates/fdc-storage/docs/storage-boundary-acceptance-report.md` to state that market-data facade integration has been validated.
- Add progress handoff under `docs/superpowers/progress/` after implementation.

## Acceptance criteria

S12 is complete when:

1. `QueryableMarketDataStore` can be backed by `TieredStorageStore`.
2. Existing in-memory behavior remains backward compatible.
3. Server market-data test write/query flow passes using a tiered-backed facade.
4. API market-data query test passes using a tiered-backed facade.
5. `fdc-storage` still has no dependency on business/server/API modules.
6. The three package test commands pass.

## Risks and mitigations

- Risk: `record_count()` becomes expensive on tiered storage because it queries all market-data records.
  - Mitigation: acceptable for S12 baseline; indexes/count caches belong to P3.

- Risk: synchronous `query()` blocks on async tiered storage.
  - Mitigation: match current facade behavior for compatibility; add async/fallible API in a later runtime cleanup if needed.

- Risk: `ProductionServerState::new()` cannot initialize tiered storage asynchronously.
  - Mitigation: add explicit `with_market_data_store()` constructor and use it where integration tests/runtime assembly can prebuild the store.

## Out of scope for later phases

After S12, good next candidates are:

1. server/API runtime unification so one shared storage-backed state is used consistently;
2. ingestion → transform → storage end-to-end pipeline;
3. storage P2 runtime scheduler/metrics/audit/disk-health production hardening.
