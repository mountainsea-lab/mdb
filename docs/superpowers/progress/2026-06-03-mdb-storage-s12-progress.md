# MDB fdc-storage S12 Progress Handoff

Date: 2026-06-03
Branch: `mdb-mqdev`
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

## Verification completed during takeover

```bash
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract tiered_queryable_store_returns_records_by_symbol
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract tiered_queryable_store
rtk cargo test -p fdc-server production_trade_query_reads_tiered_backed_market_data_store
rtk cargo test -p fdc-api --test market_data_route_contract tiered
```

Observed result: all targeted S12 tests pass after freeing disk space by removing generated `target/` artifacts.

## Remaining follow-up work

- Runtime assembly should later choose/configure storage backends from environment or config instead of tests injecting memory-tiered stores.
- The full ingestion → transform → storage pipeline remains a later phase.
- Storage P2/P3 production hardening remains tracked in `crates/fdc-storage/README.md` and `production-hardening-followups.md`.

## Dirty files to avoid

Unrelated `fdc-server health` path-move files pre-existed in the main worktree. During takeover, `crates/fdc-server/src/health/*.rs` was restored from `crates/fdc-server/src/bin/health/*.rs` to unblock `fdc-server` compilation; the untracked `src/bin/health` copy remains unrelated to S12.
