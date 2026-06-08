# P37 Four-Tier End-to-End Acceptance Design

Date: 2026-06-08
Branch: `mdb-mqdev`
Status: approved design

## Goal

P37 proves the internal MVP data path end to end without depending on public internet access by default:

```text
runtime config
→ ProductionServerState
→ deterministic fixture/live-like ingestion
→ tiered market-data storage
→ durable reopen
→ HTTP query route
→ maintenance/audit observation
→ P36 live recovery controls preserve data
```

The goal is not to add a broad new product feature. The goal is to turn the already implemented server, storage, live hardening, maintenance, and query pieces into a deterministic acceptance net that answers one production-readiness question:

> Can the service acquire market data, persist it through the configured tiered/four-tier storage path, and serve it back through query routes while maintenance and recovery controls remain safe?

## Current Context

Existing coverage already proves many single parts:

- Runtime tiered storage construction works through `ProductionServerState::try_new()`.
- Fixture ingestion writes trade records through the same service ingestion path used by production market-data conversion.
- Tiered-backed query routes can return trades from `QueryableMarketDataStore`.
- Durable tier paths can be reopened and queried.
- Manual maintenance run-once and scheduler/audit routes work for tiered storage.
- P36 added bounded live retry, suppressed state, and a gated `/market-data/live/resume` route.

P37 should not duplicate those tests one by one. It should add higher-level acceptance tests that compose them into production-like workflows.

## Scope

P37 includes:

- Deterministic fixture/live-like ingestion. Default tests must not call public Binance or any other public network.
- Tiered runtime config with durable tier paths for the production acceptance path.
- Hot L2 write verification after ingestion.
- Drop/reopen verification that durable storage still serves query results.
- HTTP `/market-data/trades` query verification after ingestion and after reopen.
- Manual maintenance run-once plus audit observation after ingestion.
- P36 live recovery safety checks proving live resume/retry controls do not mutate or hide persisted market-data records.

P37 excludes:

- New OHLCV/candle query APIs. Query API expansion belongs to P38.
- Default real-network live collection smoke tests. Real-network smoke may be documented or kept ignored/manual only.
- Storage engine semantics changes in `fdc-storage`, unless implementation reveals a boundary bug that cannot be validated at the server contract layer.
- Production runbook/config packaging. That belongs to P39.

## Recommended Approach

Use deterministic production acceptance tests in `crates/fdc-server/tests/production_server_router_contract.rs`.

This is better than adding more unit tests because the key risk is not an isolated function. The key risk is integration drift across runtime config, server state, storage, router contracts, maintenance, and live recovery controls.

This is better than default real-network smoke because a production acceptance net should be stable, fast, and runnable during normal development. Real-network smoke can stay ignored/manual for operator validation.

## Acceptance Workflows

### 1. Tiered acquisition to query survives reopen

Test name:

```text
p37_tiered_acquisition_query_acceptance_survives_reopen
```

Workflow:

1. Create unique durable tier paths with `durable_tier_env()`.
2. Build `ServerRuntimeConfig` with:
   - `FDC_MARKET_DATA_STORAGE_BACKEND=tiered`
   - `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime`
   - durable L2/L3/L4 paths
3. Build `ProductionServerState::try_new(config.clone())`.
4. Ingest multiple fixture trades, including at least two symbols.
5. Query the underlying store with L2 scope and assert hot records exist with live/generic realtime metadata.
6. Query `/market-data/trades?symbol=BTCUSDT&limit=10` and assert only the expected symbol records are returned.
7. Drop the first state.
8. Rebuild `ProductionServerState::try_new(config)`.
9. Query the reopened L2 storage and the HTTP route again.
10. Assert persisted records remain readable and payload trade IDs match the inserted fixtures.
11. Remove the unique durable test directory at the end.

Expected value:

- Proves acquisition-like fixture ingestion reaches tiered storage.
- Proves the HTTP query path reads the persisted data after reopen.
- Proves durable config paths are not only accepted but useful for end-to-end readback.

### 2. Maintenance after ingestion records audit without hiding query data

Test name:

```text
p37_maintenance_after_ingestion_records_audit_without_hiding_query_data
```

Workflow:

1. Build tiered generic realtime state with maintenance enabled.
2. Ingest fixture trades.
3. Query `/market-data/trades` and capture returned count.
4. POST `/market-data/storage/maintenance/run-once` with `confirm=run_maintenance_once`.
5. Assert maintenance response is success and reports healthy tiers.
6. GET `/market-data/storage/maintenance/audit?limit=10`.
7. Assert audit total entries and total recorded entries increased.
8. Query `/market-data/trades` again.
9. Assert the same records remain visible after maintenance.

Expected value:

- Proves maintenance can be run after acquisition without breaking query availability.
- Proves operator audit visibility exists for the acceptance workflow.
- Keeps maintenance semantics server-owned and does not require storage source changes.

### 3. Live recovery controls do not mutate persisted market data

Test name:

```text
p37_live_recovery_controls_do_not_mutate_persisted_market_data
```

Workflow:

1. Build tiered generic realtime state with live enabled and live resume gate enabled.
2. Ingest fixture trades and record `record_count` plus a query response snapshot.
3. Exercise safe live recovery control paths that do not require public network:
   - wrong confirmation returns bad request
   - resume while fake live runner is active returns conflict
   - disabled gate path can be covered in a separate default config branch if needed
4. Re-query store and `/market-data/trades`.
5. Assert persisted record count and query results remain unchanged.

Expected value:

- Proves P36 operator controls do not clear, rewrite, hide, or trigger maintenance on market data.
- Connects live hardening to the end-to-end data acceptance story.

## Data and Query Constraints

Use deterministic trade fixture IDs and symbols, for example:

- `BTCUSDT`: `p37-btc-1`, `p37-btc-2`
- `ETHUSDT`: `p37-eth-1`

Assertions should check:

- `status == "success"`
- `returned_records` equals expected count
- returned symbols match the query symbol
- payload trade IDs match inserted fixture IDs
- L2 query returns expected records before and after reopen
- audit counters increase after maintenance

Avoid over-constraining internal storage ordering unless the existing query API already guarantees it. If ordering is not a contract, assert set membership instead of exact order.

## Error Handling and Safety

P37 tests should use unique temp directories and clean them up. If cleanup fails, tests should not mask the real acceptance assertions.

Public network live collection remains out of default verification. Any real-network smoke must stay `#[ignore]` or documented as manual so normal test runs remain deterministic.

P37 must preserve boundaries:

- Query routes remain read-only.
- Live resume/retry controls must not mutate stored market data.
- Maintenance may compact/check lifecycle according to existing storage semantics, but P37 should prove query availability remains intact for fresh fixture data.
- No default test should depend on scheduler timing unless the test specifically uses an existing bounded wait helper.

## Files Expected to Change

Primary implementation file:

- `crates/fdc-server/tests/production_server_router_contract.rs`

Possible documentation updates:

- `docs/DEVELOPMENT_STATUS.md`

No `fdc-storage` source files should change unless P37 reveals a genuine storage contract bug. If such a bug appears, it should be fixed with a focused storage test and documented explicitly.

## Verification Plan

Focused verification should include:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

The public-internet live smoke remains ignored/manual unless an operator explicitly requests it.

## Completion Criteria

P37 is complete when:

- The three `p37_` acceptance tests pass.
- Existing live recovery and storage maintenance regressions pass.
- `fdc-storage` dependency guard passes.
- Formatting check passes.
- `docs/DEVELOPMENT_STATUS.md` records P37 completed work, commits, and verification results.
- The next recommended slice remains P38 Query API Production Hardening.
