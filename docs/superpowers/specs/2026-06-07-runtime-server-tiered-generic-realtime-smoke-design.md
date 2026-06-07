# P18 Runtime Server Path Smoke with Tiered Generic Realtime Design

Date: 2026-06-07
Module: `crates/fdc-server` production runtime contracts
Status: approved design for implementation

## Goal

Verify the production-facing server runtime path can assemble `tiered + generic_realtime` market-data storage, ingest realistic fixture trades through existing server helpers, serve them through the HTTP trade query route, and expose the expected tier placement through generic storage queries.

## Boundary rule

`fdc-storage` remains generic and unchanged in this slice. The server test may inspect generic storage tiers through the public `QueryableStorage` API, but it must not add market-data-specific behavior to storage.

No new production API is required. P18 is a smoke/contract slice that proves the runtime pieces added in P14-P17 work together.

## Current state

Completed prerequisites:

- P14: server runtime can choose `memory` or `tiered` backend.
- P15: `generic_realtime` policy profile exists.
- P16: server runtime profile injection reaches the tiered write path.
- P17: orchestrator maps live/backfill/replay/kind facts into generic storage tags.

Existing server pieces:

- `ProductionServerState::try_new(config).await` builds storage from runtime config.
- `ProductionServerState::ingest_test_trade(symbol, trade_id).await` writes a realistic Barter fixture through the realtime/orchestrator/storage path.
- `build_production_router(state)` exposes `GET /market-data/trades`.
- `ProductionServerState::market_data_store()` returns the shared `QueryableMarketDataStore`.
- `QueryableMarketDataStore` implements `QueryableStorage`, so tests can query by `StorageTierScope`.

## Design

Add one focused contract test to `crates/fdc-server/tests/production_server_router_contract.rs`.

The test should:

1. Build config from env pairs:
   - `FDC_MARKET_DATA_STORAGE_BACKEND=tiered`
   - `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime`
2. Create state with `ProductionServerState::try_new(config).await`.
3. Call `state.ingest_test_trade("BTCUSDT", "generic-runtime-live-1").await`.
4. Query the shared store generically with:
   - `StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Only(StorageTier::L2))`
5. Assert one live record exists in L2 and contains `mode=live`.
6. Build the production router from the same state.
7. Call `GET /market-data/trades?symbol=BTCUSDT&limit=10`.
8. Assert the HTTP response returns the same live fixture.

This proves runtime config, server state assembly, fixture ingestion, orchestrator tag mapping, storage policy routing, and production router querying are connected.

## Why only live trade in P18

Backfill/replay fixture injection is currently not part of the production server helper surface. P17 already verifies live vs backfill tier divergence through the orchestrator contract. P18 should remain a narrow production-facing smoke test and avoid adding a new server-only test helper for backfill.

If future runtime APIs support historical/backfill ingestion, that can become a separate P19/P20 slice.

## Testing strategy

Use TDD.

- Add the failing test first.
- It may already pass after P14-P17, but it is still valuable as a missing contract test. If it passes immediately, record that the implementation was already present and keep the test as regression coverage.
- Run the targeted test.
- Run broader server and storage verification.

## Out of scope

- Adding a public tier-inspection HTTP endpoint.
- Adding server APIs for historical/backfill ingestion.
- Durable physical tier path configuration.
- Changing storage policy rules.
- Changing live network runner behavior.

## Verification commands

```bash
rtk cargo fmt --package fdc-server --package fdc-storage --check
rtk cargo test -p fdc-server --test production_server_router_contract runtime_server_path_routes_live_fixture_with_tiered_generic_realtime
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server --test production_server_router_contract
rtk cargo test -p fdc-storage --test dependency_guard
```

## Success criteria

- Runtime server config `tiered + generic_realtime` assembles successfully.
- Existing server fixture ingest writes through the configured tiered store.
- The live fixture is observable in generic hot storage tier L2.
- The HTTP trade query route returns the fixture from the same shared store.
- Storage dependency guard remains green.

## Self-review

- Placeholder scan: no placeholders remain.
- Scope check: this is one smoke/contract slice, not a runtime API expansion.
- Boundary check: no storage boundary violation is introduced.
- Compatibility check: no existing API behavior is changed.
