# P20 Durable Runtime Persistence Reopen Design

Date: 2026-06-07

## Goal

Verify and, if necessary, fix the runtime durable tier path so a server-configured tiered market-data store can write records, be dropped, be rebuilt with the same L2/L3/L4 paths, and read the records back.

## Non-goals

- No new storage engine types.
- No market-data DTO knowledge inside `fdc-storage`.
- No runtime per-write physical tier selection.
- No broad persistence/migration framework.
- No repo-wide formatting cleanup.

## Current state

P19 added server runtime durable path configuration:

- `FDC_MARKET_DATA_STORAGE_L2_REDB_PATH`
- `FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH`
- `FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH`

The server translates these into generic `TierConfig.engine_config["db_path"]` values and passes them to `fdc-storage`. P19 verifies that configured files/directories are created and that `generic_realtime` routes live writes to L2 and backfill writes to L3.

What is not yet verified is reopen persistence: the data must survive dropping the runtime store and rebuilding it with the same paths.

## Options considered

### Option A: Storage-engine-only reopen tests

Test `RedbEngine`, `DuckDBEngine`, and `RocksDBEngine` directly by writing values, dropping engines, reopening the same paths, and reading values.

Pros:

- Small, focused tests.
- Pinpoints engine-specific reopen bugs quickly.

Cons:

- Does not prove server runtime config assembly works end-to-end.
- Does not prove market-data route queries can read persisted tiered records.

### Option B: Server runtime reopen smoke only

Use `ProductionServerState::try_new(config)` with durable tier paths, write a fixture through existing server/orchestrator-facing helpers, drop the state, rebuild with the same config, and query through storage and router.

Pros:

- Directly verifies the roadmap concern: configured production runtime survives rebuild.
- Exercises server config, storage builder, tier policy, tier engines, storage codec, and route query path.

Cons:

- If it fails, the first failure may be less localized.

### Option C: Both server smoke plus targeted engine fixes/tests if exposed

Start with the server runtime reopen smoke as the acceptance test. If it fails due to an engine reopen bug, add a focused storage engine regression test for that engine and fix it.

Pros:

- Keeps the top-level roadmap acceptance test.
- Adds lower-level regression coverage only where needed.
- Avoids speculative tests for engines that already behave correctly.

Cons:

- Slightly more work if a bug is exposed.

## Decision

Use Option C.

P20 will start with a server-level durable reopen smoke. If the smoke fails because an engine cannot reopen an existing path without data loss, add a targeted engine regression test and fix only that behavior.

## Acceptance scenario

1. Create unique temp paths for L2 redb, L3 DuckDB, and L4 RocksDB.
2. Build `ServerRuntimeConfig` using:
   - `FDC_MARKET_DATA_STORAGE_BACKEND=tiered`
   - `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime`
   - the three durable tier path env vars
3. Create `ProductionServerState::try_new(config.clone()).await`.
4. Write through `state.ingest_test_trade("BTCUSDT", "durable-reopen-live-1")`.
5. Verify the live fixture is in L2 through `QueryableStorage` before dropping the state.
6. Drop the state.
7. Rebuild `ProductionServerState::try_new(config).await` with the same paths.
8. Query the rebuilt store through `QueryableStorage` scoped to L2 and verify the persisted trade record is present.
9. Build `build_production_router(reopened_state)` and verify `GET /market-data/trades?symbol=BTCUSDT&limit=10` returns the same trade id.

This test is server-owned because it uses market-data fixture helpers and production routing. `fdc-storage` remains generic.

## Boundary constraints

- `fdc-storage` may receive generic `TierConfig`s and may fix generic engine open/create behavior.
- `fdc-storage` must not import or mention market-data DTOs, server runtime config, routes, or orchestrator types.
- Server/orchestrator may continue to map market-data facts into generic tags.

## Error handling

- Reopen test failures should surface normal `fdc_core::Error` values.
- Engine fixes should prefer open-or-create semantics for existing paths rather than deleting, truncating, or recreating stored data.
- The test must clean up temp directories after success. On failure, OS temp cleanup can handle leftovers.

## Testing plan

TDD steps:

1. Add the server-level durable reopen smoke and run it red.
2. If red is caused by existing code not reopening correctly, add the smallest targeted lower-level regression test.
3. Implement the minimal engine or assembly fix.
4. Verify:
   - durable reopen smoke passes
   - P19 durable path assembly smoke still passes
   - runtime config contract still passes
   - storage dependency guard still passes
   - package-scoped fmt check passes

## Success criteria

- A configured durable runtime store can be rebuilt with the same tier paths and read previously written records.
- The production market-data route can serve the persisted record after rebuild.
- Existing generic realtime tier routing remains intact.
- `fdc-storage` remains generic and dependency-clean.
