# P40 Production Runtime Assembly and Version Readiness Design

## Summary

P40 fixes the production startup mismatch found during the P39 internal MVP smoke run. The `fdc_server` binary parses tiered storage config from the environment, but currently assembles `ProductionServerState` with the memory-store constructor. This makes `/market-data/storage/status` report tiered config while `/market-data/storage/health` and maintenance operate on a memory store. P40 also fixes the P39 runbook readiness check where `/version` returns 404.

## Goals

- Make the production binary assemble market-data storage from `ServerRuntimeConfig`.
- Ensure tiered runtime config produces a tiered `QueryableMarketDataStore` for the actual running server.
- Ensure storage status, storage health, and manual maintenance agree for the safe local production config.
- Provide a minimal `/version` readiness route expected by the production runbook.
- Keep the public market-data query surface unchanged.
- Preserve default offline deterministic verification.

## Non-goals

- No candle/OHLCV query expansion.
- No live acquisition behavior expansion.
- No scheduler behavior expansion beyond verifying the existing safe disabled profile remains safe.
- No storage engine redesign.
- No external network smoke as a default test.

## Root Cause

The production binary currently does this:

```rust
let config = ServerRuntimeConfig::from_env()?;
let state = ProductionServerState::new(config);
```

`ProductionServerState::new` always creates `QueryableMarketDataStore::new()`, which is a memory-backed store. `storage_status` reads `state.config().market_data_storage`, so it reports the configured tiered backend. `storage_health` and `run_storage_maintenance_once` use `state.market_data_store()`, so they observe the actual memory store and return `tiered=false` or `unsupported_backend`.

The existing async constructor already handles the production behavior correctly:

```rust
ProductionServerState::try_new(config).await?
```

It calls `build_market_data_store_from_runtime_config`, which constructs the tiered store and starts the scheduler task according to config.

## Proposed Architecture

### 1. Production binary runtime assembly

Change `crates/fdc-server/src/bin/fdc_server.rs` to use `ProductionServerState::try_new(config).await?` instead of `ProductionServerState::new(config)`.

This keeps test constructors available:

- `ProductionServerState::new(config)` remains for tests that intentionally want a memory store with arbitrary config metadata.
- `ProductionServerState::with_market_data_store(...)` remains for tests needing explicit fixture stores.
- `ProductionServerState::try_new(config).await` becomes the binary production path.

### 2. Contract coverage for production runtime assembly

Add tests that reproduce the P39 smoke gap without requiring a long-running external process.

The main regression should build state using the same constructor the binary uses and assert:

- `storage_status` reports `backend=tiered`, `tiered=true`, durable tiers configured.
- `storage_health` reports `backend=tiered`, `tiered=true`, four initialized tiers.
- Manual maintenance `run-once` succeeds instead of returning `unsupported_backend` when maintenance is enabled and confirmation is correct.

The test should use temporary durable paths, not `./var`, to avoid polluting the repo.

### 3. Minimal `/version` readiness route

Add a minimal route for `GET /version` that returns JSON version metadata. Keep it simple and stable:

```json
{
  "status": "success",
  "data": {
    "service": "fdc-server",
    "version": "env!(\"CARGO_PKG_VERSION\") value"
  },
  "message": null
}
```

This should be added to the existing health/router boundary or a small adjacent handler. It should not add operator controls or market-data behavior.

### 4. Runbook alignment

Keep the P39 runbook readiness check for `/version`; after P40 it becomes executable. If command examples need proxy-safe local curl wording, update the runbook to recommend `curl --noproxy '*'` for localhost checks because the smoke environment had proxy variables that returned false 503 responses.

## Error Handling

- If tiered store construction fails at startup, the binary should fail fast by propagating the existing error from `try_new`.
- The server must not silently fall back to memory when config requests tiered storage.
- `/version` should be read-only and always return HTTP 200 once the router is serving.
- Maintenance errors should continue using existing JSON envelopes; P40 only changes the actual production store so the existing tiered path succeeds.

## Testing Strategy

- Add a RED regression that demonstrates the production assembly path must use a runtime-built tiered store.
- Add a router contract for `/version` returning HTTP 200 and package version metadata.
- Run focused tests:
  - `runtime_config_contract production_local_example_env`
  - `production_server_router_contract` tests for storage health/status/maintenance and version
- Run P39 smoke regressions after implementation:
  - `p38_`
  - `production_live_resume`
  - `storage_maintenance_run_once`
  - `storage_maintenance_scheduler_resume`
  - `fdc-storage dependency_guard`
  - `cargo fmt --check`
- Run a local no-proxy internal MVP smoke after merge-ready implementation, using safe config and no live network.

## Success Criteria

- Starting the production binary with `config/production.local.example.env` produces a server whose storage status and health both report tiered.
- Manual maintenance run-once succeeds with the safe local production config.
- `/version` returns HTTP 200 with `service=fdc-server` and package version metadata.
- Existing P38/P39 contract suites still pass.
- Worktree remains clean after commits and verification.

## Scope Control

P40 is limited to production runtime assembly, version readiness, test coverage, and runbook alignment. It does not change trade query semantics, storage policies, live acquisition defaults, scheduler defaults, or maintenance confirmation strings.
