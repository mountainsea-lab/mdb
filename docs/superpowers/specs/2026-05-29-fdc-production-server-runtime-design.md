# Production Server Runtime Design

## Status

Approved direction: move the production startup and runtime lifecycle into `fdc-server`, keep `fdc-api` as route/API compatibility where useful, and organize new server code by business module instead of placing router, server, and model code in one flat directory.

## Problem

The realtime MVP is proven: real Binance Spot trade data can be acquired, written to in-memory storage, and queried through HTTP. The current runnable entrypoint is still a demo API binary in `fdc-api`:

```bash
cargo run -p fdc-api --bin fdc_demo_api
```

That is useful for verification but not a production server shape. The next production slice needs a real server runtime that owns startup, configuration, business-module composition, background task lifecycle, health/readiness, and graceful shutdown.

## Goals

1. Add a production-oriented `fdc-server` binary entrypoint.
2. Organize server code by business module and responsibility.
3. Keep live market-data runtime state inside a server-owned supervisor/service boundary.
4. Expose operational HTTP routes for health, readiness, live-runner control/status, and trade query.
5. Preserve the existing `fdc-api` demo entrypoint and tests during migration.
6. Avoid durable persistence and SQL integration in this slice.

## Non-Goals

This slice does not add:

- Durable database persistence.
- SQL query engine integration for market-data records.
- Authentication/authorization.
- TLS configuration.
- Multi-process deployment tooling.
- Kubernetes manifests or systemd units.
- Full observability stack integration.
- Multi-exchange orchestration beyond the current Binance Spot first slice.

## Recommended Architecture

New production code should live primarily under `crates/fdc-server/src`, grouped by business module:

```text
crates/fdc-server/src/
  bin/
    fdc_server.rs                  # production startup entrypoint
  runtime/
    mod.rs
    config.rs                      # runtime config loaded from env/defaults
    app.rs                         # top-level server runtime assembly
    shutdown.rs                    # ctrl-c/graceful shutdown helper
  health/
    mod.rs
    model.rs                       # HealthResponse, ReadinessResponse
    router.rs                      # /health, /ready
    service.rs                     # readiness computation
  market_data/
    mod.rs
    model.rs                       # StartLiveRequest, runner status, query response models
    router.rs                      # /market-data/* and /runner/* market-data routes
    service.rs                     # business operations over store + supervisor
    supervisor.rs                  # live runner lifecycle/state
```

### Layering Rules

- `router.rs` owns HTTP extraction and response conversion only.
- `service.rs` owns use-case orchestration and validates request-level decisions.
- `supervisor.rs` owns long-running/background runner state transitions.
- `model.rs` owns public request/response DTOs for that module.
- `runtime/*` owns application startup, configuration, shutdown, and module composition.
- Lower-level mapping remains in existing crates:
  - `fdc-barter`: exchange/live acquisition adapter.
  - `fdc-orchestrator`: envelope-to-storage pipeline.
  - `fdc-storage`: queryable storage boundary.

## Production Binary

Add a formal binary:

```bash
cargo run -p fdc-server --bin fdc_server
```

It should:

1. Load `ServerRuntimeConfig` from env/defaults.
2. Build shared app state.
3. Build the production router from business modules.
4. Bind an HTTP listener.
5. Serve until shutdown signal.
6. Stop live runner tasks on shutdown if active.

Initial env vars:

```text
FDC_SERVER_ADDR=127.0.0.1:18080
FDC_LIVE_ENABLED=0|1
FDC_LIVE_DEFAULT_TIMEOUT_SECS=30
FDC_LIVE_DEFAULT_MAX_ENVELOPES=100
```

`FDC_LIVE_ENABLED=1` gates real public internet acquisition. Without it, live-start routes should return a clear error instead of silently doing nothing.

## Market Data Module

### Routes

Initial production market-data routes:

```text
POST /market-data/live/start
POST /market-data/live/stop
GET  /market-data/live/status
GET  /market-data/trades?symbol=BTCUSDT&limit=100
```

Compatibility aliases may be kept temporarily if useful:

```text
POST /runner/start-live
GET  /runner/status
```

But new production docs should prefer `/market-data/live/*`.

### Supervisor State

The live runner supervisor should expose typed states:

```text
idle
starting
running
completed
stopping
stopped
failed
```

For the first production slice, a bounded run is acceptable. A later slice can add indefinite streaming with explicit stop/cancel. The supervisor must still own state and prevent ambiguous concurrent starts.

### Start Behavior

`POST /market-data/live/start` should:

1. Reject if `FDC_LIVE_ENABLED != 1`.
2. Reject if a run is already starting/running.
3. Start live Binance Spot public trade acquisition.
4. Log start parameters.
5. Write collected live envelopes to the shared queryable store.
6. Update status with counts and any failure message.

For the first slice, the route may wait for a bounded collection window and return summary. Later production slices can switch it to async background start immediately returning `202 Accepted` style semantics.

### Query Behavior

`GET /market-data/trades` should query the shared in-memory store. Empty results are valid only before any successful ingestion. After a successful live start summary with `storage_records_written > 0`, query must return records.

## Health Module

### Routes

```text
GET /health
GET /ready
```

`/health` should report process liveness. `/ready` should report:

- server initialized.
- HTTP router is serving.
- market-data store is available.
- live runner supervisor status.
- whether live acquisition is enabled by config.

## Runtime Module

### Config

`ServerRuntimeConfig` should include:

- bind address.
- environment label.
- live acquisition enabled flag.
- default live timeout.
- default live max envelopes.

Use env parsing with deterministic defaults. Invalid env values should fail startup with a clear error.

### Shutdown

Initial graceful shutdown should respond to `ctrl-c` and stop the HTTP server. If a live runner is active, it should request stop/cancel where supported. For bounded first-slice runs, shutdown can record that stop was requested and rely on task termination as the process exits.

## Compatibility Strategy

Keep existing `fdc-api` demo binary and routes during this migration. The new `fdc-server` runtime should not break existing tests:

- `fdc-api --bin fdc_demo_api` remains available.
- `fdc-api` demo route tests remain passing.
- New production routes are tested in `fdc-server` tests.

After production runtime is stable, a later cleanup slice can decide whether to move or deprecate demo-only routes.

## Testing Strategy

Use TDD with three test levels:

1. Unit/contract tests for config parsing:
   - default bind address.
   - live disabled by default.
   - valid env overrides.
   - invalid env errors.

2. Router/service tests with fake/offline envelopes:
   - `/health` and `/ready` return expected JSON.
   - live start rejects when disabled.
   - fake live service writes multiple records and query returns them.
   - concurrent start is rejected or serialized according to supervisor rules.

3. Ignored/gated live smoke:
   - `FDC_LIVE_ENABLED=1` and public internet required.
   - `POST /market-data/live/start` writes real Binance Spot trades.
   - `GET /market-data/trades` returns at least one real record.

Default tests must not depend on public internet.

## Acceptance Criteria

- `cargo run -p fdc-server --bin fdc_server` starts a production-oriented HTTP server.
- The server exposes `/health`, `/ready`, `/market-data/live/start`, `/market-data/live/status`, and `/market-data/trades`.
- Live acquisition is disabled by default and returns an explicit error when start is requested without enabling config.
- With live enabled, the server can acquire real Binance Spot trades, write them to the shared store, and query them back.
- Code is organized by module with separate `model`, `router`, `service`, and `supervisor` responsibilities.
- Existing `fdc-api` demo entrypoint and tests continue to pass.

## First Implementation Slice

Build the smallest production-ready vertical slice:

1. `runtime::config` and `ServerRuntimeConfig`.
2. `health` module with `/health` and `/ready`.
3. `market_data` module with models, service, supervisor, router.
4. `bin/fdc_server.rs` startup.
5. Tests for disabled live start and offline ingestion/query behavior.
6. Optional ignored live HTTP smoke for real Binance Spot validation.

## Future Slices

1. Convert bounded live start into true background indefinite streaming with explicit stop.
2. Add persistent storage backend and recovery.
3. Add auth, rate limit, and production CORS policy.
4. Add metrics/tracing export.
5. Add deployment packaging.
6. Decide deprecation plan for demo-only `fdc-api` routes.
