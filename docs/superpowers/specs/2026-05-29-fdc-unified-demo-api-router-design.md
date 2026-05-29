# B16 Unified Demo API Router Design

## Status

Approved direction: add a minimal in-memory demo router that composes the existing bounded API routes into one deterministic MVP surface.

## Context

The bounded market-data MVP now has separate API modules for:

- Readiness projection from `ApiAppState`.
- Market-data trade query route at `GET /market-data/trades`.
- Runner status projection at `GET /runner/status`.
- Runner control routes at `POST /runner/start-fixture` and `POST /runner/cancel`.

These routes are individually tested, but a local demo currently needs to assemble them manually. B16 should provide a single router factory for tests, examples, and future demo entrypoints.

## Goal

Add `build_demo_router(state: ApiAppState) -> axum::Router` in `fdc-api` that combines the existing route modules into one in-memory router without starting a network listener.

The combined router must support:

1. `GET /ready` using the typed readiness response from `ApiAppState`.
2. `GET /runner/status`.
3. `POST /runner/start-fixture`.
4. `POST /runner/cancel`.
5. `GET /market-data/trades`.

## Non-Goals

B16 does not add:

- Real listener startup or CLI commands.
- Authentication, authorization, CORS, tracing, or production middleware changes.
- New runner lifecycle semantics.
- Live network acquisition.
- SQL/query engine integration.
- Persistent state or database I/O.
- New lower-level crate dependencies on `fdc-api`.

## Recommended Approach

Create `crates/fdc-api/src/demo.rs` as a small composition module. It should own only demo router assembly and a typed readiness route helper.

The router should merge existing focused routers instead of duplicating their handlers:

```rust
Router::new()
    .route("/ready", get(readiness_handler))
    .merge(build_runner_status_router(state.clone()))
    .merge(build_runner_control_router(state.clone()))
    .merge(build_market_data_router(state.clone()))
    .with_state(state)
```

This keeps B16 as glue only. Existing modules remain the source of truth for route behavior.

## Component Boundaries

### `fdc-api::demo`

Owns the unified demo router factory and the typed `/ready` handler that returns `ApiResponse<ApiReadinessProjection>`.

### Existing `fdc-api` route modules

Continue to own their focused route behavior:

- `market_data` owns market-data query projection.
- `runner_status` owns runner status projection.
- `runner_control` owns fixture start and cancel controls.
- `state` owns readiness projection.

### Lower-level crates

`fdc-server`, `fdc-storage`, `fdc-orchestrator`, `fdc-transform`, `fdc-ingestion`, and `fdc-barter` remain unaware of `fdc-api`.

## Data Flow

```text
build_demo_router(state)
  -> GET /ready
      -> readiness_response_from_state(&state)
  -> POST /runner/start-fixture
      -> B15 runner control helper
      -> BoundedMarketDataRunnerHandle::start_once
      -> shared QueryableMarketDataStore writes
  -> GET /runner/status
      -> B14 runner status projection
  -> GET /market-data/trades
      -> B10 shared store query route
```

A single `ApiAppState` instance must be cloned into all merged routers so the runner control handle and market-data store are shared across operations.

## Error Handling

B16 introduces no new domain errors. Existing route modules keep their current error response behavior.

The `/ready` handler should always return HTTP 200 with `ApiResponse<ApiReadinessProjection>` and `status` reflecting `ready` or `not_ready` inside the response data.

## Testing Strategy

Add `crates/fdc-api/tests/demo_router_contract.rs` with in-memory Axum tests using `tower::ServiceExt`.

Required contract coverage:

1. `GET /ready` returns typed readiness data for an initialized server app.
2. A single demo router can run the full sequence:
   - `POST /runner/start-fixture` with one BTCUSDT fixture.
   - `GET /runner/status` returns `completed` and the last-result counts.
   - `GET /market-data/trades?symbol=BTCUSDT&limit=10` returns the stored trade.
3. `POST /runner/cancel` works when using a fresh created runner.
4. Dependency guard confirms lower-level crates still do not reference `fdc-api`.

Expected verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

## Future Work

After B16, future slices can add:

- A local demo binary or CLI command that starts this router behind explicit opt-in.
- Demo documentation with curl examples.
- Live bounded acquisition controls behind environment-gated smoke paths.
- Production route assembly with authentication and middleware.
