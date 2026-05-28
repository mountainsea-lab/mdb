# B14 Runner Status API Projection Design

## Status

Approved direction: add a pure API projection and in-memory route for B13 bounded runner status.

## Context

The project has a bounded market-data demo path and runner lifecycle:

- B11 writes finite Barter envelopes into a shared `QueryableMarketDataStore` through the MVP helper.
- B13 added `BoundedMarketDataRunnerHandle` with `Created`, `Running`, `Completed`, `Cancelled`, and `Failed` states plus last-result/failure accessors.
- `fdc-api` already exposes readiness projections and in-memory routes for market-data queries.

B14 should make the B13 runner lifecycle observable through the API boundary without adding production background task management.

## Goal

Expose a stable API-facing runner status shape that can report:

1. Whether a runner is attached to `ApiAppState`.
2. The runner lifecycle state as a stable snake_case string.
3. Optional last B11 result counts.
4. Optional failure message.
5. A bounded in-memory route, `GET /runner/status`, for tests and demos.

## Non-Goals

B14 does not add:

- Background runner startup or task supervision.
- Live network acquisition.
- Persistence or SQL integration.
- Mutating API endpoints for starting or cancelling runners.
- Changes to lower-level crates or dependencies on `fdc-api`.
- Merging runner state into readiness semantics. Readiness can remain server-app readiness for this slice.

## Recommended Approach

Add `crates/fdc-api/src/runner_status.rs` with:

- `ApiRunnerLifecycleStatus`
  - serialized as snake_case enum values: `created`, `running`, `completed`, `cancelled`, `failed`, and `not_configured`.
- `ApiRunnerLastResultProjection`
  - counts from `BoundedMarketDataMvpResult`.
- `ApiRunnerStatusProjection`
  - fields:
    - `configured: bool`
    - `state: ApiRunnerLifecycleStatus`
    - `last_result: Option<ApiRunnerLastResultProjection>`
    - `failure_message: Option<String>`
- `runner_status_response_from_state(&ApiAppState)` pure helper.
- `build_runner_status_router(state: ApiAppState)` in-memory Axum router for `GET /runner/status`.

Extend `ApiAppState` with an optional `Arc<BoundedMarketDataRunnerHandle>`:

- default state has no runner attached;
- `with_market_data_runner(runner)` attaches a runner;
- `market_data_runner()` returns `Option<Arc<BoundedMarketDataRunnerHandle>>`.

Use `Arc<BoundedMarketDataRunnerHandle>` rather than mutable API ownership. B14 is read-only status projection; mutation/start/cancel routes belong in a future explicitly designed slice.

## Component Boundaries

### `fdc-api`

Owns API serialization, pure projection helpers, and the in-memory status route.

### `fdc-server`

Continues to own `BoundedMarketDataRunnerHandle` lifecycle and result/failure state.

### Lower-level crates

`fdc-storage`, `fdc-orchestrator`, `fdc-transform`, `fdc-ingestion`, and `fdc-barter` remain unaware of API runner status.

## Data Flow

```text
BoundedMarketDataRunnerHandle
  -> ApiAppState::with_market_data_runner
  -> runner_status_response_from_state
  -> ApiRunnerStatusProjection
  -> GET /runner/status JSON
```

Default no-runner flow:

```text
ApiAppState::new(FdcServerApp::with_defaults())
  -> no runner attached
  -> state = not_configured, configured = false
```

## Error Handling

- The route is read-only and should not fail under normal conditions.
- Missing runner is represented as a successful response with `configured = false` and `state = not_configured`.
- Failure state is represented by `state = failed` and `failure_message = Some(...)`.

## Testing Strategy

Add contract coverage in `crates/fdc-api/tests/runner_status_contract.rs`:

1. Default API state projects `not_configured` and no result/failure.
2. Created runner projects `created` and `configured = true`.
3. Completed runner projects `completed` and includes last result counts.
4. Cancelled runner projects `cancelled` without result/failure.
5. In-memory route `GET /runner/status` returns JSON for attached completed runner.
6. Dependency guard remains covered by existing API tests.

Expected verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

## Future Work

After B14, future slices can add:

- Mutating API endpoints to start/cancel bounded runs.
- A background task runner with cancellation tokens.
- Live acquisition orchestration behind explicit opt-in controls.
- Persistent status history and SQL-backed operational queries.
