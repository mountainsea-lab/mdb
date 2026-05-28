# B15 Bounded Runner Control API Design

## Status

Approved direction: add test/demo-only in-memory runner control APIs using a mutable shared runner handle.

## Context

The MVP path now has:

- B13 `BoundedMarketDataRunnerHandle`, which can run finite fixture envelopes once and supports pre-start cancellation.
- B14 read-only API status projection and `GET /runner/status` for observing runner lifecycle.
- API market-data query route for reading records written into a shared `QueryableMarketDataStore`.

B15 should make the bounded runner controllable through an in-memory API surface for local MVP demos, without promoting it to a production runtime.

## Goal

Add API controls that can:

1. Attach a mutable runner control handle to `ApiAppState`.
2. Start a finite fixture run from a JSON request.
3. Cancel a runner before it starts.
4. Return the same runner status projection shape used by B14.
5. Keep all behavior deterministic, offline, and bounded.

## Non-Goals

B15 does not add:

- Live network acquisition.
- Background tasks or daemon supervision.
- Persistent runner state.
- SQL integration.
- Authentication/authorization.
- Production-safe concurrent orchestration semantics.
- A generic arbitrary-envelope ingestion API.

## Recommended Approach

Use a separate mutable control handle in `ApiAppState`:

```rust
Option<Arc<tokio::sync::Mutex<BoundedMarketDataRunnerHandle>>>
```

Keep B14's read-only `Arc<BoundedMarketDataRunnerHandle>` compatibility for existing status tests. B15 status helpers can prefer the mutable control handle when present, otherwise fall back to the read-only handle.

Add `crates/fdc-api/src/runner_control.rs` with:

- `RunnerFixtureTradeInput`
  - `symbol: String`
  - `trade_id: String`
  - `sequence: Option<String>`
- `RunnerStartFixtureRequest`
  - `trades: Vec<RunnerFixtureTradeInput>`
- `start_fixture_runner_from_state(state, request)` pure async helper.
- `cancel_runner_from_state(state)` pure async helper.
- `build_runner_control_router(state)` with:
  - `POST /runner/start-fixture`
  - `POST /runner/cancel`

The fixture start helper should:

1. Require an attached mutable runner control handle.
2. Validate that request contains at least one trade.
3. Convert each trade into a deterministic `BarterIngestionEnvelope` fixture.
4. Lock the runner and call `start_once`.
5. Return the B14 `ApiRunnerStatusProjection` after the operation.

The cancel helper should:

1. Require an attached mutable runner control handle.
2. Lock the runner and call `cancel`.
3. Return the B14 `ApiRunnerStatusProjection` after the operation.

## Component Boundaries

### `fdc-api`

Owns request DTOs, control helpers, and in-memory routes. It composes `fdc-server` runner APIs for demo control only.

### `fdc-server`

Continues to own lifecycle rules. B15 should not move lifecycle validation into `fdc-api`.

### Lower-level crates

Storage, orchestration, transform, ingestion, and adapter crates stay unchanged and remain unaware of API control endpoints.

## Data Flow

Fixture start:

```text
POST /runner/start-fixture
  -> RunnerStartFixtureRequest
  -> deterministic BarterIngestionEnvelope fixtures
  -> BoundedMarketDataRunnerHandle::start_once
  -> QueryableMarketDataStore writes
  -> ApiRunnerStatusProjection response
```

Cancel:

```text
POST /runner/cancel
  -> BoundedMarketDataRunnerHandle::cancel
  -> ApiRunnerStatusProjection response
```

## Error Handling

- Missing mutable runner control handle returns an API error response with a validation-style message.
- Empty fixture trade list returns an API error response.
- Runner lifecycle validation errors, such as starting after cancellation or rerunning after completion, return API error responses.
- Successful operations return `ApiResponse::success(ApiRunnerStatusProjection)`.

## Testing Strategy

Add contract tests in `crates/fdc-api/tests/runner_control_contract.rs`:

1. `POST /runner/start-fixture` writes records and returns completed status.
2. After start, existing market-data query route can read the written trade from the same store.
3. `POST /runner/cancel` transitions a created runner to cancelled.
4. Starting without a control handle returns an error response and does not panic.
5. Empty trade list returns an error response.

Expected verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

## Future Work

After B15, future slices can add:

- Unified demo router combining readiness, market-data, runner status, and runner control.
- A CLI or server demo entrypoint.
- Live bounded runner controls behind explicit opt-in gates.
- Authentication and production control-plane safety.
