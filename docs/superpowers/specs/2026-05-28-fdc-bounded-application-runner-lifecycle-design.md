# B13 Bounded Application Runner Lifecycle Design

## Status

Approved direction: implement a bounded, fixture-only application runner lifecycle in `fdc-server`.

## Context

The project now has a deterministic market-data MVP path:

- B11 added `BoundedMarketDataMvpRunner` and `run_barter_fixture_mvp_once` to write finite Barter envelopes into a shared `QueryableMarketDataStore`.
- B12 added an ignored, network-gated live smoke that can collect one Binance Spot trade and query it through the B10 API route.
- `FdcServerApp` currently has a coarse lifecycle: `Created`, `Initialized`, and `Stopped`.

B13 should add a narrower runner lifecycle for finite demo runs without turning `FdcServerApp` into a production task supervisor.

## Goal

Provide a small assembly-level runner handle that can:

1. Start in `Created` state.
2. Run one finite batch of `BarterIngestionEnvelope` fixtures through the existing B11 MVP helper.
3. Transition to `Running` while work is in progress.
4. Transition to `Completed` with a stored result after success.
5. Support deterministic cancellation before start.
6. Surface current state and last result for tests and later readiness projections.

## Non-Goals

B13 does not add:

- Long-running live stream supervision.
- Background daemon/task management.
- Production cancellation of in-flight network streams.
- Persistent storage, SQL integration, or tier-aware runtime routing.
- New dependencies from lower-level crates to `fdc-server` or `fdc-api`.
- Changes to B10 API routes unless a later slice explicitly designs a runner status API.

## Recommended Approach

Add `crates/fdc-server/src/runner.rs` with:

- `BoundedRunnerState`
  - `Created`
  - `Running`
  - `Completed`
  - `Cancelled`
  - `Failed`
- `BoundedRunnerFailure`
  - stores a human-readable error message for failed runs.
- `BoundedMarketDataRunnerHandle`
  - owns a shared `Arc<QueryableMarketDataStore>`;
  - tracks state and last result;
  - exposes `state()`, `last_result()`, `failure()`, and `market_data_store()`;
  - exposes `cancel()` for pre-start cancellation;
  - exposes `start_once(envelopes)` for one finite run.

`start_once` should be intentionally simple and synchronous from the caller perspective:

- If state is `Created`, set `Running`, call `run_barter_fixture_mvp_once`, then store success or failure.
- If state is `Cancelled`, return a validation error and keep state cancelled.
- If state is any terminal state or already running, return a validation error rather than silently rerunning.

This is a bounded lifecycle helper, not a general-purpose runtime.

## Component Boundaries

### `fdc-server`

Owns the runner lifecycle because it is the application assembly crate and already composes `fdc-barter`, `fdc-storage`, and the B11 MVP helper.

### `fdc-api`

No B13 production change. API readiness projection can consume runner state in a later slice after a dedicated status API design.

### Lower-level crates

`fdc-storage`, `fdc-orchestrator`, `fdc-transform`, `fdc-ingestion`, and `fdc-barter` remain unaware of the runner handle.

## Data Flow

```text
Vec<BarterIngestionEnvelope>
  -> BoundedMarketDataRunnerHandle::start_once
  -> BoundedRunnerState::Running
  -> run_barter_fixture_mvp_once
  -> QueryableMarketDataStore
  -> BoundedRunnerState::Completed + last_result
```

Pre-start cancellation flow:

```text
BoundedMarketDataRunnerHandle::new
  -> Created
  -> cancel()
  -> Cancelled
  -> start_once(...)
  -> validation error, state remains Cancelled
```

## Error Handling

- Invalid lifecycle transitions return `fdc_core::Error::validation`.
- B11 MVP helper errors transition the runner to `Failed` and store `BoundedRunnerFailure`.
- Successful runs clear any prior failure and store the B11 result.
- `cancel()` is idempotent before work starts: calling it in `Created` or `Cancelled` results in `Cancelled`.
- `cancel()` after `Completed` or `Failed` should return a validation error, preserving terminal state.

## Testing Strategy

Add offline contract tests in `crates/fdc-server/tests/bounded_runner_contract.rs`:

1. New runner starts in `Created`, has no result/failure, and exposes the injected store.
2. `start_once` with fixture envelopes transitions to `Completed`, stores the result, and writes queryable market-data records.
3. `cancel()` before start transitions to `Cancelled`, and a later `start_once` returns a validation error without writing records.
4. Re-running after `Completed` returns a validation error and preserves the original result.
5. Dependency guard remains covered by existing server assembly tests.

Expected verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

## Future Work

After B13, later slices can add:

- API projection of runner state and last result.
- A deterministic async background runner with explicit cancellation tokens.
- Production live stream supervision.
- Persistent storage runtime and SQL-backed querying.
