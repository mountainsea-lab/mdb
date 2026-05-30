# Production Background Live Runner and Autostart Design

## Status

Approved design for the next `fdc-server` production market-data slice after bounded enabled live start. This slice promotes live acquisition from a request-bound bounded operation to a production-shaped long-running background service with optional startup autostart and explicit stop control.

## Context

Current production server state:

- `fdc-server` exposes production routes:
  - `GET /health`
  - `GET /ready`
  - `POST /market-data/live/start`
  - `GET /market-data/live/status`
  - `GET /market-data/trades`
- `FDC_LIVE_ENABLED=1` currently allows `POST /market-data/live/start` to synchronously collect a bounded number of Binance Spot public trades and write them to the shared `QueryableMarketDataStore`.
- `MarketDataSupervisor` tracks coarse states and rejects concurrent starts.
- The bounded production live smoke has validated real Binance data flowing through start -> storage -> query.

Production expectation after review:

- Market-data acquisition should normally begin when the service starts, not only when an operator calls a trigger route.
- Manual start and stop routes should still exist for operational recovery and testability.

## Goals

1. Add a true background live runner for production market data.
2. Add optional service-start autostart controlled by configuration.
3. Keep default local and CI behavior offline and network-free.
4. Keep `POST /market-data/live/start` as a manual control that starts the background runner when it is not already active.
5. Add `POST /market-data/live/stop` to request cancellation of an active background runner.
6. Extend live status with operational metadata:
   - task id
   - started/stopped timestamps
   - stop reason
   - subscriptions
   - last record timestamp
   - cumulative envelope/storage/store counts
7. Preserve current dependency boundaries and keep production orchestration inside `fdc-server::market_data`.

## Non-Goals

This slice does not implement:

- Actor/event-loop supervisor architecture.
- Dynamic subscription changes at runtime.
- Multiple simultaneous live data sources.
- Persistent checkpoints or durable storage.
- Reconnect policy beyond what Barter-rs already provides.
- Auth, TLS, deployment packaging, metrics export, or alerting.
- SQL query engine integration.

Actor-style supervision is the recommended later evolution once command routing, multiple sources, or dynamic subscriptions are needed.

## Configuration

Add one runtime configuration flag:

```text
FDC_LIVE_AUTOSTART=false
```

Rules:

- `FDC_LIVE_ENABLED` remains the master gate. Default is false.
- `FDC_LIVE_AUTOSTART` default is false.
- Autostart only runs when both are true:
  - `FDC_LIVE_ENABLED=1`
  - `FDC_LIVE_AUTOSTART=1`
- If `FDC_LIVE_AUTOSTART=1` but live is disabled, startup should not touch network and readiness/status should expose a clear disabled/not-started state rather than failing server startup.

## Architecture

Use a single background task owned by the production market-data supervisor.

### Components

#### `market_data::model`

Extend DTOs:

- `StartLiveMarketDataResponse`
  - Keep existing counts.
  - For background start, return `state=running` after spawn succeeds.
- `LiveMarketDataStatusResponse`
  - Existing: `state`, `last_result`, `failure_message`.
  - Add:
    - `task_id: Option<String>`
    - `started_at_ns: Option<u64>`
    - `stopped_at_ns: Option<u64>`
    - `stop_reason: Option<String>`
    - `subscriptions: Vec<String>`
    - `last_record_at_ns: Option<u64>`
    - `envelopes_received: usize`
    - `storage_records_written: usize`
    - `market_data_store_records: usize`
- Add `StopLiveMarketDataResponse` mirroring the operational status fields needed by `POST /market-data/live/stop`.

Use serializable primitive nanoseconds for public API timestamp fields so the HTTP DTOs stay stable and do not expose core wrapper types.

#### `market_data::supervisor`

Evolve `MarketDataSupervisor` from only state transitions into a single-runner control handle.

Responsibilities:

- Own current live state.
- Own the current task id.
- Own cancellation token/signal for the active task.
- Store the background task join handle if it is safe to keep there.
- Store status counters and operational metadata.
- Reject concurrent start attempts while `starting`, `running`, or `stopping`.
- Allow idempotent stop when already idle/completed/failed/stopped.
- Expose `status()` as a snapshot without awaiting the background task.

State transitions:

```text
idle -> starting -> running -> stopping -> stopped
idle -> starting -> running -> failed
running -> failed
failed -> starting
stopped -> starting
completed -> starting
```

`completed` may remain available for bounded/manual compatibility, but the long-running background path should normally end as `stopped` or `failed`.

#### `market_data::service`

Add service operations:

- `start_background_live(state, request) -> Result<StartLiveMarketDataResponse, String>`
- `stop_live(state) -> StopLiveMarketDataResponse`
- `maybe_autostart_live(state) -> Result<(), String>`

Manual start behavior:

1. Check `live_enabled`.
2. Ask supervisor to reserve/start a new task.
3. Spawn the background runner.
4. Return immediately with `state=running` and current counters.

Autostart behavior:

1. Called during production app/server assembly after state creation and before serving requests.
2. If `live_enabled && live_autostart`, call the same background-start path with default subscriptions/config.
3. If autostart fails to spawn because a runner is already active, keep the existing runner.
4. If stream initialization later fails in the task, status becomes `failed`; server process stays up so operators can inspect `/market-data/live/status`.

Stop behavior:

1. If no active runner, return current status idempotently.
2. If active, mark `stopping` and signal cancellation.
3. Return promptly. The background task records final `stopped` when it observes cancellation and exits.
4. Status should show `stopping` until the task finalizes.

#### Background runner

The background runner should:

1. Initialize Binance Spot public trade streams using the same proven `fdc-barter` path.
2. Continuously consume public trades until cancellation or unrecoverable error.
3. Convert trade results to `BarterIngestionEnvelope` values.
4. Write envelopes to the shared `QueryableMarketDataStore` through the existing realtime/orchestrator path or a small focused helper.
5. Update supervisor counters after each successful envelope or batch:
   - `envelopes_received`
   - `storage_records_written`
   - `market_data_store_records`
   - `last_record_at_ns`
6. On cancellation, set `stopped_at` and `stop_reason="requested"`.
7. On failure, set `state=failed` and `failure_message`.

Because Barter stream types previously required isolation from Axum `Send` futures, keep using the proven `spawn_blocking + current-thread Tokio runtime` pattern unless implementation proves a direct spawned async task is safe.

## Routes

### `POST /market-data/live/start`

- Disabled: returns `status=error` and message mentioning `FDC_LIVE_ENABLED=1`.
- Already active: returns `status=error` and current status-like response.
- Success: returns `status=success`, `data.state=running`, `task_id`, and current counters.

### `POST /market-data/live/stop`

- No active runner: returns `status=success` with current state and no mutation beyond optional stop reason preservation.
- Active runner: returns `status=success`, `data.state=stopping`, and task metadata.
- Repeated stop while stopping: idempotent success with current status.

### `GET /market-data/live/status`

Returns the latest supervisor snapshot. It must not block on live acquisition or task joining.

### `GET /market-data/trades`

Unchanged. It reads records written by the background runner from the shared queryable store.

## Testing Strategy

Default tests remain offline.

### Unit/contract tests

Add or extend `production_server_router_contract.rs` and/or a focused background-runner contract:

1. Runtime config parses `FDC_LIVE_AUTOSTART` with default false.
2. Disabled start still returns explicit disabled error.
3. Supervisor reserves one active task and rejects concurrent starts.
4. Stop is idempotent when idle/stopped.
5. Stop transitions active runner from `running` to `stopping` and then `stopped` using a fake controllable runner.
6. Status includes task id, timestamps, subscriptions, last record timestamp, stop reason, and counters.
7. Autostart hook does nothing when disabled by default.
8. Autostart hook starts a fake runner when both live flags are enabled.

Use test-only/fake stream injection at the service boundary where practical. Do not make default tests depend on public internet.

### Ignored live smoke

Extend or add an ignored smoke test requiring public internet and live flags:

```bash
FDC_LIVE_ENABLED=1 FDC_LIVE_AUTOSTART=1 cargo test -p fdc-server --test production_background_live_smoke ignored_background_live_autostart_writes_trades_and_stop_finishes -- --ignored --nocapture
```

Smoke acceptance:

1. Production app starts with autostart enabled.
2. Status reaches `running` and counters increase.
3. `/market-data/trades?limit=5` returns at least one real record.
4. `/market-data/live/stop` returns `stopping` or `stopped`.
5. Status eventually reaches `stopped` with `stop_reason="requested"`.

## Acceptance Criteria

- `FDC_LIVE_AUTOSTART` is parsed and documented.
- Default test runs do not touch the network.
- `POST /market-data/live/start` starts a long-running background runner and returns promptly.
- `GET /market-data/live/status` exposes C-level operational status and live counters.
- `POST /market-data/live/stop` cancels an active runner and is idempotent when inactive.
- With `FDC_LIVE_ENABLED=1` and `FDC_LIVE_AUTOSTART=1`, service startup can begin live acquisition without a start-route trigger.
- Real ignored smoke proves autostart -> live write -> query -> stop.
- The bounded synchronous implementation can be removed or kept only as a private helper if it does not conflict with background semantics.

## Future Work

1. Actor/event-loop supervisor for richer command handling.
2. Dynamic subscription management.
3. Multi-source live runners.
4. Explicit reconnect/backoff policy and health metrics.
5. Durable checkpoints and persistence.
6. Production metrics/observability integration.
