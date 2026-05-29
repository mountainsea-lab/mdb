# Production Live Supervisor Enabled Mode Design

## Status

Approved continuation after the first production server runtime slice. The goal is to move real live acquisition support from the `fdc-api` demo route into the production `fdc-server` market-data module.

## Context

Current state:

- Realtime MVP is validated with real Binance Spot data through `fdc-api` demo route `POST /runner/start-live`.
- Production `fdc-server` runtime exists with:
  - `cargo run -p fdc-server --bin fdc_server`
  - `/health`
  - `/ready`
  - `/market-data/live/start`
  - `/market-data/live/status`
  - `/market-data/trades`
- Production `/market-data/live/start` currently has disabled-by-default gate only. It returns an explicit error unless `FDC_LIVE_ENABLED=1`, but enabled live acquisition is not implemented yet.

## Goal

Implement enabled real live acquisition in `fdc-server`:

1. `FDC_LIVE_ENABLED=1` allows `POST /market-data/live/start` to acquire real Binance Spot public trades.
2. The route collects a bounded set of live envelopes, writes them to the shared `QueryableMarketDataStore`, and returns a typed summary.
3. `GET /market-data/live/status` reflects supervisor state, last result, and failure message.
4. `GET /market-data/trades` queries the records written by production live acquisition.
5. The production path no longer depends on the `fdc-api` demo live route for real-data validation.

## Non-Goals

This slice does not implement:

- Indefinite streaming background task.
- True asynchronous `202 Accepted` start semantics.
- `POST /market-data/live/stop` cancellation of a running infinite stream.
- Durable persistence.
- Auth, TLS, deployment packaging, or metrics export.
- Additional exchanges beyond Binance Spot BTC/USDT and ETH/USDT defaults.

## Chosen Approach

Use a bounded synchronous production start for this slice.

`POST /market-data/live/start` will:

1. Check `ServerRuntimeConfig.live_enabled`.
2. Use request overrides or config defaults:
   - `timeout_secs`
   - `max_envelopes`
3. Ask `MarketDataSupervisor` to transition from `idle/completed/failed/stopped` to `starting`.
4. Initialize Binance Spot public trade streams through `fdc-barter`.
5. Collect up to `max_envelopes` live envelopes within `timeout_secs`.
6. Write collected envelopes through `run_realtime_barter_envelope_stream` into the production shared store.
7. Mark supervisor `completed` with summary, or `failed` with message.
8. Return `status=success` with `state=completed` and counts when at least one record is written.

This is production-shaped but still bounded. It gives a stable operator flow and avoids the complexity of long-running cancellation until the next slice.

## Business Module Responsibilities

### `market_data/router.rs`

- HTTP extraction and JSON response only.
- Calls service functions.
- Does not initialize Barter streams directly.

### `market_data/service.rs`

- Validates live enabled config.
- Applies request/config defaults.
- Runs live acquisition and storage write.
- Converts service results into response DTOs.

### `market_data/supervisor.rs`

- Owns state transitions.
- Prevents concurrent starts while `starting` or `running`.
- Stores last result and failure message.
- Exposes read-only status.

### `market_data/model.rs`

- Owns request/response/status DTOs and serializable state enum.

## Supervisor State Rules

Allowed start states:

- `idle`
- `completed`
- `failed`
- `stopped`

Rejected start states:

- `starting`
- `running`
- `stopping`

Transitions:

```text
idle -> starting -> completed
idle -> starting -> failed
completed -> starting -> completed
failed -> starting -> completed
```

For this bounded slice, `running` may be reserved for the future background-stream implementation. `starting` covers the active HTTP request.

## Testing Strategy

Default tests remain offline:

1. Existing disabled-gate test continues to pass.
2. Add supervisor state tests:
   - default state is `idle`.
   - `try_start` transitions to `starting`.
   - concurrent `try_start` is rejected.
   - `complete` records `completed` and last result.
   - `fail` records `failed` and failure message.
3. Add service/router fake-stream test if practical:
   - test helper writes fake live-style envelopes through production service.
   - query returns records.

Ignored/gated live smoke:

- Requires public internet and `FDC_LIVE_ENABLED=1`.
- Starts production `fdc-server` route or calls service with live enabled.
- Asserts at least one live record is written and queryable.

## Acceptance Criteria

- With default config, `POST /market-data/live/start` returns error mentioning `FDC_LIVE_ENABLED=1`.
- With `FDC_LIVE_ENABLED=1`, production live start can collect real Binance Spot trades and write them to the shared store.
- After live start succeeds, `GET /market-data/trades` returns live records.
- `/market-data/live/status` reports `completed` and last-result counts after success.
- Supervisor rejects concurrent start attempts.
- Existing `fdc-api` demo route continues to pass compatibility tests.

## Future Work

Next production slice should convert bounded start into a true background runner:

- `POST /market-data/live/start` returns immediately after spawning.
- `GET /market-data/live/status` reports ongoing counters.
- `POST /market-data/live/stop` cancels the stream.
- The runner continuously writes while active, not bounded by `max_envelopes`.
