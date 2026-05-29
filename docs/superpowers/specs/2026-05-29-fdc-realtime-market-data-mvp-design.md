# Realtime Market Data MVP Design

## Status

Approved direction: upgrade the first MVP target from fixture/no-listener demo to a real realtime data flow: live market-data acquisition -> storage -> query.

Important scope correction from user feedback: this MVP should not be limited to one record. It should model a real realtime stream. Tests and demos may use a bounded runtime window or explicit stop signal, but acquisition itself should continuously process events while running.

## Context

Current implemented capabilities already provide most building blocks:

- `fdc-barter` can initialize Binance Spot public trade streams via Barter-rs.
- `fdc-barter` can map live Barter stream results into `BarterIngestionEnvelope` values.
- `fdc-orchestrator` can map Barter envelopes through source validation, DTO mapping, and storage writes.
- `fdc-storage` has `QueryableMarketDataStore` for API-readable in-memory market-data records.
- `fdc-api` has market-data query routes and MVP documentation.
- Existing ignored live smoke proves one live trade can be acquired, written, and queried, but it is test-shaped and record-limited.

The missing MVP capability is a reusable realtime runner that streams real market data for a runtime window, writes records continuously, and lets the API query what was written.

## Goal

Add a realtime MVP path that:

1. Starts real Binance Spot public trade acquisition through `fdc-barter`.
2. Processes events continuously while the runner is active.
3. Writes live Barter envelopes into `QueryableMarketDataStore` through the existing orchestrator path.
4. Tracks runtime summary metrics: events observed, envelopes written, storage records written, queryable records, errors, started/stopped timestamps.
5. Exposes a typed helper for gated demos and ignored live smoke tests.
6. Allows querying stored live data through existing market-data API query helpers/routes.

## Non-Goals

This MVP does not add:

- Production daemon supervision.
- Infinite unbounded tests.
- Durable persistence or recovery.
- SQL integration.
- Authentication/authorization.
- Custom reconnect logic beyond Barter-rs behavior.
- Latency/throughput performance claims.
- Multi-exchange support beyond Binance Spot public trades.

## Recommended Approach

Introduce a bounded realtime runner helper, not a production daemon.

### Runtime shape

Add a new server/API-facing helper that accepts:

- subscriptions, defaulting to Binance Spot BTC/USDT and ETH/USDT public trades.
- a runtime window, such as `Duration::from_secs(10)` for demos.
- an optional maximum error count.
- a shared `QueryableMarketDataStore`.

The runner should:

1. Initialize Barter-rs live streams.
2. Convert stream items with `public_trade_result_to_data_kind`.
3. For each emitted `BarterIngestionEnvelope`, write it through `run_barter_envelopes_to_storage_once` or a small batch wrapper using the existing orchestrator boundary.
4. Continue until the runtime window expires or an explicit cancellation token is triggered.
5. Return a `RealtimeMarketDataMvpSummary`.

This avoids record-count limiting while still making tests and demos finite.

### Default real demo

The default real demo should be environment-gated:

```text
FDC_BARTER_LIVE_SMOKE=1
```

Without the env var, live tests should skip. This preserves offline default verification while supporting real flow validation on demand.

### Query after streaming

After the runner stops, use existing API query helpers/routes:

- `query_market_data_trades`
- `build_market_data_router`
- `GET /market-data/trades?limit=100`

The MVP success condition is not exactly one record. It is:

- at least one live trade was acquired during the runtime window, when network conditions allow it.
- at least one storage record was written.
- API query returns records from the same shared store.

## Component Boundaries

### `fdc-barter`

Owns live stream initialization and raw Barter result mapping into adapter-owned envelopes.

### `fdc-orchestrator`

Owns envelope -> source validation -> DTO -> storage write mapping. Realtime MVP must not duplicate mapping logic in API/server.

### `fdc-storage`

Owns queryable in-memory store for MVP readback. No persistence is added in this slice.

### `fdc-server` or `fdc-api`

Owns realtime MVP assembly helper. Prefer `fdc-server` if the helper is application assembly, with API tests proving queryability. Use `fdc-api` only for route/demo integration.

## Testing Strategy

Use two levels of tests:

1. Offline contract test with a fake stream:
   - Simulates multiple live trade envelopes over time.
   - Verifies the realtime runner writes all stream events observed during the bounded window.
   - Verifies API query reads the stored records.
   - Does not use network.

2. Ignored live smoke test with real Binance Spot stream:
   - Requires `FDC_BARTER_LIVE_SMOKE=1`.
   - Runs for a short duration window, for example 10-30 seconds.
   - Does not cap at one record.
   - Asserts `storage_records_written >= 1` and API query returns at least one live record.
   - Prints summary for manual validation.

## Acceptance Criteria

- The realtime MVP runner processes a stream until a duration/cancellation boundary, not until a fixed one-record limit.
- Offline tests prove multi-event streaming writes multiple records to queryable storage.
- Ignored live smoke test proves real Binance Spot public trades can flow through acquisition -> storage -> query.
- Existing fixture/no-listener MVP tests continue to pass.
- Documentation and acceptance report are updated to reflect that the first MVP target is realtime data acquisition to storage to query, with live validation gated by env var.

## Future Work

After this realtime MVP:

1. Add a gated HTTP demo entrypoint for interactive realtime query demos.
2. Add production runner lifecycle and cancellation APIs.
3. Add persistent storage boundary for live data.
4. Add health/error telemetry for live streams.
5. Add SQL query integration over stored market-data records.
