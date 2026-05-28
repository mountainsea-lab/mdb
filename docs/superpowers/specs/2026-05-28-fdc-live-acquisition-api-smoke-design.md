# B12 Ignored Live Acquisition Smoke to MVP Store Design

## Status

Approved direction: add an ignored, network-gated live smoke contract that proves one live Barter trade can travel through the B11 MVP store path and be read through the B10 API route.

## Context

The project has completed the bounded offline MVP path:

- B3 added `fdc-barter` live Binance Spot public trade helpers and ignored live smoke tests.
- B10 added the in-memory API route `GET /market-data/trades` backed by an injected `QueryableMarketDataStore`.
- B11 added `run_barter_fixture_mvp_once`, which writes finite Barter envelopes into a shared `QueryableMarketDataStore` through the orchestrator mapping path.

B12 should add the smallest live validation that connects those pieces without changing production lifecycle behavior.

## Goal

Demonstrate this opt-in path:

1. Initialize Binance Spot public trade streams through `fdc-barter`.
2. Collect a tiny bounded set of live trade envelopes, starting with one trade.
3. Write those envelopes through the B11 MVP helper into a shared `QueryableMarketDataStore`.
4. Query that same store through the B10 in-memory API route.
5. Assert the route returns at least one live trade payload.

## Non-Goals

B12 does not add:

- A long-running service runner.
- Production lifecycle management, cancellation, or background task supervision.
- Persistence, SQL integration, or tier-aware storage runtime routing.
- Any default network-dependent test.
- Any new lower-level dependency on `fdc-api` or `fdc-server`.

## Recommended Approach

Add one ignored contract test in `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`.

The test should be annotated with:

```rust
#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
```

At runtime, the test should also check `FDC_BARTER_LIVE_SMOKE=1`; if the variable is not set, it should print a skip message and return. This keeps default test runs fully offline while allowing explicit live validation.

The smoke should use existing public helpers:

- `default_binance_spot_trade_subscriptions`
- `init_binance_spot_public_trades`
- `public_trade_result_to_data_kind`
- `collect_live_trade_envelopes`
- `run_barter_fixture_mvp_once`
- `build_market_data_router`

## Component Boundaries

### `fdc-api` test boundary

Owns the end-to-end smoke assertion because it can legally depend on `fdc-api`, `fdc-server`, `fdc-storage`, and dev-only `fdc-barter` test helpers.

### `fdc-barter`

Continues to own live stream initialization and live trade envelope collection. B12 must not duplicate Barter stream mapping logic.

### `fdc-server`

Continues to own the B11 finite MVP assembly helper. B12 should reuse it rather than adding API-to-orchestrator glue in `fdc-api` production code.

### Lower-level crates

`fdc-storage`, `fdc-orchestrator`, `fdc-transform`, `fdc-ingestion`, and `fdc-barter` must not depend on `fdc-api`.

## Data Flow

```text
Binance Spot public trade stream
  -> fdc-barter collect_live_trade_envelopes(limit = 1)
  -> fdc-server run_barter_fixture_mvp_once
  -> shared fdc-storage QueryableMarketDataStore
  -> fdc-api ApiAppState::with_market_data_store
  -> fdc-api build_market_data_router
  -> GET /market-data/trades?limit=10
```

## Error Handling

- Stream initialization errors should fail the live smoke only when explicitly enabled.
- Collection should be wrapped in a bounded timeout, defaulting to 30 seconds.
- If the environment variable is absent, the ignored test should return successfully after printing a skip message.
- Storage/orchestration errors should propagate as test failures only in the explicit live smoke run.

## Test Strategy

Default verification should compile the ignored test but not run network I/O:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Explicit live verification should be manual/opt-in:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture
```

The live test should assert:

- one or more live envelopes were collected;
- B11 wrote at least one storage record;
- the B10 route returns `200 OK`;
- the JSON response reports `status = "success"` and at least one returned record;
- the first returned record has `kind = "trade"` and a payload symbol.

## Future Work

After B12, future slices can separately add:

- A bounded application runner lifecycle with cancellation and readiness transitions.
- Production storage runtime routing and persistence.
- SQL-backed market-data querying.
- Stateful dedupe/gap detection for live streams.
