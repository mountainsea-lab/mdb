# First MVP Demo Guide

This guide explains the first internally verifiable Financial Data Center MVP. The MVP target is realtime market data acquisition -> storage -> query. Default verification remains no-listener, in-memory, and offline by using deterministic live-style streams; optional ignored smoke validation uses real Binance Spot public trades when explicitly enabled.

## What this MVP demonstrates

The first MVP demonstrates a realtime market-data path to API-readable market-data records:

1. A realtime MVP runner consumes Barter ingestion envelopes from a stream during a configured runtime or idle window.
2. The runner does not stop after exactly one record; it processes all available stream events while active.
3. Barter envelopes are written through the orchestrator storage path into `QueryableMarketDataStore`.
4. Written market-data records can be queried through `GET /market-data/trades`.
5. Optional ignored live smoke validation can use real Binance Spot public trade data with `FDC_BARTER_LIVE_SMOKE=1`.

The earlier deterministic fixture demo still exists for stable local demos:

1. API readiness can be projected from an initialized server app.
2. A fixture trade can be submitted through the unified demo router with `POST /runner/start-fixture`.
3. Runner lifecycle can be read through `GET /runner/status`.
4. The whole fixture flow can be run in memory through `run_demo_flow_once` without binding a socket.

## What this MVP does not demonstrate

The MVP deliberately excludes production concerns:

- no-listener: no HTTP listener is started by the default demo flow.
- in-memory only: the queryable market-data store is created for one demo run/test run.
- no persistence: records are not written to disk or an external database.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and lifecycle management are out of scope.
- live network acquisition is not part of the default MVP verification. Optional live smoke validation remains ignored and environment-gated.

## Quick verification

Run the realtime MVP contract:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract
```

Expected result:

```text
cargo test: 2 passed
```

Run the API-facing realtime query contract:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Expected result:

```text
cargo test: 3 passed, 1 ignored
```

Useful adjacent checks:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

These verify the deterministic fixture demo and the broader API/server boundary.

## Programmatic realtime MVP usage

Use `run_realtime_barter_envelope_stream` with a stream of `BarterIngestionEnvelope` values and a shared `QueryableMarketDataStore`:

```rust
use std::{sync::Arc, time::Duration};

use fdc_server::{run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig};
use fdc_storage::QueryableMarketDataStore;
use futures::stream;

#[tokio::main]
async fn main() -> fdc_core::Result<()> {
    let store = Arc::new(QueryableMarketDataStore::new());
    let live_style_stream = stream::iter(Vec::new()); // Replace with Barter live envelopes.

    let summary = run_realtime_barter_envelope_stream(
        live_style_stream,
        Arc::clone(&store),
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(2),
            max_errors: 0,
        },
    )
    .await?;

    println!("envelopes: {}", summary.envelopes_received);
    println!("storage_records: {}", summary.storage_records_written);
    println!("queryable_records: {}", summary.market_data_store_records);

    Ok(())
}
```

The offline contract tests use deterministic live-style envelopes so the default verification is stable. The ignored live smoke uses real Binance Spot acquisition and then runs those envelopes through the realtime runner.

## Programmatic no-listener fixture usage

Use `default_demo_flow_request` for the deterministic BTCUSDT fixture flow:

```rust
use fdc_api::{default_demo_flow_request, run_demo_flow_once};

#[tokio::main]
async fn main() -> Result<(), fdc_api::ApiError> {
    let summary = run_demo_flow_once(default_demo_flow_request()).await?;

    println!("readiness: {:?}", summary.readiness.status);
    println!("runner: {:?}", summary.final_status.state);
    println!("records: {}", summary.market_data.returned_records);

    Ok(())
}
```

`run_demo_flow_once` builds an initialized in-memory `ApiAppState`, creates the B16 unified router, sends in-memory Axum/Tower requests, and returns a typed `DemoFlowSummary`.

## Default fixture request shape

`default_demo_flow_request` is equivalent to this logical request:

```json
{
  "trades": [
    {
      "symbol": "BTCUSDT",
      "trade_id": "btc-demo-1",
      "sequence": "seq-demo-1"
    }
  ],
  "query_symbol": "BTCUSDT",
  "query_limit": 10
}
```

The request is deterministic so tests and documentation can rely on stable values.

## Expected DemoFlowSummary highlights

A successful fixture `DemoFlowSummary` includes:

```text
readiness.status = ready
readiness.server_lifecycle_state = initialized
start_status.state = completed
final_status.state = completed
final_status.last_result.storage_records_written = 1
market_data.returned_records = 1
market_data.records[0].symbol = BTCUSDT
```

The full `DemoFlowSummary` type contains:

- `readiness`: typed readiness projection from `GET /ready`.
- `start_status`: runner status returned by `POST /runner/start-fixture`.
- `final_status`: runner status returned by `GET /runner/status` after fixture ingestion.
- `market_data`: typed trade query response returned by `GET /market-data/trades`.

## Route-to-summary mapping

| B16 route | B17 summary field | Purpose |
| --- | --- | --- |
| `GET /ready` | `DemoFlowSummary.readiness` | Confirms the demo server app is initialized and API-enabled. |
| `POST /runner/start-fixture` | `DemoFlowSummary.start_status` | Runs finite fixture trades through the bounded runner and writes market data. |
| `GET /runner/status` | `DemoFlowSummary.final_status` | Confirms final runner lifecycle and last-result counts. |
| `GET /market-data/trades` | `DemoFlowSummary.market_data` | Reads records written by the fixture flow from the shared in-memory store. |

## Optional real live smoke validation

Optional Binance Spot live smoke validation exists in an ignored test. It requires public internet access and explicit environment opt-in:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture
```

The live smoke initializes Binance Spot public trade streams, collects real trade envelopes during a bounded timeout, writes them through `run_realtime_barter_envelope_stream`, and queries them through the API route. It asserts at least one real live trade is queryable, not exactly one.

## Current MVP status

For an internal developer/reviewer, this is enough to verify the first realtime MVP without starting a service:

1. Run the realtime MVP contract command.
2. Run the API-facing acquisition/query contract command.
3. Confirm optional live smoke is available for real exchange validation.
4. Confirm explicit non-goals are acceptable for the first MVP.

If an interactive external demo is needed next, the next slice should add a gated local HTTP demo entrypoint that runs the realtime runner and reuses `build_demo_router` instead of creating new route behavior.
