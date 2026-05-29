# First MVP Demo Guide

This guide explains the first internally verifiable Financial Data Center MVP. It is intentionally a no-listener, in-memory demo flow. It does not start a production service, bind a port, write persistent storage, or require live network access.

## What this MVP demonstrates

The first MVP demonstrates a bounded market-data path from deterministic fixture input to API-readable market-data records:

1. API readiness can be projected from an initialized server app.
2. A fixture trade can be submitted through the unified demo router with `POST /runner/start-fixture`.
3. Runner lifecycle can be read through `GET /runner/status`.
4. Written market-data records can be queried through `GET /market-data/trades`.
5. The whole flow can be run in memory through `run_demo_flow_once` without binding a socket.

## What this MVP does not demonstrate

The MVP deliberately excludes production concerns:

- no-listener: no HTTP listener is started by the default demo flow.
- in-memory only: the queryable market-data store is created for one demo run.
- no persistence: records are not written to disk or an external database.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and lifecycle management are out of scope.
- live network acquisition is not part of the default MVP. Optional live smoke validation remains ignored and environment-gated.

## Quick verification

Run the core demo-flow contract:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
```

Expected result:

```text
cargo test: 5 passed
```

Useful adjacent checks:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

These verify the B16 unified router and the broader API/server boundary.

## Programmatic no-listener usage

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

A successful `DemoFlowSummary` includes:

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

## Optional live smoke validation

Optional Binance Spot live smoke validation exists in ignored tests from earlier slices. It is not part of the default MVP because it requires live network access and environment opt-in.

Use it only when validating external connectivity and Barter-rs integration manually. The default MVP remains deterministic and offline.

## Current MVP status

For an internal developer/reviewer, this is enough to verify the first MVP without starting a service:

1. Run the demo-flow contract command.
2. Read the deterministic fixture request.
3. Compare the expected `DemoFlowSummary` highlights.
4. Confirm explicit non-goals are acceptable for the first MVP.

If an interactive external demo is needed next, the next slice should add a gated local HTTP demo entrypoint that reuses `build_demo_router` instead of creating new route behavior.
