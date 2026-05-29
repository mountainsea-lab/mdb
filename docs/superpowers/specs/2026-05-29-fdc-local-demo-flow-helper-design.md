# B17 Local Demo Flow Helper Design

## Status

Approved direction: add a no-listener, in-memory demo flow helper that exercises the B16 unified demo router end to end.

## Context

B16 added `fdc_api::build_demo_router(state)` for a shared in-memory Axum router. It combines:

- `GET /ready`
- `POST /runner/start-fixture`
- `GET /runner/status`
- `GET /market-data/trades`
- `POST /runner/cancel`

The next step is to make this local MVP path easy to exercise without introducing real HTTP listener lifecycle, port management, authentication, persistence, live network acquisition, or daemon supervision.

## Goal

Add a reusable demo flow helper in `fdc-api` that:

1. Builds an initialized test/demo `ApiAppState` with a shared `QueryableMarketDataStore` and mutable `BoundedMarketDataRunnerHandle`.
2. Builds the B16 unified demo router from that state.
3. Runs a deterministic in-memory request sequence:
   - `GET /ready`
   - `POST /runner/start-fixture`
   - `GET /runner/status`
   - `GET /market-data/trades?symbol=BTCUSDT&limit=10`
4. Returns a typed summary that can be used by tests, future examples, or documentation.

## Non-Goals

B17 does not add:

- Real listener binding.
- CLI commands or binaries.
- Curl-based documentation that assumes a running local server.
- Authentication, authorization, production middleware, or CORS changes.
- Live network acquisition.
- Persistent storage or SQL integration.
- New lower-level dependencies on `fdc-api`.

## Recommended Approach

Create `crates/fdc-api/src/demo_flow.rs` as a focused module for no-listener demo orchestration.

Public API:

- `DemoFixtureTrade`
  - `symbol: String`
  - `trade_id: String`
  - `sequence: Option<String>`
- `DemoFlowRequest`
  - `trades: Vec<DemoFixtureTrade>`
  - `query_symbol: String`
  - `query_limit: usize`
- `DemoFlowSummary`
  - `readiness: ApiReadinessProjection`
  - `start_status: ApiRunnerStatusProjection`
  - `final_status: ApiRunnerStatusProjection`
  - `market_data: MarketDataTradesResponse`
- `run_demo_flow_once(request) -> Result<DemoFlowSummary, ApiError>`
- `default_demo_flow_request() -> DemoFlowRequest`

The helper should use Axum/Tower in-memory `oneshot` calls against `build_demo_router(state)`. That makes the demo exercise the same HTTP route stack as B16 without binding a socket.

## Component Boundaries

### `fdc-api::demo_flow`

Owns the no-listener demo sequence and typed summary. It may compose `fdc-server`, `fdc-storage`, B16 `build_demo_router`, and existing API DTOs.

### `fdc-api::demo`

Remains the router assembly boundary. B17 should not move route definitions out of B16 modules.

### Lower-level crates

No lower-level crate should reference `fdc-api`. The dependency direction remains one-way: `fdc-api` composes server/storage abstractions for local demo use.

## Data Flow

```text
run_demo_flow_once(default_demo_flow_request())
  -> build initialized FdcServerApp
  -> build shared QueryableMarketDataStore
  -> build mutable BoundedMarketDataRunnerHandle
  -> ApiAppState with shared store and control handle
  -> build_demo_router(state)
  -> GET /ready
  -> POST /runner/start-fixture
  -> GET /runner/status
  -> GET /market-data/trades?symbol=BTCUSDT&limit=10
  -> DemoFlowSummary
```

## Error Handling

- HTTP non-success statuses from in-memory route calls should become `ApiError::internal` with route context.
- JSON decode failures should become `ApiError::internal` with response context.
- API-level error responses from `POST /runner/start-fixture` should become `ApiError::validation` or `ApiError::internal` with the response message.
- Empty trade requests should be rejected before route execution with a validation error.
- Query limit `0` is allowed but will return no records.

## Testing Strategy

Add `crates/fdc-api/tests/demo_flow_contract.rs` with tests that verify:

1. `default_demo_flow_request` is deterministic and queries `BTCUSDT`.
2. `run_demo_flow_once(default_demo_flow_request())` returns ready readiness, completed runner status, one stored BTCUSDT trade, and matching result counts.
3. A request with two trades and query symbol `ETHUSDT` returns only the ETHUSDT trade.
4. An empty trade request returns an error and does not panic.
5. Dependency guard confirms lower-level crates do not reference `fdc-api`.

Expected verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

## Future Work

After B17, future slices can add:

- Documentation showing the no-listener demo flow and response shape.
- A gated local HTTP demo binary that reuses B16 router assembly.
- A CLI wrapper around the B17 helper.
- Production route assembly with authentication and middleware.
