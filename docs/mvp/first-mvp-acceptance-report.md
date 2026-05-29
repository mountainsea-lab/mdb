# First MVP Acceptance Report

## Acceptance decision

First internal MVP: accepted as a realtime market-data MVP.

This acceptance covers an internal, no-listener, in-memory market-data flow that proves realtime-style market data can move through acquisition, storage, and query boundaries. Default verification uses deterministic live-style streams so it is repeatable offline; optional ignored live smoke validation uses real Binance Spot public trades when `FDC_BARTER_LIVE_SMOKE=1` is set.

This is not an external production release and not an interactive HTTP demo.

## Accepted user journey

A developer or reviewer can verify the first MVP by reading `docs/mvp/first-mvp-demo.md` and running the realtime MVP contract tests.

The accepted realtime journey is:

1. Build an initialized in-memory API state.
2. Start a realtime MVP runner over a live-style market-data stream.
3. Process all available stream events during the configured runtime or idle window, not a fixed one-record limit.
4. Write Barter ingestion envelopes through the orchestrator storage path into `QueryableMarketDataStore`.
5. Query written records through `GET /market-data/trades` or `query_market_data_trades`.
6. Optionally run the ignored Binance Spot live smoke with `FDC_BARTER_LIVE_SMOKE=1` to validate real exchange connectivity and real data.

The earlier fixture/no-listener demo remains available through `build_demo_router` and `run_demo_flow_once` as a deterministic demo helper, but it is now supporting evidence rather than the complete MVP definition.

## Included capabilities

The accepted MVP includes these capabilities:

- Barter-owned market-data event and ingestion-envelope models.
- Binance Spot public trade acquisition boundary in `fdc-barter`.
- Realtime MVP runner `run_realtime_barter_envelope_stream` that consumes a stream for a runtime/idle window and does not stop after exactly one record.
- Generic source envelope, validation, batch, and finite pipeline helpers.
- Neutral market-data DTO and storage write boundaries.
- Orchestration glue from Barter envelopes to queryable storage records.
- Server assembly boundary with deterministic lifecycle state.
- API app state, readiness projection, market-data query route, runner status route, and runner control route.
- Unified in-memory demo router via `build_demo_router`.
- No-listener demo flow helper via `run_demo_flow_once`.
- Human-facing demo guide at `docs/mvp/first-mvp-demo.md`.

## Verification evidence

Run these commands from the repository root:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected results at acceptance time:

```text
realtime_mvp_contract: 2 passed
acquisition_api_mvp_contract: 3 passed, 1 ignored
demo_documentation_contract: 2 passed
demo_flow_contract: 5 passed
demo_router_contract: 4 passed
fdc-api + fdc-server: all default tests pass, live smoke ignored by default
```

The ignored live test is not part of default verification because live network acquisition is not part of default verification. It is the manual real-data validation path:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture
```

## Explicit non-goals frozen for this MVP

The first internal realtime MVP deliberately excludes these capabilities:

- no-listener: default verification does not bind an HTTP port.
- in-memory only: storage is created per demo flow/test flow.
- no persistence: no durable database, file sink, or recovery contract is included.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and long-running lifecycle management are out of scope.
- live network acquisition is not part of default verification, but optional real-data smoke remains ignored and environment-gated.
- Performance claims are out of scope. Current verification proves behavior and boundaries, not latency or throughput.

## Key artifacts

- Demo guide: `docs/mvp/first-mvp-demo.md`
- Acceptance report: `docs/mvp/first-mvp-acceptance-report.md`
- Realtime runner: `fdc_server::run_realtime_barter_envelope_stream`
- Realtime config: `fdc_server::RealtimeMarketDataMvpConfig`
- Demo router: `fdc_api::build_demo_router`
- Demo flow helper: `fdc_api::run_demo_flow_once`
- Default fixture helper: `fdc_api::default_demo_flow_request`
- Development status: `docs/DEVELOPMENT_STATUS.md`

## Residual risks and caveats

- Default verification uses deterministic live-style streams, so it proves realtime runner semantics without requiring public internet.
- Optional live smoke uses real Binance Spot public trades, so it can fail because of network, exchange, or rate-limit conditions outside this repository.
- The queryable store is in-memory and per-run, so it does not prove persistence or recovery.
- API tests use in-memory Axum/Tower requests, so they do not prove socket binding, deployment, TLS, or middleware configuration.
- Existing warning cleanup remains a separate stabilization task.
- The workspace currently has a `Cargo.lock` policy caveat documented in `docs/DEVELOPMENT_STATUS.md`.

## Recommended next slices

Choose the next track based on audience:

1. For external interactive demos: add a gated local HTTP demo entrypoint that runs the realtime runner and reuses `build_demo_router` without changing route behavior.
2. For productionization: add runner lifecycle controls, cancellation, health metrics, and durable storage.
3. For stabilization: clean warnings, decide Cargo.lock policy, audit dependency boundaries, and polish docs.
4. For product capability expansion: design SQL query integration or multi-exchange acquisition as separate milestones.
