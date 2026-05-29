# First MVP Acceptance Report

## Acceptance decision

First internal MVP: accepted.

This acceptance covers an internal, no-listener, in-memory market-data demo. It proves the bounded path from deterministic fixture input to API-readable market-data records through the current crate boundaries.

This is not an external production release and not an interactive HTTP demo.

## Accepted user journey

A developer or reviewer can verify the first MVP by reading `docs/mvp/first-mvp-demo.md` and running the no-listener demo flow tests.

The accepted journey is:

1. Build an initialized in-memory API state.
2. Build the B16 unified demo router with `build_demo_router`.
3. Run `POST /runner/start-fixture` with deterministic fixture trades.
4. Read final runner status through `GET /runner/status`.
5. Query written records through `GET /market-data/trades`.
6. Exercise the whole flow programmatically with `run_demo_flow_once`.

## Included capabilities

The accepted MVP includes these capabilities:

- Barter-owned market-data event and ingestion-envelope models.
- Generic source envelope, validation, batch, and finite pipeline helpers.
- Neutral market-data DTO and storage write boundaries.
- Orchestration glue from Barter fixture envelopes to queryable storage records.
- Server assembly boundary with deterministic lifecycle state.
- API app state, readiness projection, market-data query route, runner status route, and runner control route.
- Unified in-memory demo router via `build_demo_router`.
- No-listener demo flow helper via `run_demo_flow_once`.
- Human-facing demo guide at `docs/mvp/first-mvp-demo.md`.

## Verification evidence

Run these commands from the repository root:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

Expected results at acceptance time:

```text
demo_documentation_contract: 2 passed
demo_flow_contract: 5 passed
demo_router_contract: 4 passed
fdc-api: 54 passed, 1 ignored
```

The ignored test is not part of default MVP verification.

## Explicit non-goals frozen for this MVP

The first internal MVP deliberately excludes these capabilities:

- no-listener: default verification does not bind an HTTP port.
- in-memory only: storage is created per demo flow.
- no persistence: no durable database, file sink, or recovery contract is included.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and long-running lifecycle management are out of scope.
- live network acquisition is not part of default verification. Live smoke remains optional, ignored, and environment-gated.
- Performance claims are out of scope. Current verification proves behavior and boundaries, not latency or throughput.

## Key artifacts

- Demo guide: `docs/mvp/first-mvp-demo.md`
- Acceptance report: `docs/mvp/first-mvp-acceptance-report.md`
- Demo router: `fdc_api::build_demo_router`
- Demo flow helper: `fdc_api::run_demo_flow_once`
- Default fixture helper: `fdc_api::default_demo_flow_request`
- Development status: `docs/DEVELOPMENT_STATUS.md`

## Residual risks and caveats

- The MVP uses deterministic fixture data by default, so it does not prove live exchange reliability.
- The queryable store is in-memory and per-run, so it does not prove persistence or recovery.
- API tests use in-memory Axum/Tower requests, so they do not prove socket binding, deployment, TLS, or middleware configuration.
- Existing warning cleanup remains a separate stabilization task.
- The workspace currently has a `Cargo.lock` policy caveat documented in `docs/DEVELOPMENT_STATUS.md`.

## Recommended next slices

Choose the next track based on audience:

1. For external interactive demos: add a gated local HTTP demo entrypoint that reuses `build_demo_router` without changing route behavior.
2. For stabilization: clean warnings, decide Cargo.lock policy, audit dependency boundaries, and polish docs.
3. For product capability expansion: design persistence, SQL query integration, or production runner lifecycle as separate milestones.
