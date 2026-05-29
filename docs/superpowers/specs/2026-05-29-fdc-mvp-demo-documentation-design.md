# B18 MVP Demo Documentation Design

## Status

Approved direction: documentation-first guide for the first internally verifiable MVP. Do not add a real HTTP listener or demo binary in this slice.

## Context

B17 completed a no-listener demo flow helper in `fdc-api`:

- `default_demo_flow_request()` builds a deterministic BTCUSDT fixture request.
- `run_demo_flow_once(request)` executes the B16 unified router in memory.
- The flow performs ready -> start-fixture -> status -> market-data query and returns `DemoFlowSummary`.

This is enough for an internal MVP, but the usage path is currently only discoverable by reading tests and status notes. B18 should make the MVP understandable and reproducible for a developer or reviewer.

## Goal

Add an MVP demo guide that explains:

1. What the first MVP demonstrates.
2. What is intentionally out of scope.
3. How to run the no-listener demo verification.
4. What code API to call for programmatic demo usage.
5. What the expected request and summary shapes look like.
6. How B16 routes map to B17 `DemoFlowSummary` fields.
7. What optional live smoke validation exists, and why it is not part of the default MVP.

## Non-Goals

B18 does not add:

- HTTP listener startup.
- CLI commands or binaries.
- Real curl flows that require a running server.
- Authentication, authorization, production middleware, or CORS changes.
- Live network acquisition in default tests.
- Persistent storage or SQL integration.
- New code paths in lower-level crates.

## Recommended Approach

Create `docs/mvp/first-mvp-demo.md` as the primary guide.

The guide should be practical and concise, with these sections:

- MVP summary.
- Capability map: completed capabilities vs explicit exclusions.
- Quick verification commands.
- Programmatic no-listener usage snippet.
- Default fixture request shape.
- Expected summary highlights.
- Route mapping from B16 router to B17 summary.
- Optional live smoke note.
- Next steps after first internal MVP.

Add a contract-style documentation test in `crates/fdc-api/tests/demo_documentation_contract.rs` that ensures the guide stays anchored to real public APIs and verification commands. The test should read `docs/mvp/first-mvp-demo.md` and assert it mentions:

- `run_demo_flow_once`
- `default_demo_flow_request`
- `DemoFlowSummary`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract`
- `GET /ready`
- `POST /runner/start-fixture`
- `GET /runner/status`
- `GET /market-data/trades`
- explicit no-listener and no-persistence scope notes

This keeps B18 documentation verifiable without creating a real server.

## Component Boundaries

### `docs/mvp/first-mvp-demo.md`

Owns human-facing MVP demo explanation and usage instructions.

### `fdc-api` docs contract test

Owns verification that the guide references the public B17 API and the B16 routes. It should not test prose quality or duplicate all behavior tests.

### Runtime code

No runtime code should change in B18. B17 code and tests remain the source of truth for behavior.

## Verification Strategy

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Also run a grep-style check that the guide contains no placeholder language such as `TODO`, `TBD`, or `FIXME`.

## Acceptance Criteria

- A developer can read `docs/mvp/first-mvp-demo.md` and understand how to verify the first internal MVP without starting a server.
- The guide clearly states the MVP is no-listener, in-memory, deterministic, and default-offline.
- The guide names the exact command to run the demo flow contract test.
- The guide maps B16 routes to B17 summary fields.
- The guide explicitly excludes persistence, SQL, production daemon supervision, authentication, and default live network acquisition.
- Documentation contract tests pass.

## Future Work

After B18, choose one of:

1. Declare the first internal MVP complete and start a stabilization/cleanup pass.
2. Add a gated local HTTP demo binary/example for interactive curl/browser testing.
3. Add a higher-level MVP acceptance report that summarizes all phases B1-B18.
