# B7 Server Assembly Boundary Design

Date: 2026-05-26
Branch: `mdb-mqdev`

## Intent

Define the first real `fdc-server` application assembly boundary without starting a network service or mixing lifecycle code with cross-layer mapping logic.

B7 creates a lightweight, testable server assembly shell that can hold application-level components such as storage sinks and orchestrator handles. It prepares the project for later API, runner, and production storage integration while keeping B7 deterministic and file-free.

## Context

Current state:

- `fdc-server` is still a template crate with only an `add` function.
- `fdc-orchestrator` owns cross-layer market-data glue from Barter envelopes to storage write records.
- `fdc-storage` exposes `StorageWriteSink` and `RecordingStorageSink` for bounded, database-free writes.
- `fdc-api` contains an `ApiServer`, but its `/query` and `/insert` routes are still simulated and should not be integrated in B7.

Important existing boundaries:

- `fdc-server` may depend on application components and compose them.
- `fdc-orchestrator` owns adapter/ingestion/transform/storage mapping internals.
- `fdc-api` should remain protocol/handler facing and should not own orchestration glue.
- Core crates must not depend on `fdc-server`.
- B7 must not introduce real HTTP serving, live stream loops, checkpoint persistence, or real database writes.

## Approved Direction

Use approved option 1: lightweight server assembly shell.

B7 will replace the template `fdc-server` library with focused application assembly types:

```text
crates/fdc-server/src/
  lib.rs          # public exports and crate docs
  config.rs       # FdcServerConfig and simple defaults
  components.rs   # ServerComponents and component readiness helpers
  app.rs          # FdcServerApp assembly object and lifecycle state
```

The server assembly should be usable in tests without binding sockets, spawning background tasks, writing files, or connecting databases.

## Responsibilities

### `FdcServerConfig`

`FdcServerConfig` is the first server-level configuration type. It should remain small and avoid duplicating the full `fdc-api::ApiConfig`.

Recommended fields:

- `service_name: String`
- `environment: ServerEnvironment`
- `enable_api: bool`
- `enable_market_data_orchestrator: bool`

`ServerEnvironment` should be a small enum:

- `Development`
- `Test`
- `Production`

Default values:

- service name: `fdc-server`
- environment: `Development`
- API disabled in B7
- market-data orchestrator enabled in B7

### `ServerComponents`

`ServerComponents` owns runtime handles that the application assembly needs.

B7 should include:

- `market_data_storage_sink: Arc<dyn StorageWriteSink>`

Default construction should use `RecordingStorageSink` so B7 stays deterministic and DB-free.

The type should expose a readiness method such as `is_ready()` returning true when required handles are present.

This keeps the component boundary generic: later slices can add query engine, API server state, analytics, checkpoint store, production storage sink, or multiple adapter runner handles without changing the core ownership rule.

### `FdcServerApp`

`FdcServerApp` is the application assembly object. It should own:

- `config: FdcServerConfig`
- `components: ServerComponents`
- `state: ServerLifecycleState`

`ServerLifecycleState` should model assembly state only:

- `Created`
- `Initialized`
- `Stopped`

B7 should provide:

- `FdcServerApp::new(config, components)`
- `FdcServerApp::with_defaults()`
- `initialize()` changing `Created` to `Initialized`
- `stop()` changing current state to `Stopped`
- accessors for config, components, and state
- `is_ready()` returning true only when initialized and components are ready

`initialize()` must not bind sockets, start loops, or write to storage. It validates configuration and component readiness only.

## Orchestrator Relationship

B7 does not need to call `run_barter_envelopes_to_storage_once`. It should depend on `fdc-orchestrator` only enough to prove `fdc-server` is allowed to consume the orchestration crate as an application assembly dependency.

Recommended approach:

- Add `fdc-orchestrator` as a dependency of `fdc-server`.
- Add a small type alias or marker in `components.rs`, such as `MarketDataOrchestratorResult = fdc_orchestrator::pipeline::OrchestratorPipelineResult`, only if needed by tests.
- Do not move mapping functions into `fdc-server`.
- Do not add Barter-specific modules to `fdc-server`.

This preserves the user's requirement that `fdc-orchestrator` remains the expandable home for future adapter data sources while B7 keeps server assembly generic.

## Dependency Direction

Allowed B7 dependency:

```text
fdc-server -> fdc-core
fdc-server -> fdc-storage
fdc-server -> fdc-orchestrator
```

Optional later dependency, not B7:

```text
fdc-server -> fdc-api
```

Forbidden:

```text
fdc-core       -> fdc-server
fdc-storage    -> fdc-server
fdc-orchestrator -> fdc-server
fdc-api        -> fdc-server
```

B7 tests should include dependency guards proving no reverse references to `fdc-server` appear in lower-level crates.

## Scope

In scope:

1. Replace the template `fdc-server` library with server assembly modules.
2. Add `FdcServerConfig`, `ServerEnvironment`, `ServerComponents`, `FdcServerApp`, and `ServerLifecycleState`.
3. Use `RecordingStorageSink` as the default market-data storage sink.
4. Add contract tests for default assembly, readiness, lifecycle transitions, custom component injection, and dependency direction.
5. Update development status with B7 completion and next recommended slice.

Out of scope:

- No `main.rs` binary.
- No socket binding.
- No `fdc-api::ApiServer` construction.
- No `/query` or `/insert` handler integration.
- No live Barter runner.
- No checkpoint persistence.
- No production storage sink or tier routing.
- No real database writes.
- No shutdown signal handling beyond simple lifecycle state transitions.

## Testing Strategy

Add contract tests under:

```text
crates/fdc-server/tests/server_assembly_contract.rs
```

Required tests:

1. `FdcServerApp::with_defaults()` creates a development app with API disabled and market-data orchestration enabled.
2. Default components are ready and include a recording storage sink.
3. `initialize()` transitions the app from `Created` to `Initialized` without starting network services.
4. `stop()` transitions the app to `Stopped`.
5. Custom `Arc<dyn StorageWriteSink>` can be injected into `ServerComponents`.
6. `fdc-server` can reference an orchestrator public type while lower-level crates do not reference `fdc-server`.

Verification commands:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-orchestrator -p fdc-storage
```

## Acceptance Criteria

- `fdc-server` is no longer a template add-function crate.
- `fdc-server` exposes a small server assembly API with config, components, lifecycle state, and readiness checks.
- Default assembly uses `RecordingStorageSink` and performs no real I/O.
- `fdc-server` depends on `fdc-orchestrator` and `fdc-storage` only as an application assembly consumer.
- Orchestrator mapping logic remains in `fdc-orchestrator`, not in `fdc-server`.
- Contract tests cover lifecycle, readiness, custom component injection, and dependency direction.
- Development status docs record B7 completion and recommend the next slice.

## Future Work After B7

Recommended follow-up slices:

1. B8: API state boundary, where `fdc-api` receives application state assembled by `fdc-server` without owning orchestrator glue.
2. B9: checkpoint persistence boundary.
3. B10: finite/backfill runner with retry and observability.
4. Later: real `main.rs`, shutdown signals, live stream lifecycle, and production tier-aware storage runtime routing.
