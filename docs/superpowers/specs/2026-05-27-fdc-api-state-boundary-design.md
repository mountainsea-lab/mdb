# B8 API State Boundary Design

## Goal

Define how `fdc-api` receives and projects application state assembled by `fdc-server`, without moving server lifecycle ownership, orchestrator mapping logic, network startup, or storage writes into the API layer.

## Scope

In scope:

- Add an API-facing state module in `fdc-api`.
- Allow `fdc-api` to hold a shared `FdcServerApp` assembled by `fdc-server`.
- Project server lifecycle/config/component readiness into serializable API readiness data.
- Provide a bounded, testable readiness handler function that uses injected state but does not bind sockets.
- Preserve dependency direction: `fdc-api` may consume `fdc-server`; lower-level crates must not depend on `fdc-api`.

Out of scope:

- Starting HTTP, gRPC, GraphQL, or WebSocket services.
- Production database/storage writes.
- Barter, ingestion, transform, or storage mapping logic in `fdc-api`.
- Replacing existing broad API modules beyond adding the state boundary.

## Architecture

`fdc-server` remains the application assembly owner. `fdc-api` becomes a protocol-facing consumer of that assembled app through a small `ApiAppState` handle.

The boundary is intentionally read-only for B8. `ApiAppState` stores an `Arc<FdcServerApp>` and exposes methods that return serializable projections. This avoids making API handlers depend on internals of server components and keeps lifecycle transitions controlled by `fdc-server`.

## Components

### `ApiAppState`

Located in `crates/fdc-api/src/state.rs`.

Responsibilities:

- Own a shared `Arc<FdcServerApp>`.
- Construct from an already assembled app.
- Expose the underlying app for tests and future bounded handlers.
- Produce `ApiReadinessProjection` snapshots.

### `ApiReadinessStatus`

A serializable enum with values:

- `Ready`
- `NotReady`

It reflects `FdcServerApp::is_ready()` only. It does not infer health from network or storage I/O.

### `ApiReadinessProjection`

A serializable DTO with:

- `status`
- `service_name`
- `environment`
- `server_lifecycle_state`
- `components_ready`
- `api_enabled`
- `market_data_orchestrator_enabled`

String fields are used for API stability and simple JSON assertions.

### `readiness_response_from_state`

A pure function in `state.rs` that returns `ApiResponse<ApiReadinessProjection>`. It lets B8 verify handler semantics without requiring Axum router wiring or socket startup.

## Dependency Rules

- Add `fdc-server` as a dependency of `fdc-api`.
- Do not add `fdc-api` references to `fdc-server`, `fdc-orchestrator`, `fdc-storage`, `fdc-transform`, `fdc-ingestion`, or adapter crates.
- Do not add orchestrator mapping logic to `fdc-api`.

## Testing

Add `crates/fdc-api/tests/api_state_boundary_contract.rs` covering:

1. Default uninitialized server projects as `NotReady`.
2. Initialized server projects as `Ready` and exposes config/lifecycle fields.
3. `readiness_response_from_state` returns a standard successful `ApiResponse` wrapping the projection.
4. Dependency guard ensures lower-level/application crates do not reference `fdc-api`.

## Completion Criteria

- B8 contract tests pass.
- `fdc-api` package tests pass.
- Formatting passes for `fdc-api`.
- `docs/DEVELOPMENT_STATUS.md` records B8 completion and the next recommended slice.
