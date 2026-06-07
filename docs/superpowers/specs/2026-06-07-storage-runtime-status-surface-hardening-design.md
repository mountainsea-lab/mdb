# P28 Storage Runtime Status Surface Hardening Design

Date: 2026-06-07
Branch: `mdb-mqdev`

## Goal

Harden the existing read-only `GET /market-data/storage/status` route so operators can verify storage runtime/admin configuration without inspecting environment variables and without triggering any storage maintenance.

The route should continue to be server-owned. `fdc-storage` remains generic and should not receive server/runtime/admin semantics.

## Scope

Extend `MarketDataStorageStatusResponse` with safe runtime metadata sourced from `ServerRuntimeConfig`:

- `tiered`: whether the selected backend is tiered.
- `durable_tiers_configured`: count of tiers with configured durable paths.
- `maintenance_enabled`: value of `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED`.
- `maintenance_audit_reset_enabled`: value of `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED`.
- `maintenance_audit_capacity`: configured audit log capacity.

Keep the existing fields:

- `backend`
- `policy_profile`
- `tiers`

Keep existing tier behavior:

- `tiers` still contains L1-L4 summaries.
- `path_hint` still uses basename only.
- full configured paths are never exposed.

## Non-goals

- Do not add a new route.
- Do not modify `fdc-storage`.
- Do not expose full filesystem paths.
- Do not expose environment variable names or raw environment values in the HTTP response.
- Do not trigger maintenance, compaction, TTL deletion, retention deletion, retention demotion, or audit reset.
- Do not add scheduler behavior.
- Do not add persistent audit storage.

## Options considered

### Option A: Extend `GET /market-data/storage/status`

Add the metadata to the existing status route. This keeps config/runtime information in one place and avoids new route surface area.

Pros:

- Minimal API expansion.
- Clear semantics: status is configuration/runtime summary.
- Reuses existing router contracts.
- No `fdc-storage` change.

Cons:

- Adds more fields to an existing response.

### Option B: Extend `GET /market-data/storage/health`

Add the metadata to health. This keeps operator information near live runtime health.

Pros:

- Health already reports tiered runtime details.

Cons:

- Mixes health observations with config/admin gate state.
- Health should remain focused on current backend/tier health.

### Option C: Add `GET /market-data/storage/runtime`

Create a new runtime-only endpoint.

Pros:

- Clean separation.

Cons:

- More route surface for only a few fields.
- Duplicates much of `status`.
- YAGNI for the current need.

## Decision

Use Option A: extend `GET /market-data/storage/status`.

## API design

Example default memory response data:

```json
{
  "backend": "memory",
  "policy_profile": "compatibility",
  "tiered": false,
  "durable_tiers_configured": 0,
  "maintenance_enabled": false,
  "maintenance_audit_reset_enabled": false,
  "maintenance_audit_capacity": 32,
  "tiers": [
    {
      "tier": "L1",
      "engine": "memory",
      "durable_path_configured": false,
      "path_hint": null
    }
  ]
}
```

Example tiered durable response data should include:

```json
{
  "backend": "tiered",
  "policy_profile": "generic_realtime",
  "tiered": true,
  "durable_tiers_configured": 3,
  "maintenance_enabled": true,
  "maintenance_audit_reset_enabled": true,
  "maintenance_audit_capacity": 7
}
```

## Data flow

```mermaid
flowchart LR
    Env[Environment/runtime pairs] --> Config[ServerRuntimeConfig]
    Config --> State[ProductionServerState]
    State --> StatusService[market_data::service::storage_status]
    StatusService --> DTO[MarketDataStorageStatusResponse]
    DTO --> Route[GET /market-data/storage/status]
```

The service reads only `state.config().market_data_storage` and existing runtime config fields. It does not call storage engines, health snapshots, maintenance, or audit reset.

## File-level design

- `crates/fdc-server/src/market_data/model.rs`
  - Extend `MarketDataStorageStatusResponse` with the new fields.
- `crates/fdc-server/src/market_data/service.rs`
  - Compute the new fields from `ProductionServerState::config()`.
  - Count durable tier paths for L2/L3/L4.
- `crates/fdc-server/tests/production_server_router_contract.rs`
  - Extend existing status tests.
  - Add coverage for admin gates and audit capacity metadata.

## Testing strategy

Use TDD.

1. Extend `production_storage_status_reports_memory_defaults` with assertions for:
   - `tiered == false`
   - `durable_tiers_configured == 0`
   - `maintenance_enabled == false`
   - `maintenance_audit_reset_enabled == false`
   - `maintenance_audit_capacity == 32`
2. Extend `production_storage_status_reports_durable_tiered_config_without_full_paths` with assertions for:
   - `tiered == true`
   - `durable_tiers_configured == 3`
   - default admin gates remain false unless configured
   - full path still not leaked
3. Add a new route contract test with maintenance gates and custom audit capacity enabled:
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1`
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=1`
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY=7`
   - response reports booleans and capacity

Focused verification:

- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status`
- `rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance`
- `rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health`
- `rtk cargo test -p fdc-storage --test dependency_guard`
- `cargo fmt -p fdc-server -p fdc-storage -- --check`

## Boundary and safety checks

- `fdc-storage` remains unchanged.
- The dependency guard remains part of verification.
- The status route remains read-only.
- Mutating/admin behavior remains behind existing explicit gates and confirmation strings.
- The response exposes booleans and counts only, not secrets or full paths.

## Spec self-review

- No placeholder requirements remain.
- Scope is one existing route and one DTO/service mapper.
- API fields have explicit names and sources.
- The storage boundary remains intact.
- Safety behavior is explicit: status does not trigger maintenance or reset.
