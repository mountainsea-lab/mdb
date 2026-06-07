# P21 Runtime Storage Observability Design

Date: 2026-06-07

## Goal

Expose a safe runtime storage configuration summary so operators and tests can confirm which market-data storage backend, policy profile, and tier engines the server assembled before live ingestion.

## Non-goals

- No live engine health probing in this slice.
- No storage metrics, compaction, lifecycle, or retention scheduler wiring.
- No full path disclosure by default.
- No market-data DTO knowledge inside `fdc-storage`.
- No storage crate dependency on server/runtime config.

## Current state

P19 added durable tier path config and P20 verified persistence reopen. Operators can configure paths, but there is no production-facing summary that confirms how runtime interpreted those settings.

Current server routes include:

- `/health`
- `/ready`
- `/market-data/live/status`
- `/market-data/trades`

The readiness response already reports basic market-data store availability, but it does not describe the configured storage backend, policy, tier engines, or durable path presence.

## Options considered

### Option A: Extend `/ready`

Add storage config details to the readiness response.

Pros:

- Existing endpoint.
- Easy for deploy probes to fetch.

Cons:

- Readiness should stay small and machine-stable.
- Storage details can grow independently of readiness semantics.

### Option B: Add `/market-data/storage/status`

Add a market-data runtime storage status endpoint returning a dedicated summary model.

Pros:

- Clear ownership under market-data server routes.
- Does not overload readiness.
- Easy to add more storage runtime observability later.

Cons:

- Adds one route.

### Option C: Only expose internal summary API, no route

Add Rust API for tests/operators embedding the server, but no HTTP endpoint.

Pros:

- Smallest implementation.

Cons:

- Less useful operationally.
- Does not help HTTP smoke/ops flows.

## Decision

Use Option B with an internal model/helper.

Add a safe `MarketDataStorageStatusResponse` model and expose it through `GET /market-data/storage/status`. The route will be backed by server runtime config only. It will not inspect storage internals or require `fdc-storage` to know about server concepts.

## Response shape

The response will be wrapped by the existing server response envelope:

```json
{
  "status": "success",
  "message": null,
  "data": {
    "backend": "tiered",
    "policy_profile": "generic_realtime",
    "tiers": [
      {
        "tier": "L1",
        "engine": "memory",
        "durable_path_configured": false,
        "path_hint": null
      },
      {
        "tier": "L2",
        "engine": "redb",
        "durable_path_configured": true,
        "path_hint": "l2.redb"
      },
      {
        "tier": "L3",
        "engine": "duckdb",
        "durable_path_configured": true,
        "path_hint": "l3.duckdb"
      },
      {
        "tier": "L4",
        "engine": "rocksdb",
        "durable_path_configured": true,
        "path_hint": "l4-rocksdb"
      }
    ]
  }
}
```

For the default memory backend:

- `backend = "memory"`
- `policy_profile = "compatibility"`
- All tiers report `engine = "memory"`
- All tiers report `durable_path_configured = false`
- `path_hint = null`

## Path safety

Do not disclose full paths by default. The summary returns only a basename-style `path_hint`, derived from `Path::file_name()`. If a configured path has no file name, return `"configured"` rather than the full path.

Examples:

- `/var/lib/fdc/l2.redb` -> `l2.redb`
- `/var/lib/fdc/l4-rocksdb` -> `l4-rocksdb`
- `/` -> `configured`

## Tier engine mapping

The summary should mirror P19 assembly semantics:

- L1 is always `memory`.
- If backend is `memory`, all tiers report `memory` and no durable paths.
- If backend is `tiered`:
  - L2 is `redb` only when `l2_redb_path` is configured, otherwise `memory`.
  - L3 is `duckdb` only when `l3_duckdb_path` is configured, otherwise `memory`.
  - L4 is `rocksdb` only when `l4_rocksdb_path` is configured, otherwise `memory`.

This is a server/runtime summary of configuration intent, not a live engine introspection result.

## Boundaries

- The model and route live in `fdc-server`.
- The summary reads `ProductionServerState::config().market_data_storage`.
- `fdc-storage` remains generic and unchanged unless a later slice adds generic engine introspection.
- No market-data DTOs are involved.

## Testing

TDD steps:

1. Add model/helper tests for memory default and tiered durable summaries.
2. Add route contract test for `GET /market-data/storage/status`.
3. Verify the route does not leak full configured paths.
4. Implement the model/helper and route.
5. Run targeted server and storage boundary tests.

## Success criteria

- `GET /market-data/storage/status` returns backend, policy profile, and four tier summaries.
- Default runtime summary reports safe memory defaults.
- Durable tiered runtime summary reports L2 redb, L3 duckdb, L4 rocksdb with `durable_path_configured = true` and basename-only `path_hint`.
- Full configured paths are not present in response JSON.
- `fdc-storage` remains dependency-clean and generic.
