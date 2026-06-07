# P22 Storage Health Runtime Surface Design

Date: 2026-06-07

## Goal

Expose a server-owned, read-only storage health surface for market-data storage so operators can see whether the configured tiered store has initialized tiers and basic tier stats, without triggering destructive or costly maintenance actions.

## Non-goals

- No automatic maintenance scheduler.
- No compaction, lifecycle deletion, TTL deletion, or retention demotion from the HTTP route.
- No admin write endpoint in this slice.
- No full durable path disclosure.
- No server/runtime dependency inside `fdc-storage`.
- No market-data DTO knowledge inside `fdc-storage`.

## Current state

`fdc-storage` already has generic health and maintenance primitives:

- `TieredStorageStore::storage_health_snapshot().await`
- `TieredStorageStore::run_maintenance_once().await`
- `StorageHealthSnapshot`
- `StorageTierHealth`
- `StorageMaintenanceReport`

P21 added `GET /market-data/storage/status`, which reports configuration intent. It does not inspect the initialized store. P22 adds a read-only runtime health surface that uses existing storage-owned health snapshots when the market-data store is tiered.

## Options considered

### Option A: Add health to `/market-data/storage/status`

Extend the P21 status response with health fields.

Pros:

- Reuses the existing endpoint.

Cons:

- Mixes configuration intent with runtime health.
- Harder to keep response stable and cheap.

### Option B: Add read-only `/market-data/storage/health`

Expose a separate endpoint that returns runtime health. For memory backend, return a simple healthy response with no tiers. For tiered backend, adapt `StorageHealthSnapshot` into server response models.

Pros:

- Clear separation from config status.
- Read-only and safe.
- Exercises existing generic health primitives.
- Leaves maintenance as an explicit future admin/test hook.

Cons:

- Adds one route and a small storage facade method.

### Option C: Add maintenance trigger now

Expose `POST /market-data/storage/maintenance/run-once` with timeout/audit controls.

Pros:

- Directly surfaces maintenance primitives.

Cons:

- Maintenance can compact, delete expired records, and demote retention-expired data.
- Needs auth/admin policy before production exposure.
- Larger safety surface than this roadmap slice needs.

## Decision

Use Option B.

P22 will add a read-only health endpoint. Maintenance trigger remains the next separate slice because it needs explicit opt-in semantics and likely admin protection.

## Storage boundary design

Add a generic facade method in `fdc-storage` on `QueryableMarketDataStore`:

```rust
pub async fn storage_health_snapshot(&self) -> Result<Option<StorageHealthSnapshot>>
```

Semantics:

- In-memory backend: `Ok(None)` because there are no tiers.
- Tiered backend: delegate to `TieredStorageStore::storage_health_snapshot().await` and return `Ok(Some(snapshot))`.

This keeps server from accessing private backend internals and keeps `fdc-storage` independent of server/runtime config.

## Server response shape

Add `GET /market-data/storage/health` returning `ServerApiResponse<MarketDataStorageHealthResponse>`.

Memory backend example:

```json
{
  "status": "success",
  "message": null,
  "data": {
    "backend": "memory",
    "tiered": false,
    "status": "healthy",
    "tiers": [],
    "access_patterns": 0,
    "migration_queue_len": 0
  }
}
```

Tiered backend example:

```json
{
  "status": "success",
  "message": null,
  "data": {
    "backend": "tiered",
    "tiered": true,
    "status": "healthy",
    "tiers": [
      {
        "tier": "L1",
        "enabled": true,
        "initialized": true,
        "status": "healthy",
        "key_count": 0,
        "total_size": 0
      }
    ],
    "access_patterns": 0,
    "migration_queue_len": 0
  }
}
```

Tier statuses should be lower snake-case strings:

- `healthy`
- `missing_engine`
- `stats_unavailable`

Overall status:

- `healthy` when memory backend or all tier statuses are healthy.
- `degraded` when any tier is not healthy.

## Error handling

The route should keep the existing success/error response envelope pattern:

- If storage health collection succeeds, return `status = "success"`.
- If storage health collection fails, return `status = "error"`, include a safe message, and return a response with `status = "unavailable"` and empty tiers.

This preserves an HTTP 200 style consistent with current market-data route handlers, while making the envelope status explicit.

## Testing

TDD steps:

1. Add storage facade tests in `fdc-storage`:
   - in-memory market-data store returns `None`
   - memory-tiered store returns a snapshot with 4 healthy tiers
2. Add server route contract tests:
   - memory backend returns healthy, tiered false, empty tiers
   - tiered backend returns healthy, tiered true, four initialized tiers
3. Implement the facade and route.
4. Verify storage dependency guard and server route/config tests.

## Success criteria

- `GET /market-data/storage/health` exists.
- Memory backend returns a healthy non-tiered response.
- Tiered backend returns tier health for L1-L4.
- The endpoint does not run maintenance, compaction, lifecycle deletion, TTL deletion, or retention demotion.
- `fdc-storage` remains generic and dependency-clean.
