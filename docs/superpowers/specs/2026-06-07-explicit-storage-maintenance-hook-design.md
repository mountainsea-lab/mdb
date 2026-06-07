# P23 Explicit Storage Maintenance Admin/Test Hook Design

Date: 2026-06-07

## Goal

Expose a strictly opt-in server-owned hook for running one storage maintenance pass against market-data tiered storage, while preserving the generic `fdc-storage` boundary and preventing accidental destructive or costly lifecycle effects.

## Why this needs stronger safeguards

`TieredStorageStore::run_maintenance_once_with_options(...)` is not a harmless status check. It can run lifecycle and compaction work, including:

- TTL deletion
- retention demotion
- retention deletion
- tier compaction

P22 intentionally exposed only read-only health. P23 can expose maintenance only if accidental execution is difficult.

## Non-goals

- No background scheduler.
- No automatic maintenance on startup.
- No unauthenticated production-grade admin security model.
- No maintenance endpoint enabled by default.
- No market-data DTO dependency inside `fdc-storage`.
- No server/runtime dependency inside `fdc-storage`.

## Options considered

### Option A: Always expose `POST /market-data/storage/maintenance/run-once`

The route exists and runs when called.

Pros:

- Minimal implementation.

Cons:

- Too easy to trigger lifecycle deletion or compaction accidentally.
- Violates the roadmap requirement that destructive/costly maintenance stays opt-in.

### Option B: Runtime-gated route with request confirmation

Add a server runtime flag that defaults to false. The route exists, but returns a disabled error unless the flag is enabled. Even when enabled, the request body must include an exact confirmation token.

Pros:

- Safe default.
- Explicit per-request intent.
- Easy to test locally and in admin/test deployments.
- Keeps storage generic.

Cons:

- Not a complete auth/admin model.

### Option C: Test-only compile-gated route

Expose maintenance only under `#[cfg(test)]` or a test feature.

Pros:

- Very safe in production builds.

Cons:

- Does not help operators/admin deployments.
- Less useful for runtime smoke testing real durable stores.

## Decision

Use Option B.

P23 will add a default-disabled admin/test hook:

```http
POST /market-data/storage/maintenance/run-once
```

It runs only when all conditions are true:

1. `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED` is truthy (`1`, `true`, `yes`, or `on`).
2. Request body includes `confirm = "run_maintenance_once"`.
3. The configured/constructed market-data store is tiered.

## Runtime config

Add to `ServerRuntimeConfig`:

```rust
pub market_data_storage_maintenance_enabled: bool
```

Environment variable:

```text
FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1
```

Default: `false`.

This is intentionally server-owned. It does not enter `fdc-storage`.

## Storage facade

Add a generic facade method in `fdc-storage` on `QueryableMarketDataStore`:

```rust
pub async fn run_maintenance_once_with_options(
    &self,
    options: StorageMaintenanceOptions,
) -> Result<Option<StorageMaintenanceReport>>
```

Semantics:

- In-memory backend: `Ok(None)` because there is no tiered maintenance pass.
- Tiered backend: delegate to `TieredStorageStore::run_maintenance_once_with_options(options).await` and return `Ok(Some(report))`.

This keeps server from accessing private backend internals.

## Request model

Add server-owned request DTO:

```rust
pub struct MarketDataStorageMaintenanceRunRequest {
    pub confirm: String,
    pub timeout_ms: Option<u64>,
    pub reason: Option<String>,
}
```

Rules:

- `confirm` must be exactly `run_maintenance_once`.
- `timeout_ms`, when present, must be greater than zero.
- `timeout_ms` maps to generic `StorageMaintenanceOptions::with_timeout(Duration::from_millis(timeout_ms))`.
- `reason` is echoed in the response for audit context, but P23 does not persist server-side audit history.

## Response model

Add server-owned response DTO:

```rust
pub struct MarketDataStorageMaintenanceRunResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub duration_ms: Option<i64>,
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub compacted_tiers: usize,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
    pub healthy_tiers: usize,
    pub degraded_tiers: usize,
}
```

Status values:

- `disabled`: runtime flag is false.
- `confirmation_required`: request confirmation token is missing or wrong.
- `unsupported_backend`: constructed store is not tiered.
- `completed`: maintenance ran successfully.
- `failed`: storage returned an error.

HTTP status mapping:

- `200 OK`: completed.
- `400 Bad Request`: missing/wrong confirmation or invalid timeout.
- `403 Forbidden`: runtime maintenance flag disabled.
- `409 Conflict`: unsupported backend.
- `500 Internal Server Error`: storage maintenance failed.

The existing `ServerApiResponse` envelope remains:

- success envelope for `completed`
- error envelope for all other statuses

## Error handling

- Disabled route does not call storage.
- Wrong confirmation does not call storage.
- Memory backend does not call storage maintenance and returns `unsupported_backend`.
- Timeout errors from storage return `failed` with HTTP 500 in P23. A future slice can map storage error kinds more precisely if needed.
- The response never exposes physical storage paths.

## Testing

TDD steps:

1. Runtime config contract:
   - default maintenance flag is false
   - truthy env enables it
2. Storage facade tests:
   - in-memory store returns `None`
   - tiered store returns a maintenance report
3. Server route tests:
   - default-disabled route returns 403 and does not run maintenance
   - enabled route with wrong confirmation returns 400
   - enabled memory backend with correct confirmation returns 409
   - enabled tiered backend with correct confirmation returns 200 completed and report counters
4. Existing route/status/health regressions still pass.
5. `fdc-storage --test dependency_guard` still passes.

## Success criteria

- Maintenance hook is unavailable by default.
- The hook requires explicit request confirmation even when enabled.
- The hook runs only for tiered stores.
- Server maps timeout into generic storage maintenance options.
- Maintenance report fields are returned through server-owned DTOs.
- No maintenance action is triggered by GET status/health routes.
- `fdc-storage` remains generic and dependency-clean.
