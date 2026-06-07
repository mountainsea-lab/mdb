# P25 Storage Maintenance Audit Hardening Design

Date: 2026-06-07

## Goal

Harden P24 maintenance audit visibility by making audit capacity configurable and adding route-level tests for limit clamping and newest-first ordering after multiple successful maintenance runs.

## Scope decision

P24 recommended either an optional reset/test hook or configurable audit capacity plus route hardening tests. P25 will implement configurable capacity and route hardening only.

A reset hook is intentionally deferred because it is a write/admin action. The existing maintenance execution route is already explicitly gated; adding another mutating route should be a separate slice with its own safety policy if needed.

## Non-goals

- No audit reset route.
- No persistent audit store.
- No auth/admin model changes.
- No background maintenance scheduler.
- No storage paths, record keys, payloads, or market-data DTO facts in audit responses.
- No server/runtime dependency inside `fdc-storage`.

## Existing behavior

P24 added:

- `MarketDataStorageMaintenanceAuditLog` with fixed capacity 32.
- `GET /market-data/storage/maintenance/audit?limit=10`.
- Successful P23 maintenance runs record audit entries through generic `StorageMaintenanceAuditSink`.
- Audit responses return newest entries first from the audit log implementation.

P25 makes the capacity an explicit server runtime config value and verifies route behavior under multiple entries.

## Options considered

### Option A: Add audit reset/test hook

Expose a route such as `POST /market-data/storage/maintenance/audit/reset`.

Pros:

- Useful for tests and local admin cleanup.

Cons:

- Mutating route with operational semantics.
- Needs an explicit safety policy and likely a runtime gate.
- Less valuable than making retention behavior configurable and tested.

### Option B: Configurable audit capacity plus route hardening tests

Add runtime config for process-local audit capacity. Use it when constructing `ProductionServerState`. Add route contract tests for newest-first behavior, limit clamping, and capacity eviction.

Pros:

- Safe default remains unchanged.
- No new write route.
- Makes bounded memory behavior explicit and testable.
- Strengthens P24 without changing `fdc-storage`.

Cons:

- Does not provide runtime reset.

### Option C: Persist audit entries

Store audit history durably.

Pros:

- Survives restarts.

Cons:

- Requires schema/storage/retention policy.
- Too large for this incremental hardening slice.

## Decision

Use Option B.

## Runtime config

Add to `ServerRuntimeConfig`:

```rust
pub market_data_storage_maintenance_audit_capacity: usize
```

Environment variable:

```text
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY=32
```

Rules:

- Default: 32.
- Must be greater than zero.
- Values greater than 1024 are rejected to protect process memory from accidental unbounded growth.

The capacity is server-owned and only affects the server in-memory audit log.

## Production state integration

Replace current default audit-log construction with configured construction:

```rust
MarketDataStorageMaintenanceAuditLog::new(config.market_data_storage_maintenance_audit_capacity)
```

This applies to `ProductionServerState::new`, `try_new`, and `with_market_data_store`.

## Audit route hardening

Keep the existing route:

```http
GET /market-data/storage/maintenance/audit?limit=10
```

Route behavior:

- Default limit remains 10.
- `limit=0` returns zero entries.
- Requested limit is clamped to the configured log capacity by `MarketDataStorageMaintenanceAuditLog::recent(limit)`.
- Entries are returned newest first.
- `returned_entries` means the number of entries returned in this response, not the total stored entries.

## Testing

TDD steps:

1. Runtime config tests:
   - default audit capacity is 32
   - env override is accepted
   - zero capacity is rejected
   - capacity above 1024 is rejected
2. Audit log tests:
   - `recent(0)` returns zero entries
   - capacity continues to evict oldest entries
3. Production route contract tests:
   - configured capacity 2, three successful maintenance runs, `limit=10` returns only two entries
   - returned entries are newest-first, verified through increasing `scanned_entries` by inserting one record before each maintenance run
   - `limit=1` returns only the newest entry
4. Existing P24/P23 route tests and dependency guard still pass.

## Success criteria

- Audit capacity is configurable through server runtime env.
- Default capacity remains 32.
- Invalid capacities are rejected during config parsing.
- Production state uses the configured capacity.
- Audit route returns newest-first entries and clamps by stored capacity.
- `limit=0` is well-defined and returns no entries.
- `fdc-storage` remains generic and unchanged.
