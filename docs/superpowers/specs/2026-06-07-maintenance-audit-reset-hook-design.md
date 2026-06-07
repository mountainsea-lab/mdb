# P26 Maintenance Audit Admin Reset Hook Design

Date: 2026-06-07

## Goal

Add an explicit, gated admin/test hook to reset the server-owned in-memory market-data storage maintenance audit log.

This is a follow-up to P24/P25. P24 introduced read-only audit visibility. P25 made audit capacity configurable and hardened route behavior. P26 adds a controlled way to clear the process-local audit log for tests and operational cleanup.

## Scope

P26 implements one mutating route:

```http
POST /market-data/storage/maintenance/audit/reset
```

The route clears only `fdc-server`'s in-memory `MarketDataStorageMaintenanceAuditLog` entries.

## Non-goals

- No persistent audit store.
- No reset of storage records, tier metadata, runtime config, or maintenance state.
- No maintenance execution, compaction, TTL deletion, retention deletion, or demotion.
- No reset route inside `fdc-storage`.
- No market-data/server/admin semantics added to `fdc-storage`.
- No auth model changes beyond an explicit runtime gate and request confirmation.

## Options considered

### Option A: Test-only helper method, no HTTP route

Add a Rust-only helper to clear audit log in tests.

Pros:

- Lowest operational risk.
- No new route.

Cons:

- Does not help local admin/operator workflows.
- Does not exercise production router behavior.

### Option B: Gated explicit HTTP reset route

Add a runtime-disabled-by-default route with request confirmation.

Pros:

- Matches P23 explicit maintenance safety pattern.
- Useful for contract tests and controlled local/admin workflows.
- Keeps the mutating action visible and testable through the production API.

Cons:

- Adds a new mutating route and therefore needs a safety policy.

### Option C: Reuse maintenance enabled gate

Allow reset whenever `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1`.

Pros:

- Fewer env vars.

Cons:

- Couples audit reset to maintenance execution permission.
- Less explicit than a separate reset policy.

## Decision

Use Option B with a separate runtime gate.

## Runtime config

Add to `ServerRuntimeConfig`:

```rust
pub market_data_storage_maintenance_audit_reset_enabled: bool
```

Environment variable:

```text
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=1
```

Rules:

- Default: disabled.
- Truthy values: `1`, `true`, `yes`, `on`.
- This gate is independent from `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED`.

## Request and response models

Add server-owned DTOs:

```rust
pub struct MarketDataStorageMaintenanceAuditResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

pub struct MarketDataStorageMaintenanceAuditResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub cleared_entries: usize,
    pub remaining_entries: usize,
}
```

Required confirmation:

```text
reset_maintenance_audit
```

The response does not expose storage keys, paths, payloads, or market-data facts.

## Route behavior

Route:

```http
POST /market-data/storage/maintenance/audit/reset
```

Status behavior:

- `403 disabled` when `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED` is false.
- `400 confirmation_required` when `confirm != "reset_maintenance_audit"`.
- `200 reset` when enabled and confirmed.

Reset is idempotent:

- Resetting an empty audit log succeeds with `cleared_entries = 0` and `remaining_entries = 0`.
- Resetting a populated audit log returns the number of entries removed.

## Audit log changes

Add a server-owned method:

```rust
pub async fn clear(&self) -> usize
```

It locks the internal audit log, records `entries.len()`, clears the entries, and returns the cleared count.

No changes are required in `fdc-storage` because reset is a server-owned operation over server-owned audit memory.

## Error handling

The route is intentionally simple:

- No backend checks: memory and tiered storage both have a server audit log.
- No storage calls: reset cannot fail due to storage backend errors.
- Malformed JSON uses axum's normal rejection behavior.

## Testing

TDD coverage:

1. Runtime config:
   - default reset gate is disabled
   - env override enables reset gate
2. Audit log:
   - `clear()` returns removed count and leaves `recent(...)` empty
   - clearing an empty log returns zero
3. Route contracts:
   - disabled route returns `403 disabled` and does not clear entries
   - wrong confirmation returns `400 confirmation_required` and does not clear entries
   - enabled + confirmed route returns `200 reset`, clears entries, and subsequent `GET /audit` is empty
   - enabled + confirmed route on empty log returns `cleared_entries = 0`
4. Regression:
   - existing run-once, audit GET, health, and dependency-guard tests still pass
   - package-scoped fmt still passes

## Success criteria

- Audit reset is impossible by default.
- Audit reset requires both runtime env gate and exact request confirmation.
- Reset clears only server-owned in-memory audit entries.
- Reset does not run maintenance or call storage.
- `fdc-storage` remains generic and unchanged.
