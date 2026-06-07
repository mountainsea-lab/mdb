# P24 Maintenance Audit Visibility Design

Date: 2026-06-07

## Goal

Add server-owned visibility into explicit market-data storage maintenance runs. P23 introduced a gated `POST /market-data/storage/maintenance/run-once` hook. P24 records safe audit summaries for successful maintenance runs and exposes recent entries through a read-only route.

## Non-goals

- No persistent audit database.
- No cross-process audit sharing.
- No auth/admin model beyond the existing P23 maintenance execution gate.
- No storage paths or sensitive runtime config in audit responses.
- No market-data DTO dependency inside `fdc-storage`.
- No server/runtime dependency inside `fdc-storage`.
- No background maintenance scheduler.

## Existing primitive

`fdc-storage` already has a generic audit hook:

```rust
pub trait StorageMaintenanceAuditSink: Send + Sync {
    async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()>;
}
```

`StorageMaintenanceOptions` accepts an optional `Arc<dyn StorageMaintenanceAuditSink>`. `TieredStorageStore::run_maintenance_once_with_options(...)` records an audit entry after a successful maintenance report.

## Options considered

### Option A: Store audit entries only in logs

Emit structured logs from the server after maintenance completes.

Pros:

- Minimal state.

Cons:

- Tests cannot easily inspect recent audit entries.
- Operators need log access instead of an API surface.
- Does not exercise the generic storage audit sink path.

### Option B: Server-owned bounded in-memory audit log

Add a bounded in-memory audit log to `ProductionServerState`. Implement `StorageMaintenanceAuditSink` in `fdc-server` and pass it through `StorageMaintenanceOptions` when P23 runs maintenance. Expose a read-only route for recent entries.

Pros:

- Exercises existing generic storage audit sink.
- Safe and bounded.
- Easy to test with production router contracts.
- Keeps `fdc-storage` generic.

Cons:

- Entries are process-local and lost on restart.

### Option C: Persistent audit sink

Write audit entries to durable storage or a database.

Pros:

- Survives restarts.

Cons:

- Larger design involving retention, storage location, schema, and operational policy.
- Not necessary for the next incremental roadmap slice.

## Decision

Use Option B.

P24 will add a process-local, bounded audit log to the server runtime state and a read-only audit route.

## Server audit log

Create `crates/fdc-server/src/market_data/maintenance_audit.rs` with:

- `MarketDataStorageMaintenanceAuditLog`
- `MarketDataStorageMaintenanceAuditSnapshot`

Behavior:

- Holds recent generic `StorageMaintenanceAuditEntry` values.
- Uses a fixed capacity of 32 entries.
- Drops oldest entries when capacity is exceeded.
- Is safe to clone/share through `Arc`.
- Implements `StorageMaintenanceAuditSink`.

This module is server-owned. It contains no market-data DTO coupling and no storage path handling.

## Production state integration

Extend `ProductionServerState` with:

```rust
market_data_storage_maintenance_audit: Arc<MarketDataStorageMaintenanceAuditLog>
```

Add accessor:

```rust
pub fn market_data_storage_maintenance_audit(&self) -> Arc<MarketDataStorageMaintenanceAuditLog>
```

All constructors initialize a fresh audit log.

## Maintenance run integration

In `run_storage_maintenance_once(...)`, when building `StorageMaintenanceOptions`, attach the state audit sink:

```rust
let audit_sink = state.market_data_storage_maintenance_audit();
options = options.with_audit_sink(audit_sink);
```

Only successful storage maintenance reports produce audit entries because that is how the generic storage primitive works today. Disabled, confirmation-required, unsupported-backend, and failed-before-report requests do not create audit entries.

## Read-only route

Add:

```http
GET /market-data/storage/maintenance/audit?limit=10
```

Response envelope remains `ServerApiResponse<T>`.

Response DTOs:

```rust
pub struct MarketDataStorageMaintenanceAuditResponse {
    pub returned_entries: usize,
    pub entries: Vec<MarketDataStorageMaintenanceAuditEntryResponse>,
}

pub struct MarketDataStorageMaintenanceAuditEntryResponse {
    pub recorded_at: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: i64,
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

Route semantics:

- Default limit: 10.
- Maximum limit: 32.
- Returns newest entries first.
- Empty log returns success with `returned_entries = 0` and `entries = []`.
- Does not require P23 maintenance execution gate because it is read-only and exposes only safe counters/timestamps.

## Safety and boundary

- Audit response does not include physical paths, request reason, raw error messages, record keys, payloads, or market-data DTO fields.
- `fdc-storage` remains generic and unchanged unless small test-only imports are needed.
- Server owns routing, retention capacity, response models, and visibility policy.

## Testing

TDD steps:

1. Add unit tests for the server audit log:
   - records and returns newest-first entries
   - capacity drops oldest entries
2. Add route contract tests:
   - empty audit route returns success with zero entries
   - after a successful gated tiered maintenance run, audit route returns one entry matching maintenance counters
   - repeated runs respect limit and newest-first order
3. Verify existing P23 maintenance route tests still pass.
4. Verify P22 health/status regressions and storage dependency guard still pass.

## Success criteria

- Successful explicit maintenance runs record an audit entry through `StorageMaintenanceOptions.audit_sink`.
- `GET /market-data/storage/maintenance/audit` returns recent safe audit entries.
- Audit history is bounded to 32 entries.
- Audit route does not trigger maintenance.
- Disabled/wrong-confirmation/memory-backend maintenance requests do not add audit entries.
- `fdc-storage` remains generic and dependency-clean.
