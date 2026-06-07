# P27 Storage Runtime Observability Hardening Design

Date: 2026-06-07

## Goal

Harden storage runtime observability by exposing safe, read-only metadata for the server-owned maintenance audit log through the existing audit read route.

P24 exposed recent successful maintenance audit entries. P25 made retention capacity configurable. P26 added an explicit reset hook. P27 adds operational context around those actions so callers can tell whether an empty audit response means "nothing has happened" or "entries were reset/evicted".

## Scope

P27 extends the existing read-only route:

```http
GET /market-data/storage/maintenance/audit?limit=10
```

with server-owned observability metadata:

- configured audit capacity
- current stored audit entry count
- total successful maintenance entries recorded since process start
- total reset operations since process start
- total entries cleared by resets since process start
- last maintenance audit entry recorded timestamp
- last reset timestamp

## Non-goals

- No new route.
- No new mutating endpoint.
- No persistent metrics store.
- No Prometheus/exporter integration.
- No storage backend calls from the audit read route.
- No maintenance, compaction, TTL deletion, retention deletion, or demotion from read-only routes.
- No market-data/server/admin semantics added to `fdc-storage`.

## Options considered

### Option A: Add a new metrics route

Add a route such as `GET /market-data/storage/maintenance/audit/metrics`.

Pros:

- Separates entry listing from metadata.

Cons:

- Adds route surface area.
- Duplicates most state already needed by the existing audit route.
- P27 can stay smaller by enhancing the existing response.

### Option B: Extend existing audit response

Add metadata fields to `MarketDataStorageMaintenanceAuditResponse` and populate them from the audit log snapshot.

Pros:

- Smallest useful API change.
- Keeps observability read-only.
- Lets clients interpret empty entry lists and capacity clamping.
- Reuses existing route tests and helpers.

Cons:

- Changes the existing response shape by adding fields.

### Option C: Add metrics to storage health response

Add audit metadata to `GET /market-data/storage/health`.

Pros:

- Consolidates runtime status in one route.

Cons:

- Audit state is distinct from storage tier health.
- Would mix maintenance audit semantics into the health response.

## Decision

Use Option B: extend the existing audit response.

## Audit log data model

Extend `MarketDataStorageMaintenanceAuditLog` with server-owned metadata counters.

Proposed internal shape:

```rust
struct MarketDataStorageMaintenanceAuditState {
    entries: VecDeque<StorageMaintenanceAuditEntry>,
    total_recorded_entries: u64,
    reset_count: u64,
    total_cleared_entries: u64,
    last_recorded_at: Option<DateTime<Utc>>,
    last_reset_at: Option<DateTime<Utc>>,
}
```

`MarketDataStorageMaintenanceAuditLog` keeps `capacity` and a single `Mutex<MarketDataStorageMaintenanceAuditState>` so entry and metadata updates are atomic relative to each other.

## Snapshot data model

Extend `MarketDataStorageMaintenanceAuditSnapshot`:

```rust
pub struct MarketDataStorageMaintenanceAuditSnapshot {
    pub capacity: usize,
    pub total_entries: usize,
    pub total_recorded_entries: u64,
    pub reset_count: u64,
    pub total_cleared_entries: u64,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub last_reset_at: Option<DateTime<Utc>>,
    pub entries: Vec<StorageMaintenanceAuditEntry>,
}
```

## Response model

Extend `MarketDataStorageMaintenanceAuditResponse` with safe metadata:

```rust
pub capacity: usize,
pub total_recorded_entries: u64,
pub reset_count: u64,
pub total_cleared_entries: u64,
pub last_recorded_at: Option<String>,
pub last_reset_at: Option<String>,
```

Existing fields remain:

```rust
pub total_entries: usize,
pub returned_entries: usize,
pub entries: Vec<...>,
```

Timestamps are RFC3339 strings when present.

## Behavior

### Recording maintenance audit entries

On successful maintenance audit recording:

- push entry into the bounded log
- evict oldest entries if capacity is exceeded
- increment `total_recorded_entries`
- set `last_recorded_at` to the entry's `recorded_at`

Eviction does not decrement `total_recorded_entries`; it is a process-lifetime counter.

### Resetting audit entries

On reset:

- clear the current stored entries
- increment `reset_count`
- add cleared count to `total_cleared_entries`
- set `last_reset_at` to current server time

Resetting an empty log still increments `reset_count` and sets `last_reset_at`, because a reset operation occurred.

### Reading audit entries

`GET /market-data/storage/maintenance/audit` remains read-only:

- no storage calls
- no maintenance calls
- no mutation
- returns metadata plus recent entries

`limit=0` continues returning no entries while preserving metadata.

## Safety and boundaries

All new state is server-owned. `fdc-storage` remains unchanged and generic.

The response exposes only counts and timestamps. It does not expose:

- storage paths
- record keys
- payloads
- symbols or market-data DTO details
- backend-specific internals

## Testing

TDD coverage:

1. Audit log unit tests:
   - default snapshot has capacity, zero counters, and no timestamps
   - recording entries increments `total_recorded_entries` and sets `last_recorded_at`
   - capacity eviction keeps `total_recorded_entries` as process-lifetime count
   - reset increments `reset_count`, adds `total_cleared_entries`, sets `last_reset_at`, and clears entries
   - empty reset increments `reset_count` with `total_cleared_entries` unchanged
2. Route contract tests:
   - empty audit route returns metadata defaults
   - successful maintenance run exposes `total_recorded_entries = 1` and `last_recorded_at`
   - reset route followed by audit GET exposes `reset_count = 1`, `total_cleared_entries`, `last_reset_at`, and `total_entries = 0`
   - `limit=0` returns no entries but still returns metadata
3. Regression tests:
   - P23 maintenance run-once route tests
   - P24/P25 audit route tests
   - P26 reset route tests
   - storage health route tests
   - `fdc-storage` dependency guard
   - package-scoped fmt

## Success criteria

- Existing audit GET returns safe process-lifetime observability metadata.
- Empty audit responses can distinguish never-recorded from reset-cleared logs.
- Metadata updates are covered by unit and route tests.
- Read-only routes remain read-only.
- No `fdc-storage` changes are required.
