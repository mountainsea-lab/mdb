# P30 Maintenance Scheduler Design

Date: 2026-06-07
Branch: `mdb-mqdev`

## Goal

Design a default-disabled periodic storage maintenance scheduler for the market-data server runtime, with enough detail to decide exactly when and how to implement it in later slices.

This P30 slice is design-only. It does not implement background tasks, runtime config parsing, new routes, or automatic maintenance execution.

## Current state

Existing storage maintenance behavior is explicit and operator-triggered:

- `POST /market-data/storage/maintenance/run-once`
  - default disabled by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=false`
  - requires exact confirmation string `run_maintenance_once`
  - delegates through server-owned market-data facade to generic storage maintenance
- `GET /market-data/storage/maintenance/audit`
  - read-only in-memory audit visibility
- `GET /market-data/storage/status`
  - read-only runtime/config/admin gate summary
- `GET /market-data/storage/health`
  - read-only runtime tier health and durable path readiness

There is no scheduler/background maintenance loop in `fdc-server` today.

## Non-negotiable safety rules

- Automatic maintenance must be default disabled.
- Automatic maintenance must use a separate scheduler gate, not the existing manual run-once gate.
- Enabling manual run-once must not enable scheduler behavior.
- Scheduler status routes must be read-only and must not trigger maintenance.
- Scheduler must never reset audit logs.
- Scheduler must not bypass existing maintenance timeout/audit/reporting behavior.
- Scheduler must not run on memory backend unless explicitly designed later; initial implementation should only support tiered backend.
- `fdc-storage` must remain generic. Scheduler/runtime/admin semantics stay in `fdc-server`.

## Proposed runtime config

Add these fields in a future implementation slice:

| Env var | Type | Default | Validation | Meaning |
| --- | --- | --- | --- | --- |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED` | bool | false | bool parser | Enables background scheduler only. |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS` | u64 | 3600 | `60..=86400` | Period between scheduled attempts. |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS` | u64 | 30000 | `1000..=600000` | Per-run timeout passed into maintenance options. |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS` | u64 | 0 | `0..=min(interval/2, 3600)` | Optional startup/interval jitter to avoid synchronized runs. |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES` | u32 | 3 | `1..=100` | Scheduler remains observable after failures; implementation may suppress further automatic attempts after threshold. |

Important separation:

- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED` remains the manual run-once route gate.
- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED` controls only background scheduler.

## Scheduler state model

Future server-owned scheduler state should be an `Arc`-shared structure behind a mutex or atomics. Suggested fields:

```rust
pub struct MarketDataStorageMaintenanceSchedulerState {
    pub enabled: bool,
    pub running: bool,
    pub interval_seconds: u64,
    pub timeout_ms: u64,
    pub jitter_seconds: u64,
    pub max_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub total_runs: u64,
    pub successful_runs: u64,
    pub failed_runs: u64,
    pub skipped_runs: u64,
    pub last_started_at: Option<DateTime<Utc>>,
    pub last_finished_at: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub next_run_at: Option<DateTime<Utc>>,
}
```

The state is server-owned and should not be added to `fdc-storage`.

## Read-only status route

Future implementation should add one read-only route:

```text
GET /market-data/storage/maintenance/scheduler/status
```

Response fields should mirror the scheduler state and include the selected backend:

```json
{
  "enabled": false,
  "running": false,
  "backend": "tiered",
  "tiered": true,
  "interval_seconds": 3600,
  "timeout_ms": 30000,
  "jitter_seconds": 0,
  "max_consecutive_failures": 3,
  "consecutive_failures": 0,
  "total_runs": 0,
  "successful_runs": 0,
  "failed_runs": 0,
  "skipped_runs": 0,
  "last_started_at": null,
  "last_finished_at": null,
  "last_status": null,
  "last_error": null,
  "next_run_at": null
}
```

This route must not start, stop, or trigger maintenance.

## Execution behavior for future implementation

Initial implementation should follow these rules:

1. Scheduler task is spawned only when:
   - scheduler config is enabled
   - backend is tiered
   - production state construction succeeds
2. On each tick:
   - if another run is already active, increment `skipped_runs` and do not overlap
   - call existing server-owned maintenance facade with `StorageMaintenanceOptions`
   - pass the configured timeout
   - pass an audit sink so normal audit route observes scheduler runs
   - use reason string `scheduled_maintenance`
3. On success:
   - increment `total_runs` and `successful_runs`
   - reset `consecutive_failures`
   - record `last_status="completed"`
4. On failure:
   - increment `total_runs`, `failed_runs`, and `consecutive_failures`
   - record sanitized `last_error`
   - do not panic the server
5. If `consecutive_failures >= max_consecutive_failures`:
   - status remains enabled but automatic attempts may be suppressed
   - `last_status` should become `suppressed_after_failures`
   - status route must expose this clearly

## Implementation slicing decision

P30 is design-only. Implementation should start when all of these are true:

1. P30 design has been committed and recorded in `docs/DEVELOPMENT_STATUS.md`.
2. P29 durable path readiness is complete and green, so operators can verify tier paths before automatic maintenance.
3. The next requested work is explicitly to implement scheduler behavior, not only to continue design.
4. A new implementation plan is written and committed before code changes.

Recommended implementation slices:

### P31 Scheduler Config and Status Surface

Implement only config parsing, scheduler state DTO, and read-only status route. Do not spawn a background task yet.

Acceptance criteria:

- Runtime config tests cover defaults and validation.
- Status route reports disabled defaults.
- Status route reports configured enabled values.
- Status route is read-only and does not run maintenance.
- `fdc-storage` dependency guard passes.

### P32 Scheduler Task Skeleton, Disabled by Default

Add background task wiring that only activates when scheduler gate is enabled. Include non-overlap protection and state counters.

Acceptance criteria:

- Default runtime does not spawn or run maintenance.
- Enabled tiered runtime runs scheduled maintenance in a controlled test with short interval.
- Memory backend is skipped/reported unsupported.
- No overlapping runs occur.
- Audit route records scheduled runs.

### P33 Scheduler Failure Handling and Suppression

Add failure accounting, suppression after max consecutive failures, sanitized error exposure, and regression tests.

Acceptance criteria:

- Consecutive failures increment.
- Suppression threshold stops further automatic attempts.
- Status route exposes suppression state.
- Server does not panic on scheduler failure.

## Options considered

### Option A: Design only now, implement in P31-P33

Pros:

- Keeps automatic mutating behavior out of this slice.
- Allows safety review before background maintenance exists.
- Produces clear implementation gates and acceptance criteria.

Cons:

- Does not immediately add scheduler runtime behavior.

### Option B: Implement config/status and scheduler loop in one slice

Pros:

- Faster path to runtime automation.

Cons:

- Larger blast radius.
- More difficult to verify safely.
- Higher chance of accidental background destructive behavior.

### Option C: Add only a cron-style external recommendation

Pros:

- No server background task complexity.

Cons:

- Operators still need external scheduling.
- No in-process status or failure counters.
- Harder to integrate with audit/health state.

## Decision

Choose Option A. P30 only designs the scheduler and defines when to implement it. P31 should be the first implementation slice and should stop at config plus read-only status surface.

## Verification for P30

Because P30 is design-only, verification is document-level:

- The design explicitly separates manual maintenance gate from scheduler gate.
- The design includes default-disabled behavior.
- The design includes read-only scheduler status semantics.
- The design defines implementation trigger criteria.
- The design defines P31/P32/P33 acceptance criteria.
- No code changes are made.

## Boundary review

- `fdc-storage` remains unchanged.
- Scheduler concepts are server-owned.
- Maintenance execution still goes through existing generic storage options and audit sink.
- No market-data DTO dependency is introduced into storage.

## Spec self-review

- No placeholders remain.
- Implementation timing is explicit.
- Future implementation is decomposed into P31/P32/P33.
- Safety rules are explicit and testable.
- The design does not authorize automatic maintenance without a future implementation plan and tests.
