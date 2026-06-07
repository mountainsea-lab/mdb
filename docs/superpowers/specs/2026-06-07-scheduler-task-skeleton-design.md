# P32 Scheduler Task Skeleton Design

Date: 2026-06-07
Branch: `mdb-mqdev`

## Goal

Implement the first runtime scheduler behavior slice for market-data storage maintenance while keeping automatic maintenance default-disabled and tightly observable.

P32 adds server-owned scheduler lifecycle wiring, state counters, and a controlled background task that runs only when the dedicated scheduler gate is enabled and the storage backend is tiered. It must not change default behavior.

## Current state

P31 completed:

- Scheduler runtime config parsing on `ServerRuntimeConfig`.
- Read-only scheduler status DTO and route:
  - `GET /market-data/storage/maintenance/scheduler/status`
- Status currently reports configured values plus zero runtime counters.
- No background task exists.

Existing manual maintenance remains operator-triggered:

- `POST /market-data/storage/maintenance/run-once`
  - gated by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED`
  - requires exact confirmation string `run_maintenance_once`

P32 must keep the manual gate separate from the scheduler gate.

## Non-goals

- Do not implement suppression after max consecutive failures. That remains P33.
- Do not add new mutating admin routes.
- Do not reset audit logs from the scheduler.
- Do not add scheduler concepts to `fdc-storage`.
- Do not make memory backend run maintenance automatically.
- Do not add per-write physical tier controls.

## Safety rules

- Default runtime must not spawn a scheduler task.
- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED` must not enable the scheduler.
- Scheduler task may spawn only when `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=true` and backend is tiered.
- Read-only status routes must not trigger maintenance.
- Scheduler execution must use existing generic `StorageMaintenanceOptions` with timeout and audit sink.
- Scheduler execution must be non-overlapping.
- Scheduler failures must update observable counters and never panic the server.

## Recommended approach

Use a minimal but real scheduler skeleton:

1. Add a server-owned scheduler state object shared through `ProductionServerState`.
2. On production state construction, decide whether to spawn the scheduler:
   - disabled config: create state, do not spawn
   - enabled memory backend: create state, mark unsupported/skipped, do not run maintenance
   - enabled tiered backend: spawn one background task
3. Background task waits for interval ticks and attempts maintenance.
4. Each attempt uses the existing maintenance facade/options, records audit through the existing audit sink, and updates scheduler counters.
5. Status route maps the shared scheduler state rather than returning zero literals.

This provides actual scheduler behavior while keeping the blast radius small and testable.

## Scheduler state

Add a server-owned state type, likely in a new module such as `market_data/maintenance_scheduler.rs`.

Suggested model:

```rust
pub struct MarketDataStorageMaintenanceSchedulerState {
    pub enabled: bool,
    pub running: bool,
    pub backend: String,
    pub tiered: bool,
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

Implementation can use an `Arc<tokio::sync::Mutex<...>>` for simplicity. The scheduler is low-frequency and the route is read-only, so a mutex is acceptable.

## Runtime integration

`ProductionServerState` should own:

- the scheduler shared state
- an optional scheduler task handle or guard

Construction behavior:

- `ProductionServerState::new(config)` keeps tests simple and may create a state with no scheduler task.
- `ProductionServerState::try_new(config).await` should initialize runtime-backed components and can spawn the scheduler when enabled and tiered.

If existing constructors make this split awkward, prefer a small internal builder/helper rather than duplicating scheduler initialization logic.

## Scheduler execution flow

For enabled tiered runtime:

1. Initialize scheduler state:
   - `enabled=true`
   - `running=false`
   - counters zero
   - `next_run_at` set to the first scheduled time
2. Apply startup jitter if configured.
3. On each tick:
   - if a run is already active, increment `skipped_runs`, set `last_status="skipped_overlap"`, and return
   - set `running=true`, increment `total_runs`, set `last_started_at`
   - run maintenance with:
     - configured timeout
     - existing audit sink
     - reason string `scheduled_maintenance`
   - on success:
     - increment `successful_runs`
     - reset `consecutive_failures`
     - set `last_status="completed"`
   - on unsupported/no-op result:
     - increment `skipped_runs`
     - set `last_status="unsupported_backend"`
   - on failure:
     - increment `failed_runs`
     - increment `consecutive_failures`
     - set sanitized `last_error`
     - set `last_status="failed"`
   - set `running=false`, `last_finished_at`, and next scheduled time

P32 should update failure counters but should not suppress future attempts. Suppression is P33.

## Memory backend behavior

If scheduler is enabled with memory backend:

- Do not spawn a maintenance loop.
- Status should show:
  - `enabled=true`
  - `running=false`
  - `backend="memory"`
  - `tiered=false`
  - `skipped_runs=1` or `last_status="unsupported_backend"`
- No maintenance or audit entry should be recorded.

This makes misconfiguration visible without attempting unsupported work.

## Status route behavior

`GET /market-data/storage/maintenance/scheduler/status` remains read-only. It should now return a snapshot of the shared scheduler state rather than static zero counters.

The route must still include P31 config-derived fields and selected backend metadata.

## Testing strategy

Use TDD.

Add RED tests before implementation:

1. Default disabled runtime:
   - scheduler status reports `enabled=false`, `running=false`, zero counters, no next run
   - no audit entry is created after a short wait
2. Manual maintenance gate only:
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1` with scheduler disabled does not spawn scheduler
3. Enabled memory backend:
   - scheduler status reports unsupported/skipped
   - no audit entry is recorded
4. Enabled tiered backend with short interval:
   - scheduler runs at least once in a bounded wait
   - status increments `total_runs` and `successful_runs`
   - audit route records a scheduled maintenance entry
5. Non-overlap:
   - use a controlled or delayed maintenance path if existing test hooks permit it
   - verify overlapping ticks increment `skipped_runs` rather than running concurrently

If non-overlap is difficult to test through current public routes, add a small server-owned test seam for maintenance execution. Keep the seam inside `fdc-server`; do not add scheduler test hooks to `fdc-storage`.

## Validation commands

Focused verification should include:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

## Rollout and compatibility

- Existing default deployments remain unchanged because scheduler gate defaults to false.
- Operators must explicitly enable `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED` for automatic maintenance.
- Manual run-once behavior remains separately gated and unchanged.
- Status surface remains backward-compatible with P31 fields, with runtime counters now populated.

## Boundary review

- Scheduler state, task lifecycle, route DTOs, and admin semantics stay in `fdc-server`.
- `fdc-storage` remains generic and only receives generic maintenance options/audit sink that already exist.
- No market-data/server DTOs are introduced into storage.
- No barter/ingestion/API dependencies are added to storage.

## Spec self-review

- No placeholders remain.
- Scope is limited to P32 scheduler skeleton and explicitly excludes P33 suppression.
- Safety gates are explicit and testable.
- Default-disabled behavior is preserved.
- Storage boundary is preserved.
