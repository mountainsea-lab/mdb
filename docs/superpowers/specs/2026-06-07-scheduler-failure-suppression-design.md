# P33 Scheduler Failure Handling and Suppression Design

Date: 2026-06-07
Branch: `mdb-mqdev`

## Goal

Add robust failure handling and suppression to the server-owned market-data storage maintenance scheduler.

P33 completes the safety loop started in P31/P32: scheduled maintenance remains default-disabled and server-owned, but when enabled it must be observable, must not panic the server on failure, and must stop automatic attempts after a configured consecutive failure threshold.

## Current state

P32 added:

- `StorageMaintenanceSchedulerState` with counters and timestamps.
- Gated background scheduler execution for tiered storage only.
- Read-only scheduler status route backed by scheduler state:
  - `GET /market-data/storage/maintenance/scheduler/status`
- Basic failure accounting through `mark_failed()`.
- Non-overlap accounting.
- Audit recording for successful scheduled runs.

Current gap:

- `max_consecutive_failures` is parsed and exposed but does not yet suppress automatic attempts.
- Failure test coverage is limited.
- Error sanitization exists but is not contract-tested.

## Non-goals

- Do not add new mutating admin routes.
- Do not add scheduler reset/resume endpoints.
- Do not reset audit logs.
- Do not change manual run-once semantics.
- Do not add scheduler/runtime/admin concepts to `fdc-storage`.
- Do not introduce market-data DTOs into storage.

## Safety rules

- Default runtime remains scheduler-disabled.
- Manual run-once gate remains separate from scheduler gate.
- Scheduler status route remains read-only and never triggers work.
- Failed scheduled attempts must not panic the server.
- Failure errors exposed through status must be sanitized and bounded.
- Once `consecutive_failures >= max_consecutive_failures`, automatic attempts must stop until a future explicit recovery design is implemented.
- Suppression must be visible through status while keeping `enabled=true`, because config remains enabled even though attempts are suppressed.

## Recommended approach

Use state-level suppression with a scheduler-owned decision point.

Add a suppression flag derived from existing state rather than adding new config:

- `suppressed = consecutive_failures >= max_consecutive_failures`
- When suppressed:
  - `running=false`
  - `last_status="suppressed_after_failures"`
  - `next_run_at=null`
  - scheduler loop stops making attempts
  - no new audit entries are recorded

Expose suppression through existing status fields, primarily `last_status`, `consecutive_failures`, `failed_runs`, and `next_run_at`. Avoid adding new DTO fields unless tests show ambiguity; P31/P32 status shape is already sufficient.

## Scheduler state changes

Add helpers on `StorageMaintenanceSchedulerState`:

- `is_suppressed().await -> bool`
- `mark_suppressed().await`
- Update `mark_failed()` so when the incremented `consecutive_failures` reaches `max_consecutive_failures`, it sets:
  - `last_status="suppressed_after_failures"`
  - `next_run_at=None`
  - `running=false`

Keep `failed_runs` and `total_runs` accurate. The failed attempt that reaches the threshold still counts as a failed run.

## Scheduler loop behavior

Before scheduling a next tick and before running an attempt, the scheduler must check suppression.

Proposed loop behavior:

1. At top of loop, if suppressed:
   - call `mark_suppressed()` defensively
   - break out of the background loop
2. Otherwise, set `next_run_at` and sleep.
3. After sleep, call `run_scheduler_attempt()`.
4. After the attempt, if suppressed, break; otherwise continue with the normal interval.

This stops automatic attempts after the threshold and avoids continually waking a suppressed task.

## Failure test seam

Use a server-owned test seam rather than modifying `fdc-storage`.

Recommended seam:

- Add an optional scheduler maintenance executor abstraction inside `fdc-server`, or a small `run_scheduler_attempt_with_executor()` helper in `market_data::maintenance_scheduler` used only by tests.
- The helper should accept an async closure or trait that returns one of:
  - success
  - unsupported
  - failure string
- Production `run_scheduler_attempt()` continues to call `QueryableMarketDataStore::run_maintenance_once_with_options()` directly or delegates through the production executor.

The seam must stay in `fdc-server` and must not add hooks to `fdc-storage`.

## Error sanitization

Status `last_error` should be bounded and safe:

- Maximum length: 240 characters plus optional ellipsis.
- Newlines and control characters should be replaced with spaces.
- Full filesystem paths should not be intentionally added by scheduler code.
- Storage errors may still include generic details; status should expose only the sanitized string.

## Testing strategy

Use TDD.

Add RED tests before implementation:

1. State-level failure threshold:
   - with max consecutive failures set to 2
   - two failures increment `failed_runs` and `consecutive_failures`
   - second failure sets `last_status="suppressed_after_failures"`
   - `next_run_at` becomes null
2. Scheduler attempt suppression:
   - a fake failing executor reaches threshold
   - subsequent attempts do not call the executor
   - counters do not increase after suppression
3. Error sanitization:
   - long multiline error becomes bounded single-line status text
4. Route-level status visibility:
   - a suppressed scheduler snapshot serializes correctly through `GET /market-data/storage/maintenance/scheduler/status`
5. Regression:
   - enabled tiered success path still records audit
   - manual run-once tests still pass
   - dependency guard still passes

If route-level forced failure requires too much production plumbing, keep the failure forcing at the scheduler module unit-test level and route-test only the status serialization from scheduler state.

## Status semantics

Suppressed status example:

```json
{
  "enabled": true,
  "running": false,
  "backend": "tiered",
  "tiered": true,
  "max_consecutive_failures": 2,
  "consecutive_failures": 2,
  "total_runs": 2,
  "successful_runs": 0,
  "failed_runs": 2,
  "skipped_runs": 0,
  "last_status": "suppressed_after_failures",
  "last_error": "storage maintenance failed: simulated failure",
  "next_run_at": null
}
```

No new route is needed for P33.

## Validation commands

Focused verification should include:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

## Boundary review

- Failure/suppression logic remains in `fdc-server`.
- `fdc-storage` remains generic and unchanged except for dependency-guard verification.
- Scheduler status remains a server DTO.
- No barter, ingestion, API, or market-data semantics are introduced into storage.

## Rollout and compatibility

- Existing default deployments remain unchanged because scheduler is still default-disabled.
- Enabled schedulers become safer: repeated failures stop automatic attempts instead of retrying forever.
- Operators can observe suppression through the existing scheduler status route.
- Future work can add an explicit admin recovery/reset route, but P33 does not add one.

## Spec self-review

- No placeholders remain.
- P33 scope is focused on failure accounting, sanitized error exposure, and suppression.
- No new mutating route is introduced.
- The design preserves the `fdc-storage` boundary.
- Validation commands are explicit.
