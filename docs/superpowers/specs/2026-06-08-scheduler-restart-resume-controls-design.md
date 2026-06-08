# P35 Scheduler Restart/Resume Controls Design

Date: 2026-06-08
Branch: `mdb-mqdev`

## Goal

Add a safe, explicit, server-owned restart/resume control for the market-data storage maintenance scheduler.

P35 lets an operator recover the in-process scheduler loop after P33 suppression and P34 reset semantics, without directly running storage maintenance and without adding a broad start/stop lifecycle surface.

## Current state

P31 through P34 implemented these scheduler capabilities:

- Runtime scheduler config and read-only status route.
- Default-disabled background scheduler loop for tiered storage only.
- Scheduler state counters, timestamps, `next_run_at`, and sanitized failure status.
- Failure suppression after `max_consecutive_failures`.
- Explicit P34 reset route that clears retry/suppression accounting only.

Current gap:

- The scheduler loop exits when the state becomes suppressed.
- P34 reset clears visible suppression state but intentionally does not spawn or restart a loop.
- In the same process, operators need a controlled way to resume scheduled attempts after the root cause is fixed.

## Decision

Implement **Option B: explicit gated restart/resume control**.

This is the best fit for a data acquisition platform because it recovers automatic maintenance after failure suppression while avoiding the risk and complexity of a full scheduler lifecycle API.

Rejected alternatives:

- **Resume only:** too weak if the background loop has already exited after suppression.
- **Full lifecycle start/stop/restart:** too large for this slice and more prone to operator mistakes or concurrency hazards.

## Runtime config

Add one server-owned runtime gate:

| Env var | Type | Default | Meaning |
| --- | --- | --- | --- |
| `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED` | bool | `false` | Enables the explicit scheduler resume control only. |

The gate is separate from:

- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED`, the manual run-once gate.
- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED`, the automatic scheduler config gate.
- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED`, the P34 reset gate.

Enabling resume must not enable manual run-once, audit reset, or storage maintenance by itself.

## Route

Add one mutating admin route:

```text
POST /market-data/storage/maintenance/scheduler/resume
```

Request body:

```json
{
  "confirm": "resume_scheduler",
  "reason": "operator fixed the root cause and wants scheduled maintenance to resume"
}
```

Response DTO should be server-owned in `fdc-server::market_data::model`:

```rust
pub struct MarketDataStorageMaintenanceSchedulerResumeRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

pub struct MarketDataStorageMaintenanceSchedulerResumeResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub task_started: bool,
}
```

Recommended response statuses:

- `resumed`: accepted and a scheduler loop was spawned.
- `disabled`: resume gate is disabled.
- `confirmation_required`: confirmation string is wrong.
- `scheduler_disabled`: scheduler config gate is disabled.
- `unsupported_backend`: backend is not tiered.
- `already_running`: scheduler loop is already active.
- `running`: a maintenance attempt is currently running, so resume would race with active accounting.

## State and lifecycle semantics

P35 should add lifecycle tracking around the scheduler task because the existing `ProductionServerState` stores the startup task handle in an immutable private field.

Recommended server-owned structure:

- Store scheduler task ownership in an `Arc<Mutex<Option<JoinHandle<()>>>>` or equivalent lifecycle handle.
- Keep this lifecycle handle in `fdc-server`; do not add scheduler/admin semantics to `fdc-storage`.
- At startup, use the same spawn rules as P32:
  - scheduler config enabled
  - tiered backend
  - production state construction succeeds
- When the loop exits after suppression, it should clear or mark the lifecycle handle so a later resume can spawn a new loop.

Successful resume should:

1. Require the resume gate to be enabled.
2. Require exact confirmation string `resume_scheduler`.
3. Require `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=true`.
4. Require tiered backend.
5. Reject if a scheduler attempt is currently `running=true`.
6. Reject if the lifecycle handle says a loop is already active.
7. Clear retry/suppression state using the same accounting semantics as P34 reset.
8. Set `last_status="resumed"`.
9. Recompute and publish `next_run_at` for the new loop's first scheduled attempt.
10. Spawn a new scheduler loop with the existing config, store, audit log, and scheduler state.

A successful resume must not immediately execute maintenance. It only schedules the next attempt according to the existing loop delay rules.

## Safety rules

- Resume route is default-disabled.
- Resume route is POST-only.
- Resume route requires exact confirmation.
- Resume route does not call `run_maintenance_once_with_options` directly.
- Resume route does not clear audit entries.
- Resume route does not reset storage data, tiers, compaction state, lifecycle deletion state, or persistence paths.
- Resume route does not enable the scheduler config gate at runtime.
- Resume route does not support memory backend.
- Scheduler status GET remains read-only and never triggers resume, reset, or maintenance.
- Manual run-once, audit reset, scheduler reset, and scheduler resume gates remain separate.

## Error handling

Use the same route envelope and HTTP-status pattern as run-once and P34 reset.

- Success: HTTP 200, envelope `status="success"`, response `accepted=true`, `status="resumed"`, `task_started=true`.
- Gate disabled: HTTP 403, response `status="disabled"`.
- Wrong confirmation: HTTP 400, response `status="confirmation_required"`.
- Scheduler config disabled: HTTP 403, response `status="scheduler_disabled"`.
- Unsupported backend: HTTP 400, response `status="unsupported_backend"`.
- Existing active loop: HTTP 409, response `status="already_running"`.
- Active maintenance attempt: HTTP 409, response `status="running"`.

All error responses should leave scheduler state unchanged except for safe observability fields if a lifecycle probe determines that a stale completed handle should be cleared.

## Testing strategy

Use TDD in the implementation plan.

Add RED tests before implementation for:

1. Runtime config:
   - default resume gate is false.
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=1` enables the gate.
2. State/lifecycle:
   - resume clears suppression counters and marks `last_status="resumed"`.
   - resume rejects while a maintenance attempt is running.
   - resume rejects when an active loop already exists.
   - resume can spawn a new loop after a suppressed loop exits.
3. Route behavior:
   - disabled gate returns 403.
   - wrong confirmation returns 400.
   - scheduler config disabled returns 403.
   - memory backend returns unsupported.
   - successful resume returns 200 and follow-up status shows resumed state and `next_run_at`.
4. Regression:
   - status GET remains read-only.
   - P34 reset route remains accounting-only and does not spawn a loop.
   - manual run-once and audit reset still use their own gates.
   - `fdc-storage` dependency guard still passes.

## Validation commands

Focused verification should include:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

## Documentation updates

When implemented, update `docs/DEVELOPMENT_STATUS.md` with:

- P35 completed behavior.
- Design and plan paths.
- Commit list.
- RED/GREEN verification evidence.
- Recommended next slice.

Recommended next slice after P35 is live collection hardening, unless operators explicitly request additional scheduler lifecycle controls beyond resume.
