# P34 Scheduler Recovery Controls Design

Date: 2026-06-07
Branch: `mdb-mqdev`

## Goal

Add a safe, explicit, server-owned recovery control for the market-data storage maintenance scheduler after P33 suppression.

P34 provides an operator-visible way to clear scheduler suppression/retry state without triggering storage maintenance, compaction, lifecycle deletion, demotion, or storage reset. It completes the first recovery-control slice while keeping actual scheduler task restart/resume behavior out of scope.

## Current state

P31 through P33 implemented:

- Scheduler runtime config and read-only status route.
- Default-disabled scheduler background task for tiered storage only.
- Server-owned scheduler counters and timestamps.
- Failure accounting and suppression after `max_consecutive_failures`.
- Sanitized bounded `last_error` reporting.
- Suppressed state exposed through existing status fields:
  - `consecutive_failures`
  - `last_status="suppressed_after_failures"`
  - `last_error`
  - `next_run_at=null`

Current gap:

- Once suppressed, there is no explicit operator control to clear the retry/suppression state.
- P33 intentionally did not add recovery or task restart semantics.

## Non-goals

- Do not trigger a maintenance run from the reset route.
- Do not restart or spawn a scheduler background task from the reset route.
- Do not add scheduler resume/autostart lifecycle controls in P34.
- Do not clear audit entries.
- Do not reset storage data, tiers, compaction state, or persistence paths.
- Do not add market-data DTOs, server/runtime/admin semantics, or dependencies to `fdc-storage`.

## Recommended approach

Implement **Option A: explicit gated reset route**.

Add a separate runtime gate:

- `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED`
- default: `false`

Add a mutating admin route:

- `POST /market-data/storage/maintenance/scheduler/reset`

Require exact confirmation string in JSON request body:

```json
{
  "confirm": "reset_scheduler_suppression",
  "reason": "operator acknowledged root cause and wants to clear retry state"
}
```

The route clears scheduler retry/suppression status only. It does not do storage work.

## Request and response DTOs

Add server-owned DTOs in `fdc-server::market_data::model`:

```rust
pub struct MarketDataStorageMaintenanceSchedulerResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

pub struct MarketDataStorageMaintenanceSchedulerResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
}
```

Recommended statuses:

- `reset` for successful reset.
- `disabled` when the reset gate is not enabled.
- `confirmation_required` when `confirm` is wrong.
- `running` if a scheduler attempt is currently running and reset would race with active accounting.

## State semantics

Add `reset_suppression()` to `StorageMaintenanceSchedulerState`.

Successful reset should:

- Require `running=false`; if `running=true`, return a server-level conflict response.
- Capture previous `consecutive_failures` for response.
- Set `consecutive_failures=0`.
- Set `last_error=None`.
- Set `last_status=Some("reset")`.
- Set `next_run_at=None`.
- Leave these counters unchanged:
  - `total_runs`
  - `successful_runs`
  - `failed_runs`
  - `skipped_runs`
  - `last_started_at`
  - `last_finished_at`
- Leave config-derived snapshot fields unchanged:
  - `enabled`
  - `backend`
  - `tiered`
  - `interval_seconds`
  - `timeout_ms`
  - `jitter_seconds`
  - `max_consecutive_failures`

This makes reset an accounting/retry-state operation, not a scheduler lifecycle operation.

## Route behavior

Use the same route envelope and HTTP-status pattern as maintenance run-once and audit reset.

Success:

- HTTP 200
- envelope `status="success"`
- response:
  - `accepted=true`
  - `status="reset"`
  - `previous_consecutive_failures=<old value>`
  - `consecutive_failures=0`

Gate disabled:

- HTTP 403
- envelope `status="error"`
- response:
  - `accepted=false`
  - `status="disabled"`

Wrong confirmation:

- HTTP 400
- envelope `status="error"`
- response:
  - `accepted=false`
  - `status="confirmation_required"`

Running conflict:

- HTTP 409
- envelope `status="error"`
- response:
  - `accepted=false`
  - `status="running"`

## Scheduler task lifecycle

P34 intentionally does not restart a scheduler loop that exited after P33 suppression.

Rationale:

- Restart/resume is a distinct lifecycle control with separate risk and concurrency handling.
- Resetting accounting state is safe and easy to verify.
- Operators can clear visible suppression state without causing storage side effects.
- A future P35 can add explicit gated restart/resume semantics if needed.

This means P34 reset clears the status fields but does not guarantee automatic future scheduled runs in the same process if the P33 loop has already exited. That limitation must be documented in status notes and development status.

## Safety rules

- Reset route is default-disabled.
- Reset route is POST-only.
- Reset route requires exact confirmation.
- Reset route does not call `run_maintenance_once_with_options`.
- Reset route does not touch audit logs.
- Reset route does not touch storage tier data or persistence paths.
- Scheduler status GET remains read-only and never triggers reset or maintenance.
- Manual run-once gate and audit-reset gate remain separate.

## Testing strategy

Use TDD.

Add RED tests before implementation:

1. Runtime config parsing:
   - default reset gate is false.
   - `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=1` sets it true.
2. Scheduler state reset:
   - suppressed state resets `consecutive_failures` and `last_error`.
   - counters remain unchanged.
   - running state rejects reset.
3. Route gate disabled:
   - POST reset returns 403 and does not mutate state.
4. Route confirmation:
   - enabled gate with wrong confirm returns 400.
5. Route success:
   - enabled gate with correct confirm resets status and response reports previous failures.
   - follow-up GET status shows `last_status="reset"`, `consecutive_failures=0`, `last_error=null`, and `next_run_at=null`.
6. Regression:
   - scheduler status GET remains read-only.
   - manual run-once and audit reset routes still use their own gates.
   - `fdc-storage` dependency guard still passes.

## Validation commands

Focused verification should include:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_reset
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

## Boundary review

- Config, route, DTO, and state reset semantics remain in `fdc-server`.
- `fdc-storage` remains generic and unchanged except for dependency-guard verification.
- No barter, ingestion, orchestrator, server, API, or market-data DTO dependencies are introduced into storage.

## Rollout and compatibility

- Existing deployments are unchanged because the new reset gate defaults to false.
- Operators who enable the reset gate must still send the exact confirmation string.
- Reset provides observability-state recovery only; task restart/resume remains a future explicit feature.

## Spec self-review

- No placeholders remain.
- The route is explicitly gated and confirmation-protected.
- The design avoids hidden maintenance and hidden scheduler restart side effects.
- The `fdc-storage` boundary remains preserved.
- P34 is small enough for one implementation plan.
