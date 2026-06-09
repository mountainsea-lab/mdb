# P39 Production Runbook, Config Pack, and Soak Validation Design

## Goal

Make the current market-data production server operable for an internal MVP run without expanding production API surface. P39 turns the P36-P38 runtime capabilities into operator-ready documentation, a safe local production configuration pack, and verification guardrails that keep documentation and runtime config from drifting.

## Scope

P39 will produce:

1. A production runbook for the market-data server.
2. A safe local production `.env` example for tiered storage, live acquisition, storage maintenance, scheduler controls, live resume, and scheduler reset/resume gates.
3. Config contract tests proving the example parses and keeps dangerous gates default-disabled unless explicitly enabled.
4. Deterministic smoke/soak instructions that work offline by default.
5. Optional ignored/manual real-network smoke guidance that is explicitly opt-in and not part of default CI.
6. A `DEVELOPMENT_STATUS.md` update after verification.

P39 will not add new public production APIs, candles/OHLCV routes, query semantics, storage behavior, acquisition behavior, or scheduler behavior. Config parsing helpers used only inside tests are allowed if they keep production code unchanged.

## Current Runtime Capabilities to Document

The runbook will document the implemented routes and gates:

- Health and readiness:
  - `GET /health`
  - `GET /ready`
  - `GET /version`
- Market-data live controls:
  - `POST /market-data/live/start`
  - `POST /market-data/live/stop`
  - `GET /market-data/live/status`
  - `POST /market-data/live/resume`, gated by `FDC_MARKET_DATA_LIVE_RESUME_ENABLED=1` and confirmation `resume_live_collection`.
- Storage status and health:
  - `GET /market-data/storage/status`
  - `GET /market-data/storage/health`
- Storage maintenance:
  - `POST /market-data/storage/maintenance/run-once`, gated by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1` and confirmation `run_maintenance_once`.
  - `GET /market-data/storage/maintenance/audit?limit=...`
  - `POST /market-data/storage/maintenance/audit/reset`, gated by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=1` and confirmation `reset_maintenance_audit`.
- Maintenance scheduler:
  - `GET /market-data/storage/maintenance/scheduler/status`
  - `POST /market-data/storage/maintenance/scheduler/reset`, gated by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=1` and confirmation `reset_scheduler_suppression`.
  - `POST /market-data/storage/maintenance/scheduler/resume`, gated by `FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=1` and confirmation `resume_scheduler`.
- Query route:
  - `GET /market-data/trades`, including P38 metadata and limit behavior.

## Configuration Pack

Add `config/production.local.example.env` as a copyable local-production profile. It will prefer safe defaults:

- Bind to `127.0.0.1` by default.
- Use `FDC_SERVER_ENV=production` to exercise production mode intentionally.
- Use tiered market-data storage with explicit local tier paths under `./var/fdc-market-data`.
- Keep `FDC_LIVE_ENABLED=0` and `FDC_LIVE_AUTOSTART=0` by default, with comments showing how to enable operator-triggered live collection.
- Keep live resume, audit reset, scheduler reset, and scheduler resume gates disabled by default.
- Enable manual storage maintenance only if the runbook tells the operator the confirmation body required for a manual run.
- Keep scheduler disabled by default in the local profile unless the operator chooses an active-soak variant. The runbook will show the additional variables needed for scheduler-enabled soak.

The file must not contain secrets, credentials, exchange API keys, or machine-specific absolute paths.

## Runbook Structure

Add `docs/runbooks/market-data-production-runbook.md` with these sections:

1. Purpose and safety model.
2. Prerequisites and build command.
3. Configuration quick start using `config/production.local.example.env`.
4. Start procedure.
5. Readiness and status checks.
6. Query verification using `GET /market-data/trades`.
7. Manual maintenance run and audit inspection.
8. Optional live acquisition run.
9. Recovery flows:
   - live suppressed -> inspect live status -> decide whether to enable live resume gate -> resume with confirmation.
   - scheduler suppressed -> inspect scheduler status -> reset accounting or resume loop with confirmation.
   - storage health degraded -> inspect tier rows -> run manual maintenance if enabled -> verify query still works.
10. Deterministic smoke procedure.
11. Optional real-network smoke procedure with explicit env opt-in and external-dependency caveats.
12. Short soak procedure with checkpoints and expected observations.
13. Troubleshooting matrix.
14. Default verification commands for maintainers.

Every command should include expected output shape or success criteria. Destructive or mutating commands must call out the required gate and confirmation string.

## Config Contract Tests

Extend `crates/fdc-server/tests/runtime_config_contract.rs` with tests that read the example env file and feed it through `ServerRuntimeConfig::from_env_pairs`.

Test requirements:

- The example env file parses successfully after ignoring comments and blank lines.
- The example env sets `environment=Production` and a tiered market-data storage backend.
- The example env keeps dangerous gates disabled by default:
  - `live_enabled=false`
  - `live_autostart=false`
  - `market_data_live_resume_enabled=false`
  - `market_data_storage_maintenance_audit_reset_enabled=false`
  - `market_data_storage_maintenance_scheduler_reset_enabled=false`
  - `market_data_storage_maintenance_scheduler_resume_enabled=false`
- The example env includes valid tier paths for configured tiers.
- The parser rejects malformed example lines in a small unit helper if a helper is introduced.

These tests are intentionally config-level. They should not start the server, perform network I/O, or run storage maintenance.

## Smoke and Soak Validation

Default P39 validation remains deterministic and offline:

- Runtime config contract tests.
- Existing focused server route regressions for P38/P37/live resume/storage maintenance/scheduler resume.
- Storage dependency boundary guard.
- Formatting check.

The runbook may document optional real-network smoke, but it must remain ignored/manual and gated by an explicit environment variable. Network smoke results must be treated as operator validation, not default CI correctness.

## Error Handling and Safety

- If live collection fails repeatedly and becomes suppressed, the runbook should direct operators to inspect `GET /market-data/live/status` before using resume.
- If the maintenance scheduler is suppressed, the runbook should distinguish reset from resume:
  - reset clears scheduler suppression accounting and does not start a task.
  - resume starts a scheduler loop only when config and runtime state permit it.
- Manual maintenance and reset/resume examples must include explicit confirmation JSON bodies.
- The config pack must avoid accidental public bind or autostart live acquisition.

## Verification Plan

The implementation plan should verify at least:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract p38_
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

If `/Volumes/wdata` space is low, the implementation should report the environment limitation before rerunning storage builds. Only regenerated build cache may be cleaned.

## Non-Goals

- No new operator API routes.
- No new query route semantics.
- No candles/OHLCV route work.
- No storage-layer changes.
- No default network tests.
- No production deployment automation beyond a local example config and executable runbook.
