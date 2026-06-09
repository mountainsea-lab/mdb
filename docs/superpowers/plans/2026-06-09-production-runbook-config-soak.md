# P39 Production Runbook, Config Pack, and Soak Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the market-data production server operable for an internal MVP run with a safe config pack, executable runbook, deterministic smoke/soak guidance, and config drift tests.

**Architecture:** Keep production code unchanged. Add a copyable local production env example under `config/`, validate that example from `fdc-server` runtime config contract tests, and document operator procedures under `docs/runbooks/`. Verification stays offline by default; real-network smoke remains explicit opt-in and manual.

**Tech Stack:** Rust, Cargo, `fdc-server` runtime config tests, Markdown runbooks, `.env` example files.

---

## File Structure

Create or modify only these files unless a test reveals a genuine config boundary issue:

- Create: `config/production.local.example.env`
  - Safe local production env profile that parses through `ServerRuntimeConfig::from_env_pairs`.
  - No secrets, no public bind, no live autostart, no reset/resume gates enabled by default.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add test-only parser for the example env file.
  - Add contract tests that prove the example parses and keeps dangerous gates disabled.
- Create: `docs/runbooks/market-data-production-runbook.md`
  - Operator runbook with startup, checks, smoke/soak, and recovery flows.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Update only after implementation and verification complete.

Do not modify `crates/fdc-server/src/**`, `crates/fdc-storage/**`, public route definitions, storage behavior, live acquisition behavior, or query behavior for P39.

---

### Task 1: Add safe production config pack with RED contract tests

**Files:**
- Create: `config/production.local.example.env`
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Add failing config example contract tests**

Append this code to `crates/fdc-server/tests/runtime_config_contract.rs` after the existing `rejects_empty_market_data_storage_tier_path` test:

```rust
fn production_local_example_env_pairs() -> Vec<(String, String)> {
    include_str!("../../../config/production.local.example.env")
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let (key, value) = trimmed.split_once('=').unwrap_or_else(|| {
                panic!(
                    "config/production.local.example.env line {} must be KEY=VALUE, got {trimmed:?}",
                    index + 1
                )
            });
            let key = key.trim();
            let value = value.trim();
            assert!(!key.is_empty(), "env key must not be empty on line {}", index + 1);
            assert!(
                !value.contains('#'),
                "inline comments are not supported on env line {}; put comments on their own line",
                index + 1
            );
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

#[test]
fn production_local_example_env_parses_as_safe_tiered_production_config() {
    let config = ServerRuntimeConfig::from_env_pairs(production_local_example_env_pairs())
        .expect("production local example env should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert_eq!(config.market_data_storage.backend, MarketDataStorageBackendConfig::Tiered);
    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::GenericRealtime
    );
    assert_eq!(
        config.market_data_storage.tiers.l2_redb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l2.redb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l3_duckdb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l3.duckdb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l4_rocksdb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l4-rocksdb"))
    );
}

#[test]
fn production_local_example_env_keeps_dangerous_controls_disabled() {
    let config = ServerRuntimeConfig::from_env_pairs(production_local_example_env_pairs())
        .expect("production local example env should parse");

    assert!(!config.live_enabled);
    assert!(!config.live_autostart);
    assert!(!config.market_data_live_resume_enabled);
    assert!(!config.market_data_storage_maintenance_audit_reset_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_reset_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_resume_enabled);

    assert!(config.market_data_storage_maintenance_enabled);
    assert_eq!(config.market_data_storage_maintenance_audit_capacity, 64);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
    assert!(config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 1000);
    assert_eq!(config.live_retry_max_delay_ms, 30000);
    assert_eq!(config.live_max_consecutive_failures, 3);
}
```

- [ ] **Step 2: Run the focused RED test and verify it fails**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env
```

Expected: FAIL at compile time because `../../../config/production.local.example.env` does not exist yet.

- [ ] **Step 3: Create the production local env example**

Create `config/production.local.example.env` with exactly this content:

```dotenv
# Financial Data Center local production profile for internal MVP operation.
# Copy this file to a local .env file, review every value, then export it before starting the server.
# This profile intentionally binds to localhost and keeps live acquisition and recovery gates disabled by default.

FDC_SERVER_ADDR=127.0.0.1:18080
FDC_SERVER_ENV=production

# Live acquisition stays off by default. Enable only for an operator-triggered live run.
FDC_LIVE_ENABLED=0
FDC_LIVE_AUTOSTART=0
FDC_LIVE_DEFAULT_TIMEOUT_SECS=30
FDC_LIVE_DEFAULT_MAX_ENVELOPES=100
FDC_LIVE_RETRY_ENABLED=1
FDC_LIVE_RETRY_INITIAL_DELAY_MS=1000
FDC_LIVE_RETRY_MAX_DELAY_MS=30000
FDC_LIVE_MAX_CONSECUTIVE_FAILURES=3
FDC_MARKET_DATA_LIVE_RESUME_ENABLED=0

# Tiered storage profile. Paths are relative to the process working directory.
FDC_MARKET_DATA_STORAGE_BACKEND=tiered
FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime
FDC_MARKET_DATA_STORAGE_L2_REDB_PATH=./var/fdc-market-data/l2.redb
FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH=./var/fdc-market-data/l3.duckdb
FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH=./var/fdc-market-data/l4-rocksdb

# Manual maintenance is available for explicit operator runs.
FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY=64
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=0

# Background scheduler stays off in the safe local profile.
# The runbook shows the extra values for a scheduler-enabled soak.
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS=3600
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS=30000
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES=3
```

- [ ] **Step 4: Run the focused config tests and verify they pass**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env
```

Expected: PASS with 2 tests passed and the rest filtered out.

- [ ] **Step 5: Run the full runtime config contract suite**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: PASS for all runtime config contract tests.

- [ ] **Step 6: Commit config pack and tests**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
rtk cargo fmt -p fdc-server
rtk git diff --check
rtk git add config/production.local.example.env crates/fdc-server/tests/runtime_config_contract.rs
rtk git commit -m "test(server): validate p39 production config pack"
```

---

### Task 2: Add executable production runbook

**Files:**
- Create: `docs/runbooks/market-data-production-runbook.md`

- [ ] **Step 1: Create the runbook file**

Create `docs/runbooks/market-data-production-runbook.md` with this content:

````markdown
# Market Data Production Runbook

This runbook operates the Financial Data Center market-data production server for an internal MVP run. It assumes the P39 local production config pack from `config/production.local.example.env`.

## Safety model

- Default bind address is `127.0.0.1:18080`.
- Live acquisition is disabled by default.
- Live autostart is disabled by default.
- Audit reset, scheduler reset, scheduler resume, and live resume gates are disabled by default.
- Mutating recovery actions require both an environment gate and a JSON confirmation string.
- Default validation is deterministic and offline. Real-network smoke is optional and operator-triggered.

## Prerequisites

```bash
cargo --version
rustc --version
df -h /Volumes/wdata .
```

Expected:

- Rust tooling is installed.
- The build target volume has enough free space for DuckDB/libduckdb builds. Keep at least 10 GiB free before storage verification.

## Configure local production profile

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
cp config/production.local.example.env .env.p39.local
set -a
. ./.env.p39.local
set +a
mkdir -p ./var/fdc-market-data
```

Expected:

- Environment variables are exported in the current shell.
- Tiered storage paths resolve under `./var/fdc-market-data`.
- No secrets are required.

## Build and start

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo build -p fdc-server
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo run -p fdc-server
```

Expected:

- Server binds to `127.0.0.1:18080`.
- Live collection does not autostart.
- Scheduler does not autostart in the safe local profile.

## Readiness checks

```bash
curl -sS http://127.0.0.1:18080/health
curl -sS http://127.0.0.1:18080/ready
curl -sS http://127.0.0.1:18080/version
```

Expected:

- `/health` reports a healthy status.
- `/ready` reports ready.
- `/version` returns version metadata.

## Storage status checks

```bash
curl -sS http://127.0.0.1:18080/market-data/storage/status
curl -sS http://127.0.0.1:18080/market-data/storage/health
```

Expected:

- `backend` is `tiered`.
- `tiered` is `true`.
- Durable tier path hints are present without exposing full local paths.
- Tier health is initialized or healthy after startup.

## Query check

```bash
curl -sS 'http://127.0.0.1:18080/market-data/trades?limit=10'
curl -i 'http://127.0.0.1:18080/market-data/trades?limit=1001'
```

Expected:

- Valid query returns a JSON envelope with `status="success"`, `data_kind="trade"`, `query_source="market_data_store"`, `requested_limit`, `applied_limit`, `returned_records`, and `records`.
- Invalid limit returns HTTP 400 with `status="error"` and message `limit must be between 1 and 1000`.

## Manual maintenance run

Manual maintenance is enabled in the local production profile. Run it only when you intend to scan and compact configured storage tiers.

```bash
curl -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/run-once \
  -H 'content-type: application/json' \
  -d '{"confirm":"run_maintenance_once","reason":"operator_smoke"}'

curl -sS 'http://127.0.0.1:18080/market-data/storage/maintenance/audit?limit=10'
```

Expected:

- Maintenance response has `status="success"` and `data.accepted=true`.
- Audit response includes the recorded maintenance entry.
- Query results remain readable after maintenance.

## Live acquisition operator run

The safe config keeps live acquisition disabled. To perform an operator live run, start a new shell, copy the example env, then intentionally override live controls:

```bash
set -a
. ./.env.p39.local
export FDC_LIVE_ENABLED=1
export FDC_LIVE_AUTOSTART=0
set +a
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo run -p fdc-server
```

Then in another shell:

```bash
curl -sS -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":30,"max_envelopes":100}'

curl -sS http://127.0.0.1:18080/market-data/live/status
curl -sS 'http://127.0.0.1:18080/market-data/trades?limit=10'
```

Expected:

- Live start is accepted only when `FDC_LIVE_ENABLED=1`.
- Live status shows progress, completion, failure, or suppression.
- Query route returns any collected trades through the normal P38 response envelope.

## Live recovery flow

Use this only after inspecting live status and deciding resume is safe.

```bash
curl -sS http://127.0.0.1:18080/market-data/live/status
```

If live state is suppressed and operator policy allows resume, restart with:

```bash
export FDC_MARKET_DATA_LIVE_RESUME_ENABLED=1
```

Then call:

```bash
curl -sS -X POST http://127.0.0.1:18080/market-data/live/resume \
  -H 'content-type: application/json' \
  -d '{"confirm":"resume_live_collection","reason":"operator_recovery"}'
```

Expected:

- Wrong confirmation is rejected.
- Resume is rejected when the gate is disabled.
- Resume does not clear market-data records or storage tiers.

## Scheduler-enabled soak variant

The safe config disables the scheduler. For a scheduler soak, start a new shell and override:

```bash
set -a
. ./.env.p39.local
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=1
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS=300
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS=30000
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS=30
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES=3
set +a
```

Check scheduler status:

```bash
curl -sS http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/status
```

Expected:

- Scheduler status shows enabled and tiered backend.
- `next_run_at` is present when a scheduler loop is active.
- Maintenance audit entries increase only after scheduled attempts run.

## Scheduler recovery flow

For suppressed scheduler accounting, inspect first:

```bash
curl -sS http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/status
```

To reset suppression accounting without starting a task, restart with:

```bash
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=1
```

Then call:

```bash
curl -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/reset \
  -H 'content-type: application/json' \
  -d '{"confirm":"reset_scheduler_suppression","reason":"operator_reset"}'
```

To resume the scheduler loop, restart with:

```bash
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=1
```

Then call:

```bash
curl -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/resume \
  -H 'content-type: application/json' \
  -d '{"confirm":"resume_scheduler","reason":"operator_resume"}'
```

Expected:

- Reset clears scheduler suppression accounting but does not run maintenance.
- Resume starts a scheduler loop only when config and runtime state permit it.
- Wrong confirmations and disabled gates are rejected.

## Deterministic smoke procedure

Run this before an internal MVP run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract p38_
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
```

Expected:

- All commands pass without network access.
- No default smoke starts public-network live acquisition.

## Optional real-network smoke

Only run this when explicitly validating exchange connectivity. Network, exchange, and rate-limit failures are operator environment results, not default CI failures.

```bash
FDC_BARTER_LIVE_SMOKE=1 CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server --test production_live_smoke -- --ignored --nocapture
```

Expected:

- Test is ignored unless explicitly requested.
- A successful run proves public live connectivity and live write/query path at that moment.

## Short soak procedure

1. Start with the safe local production profile.
2. Run readiness and storage status checks.
3. Run manual maintenance once and inspect audit.
4. Optionally restart with live acquisition enabled and run one bounded live collection.
5. Query trades with `limit=10` and invalid `limit=1001`.
6. Optionally restart with scheduler-enabled soak values and monitor scheduler status for at least one interval.
7. Stop the server and preserve logs plus `./var/fdc-market-data` for inspection.

Checkpoint expectations:

- Query route remains read-only.
- Maintenance does not hide or delete fresh query data in normal smoke conditions.
- Recovery gates remain closed unless explicitly enabled.

## Troubleshooting matrix

| Symptom | Check | Likely cause | Action |
|---|---|---|---|
| Server refuses live start | `GET /market-data/live/status` and env | `FDC_LIVE_ENABLED=0` | Restart with `FDC_LIVE_ENABLED=1` only for operator live run |
| Live state suppressed | `GET /market-data/live/status` | repeated collection failures | inspect sanitized error, then enable resume gate only if safe |
| Scheduler disabled | scheduler status route | safe config sets scheduler enabled to `0` | use scheduler-enabled soak overrides |
| Scheduler suppressed | scheduler status route | repeated maintenance failures | choose reset or resume flow; do not use both blindly |
| Maintenance rejected | HTTP status and message | gate disabled or wrong confirmation | verify `FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1` and confirmation body |
| DuckDB/libduckdb build fails | `df -h /Volumes/wdata` | not enough disk space | free regenerated build cache only, then rerun |
| Query invalid limit returns 400 | response body | expected P38 validation | use `limit` between 1 and 1000 |

## Maintainer verification

Before marking this runbook current, run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract p38_
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```
````

- [ ] **Step 2: Run a placeholder scan on the runbook**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
grep -nE 'TBD|TODO|placeholder|fill in|later' docs/runbooks/market-data-production-runbook.md || true
```

Expected: no output.

- [ ] **Step 3: Commit the runbook**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
rtk git diff --check
rtk git add docs/runbooks/market-data-production-runbook.md
rtk git commit -m "docs(server): add market data production runbook"
```

---

### Task 3: Add P39 status update and full verification

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run full P39 verification before updating status**

Run these commands in order, not in parallel:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract p38_
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all commands exit 0. If `fdc-storage dependency_guard` fails during DuckDB/libduckdb compilation with `No space left on device`, stop and report disk pressure. Only regenerated Cargo build cache may be cleaned before rerun.

- [ ] **Step 2: Update `docs/DEVELOPMENT_STATUS.md`**

Add this section above the P38 section. First run `rtk git log --oneline -n 10`, then paste the real short commit hashes into the `Commits` list. For each verification bullet, paste the exact observed pass count or exit status from Step 1. Do not use template markers or guessed counts.

```markdown
## 2026-06-09 P39 Production Runbook, Config Pack, and Soak Validation

Completed:

- Added a safe local production config pack at `config/production.local.example.env`.
- Added runtime config contract coverage proving the config pack parses and keeps dangerous gates disabled by default.
- Added an executable market-data production runbook covering startup, readiness, query verification, manual maintenance, live recovery, scheduler recovery, deterministic smoke, optional real-network smoke, and short soak procedures.
- Kept production code and public API surface unchanged.
- Preserved default offline verification; real-network smoke remains ignored/manual and explicitly opt-in.

Design and plan:

- `docs/superpowers/specs/2026-06-09-production-runbook-config-soak-design.md`
- `docs/superpowers/plans/2026-06-09-production-runbook-config-soak.md`

Commits:

- Include `3e6cc1a docs(server): design p39 production runbook`.
- Include the short hash from `rtk git log --oneline -n 10` for `test(server): validate p39 production config pack`.
- Include the short hash from `rtk git log --oneline -n 10` for `docs(server): add market data production runbook`.
- After committing this status update, amend this section in a follow-up commit only if repository policy requires status documents to include their own final commit hash.

Verification:

- For each command from Step 1, include the exact command and the observed result, for example `2 passed`, `12 passed`, or `exit 0`.
- Include `rtk cargo fmt -p fdc-server -p fdc-storage -- --check` with `exit 0` only after the command succeeds.

Recommended next slice:

- P40 operator readiness follow-up only after an internal MVP run identifies gaps.
```

- [ ] **Step 3: Commit status update**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
rtk git diff --check
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record p39 production runbook completion"
```

---

### Task 4: Final review

**Files:**
- No planned modifications.

- [ ] **Step 1: Confirm worktree status**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook
rtk git status --short
rtk git log --oneline -n 8
```

Expected: clean worktree and P39 commits visible above P38.

- [ ] **Step 2: Request final code review**

Dispatch a reviewer with this context:

```text
Review range: 6675c23..HEAD in /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p39-production-runbook.
Scope: P39 production runbook/config pack/config contract tests only.
Requirements:
- No production API changes.
- No production runtime behavior changes.
- Config example parses as production tiered config.
- Dangerous live/reset/resume/scheduler gates disabled by default.
- Runbook commands are executable and include expected outcomes.
- Real-network smoke remains optional/ignored/manual.
- DEVELOPMENT_STATUS accurately reports implementation and verification.
Report APPROVED, APPROVED_WITH_NOTES, or CHANGES_REQUESTED with exact file/line evidence.
```

- [ ] **Step 3: Fix review feedback if needed**

If review returns `CHANGES_REQUESTED`, fix important issues, rerun relevant verification, commit fixes, and request review again.

- [ ] **Step 4: Mark branch ready**

Only after review approval or approval with notes and clean worktree, report the final HEAD and verification evidence.
