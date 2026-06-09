# Market Data Package Deployment Verification Checklist

**Purpose:** Provide an operator-executable checklist to verify that the packaged `fdc-server` can be deployed, started, collect market data, store it durably, serve queries, and prove the four-tier storage profile is active.

**Scope:** Internal MVP / controlled production-like deployment. This checklist complements `docs/runbooks/market-data-production-runbook.md` and focuses on actual package deployment validation.

**Date:** 2026-06-09
**Branch:** `mdb-mqdev`
**Primary binary:** `fdc_server` from package `fdc-server`

---

## 0. Verification Result Sheet

Fill this table during the run.

| Area | Required result | Actual result | Pass/Fail | Notes |
|---|---|---|---|---|
| Build/package | Release binary produced | | | |
| Config parse | Production env loads safely | | | |
| Startup | Server listens on expected address | | | |
| Readiness | `/health`, `/ready`, `/version` succeed | | | |
| Storage status | `backend=tiered`, `durable_tiers_configured=3` | | | |
| Four-tier health | L1/L2/L3/L4 initialized and healthy | | | |
| Query empty state | Valid query succeeds before data | | | |
| Live start gate | Disabled config rejects live start | | | |
| Live collection | Enabled config collects or reports actionable status | | | |
| Query after collection | Trade query returns collected records, or live status explains no records | | | |
| Durable files | L2/L3/L4 paths exist on disk | | | |
| Restart persistence | After restart, storage health remains tiered and query still works | | | |
| Maintenance | Manual maintenance accepted and audit recorded | | | |
| Scheduler optional | Scheduler status correct if enabled | | | |
| Recovery gates | Resume/reset gates reject when disabled | | | |
| Logs/artifacts | Logs and storage directory preserved | | | |

Decision:

- [ ] PASS: acceptable for internal MVP/gray deployment.
- [ ] CONDITIONAL PASS: only with listed operator restrictions.
- [ ] FAIL: do not promote until failures are fixed.

---

## 1. Preconditions

Run from the repository root unless your deployment environment has a separate release directory.

```bash
pwd
git rev-parse --abbrev-ref HEAD
git rev-parse --short HEAD
cargo --version
rustc --version
df -h . /Volumes/wdata 2>/dev/null || df -h .
```

Expected:

- Branch is the intended release branch, normally `mdb-mqdev`.
- Commit is recorded in the result sheet.
- Rust toolchain is available.
- At least 10 GiB free space is available before DuckDB/RocksDB build or storage validation.

Record:

```text
branch=
commit=
cargo=
rustc=
free_space=
```

---

## 2. Build and Package Verification

### 2.1 Deterministic contract tests before packaging

Run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env -- --nocapture

CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server --test production_server_router_contract p38_ -- --nocapture

CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server --test production_binary_runtime_contract production_binary_assembles_tiered_runtime_store -- --nocapture

CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume -- --nocapture

CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo test -p fdc-server live_supervisor_ -- --nocapture
```

Expected:

- Runtime config test passes.
- P38 query API tests pass.
- Production binary runtime assembly test passes.
- Live resume tests pass.
- Live supervisor tests pass.

If any command fails, stop packaging and record the failure.

### 2.2 Build release binary

Run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target \
  rtk cargo build -p fdc-server --release

ls -lh /Volumes/wdata/opensource/mountainsea-lab/mdb/target/release/fdc_server \
  /Volumes/wdata/opensource/mountainsea-lab/mdb/target/release/fdc-server 2>/dev/null || true
```

Expected:

- Build exits 0.
- One release binary exists. In current source the binary name is expected to be `fdc_server`.

Set:

```bash
export FDC_RELEASE_BIN=/Volumes/wdata/opensource/mountainsea-lab/mdb/target/release/fdc_server
```

Verify:

```bash
test -x "$FDC_RELEASE_BIN"
"$FDC_RELEASE_BIN" --help >/tmp/fdc-server-help.txt 2>&1 || true
head -40 /tmp/fdc-server-help.txt || true
```

Note: if `--help` is not supported by the binary, this is not a blocker. Executability is the important check.

### 2.3 Create deployment directory

Choose a clean deployment root. Example:

```bash
export FDC_DEPLOY_ROOT="$PWD/deploy/fdc-server-smoke"
rm -rf "$FDC_DEPLOY_ROOT"
mkdir -p "$FDC_DEPLOY_ROOT/bin" "$FDC_DEPLOY_ROOT/config" "$FDC_DEPLOY_ROOT/var/fdc-market-data" "$FDC_DEPLOY_ROOT/logs"
cp "$FDC_RELEASE_BIN" "$FDC_DEPLOY_ROOT/bin/fdc_server"
cp config/production.local.example.env "$FDC_DEPLOY_ROOT/config/production.env"
chmod +x "$FDC_DEPLOY_ROOT/bin/fdc_server"
```

Expected:

- Deployment directory has `bin/fdc_server` and `config/production.env`.
- Storage root exists at `var/fdc-market-data`.

Record:

```bash
find "$FDC_DEPLOY_ROOT" -maxdepth 3 -type f -o -type d | sort
```

---

## 3. Configure Safe Production Smoke

Edit the copied env file if needed:

```bash
cd "$FDC_DEPLOY_ROOT"
cat > config/production.smoke.env <<'ENV'
FDC_SERVER_ADDR=127.0.0.1:18080
FDC_SERVER_ENV=production

FDC_LIVE_ENABLED=0
FDC_LIVE_AUTOSTART=0
FDC_LIVE_DEFAULT_TIMEOUT_SECS=30
FDC_LIVE_DEFAULT_MAX_ENVELOPES=100
FDC_LIVE_RETRY_ENABLED=1
FDC_LIVE_RETRY_INITIAL_DELAY_MS=1000
FDC_LIVE_RETRY_MAX_DELAY_MS=30000
FDC_LIVE_MAX_CONSECUTIVE_FAILURES=3
FDC_MARKET_DATA_LIVE_RESUME_ENABLED=0

FDC_MARKET_DATA_STORAGE_BACKEND=tiered
FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime
FDC_MARKET_DATA_STORAGE_L2_REDB_PATH=./var/fdc-market-data/l2.redb
FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH=./var/fdc-market-data/l3.duckdb
FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH=./var/fdc-market-data/l4-rocksdb

FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED=1
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY=64
FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED=0

FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS=3600
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS=30000
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS=0
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES=3
ENV
```

Expected:

- Live collection is disabled by default.
- Storage backend is `tiered`.
- L2, L3, and L4 durable paths are under `./var/fdc-market-data`.
- Maintenance manual hook is enabled.
- Scheduler and recovery gates are disabled by default.

---

## 4. Start Packaged Server

Start in one shell:

```bash
cd "$FDC_DEPLOY_ROOT"
set -a
. ./config/production.smoke.env
set +a
mkdir -p ./var/fdc-market-data ./logs
./bin/fdc_server > ./logs/fdc-server.stdout.log 2> ./logs/fdc-server.stderr.log &
echo $! > ./logs/fdc-server.pid
sleep 2
cat ./logs/fdc-server.pid
```

Check process and logs:

```bash
ps -p "$(cat ./logs/fdc-server.pid)" -o pid,command
sed -n '1,80p' ./logs/fdc-server.stderr.log
```

Expected:

- Process is running.
- Log contains a listening message similar to `fdc server listening on http://127.0.0.1:18080`.
- Live collection does not autostart in safe smoke mode.

If the process exits, inspect:

```bash
cat ./logs/fdc-server.stdout.log
cat ./logs/fdc-server.stderr.log
```

Stop and fix config before continuing.

---

## 5. Readiness and Basic API Verification

Use `--noproxy '*'` for localhost checks so host proxy settings do not cause false failures.

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/health | tee ./logs/check-health.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready | tee ./logs/check-ready.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/version | tee ./logs/check-version.json
```

Expected examples:

```json
{"status":"healthy"}
{"status":"ready","live_enabled":false,"market_data_store_available":true}
{"status":"success","data":{"service":"fdc-server","version":"0.1.0"},"message":null}
```

Pass criteria:

- `/health` returns HTTP 200 and `status=healthy`.
- `/ready` returns HTTP 200 and `status=ready`.
- `/ready` shows `live_enabled=false` for safe smoke.
- `/version` returns `service=fdc-server` and package version metadata.

---

## 6. Four-Tier Storage Activation Verification

This section proves the configured four-tier profile is active. It checks configuration, runtime health, durable path readiness, and post-operation tier observations.

### 6.1 Storage status must report tiered backend

Run:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/status \
  | tee ./logs/check-storage-status.json
```

Required values:

- `status` is `success`.
- `data.backend` is `tiered`.
- `data.policy_profile` is `generic_realtime`.
- `data.tiered` is `true`.
- `data.durable_tiers_configured` is `3`.
- `data.tiers` contains four entries: `L1`, `L2`, `L3`, `L4`.
- `L1.engine` is `memory` and `L1.durable_path_configured=false`.
- `L2.engine` is `redb` and `L2.durable_path_configured=true`.
- `L3.engine` is `duckdb` and `L3.durable_path_configured=true`.
- `L4.engine` is `rocksdb` and `L4.durable_path_configured=true`.
- Path hints are present but full local paths are not exposed.

Optional jq check if `jq` is installed:

```bash
jq '.data | {backend, policy_profile, tiered, durable_tiers_configured, tiers}' ./logs/check-storage-status.json
jq -e '.data.backend == "tiered" and .data.tiered == true and .data.durable_tiers_configured == 3' ./logs/check-storage-status.json
jq -e '[.data.tiers[].tier] == ["L1","L2","L3","L4"]' ./logs/check-storage-status.json
```

### 6.2 Storage health must report all four tiers initialized

Run:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health \
  | tee ./logs/check-storage-health-before.json
```

Required values:

- `status` is `success`.
- `data.backend` is `tiered`.
- `data.tiered` is `true`.
- `data.status` is `healthy` or, during very early startup only, not worse than initialized/healthy per tier.
- `data.tiers` contains `L1`, `L2`, `L3`, `L4`.
- For every tier: `enabled=true`, `initialized=true`, `status=healthy`.
- `L2.path_exists=true`, `L2.path_parent_exists=true`, `L2.path_parent_writable=true`.
- `L3.path_exists=true`, `L3.path_parent_exists=true`, `L3.path_parent_writable=true`.
- `L4.path_exists=true`, `L4.path_parent_exists=true`, `L4.path_parent_writable=true`.
- `key_count` exists for every tier.

Optional jq checks:

```bash
jq '.data.tiers[] | {tier, enabled, initialized, status, key_count, durable_path_configured, path_hint, path_exists, path_parent_exists, path_parent_writable}' ./logs/check-storage-health-before.json
jq -e '.data.backend == "tiered" and .data.tiered == true' ./logs/check-storage-health-before.json
jq -e '[.data.tiers[].tier] == ["L1","L2","L3","L4"]' ./logs/check-storage-health-before.json
jq -e 'all(.data.tiers[]; .enabled == true and .initialized == true and .status == "healthy")' ./logs/check-storage-health-before.json
```

### 6.3 Durable files/directories must exist on disk

Run:

```bash
ls -lah ./var/fdc-market-data
find ./var/fdc-market-data -maxdepth 2 -print | sort | tee ./logs/check-storage-files-before.txt

test -e ./var/fdc-market-data/l2.redb
test -e ./var/fdc-market-data/l3.duckdb
test -d ./var/fdc-market-data/l4-rocksdb
```

Expected:

- `l2.redb` exists.
- `l3.duckdb` exists.
- `l4-rocksdb` directory exists.

Record sizes:

```bash
du -sh ./var/fdc-market-data ./var/fdc-market-data/* | tee ./logs/check-storage-size-before.txt
```

### 6.4 Query route works against the market data store

Run:

```bash
curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?limit=10' \
  | tee ./logs/check-query-before-live.json

curl --noproxy '*' -sS -o ./logs/check-query-invalid-limit.json -w '%{http_code}\n' \
  'http://127.0.0.1:18080/market-data/trades?limit=1001' \
  | tee ./logs/check-query-invalid-limit.status
cat ./logs/check-query-invalid-limit.json
```

Expected:

- Valid query returns HTTP 200 with `status=success`.
- Valid query response has `data_kind=trade`, `query_source=market_data_store`, `requested_limit=10`, `applied_limit=10`.
- Empty records are acceptable before live collection.
- Invalid limit returns HTTP 400 and message `limit must be between 1 and 1000`.

---
## 7. Live Collection Verification

The safe smoke config intentionally disables live collection. Validate both the safety gate and the operator-enabled path.

### 7.1 Disabled live start must not collect

Run while still using `production.smoke.env`:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/live/status \
  | tee ./logs/check-live-status-disabled.json

curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":30,"max_envelopes":100}' \
  | tee ./logs/check-live-start-disabled.json
```

Expected:

- Status route returns success.
- Start route returns `status=error` because `FDC_LIVE_ENABLED=0`.
- No records are written as a side effect of a disabled start request.

### 7.2 Restart with live collection enabled for bounded operator run

Stop current server:

```bash
kill "$(cat ./logs/fdc-server.pid)"
sleep 2
ps -p "$(cat ./logs/fdc-server.pid)" -o pid,command || true
```

Create live-enabled env:

```bash
cp config/production.smoke.env config/production.live.env
cat >> config/production.live.env <<'ENV'
# Operator override for bounded real-network validation.
FDC_LIVE_ENABLED=1
FDC_LIVE_AUTOSTART=0
ENV
```

Because shell env files use last assignment wins in `sh`, the appended values override the earlier safe defaults.

Start server again:

```bash
set -a
. ./config/production.live.env
set +a
./bin/fdc_server > ./logs/fdc-server-live.stdout.log 2> ./logs/fdc-server-live.stderr.log &
echo $! > ./logs/fdc-server.pid
sleep 2
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready | tee ./logs/check-ready-live.json
```

Expected:

- Server starts.
- `/ready` returns `live_enabled=true`.
- Live still does not autostart because `FDC_LIVE_AUTOSTART=0`.

### 7.3 Run bounded live collection

Run:

```bash
curl --noproxy '*' -fsS -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":60,"max_envelopes":200}' \
  | tee ./logs/check-live-start-enabled.json

sleep 5
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/live/status \
  | tee ./logs/check-live-status-running.json

sleep 70
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/live/status \
  | tee ./logs/check-live-status-after.json
```

Expected:

One of these outcomes is acceptable:

1. **Collected:** live status shows completion/progress with `envelopes_received > 0` and `storage_records_written > 0`.
2. **Environment/network failure:** live status shows failed or suppressed state with a sanitized error explaining exchange/network/rate-limit failure. This is a deployment environment result, not a storage/query failure.

Pass criteria for real production promotion should prefer outcome 1. Outcome 2 is acceptable only for documenting a network/connectivity blocker.

### 7.4 Query after live collection

Run:

```bash
curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?limit=10' \
  | tee ./logs/check-query-after-live.json

curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?symbol=BTCUSDT&limit=10' \
  | tee ./logs/check-query-btc-after-live.json
```

Expected if live collected data:

- Response `status=success`.
- `data.query_source=market_data_store`.
- `data.returned_records > 0` for at least one query.
- Records contain trade data and symbols.

Expected if live did not collect due to external network/exchange failure:

- Query still returns `status=success`.
- `returned_records` may be 0.
- `check-live-status-after.json` must explain the collection failure.

### 7.5 Four-tier health after live collection

Run:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health \
  | tee ./logs/check-storage-health-after-live.json

du -sh ./var/fdc-market-data ./var/fdc-market-data/* | tee ./logs/check-storage-size-after-live.txt
find ./var/fdc-market-data -maxdepth 2 -print | sort | tee ./logs/check-storage-files-after-live.txt
```

Required:

- Storage remains `backend=tiered`, `tiered=true`.
- All four tiers remain initialized and healthy.
- L2/L3/L4 durable paths still exist.

If live collected records, compare `key_count` and file sizes before/after:

```bash
printf '%s\n' 'Before:'
cat ./logs/check-storage-health-before.json
printf '%s\n' 'After live:'
cat ./logs/check-storage-health-after-live.json
printf '%s\n' 'Size before:'
cat ./logs/check-storage-size-before.txt
printf '%s\n' 'Size after:'
cat ./logs/check-storage-size-after-live.txt
```

Pass criteria:

- At minimum, L2 hot tier should reflect live trade writes under the `generic_realtime` policy when records are collected.
- L3/L4 may remain initialized with zero keys during a short bounded live run. That is acceptable because short live writes are hot data; L3/L4 activation is proven by engine initialization, durable path readiness, health, and maintenance compatibility. To prove L3/L4 contain records, use a longer retention/demotion-specific test or a fixture test designed for warm/cold records.

---

## 8. Restart and Durable Persistence Verification

This verifies that the packaged server can restart over the same configured durable storage directory.

Stop live server:

```bash
kill "$(cat ./logs/fdc-server.pid)"
sleep 2
ps -p "$(cat ./logs/fdc-server.pid)" -o pid,command || true
```

Restart with the same live env or safe env. For a safe post-live readback, use live disabled:

```bash
set -a
. ./config/production.smoke.env
set +a
./bin/fdc_server > ./logs/fdc-server-restart.stdout.log 2> ./logs/fdc-server-restart.stderr.log &
echo $! > ./logs/fdc-server.pid
sleep 2
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready | tee ./logs/check-ready-after-restart.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health | tee ./logs/check-storage-health-after-restart.json
curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?limit=10' | tee ./logs/check-query-after-restart.json
```

Expected:

- Server restarts successfully.
- Storage still reports `backend=tiered`, `tiered=true`.
- L2/L3/L4 durable paths still exist and are healthy.
- Query route still succeeds.
- If previous live run collected records, query after restart should still return records.

Failure criteria:

- Startup silently falls back to memory backend.
- Storage status says `tiered=false`.
- Durable path files disappear unexpectedly.
- Query route fails after restart.

---

## 9. Manual Maintenance and Audit Verification

Run manual maintenance only when you intend to scan/compact the configured tiers.

```bash
curl --noproxy '*' -fsS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/run-once \
  -H 'content-type: application/json' \
  -d '{"confirm":"run_maintenance_once","reason":"package_deployment_verification"}' \
  | tee ./logs/check-maintenance-run-once.json

curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/storage/maintenance/audit?limit=10' \
  | tee ./logs/check-maintenance-audit.json

curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health \
  | tee ./logs/check-storage-health-after-maintenance.json

curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?limit=10' \
  | tee ./logs/check-query-after-maintenance.json
```

Expected:

- Maintenance response has `status=success` and `data.accepted=true`.
- Audit response contains the maintenance entry.
- Four-tier health remains healthy after maintenance.
- Query route remains readable after maintenance.

---

## 10. Optional Scheduler Soak Verification

Use this only when validating background maintenance scheduling.

Stop current server:

```bash
kill "$(cat ./logs/fdc-server.pid)"
sleep 2
```

Create scheduler env:

```bash
cp config/production.smoke.env config/production.scheduler.env
cat >> config/production.scheduler.env <<'ENV'
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED=1
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS=300
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS=30000
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS=30
FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES=3
ENV
```

Start:

```bash
set -a
. ./config/production.scheduler.env
set +a
./bin/fdc_server > ./logs/fdc-server-scheduler.stdout.log 2> ./logs/fdc-server-scheduler.stderr.log &
echo $! > ./logs/fdc-server.pid
sleep 2
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/status \
  | tee ./logs/check-scheduler-status.json
```

Expected:

- Scheduler status route returns success.
- Scheduler reports enabled and tiered backend.
- `next_run_at` is present if a scheduler loop is active.
- Audit entries increase only after scheduled attempts run.

---

## 11. Recovery Gate Safety Verification

These checks ensure dangerous recovery controls are closed unless explicitly enabled.

### 11.1 Live resume gate disabled

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/live/resume \
  -H 'content-type: application/json' \
  -d '{"confirm":"resume_live_collection","reason":"gate_check"}' \
  -o ./logs/check-live-resume-disabled.json -w '%{http_code}\n' \
  | tee ./logs/check-live-resume-disabled.status
cat ./logs/check-live-resume-disabled.json
```

Expected:

- HTTP 403 or appropriate forbidden/error response.
- Message indicates live resume hook/gate is disabled.

### 11.2 Scheduler reset/resume gates disabled by default

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/reset \
  -H 'content-type: application/json' \
  -d '{"confirm":"reset_scheduler_suppression","reason":"gate_check"}' \
  -o ./logs/check-scheduler-reset-disabled.json -w '%{http_code}\n' \
  | tee ./logs/check-scheduler-reset-disabled.status
cat ./logs/check-scheduler-reset-disabled.json

curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/resume \
  -H 'content-type: application/json' \
  -d '{"confirm":"resume_scheduler","reason":"gate_check"}' \
  -o ./logs/check-scheduler-resume-disabled.json -w '%{http_code}\n' \
  | tee ./logs/check-scheduler-resume-disabled.status
cat ./logs/check-scheduler-resume-disabled.json
```

Expected:

- Disabled gates reject mutating recovery actions.
- No storage records are deleted.
- No maintenance audit reset occurs.

---

## 12. Shutdown and Artifact Collection

Stop server:

```bash
kill "$(cat ./logs/fdc-server.pid)" 2>/dev/null || true
sleep 2
ps -p "$(cat ./logs/fdc-server.pid)" -o pid,command || true
```

Collect artifacts:

```bash
tar -czf "fdc-deployment-verification-$(date +%Y%m%d-%H%M%S).tgz" \
  -C "$FDC_DEPLOY_ROOT" logs var/fdc-market-data config
ls -lh fdc-deployment-verification-*.tgz
```

Preserve:

- `logs/*.json`
- `logs/*.status`
- `logs/*.txt`
- `logs/*.stdout.log`
- `logs/*.stderr.log`
- `var/fdc-market-data/`
- Exact env file used for the run.

---

## 13. Pass, Conditional Pass, and Fail Rules

### PASS for internal MVP/gray deployment

All required items are true:

- Release binary builds and starts from deployment directory.
- Readiness endpoints pass.
- Storage status reports `backend=tiered`, `tiered=true`, `durable_tiers_configured=3`.
- Four tiers L1/L2/L3/L4 are present, initialized, and healthy.
- L2 Redb, L3 DuckDB, and L4 RocksDB durable paths exist and are writable.
- Query route succeeds before and after maintenance.
- Disabled gates reject unsafe operations.
- Restart over the same storage directory succeeds.
- If live collection is enabled and network is available, at least one bounded live run writes/query records.

### CONDITIONAL PASS

Allowed only with an explicit note:

- All local packaging, startup, storage, query, restart, maintenance, and safety checks pass.
- Live real-network collection fails only because of documented external network/exchange/rate-limit conditions.
- Operator accepts that live collection must be revalidated in the target network before public production promotion.

### FAIL

Do not promote if any of these occur:

- Binary cannot start from package directory.
- `/ready` fails.
- Storage falls back to memory when tiered env is configured.
- `durable_tiers_configured` is not 3.
- Any of L1/L2/L3/L4 is missing from storage health.
- L2/L3/L4 durable path does not exist or parent is not writable.
- Query route fails for a valid limit.
- Invalid limit does not return HTTP 400.
- Restart loses previously collected records when live collection had succeeded.
- Maintenance corrupts storage health or hides query data.
- Disabled recovery gates allow mutation.

---

## 14. Quick Command Summary

Minimal command sequence for an operator who has already read the full checklist:

```bash
export FDC_DEPLOY_ROOT="$PWD/deploy/fdc-server-smoke"
export FDC_RELEASE_BIN=/Volumes/wdata/opensource/mountainsea-lab/mdb/target/release/fdc_server

CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo build -p fdc-server --release
rm -rf "$FDC_DEPLOY_ROOT"
mkdir -p "$FDC_DEPLOY_ROOT/bin" "$FDC_DEPLOY_ROOT/config" "$FDC_DEPLOY_ROOT/var/fdc-market-data" "$FDC_DEPLOY_ROOT/logs"
cp "$FDC_RELEASE_BIN" "$FDC_DEPLOY_ROOT/bin/fdc_server"
cp config/production.local.example.env "$FDC_DEPLOY_ROOT/config/production.smoke.env"
chmod +x "$FDC_DEPLOY_ROOT/bin/fdc_server"

cd "$FDC_DEPLOY_ROOT"
set -a; . ./config/production.smoke.env; set +a
./bin/fdc_server > ./logs/fdc-server.stdout.log 2> ./logs/fdc-server.stderr.log & echo $! > ./logs/fdc-server.pid
sleep 2

curl --noproxy '*' -fsS http://127.0.0.1:18080/health | tee ./logs/check-health.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready | tee ./logs/check-ready.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/version | tee ./logs/check-version.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/status | tee ./logs/check-storage-status.json
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health | tee ./logs/check-storage-health-before.json
curl --noproxy '*' -fsS 'http://127.0.0.1:18080/market-data/trades?limit=10' | tee ./logs/check-query-before-live.json
find ./var/fdc-market-data -maxdepth 2 -print | sort | tee ./logs/check-storage-files-before.txt

test -e ./var/fdc-market-data/l2.redb
test -e ./var/fdc-market-data/l3.duckdb
test -d ./var/fdc-market-data/l4-rocksdb
```

