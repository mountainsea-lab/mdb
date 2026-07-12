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

> Local proxy note: for localhost smoke checks, use `curl --noproxy '*' ...` so `HTTP_PROXY`/`HTTPS_PROXY` environment variables cannot route checks through a corporate proxy and produce false 503 responses.

```bash
curl --noproxy '*' -sS http://127.0.0.1:18080/health
curl --noproxy '*' -sS http://127.0.0.1:18080/ready
curl --noproxy '*' -sS http://127.0.0.1:18080/version
```

Expected:

- `/health` reports a healthy status.
- `/ready` reports ready.
- `/version` returns version metadata.

## Storage status checks

```bash
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/status
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/health
```

Expected:

- `backend` is `tiered`.
- `tiered` is `true`.
- Durable tier path hints are present without exposing full local paths.
- Tier health is initialized or healthy after startup.

## Query check

```bash
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/trades?limit=10'
curl --noproxy '*' -i 'http://127.0.0.1:18080/market-data/trades?limit=1001'
```

Expected:

- Valid query returns a JSON envelope with `status="success"`, `data_kind="trade"`, `query_source="market_data_store"`, `requested_limit`, `applied_limit`, `returned_records`, and `records`.
- Invalid limit returns HTTP 400 with `status="error"` and message `limit must be between 1 and 1000`.

## Manual maintenance run

Manual maintenance is enabled in the local production profile. Run it only when you intend to scan and compact configured storage tiers.

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/run-once \
  -H 'content-type: application/json' \
  -d '{"confirm":"run_maintenance_once","reason":"operator_smoke"}'

curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/storage/maintenance/audit?limit=10'
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
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":30,"max_envelopes":100}'

curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/live/status
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/trades?limit=10'
```

Expected:

- Live start is accepted only when `FDC_LIVE_ENABLED=1`.
- Live status shows progress, completion, failure, or suppression.
- Query route returns any collected trades through the normal P38 response envelope.

## Candle acquisition operator run

Candle acquisition is disabled by default. Stage 5 adds a bounded Binance Spot OHLCV historical runner that uses the existing `fdc-barter -> fdc-orchestrator -> candles storage -> /market-data/candles` path. Use small page limits first because autostart runs before the HTTP listener is bound.

Safe manual/autostart configuration example:

```bash
set -a
. ./.env.p39.local
export FDC_MARKET_DATA_CANDLES_ENABLED=1
export FDC_MARKET_DATA_CANDLES_AUTOSTART=1
export FDC_MARKET_DATA_CANDLES_EXCHANGE=binance_spot
export FDC_MARKET_DATA_CANDLES_SYMBOLS=BTCUSDT,ETHUSDT
export FDC_MARKET_DATA_CANDLES_BASE_INTERVALS=1m
export FDC_MARKET_DATA_CANDLES_VERIFY_INTERVALS=1h,1d
export FDC_MARKET_DATA_CANDLES_START_NS=1700000000000000000
export FDC_MARKET_DATA_CANDLES_END_NS=1700000060000000000
export FDC_MARKET_DATA_CANDLES_LIMIT_PER_PAGE=100
export FDC_MARKET_DATA_CANDLES_MAX_PAGES_PER_RUN=1
set +a
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo run -p fdc-server
```

Expected:

- Startup remains a no-op unless both `FDC_MARKET_DATA_CANDLES_ENABLED=1` and `FDC_MARKET_DATA_CANDLES_AUTOSTART=1` are set.
- Each configured symbol/base interval expands to a bounded historical candle task.
- Successful pages are written to the existing `candles` collection.
- Query collected candles with:

```bash
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/candles?symbol=BTCUSDT&limit=10'
```

Current Stage 5 scope:

- `base_intervals` are the canonical collection intervals. Prefer `1m` first.
- `verify_intervals` are parsed and documented for reference/verification policy, but higher-interval aggregation is a later stage.
- Offline contract tests cover request expansion, storage writes, and safe disabled autostart. Real-network smoke is opt-in.

## Live recovery flow

Use this only after inspecting live status and deciding resume is safe.

```bash
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/live/status
```

If live state is suppressed and operator policy allows resume, restart with:

```bash
export FDC_MARKET_DATA_LIVE_RESUME_ENABLED=1
```

Then call:

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/live/resume \
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
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/status
```

Expected:

- Scheduler status shows enabled and tiered backend.
- `next_run_at` is present when a scheduler loop is active.
- Maintenance audit entries increase only after scheduled attempts run.

## Scheduler recovery flow

For suppressed scheduler accounting, inspect first:

```bash
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/status
```

To reset suppression accounting without starting a task, restart with:

```bash
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED=1
```

Then call:

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/reset \
  -H 'content-type: application/json' \
  -d '{"confirm":"reset_scheduler_suppression","reason":"operator_reset"}'
```

To resume the scheduler loop, restart with:

```bash
export FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED=1
```

Then call:

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/scheduler/resume \
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
