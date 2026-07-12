# Binance Futures Contract Data Maintenance Design

**Date:** 2026-07-12

## Goal

Add a production-maintainable Binance Futures USD contract market-data acquisition path after the spot candle maintenance gate. The first implementation must follow the existing project architecture: `fdc-barter` fetchers produce `BarterIngestionEnvelope`, `fdc-orchestrator` maps envelopes to DTO/storage records, and `fdc-storage` persists canonical records plus maintenance metadata. No sidecar files, local temp stores, or bypass writes are allowed.

Implementation should proceed incrementally: first run through one contract data kind end-to-end, validate its acquisition/checkpoint/audit/storage behavior, then expand other contract kinds using the same architecture and test pattern. The recommended first kind is Binance Futures USD candle/OHLCV because it is closest to the already validated spot candle maintenance path.

## Scope

Included in the overall contract maintenance design:

- Exchange: `binance_futures_usd`.
- Market type: perpetual contract data.
- Phase 1 implementation target:
  - futures candle/OHLCV
- Follow-up implementation targets, added only after Phase 1 validates the maintenance pattern:
  - funding rate
  - open interest
  - mark price
  - index price only when represented by existing adapter/orchestrator contracts. If Binance Futures currently exposes index data as a field on mark price payloads, do not invent a separate endpoint in this slice.
- Canonical storage collections:
  - `candles`
  - `funding_rates`
  - `open_interest`
  - `mark_prices`
  - `index_prices` only for true `IndexPrice` DTOs produced by the existing mapping path.
- Maintenance metadata collections:
  - `contract_checkpoints`
  - `contract_acquisition_audits`
- Offline contract tests and bounded real-network operator smoke instructions.

Excluded from this slice:

- Multi-exchange contract acquisition.
- Non-Binance contract APIs.
- Full generalized historical acquisition framework refactor.
- Futures candle cross-interval verification/aggregation. This may be added later once the first contract data maintenance path is stable.
- Long-running unattended production scheduling beyond existing autostart/manual run controls.

## Architecture

Create a new server-side acquisition module, tentatively `market_data::contract_acquisition`, rather than modifying the spot candle runner into a broad generic framework immediately. This keeps the validated spot candle acquisition path stable while still reusing its proven maintenance patterns.

The module owns:

- runtime config expansion into bounded historical tasks
- checkpoint key/value serialization
- per-task audit serialization
- runner status aggregation
- Binance Futures USD source wiring

The module does not own:

- payload DTO conversion
- canonical collection selection
- storage schema tags for market-data records

Those stay in existing `fdc-orchestrator` and `fdc-storage` boundaries.

## Data Flow

```text
Runtime config
  -> expand contract historical tasks
  -> fdc-barter Binance Futures USD page fetcher/executor
  -> BarterIngestionEnvelope
  -> fdc-orchestrator DTO/storage mapping
  -> fdc-storage canonical collections
  -> contract_checkpoints + contract_acquisition_audits
  -> status API / runbook validation
```

## Runtime Configuration

For Phase 1, configure only `candle` with a small symbol/interval/page scope. After Phase 1 validates the architecture, enable additional kinds one at a time using the same config shape.

Add contract acquisition runtime config fields with env support:

```text
FDC_MARKET_DATA_CONTRACTS_ENABLED
FDC_MARKET_DATA_CONTRACTS_AUTOSTART
FDC_MARKET_DATA_CONTRACTS_EXCHANGE=binance_futures_usd
FDC_MARKET_DATA_CONTRACTS_SYMBOLS=BTCUSDT,ETHUSDT
FDC_MARKET_DATA_CONTRACTS_KINDS=candle,funding_rate,open_interest,mark_price
FDC_MARKET_DATA_CONTRACTS_INTERVALS=1m
FDC_MARKET_DATA_CONTRACTS_START_NS=<optional>
FDC_MARKET_DATA_CONTRACTS_END_NS=<optional>
FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE=<bounded>
FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN=<bounded>
```

`INTERVALS` applies only to candle/OHLCV tasks. Non-candle derivative kinds use `interval_or_none=none` in maintenance keys.

## Task Expansion

Phase 1 expands only futures candle/OHLCV tasks:

For each configured symbol:

- for `candle`, expand one task per configured interval

Follow-up phases reuse the same task model:

- for `funding_rate`, expand one task with no interval
- for `open_interest`, expand one task with no interval
- for `mark_price`, expand one task with no interval
- for `index_price`, expand only if the existing adapter/orchestrator path can produce true `IndexPrice` payloads

Each task is bounded by `start_ns`, `end_ns`, `limit_per_page`, and `max_pages_per_run`.

## Checkpoints

Use collection `contract_checkpoints`.

Key format:

```text
exchange:symbol:kind:interval_or_none
```

Value schema:

```json
{
  "cursor": "HistoricalCursor",
  "updated_at_ns": 0
}
```

Checkpoint writes occur when a task returns a final cursor. On the next run, the task resumes from the stored cursor. This mirrors the spot candle checkpoint model but generalizes the key with `kind`.

## Audits

Use collection `contract_acquisition_audits`.

Key format:

```text
run_id:symbol:kind:interval_or_none:updated_at_ns
```

Value schema:

```json
{
  "run_id": "string",
  "exchange": "binance_futures_usd",
  "symbol": "BTCUSDT",
  "kind": "candle|funding_rate|open_interest|mark_price|index_price",
  "interval": "1m|null",
  "pages_fetched": 0,
  "envelopes_received": 0,
  "storage_records_written": 0,
  "final_cursor": "HistoricalCursor|null",
  "updated_at_ns": 0
}
```

Each expanded task writes one audit record after execution. Audits are for operational visibility and smoke validation, not canonical market data.

## Status API

Add:

```text
GET /market-data/contracts/acquisition/status
```

The response should include:

- enabled/autostart
- exchange/symbols/kinds/intervals/page bounds summary
- last run status
- last error

Last run status should include:

- tasks started/completed
- pages fetched
- envelopes received
- storage records written
- final cursors
- audit records written

## Error Handling

- Invalid config fails validation before any network or storage work.
- Unsupported exchange is rejected with a validation error.
- Unsupported kind is rejected during config parsing/validation.
- A task failure returns an error for the run and records last error in server state.
- The runner remains bounded by `max_pages_per_run` to avoid accidental unbounded network execution.
- Audit/checkpoint write failures fail the run because they are part of the production maintenance contract.

## Testing Strategy

Offline contract tests for Phase 1:

1. config env parsing and validation for contract candle acquisition
2. task expansion for futures candle intervals
3. canonical storage routing to `candles` through existing orchestrator mapping
4. storage-backed `contract_checkpoints` roundtrip
5. per-task `contract_acquisition_audits` persistence
6. status API default/disabled response
7. status API last-run response
8. bounded source-injected futures candle acquisition runner writes expected records

Follow-up tests add each derivative kind one at a time after Phase 1 passes, reusing the same checkpoint/audit/status assertions.

Final local validation:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config -- --nocapture
rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition_status_reports_ -- --nocapture
rtk cargo check -p fdc-server
```

Operator smoke after Phase 1 implementation:

- Start with `BTCUSDT`, `candle`, `1m`, `max_pages_per_run=1`.
- Confirm canonical storage has records in `candles` tagged as Binance Futures USD/perpetual contract data.
- Confirm `contract_checkpoints` records exist.
- Confirm `contract_acquisition_audits` records exist for the candle task.
- Restart and confirm checkpoint resume behavior.

Follow-up smoke after each additional kind:

- Enable exactly one new kind, for example `funding_rate`, with the same symbol/page bounds.
- Confirm the expected canonical collection receives records.
- Confirm the same checkpoint/audit/status pattern holds.

## Documentation Updates

Update:

- `docs/roadmaps/factor-data-stage-status.md`
- `docs/runbooks/market-data-production-runbook.md`
- implementation plan under `docs/superpowers/plans/`

The roadmap must explicitly state that this starts with one contract data kind to validate the maintenance pattern, then expands other Binance Futures USD contract data using the same architecture. It is not a broad multi-exchange/general acquisition rewrite.
