# Contract Candle Acquisition Completeness Design

Date: 2026-07-12

## Goal

Complete the Binance Futures USD perpetual candle/OHLCV acquisition business loop so it is a reliable data-source foundation for later factor and strategy research. This work stays in the data acquisition and maintenance layer. It does not implement factors, strategy logic, or additional derivatives data kinds.

## Scope

In scope:

1. Add an operator-triggered run-once endpoint for contract candle acquisition.
2. Verify that acquired contract candles are written to canonical candle storage and are readable through the existing candle query path.
3. Preserve safe checkpoint semantics: checkpoints advance only after canonical candle writes and audit writes succeed.
4. Update the production runbook with a complete operator flow: configure, run, check status, query candles, inspect checkpoint/audit semantics.

Out of scope:

- Factor computation.
- Strategy execution.
- Funding rate, open interest, mark price, index price, liquidation, or other derivative data kinds.
- Sidecar or temporary storage paths.

## Current baseline

The previous phase merged into `mdb-mqdev` with:

- `MarketDataContractAcquisitionRuntimeConfig` guarded to Phase 1: `binance_futures_usd` and `candle` only.
- `BinanceFuturesUsdOhlcvHistoricalPageFetcher` exported from `fdc-barter`.
- `run_contract_acquisition_once_with_checkpoints` and storage-backed contract checkpoints/audits.
- `GET /market-data/contracts/acquisition/status`.
- Binary autostart support when both enabled and autostart are set.
- Tests proving storage writes, checkpoints, audits, status, and checkpoint-after-storage ordering.

## Approach

### API

Add:

`POST /market-data/contracts/acquisition/run-once`

The endpoint uses the already-loaded runtime config and the existing production state runner:

`ProductionServerState::start_contract_acquisition_autostart_if_enabled` is not used because it requires `autostart=true`. Instead, expose a manual run method that requires `enabled=true`, validates the config already parsed at startup, runs the Binance Futures USD candle acquisition once, records last-run/last-error state, and returns a status response.

Recommended response shape mirrors the status DTO:

- `tasks_started`
- `tasks_completed`
- `pages_fetched`
- `envelopes_received`
- `storage_records_written`
- `audit_records_written`
- `final_cursors`
- `market_data_store_records`

If disabled, return an error envelope and a response body with zero counters. The existing router pattern often returns `ServerApiResponse::error` with HTTP 200 for domain-level disabled states, so follow the local style unless a nearby endpoint already uses non-200 for equivalent operator actions.

### Canonical query validation

Extend production router tests to prove this sequence:

1. Configure contract acquisition enabled for one symbol/interval/window.
2. Inject a scripted historical source through state-level test helper or call the runner directly.
3. Run acquisition once.
4. Query existing canonical candle path, not metadata collections.
5. Assert the expected candle is readable with the expected symbol/kind/interval metadata.

If a real HTTP `run-once` endpoint cannot accept a scripted source without test-only hooks, use service/state tests for injected source and router tests for endpoint behavior separately. The important business contract is that canonical `candles` are the factor-data source, while `contract_checkpoints` and `contract_acquisition_audits` remain maintenance metadata.

### Checkpoint and audit semantics

Keep the current ordering:

1. Fetch historical pages.
2. Write page envelopes through `run_barter_envelopes_to_storage_once`.
3. Write acquisition audit.
4. Save checkpoint.

This avoids advancing checkpoints if canonical storage fails.

### Documentation

Update `docs/runbooks/market-data-production-runbook.md` with:

- Safe one-symbol, one-interval run-once example.
- Status endpoint check.
- Existing candle query check.
- Explanation of canonical candles versus checkpoint/audit metadata.
- Reminder that derivative-specific kinds are deferred.

## Testing

Minimum verification:

- `cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture`
- `cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition -- --nocapture`
- `cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config -- --nocapture`
- `cargo check -p fdc-server`

If the changed code touches `fdc-barter`, also run the Binance Futures historical REST execution contract suite. This follow-up is expected to stay in `fdc-server` and docs.

## Acceptance criteria

- Operators can trigger one contract candle acquisition run without restarting the service.
- The run result is observable in the response and status endpoint.
- Tests prove acquired contract candles are readable via canonical candle storage/query path.
- Checkpoint/audit metadata does not become the factor input surface.
- Documentation explains the complete candle acquisition maintenance flow.
