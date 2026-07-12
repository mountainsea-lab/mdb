# Candle Acquisition Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Stage 5's minimum viable candle acquisition maintenance: config-driven historical candle backfill for configured symbols/base intervals, writing into the existing candles storage/query loop with observable run status.

**Architecture:** Keep exchange fetching in `fdc-barter`, transformation/storage mapping in `fdc-orchestrator`, and application scheduling/config in `fdc-server`. Add a focused `fdc-server::market_data::candle_acquisition` module that parses runtime config, builds `HistoricalBackfillRequest`s, runs bounded pages through a generic `HistoricalBackfillSource`, writes pages through `run_barter_envelopes_to_storage_once`, and returns a status summary. First slice is manual/autostart-gated runner and in-memory status, not durable checkpoint persistence.

**Tech Stack:** Rust, Tokio async tests, `fdc-barter` historical backfill traits, `fdc-orchestrator` pipeline, `fdc-storage::QueryableMarketDataStore`, env-driven runtime config with a future TOML file shape documented.

---

## File Structure

- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add `MarketDataCandleAcquisitionRuntimeConfig` and env parsing for the Stage 5 MVP.
  - Add safe defaults: disabled, no autostart, empty symbols/intervals.
- Create `crates/fdc-server/src/market_data/candle_acquisition.rs`
  - Own config validation helpers, request expansion, run status structs, and generic bounded runner.
- Modify `crates/fdc-server/src/market_data/mod.rs`
  - Export the new module.
- Modify `crates/fdc-server/src/runtime/app.rs`
  - Add a manual `run_candle_acquisition_once_with_source` testable method and an autostart gate stub that is disabled unless config says enabled.
- Modify `crates/fdc-server/src/lib.rs`
  - Re-export candle acquisition config/status if needed by tests.
- Test `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add RED tests for candle acquisition defaults, env parsing, and validation errors.
- Test `crates/fdc-server/tests/candle_acquisition_contract.rs`
  - Add RED tests for request expansion and runner writing candle envelopes into existing storage.
- Modify docs
  - Update `docs/roadmaps/factor-data-stage-status.md` with Stage 5 in-progress/validated evidence.
  - Add or update runbook section in `docs/runbooks/market-data-production-runbook.md`.

## Task 1: Runtime config contract

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing tests**

Add tests asserting:

```rust
#[test]
fn candle_acquisition_config_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap();
    assert!(!config.market_data_candle_acquisition.enabled);
    assert!(!config.market_data_candle_acquisition.autostart);
    assert_eq!(config.market_data_candle_acquisition.exchange, "binance_spot");
    assert!(config.market_data_candle_acquisition.symbols.is_empty());
    assert!(config.market_data_candle_acquisition.base_intervals.is_empty());
    assert!(config.market_data_candle_acquisition.verify_intervals.is_empty());
}

#[test]
fn candle_acquisition_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_ENABLED", "1"),
        ("FDC_MARKET_DATA_CANDLES_AUTOSTART", "1"),
        ("FDC_MARKET_DATA_CANDLES_EXCHANGE", "binance_spot"),
        ("FDC_MARKET_DATA_CANDLES_SYMBOLS", "BTCUSDT, ethusdt"),
        ("FDC_MARKET_DATA_CANDLES_BASE_INTERVALS", "1m,5m"),
        ("FDC_MARKET_DATA_CANDLES_VERIFY_INTERVALS", "1h,1d"),
        ("FDC_MARKET_DATA_CANDLES_START_NS", "1000000000"),
        ("FDC_MARKET_DATA_CANDLES_END_NS", "2000000000"),
        ("FDC_MARKET_DATA_CANDLES_LIMIT_PER_PAGE", "500"),
        ("FDC_MARKET_DATA_CANDLES_MAX_PAGES_PER_RUN", "3"),
    ]).unwrap();

    assert!(config.market_data_candle_acquisition.enabled);
    assert!(config.market_data_candle_acquisition.autostart);
    assert_eq!(config.market_data_candle_acquisition.symbols, vec!["BTCUSDT", "ETHUSDT"]);
    assert_eq!(config.market_data_candle_acquisition.base_intervals, vec!["1m", "5m"]);
    assert_eq!(config.market_data_candle_acquisition.verify_intervals, vec!["1h", "1d"]);
}
```

- [ ] **Step 2: Run RED**

Run: `rtk cargo test -p fdc-server --test runtime_config_contract candle_acquisition_config -- --nocapture`

Expected: FAIL because `market_data_candle_acquisition` does not exist.

- [ ] **Step 3: Implement minimal config**

Add the config struct, defaults, env parsing, positive range validation, and comma-list normalization.

- [ ] **Step 4: Run GREEN**

Run the same command. Expected: PASS.

- [ ] **Step 5: Commit**

Commit: `feat: add candle acquisition runtime config`

## Task 2: Request expansion and runner contract

**Files:**
- Create: `crates/fdc-server/tests/candle_acquisition_contract.rs`
- Create: `crates/fdc-server/src/market_data/candle_acquisition.rs`
- Modify: `crates/fdc-server/src/market_data/mod.rs`

- [ ] **Step 1: Write failing request expansion test**

Test that one config with two symbols and two base intervals expands into four `HistoricalBackfillRequest`s with kind `Candle`, interval set, exchange copied, and market type `Spot`.

- [ ] **Step 2: Run RED**

Run: `rtk cargo test -p fdc-server --test candle_acquisition_contract expands_candle_acquisition_requests -- --nocapture`

Expected: FAIL because module/functions do not exist.

- [ ] **Step 3: Implement minimal request expansion**

Add `expand_candle_backfill_requests(config)` returning validated requests.

- [ ] **Step 4: Run GREEN**

Run the same test. Expected: PASS.

- [ ] **Step 5: Write failing runner test**

Create fake `HistoricalBackfillSource` returning one candle page. Assert `run_candle_acquisition_once` writes one record into `QueryableMarketDataStore` and status reports one completed task, one page, one record.

- [ ] **Step 6: Run RED**

Run: `rtk cargo test -p fdc-server --test candle_acquisition_contract candle_acquisition_runner_writes_candles_to_storage -- --nocapture`

Expected: FAIL because runner does not exist.

- [ ] **Step 7: Implement minimal runner**

Use `run_historical_backfill_pages` and `fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once` for each page.

- [ ] **Step 8: Run GREEN**

Run the contract test. Expected: PASS.

- [ ] **Step 9: Commit**

Commit: `feat: add candle acquisition runner`

## Task 3: Production state integration and runbook

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/bin/fdc_server.rs` if autostart hook needs explicit call
- Modify: `docs/runbooks/market-data-production-runbook.md`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [ ] **Step 1: Write failing state integration test**

Add test that disabled candle acquisition autostart is a no-op and enabled config can run once with a fake source.

- [ ] **Step 2: Run RED**

Run: `rtk cargo test -p fdc-server --test candle_acquisition_contract production_state_runs_configured_candle_acquisition_once -- --nocapture`

Expected: FAIL because state method does not exist.

- [ ] **Step 3: Implement minimal state methods**

Add `ProductionServerState::run_candle_acquisition_once_with_source` and `start_candle_acquisition_autostart_if_enabled` as a safe no-op unless enabled/autostart. First production network source wiring may remain manual unless tested with opt-in smoke.

- [ ] **Step 4: Run GREEN and regression tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract candle_acquisition_config -- --nocapture
rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Update docs and commit**

Document env config, manual/auto behavior, and verification commands. Commit: `docs: document candle acquisition maintenance`

## Final verification

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract candle_acquisition_config -- --nocapture
rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test realtime_mvp_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture
rtk cargo check -p fdc-server
```

Record output summaries in `docs/roadmaps/factor-data-stage-status.md` before final commit.


---

## Stage 5.x Follow-up Gate Before Next Development Stage

The Stage 5 MVP validates config-driven Binance Spot historical candle maintenance only. Do not enter Stage 6 analytics or broader next-stage development until the following Stage 5.x slices are completed or explicitly re-scoped in the roadmap.

### Task 5.1: Durable Candle Checkpoint Persistence

**Goal:** Persist and resume candle acquisition cursor/checkpoint state across server restarts.

**Files:**
- Create or modify: `crates/fdc-server/src/market_data/candle_acquisition.rs`
- Create or modify: `crates/fdc-server/tests/candle_acquisition_contract.rs`
- Modify: `docs/runbooks/market-data-production-runbook.md`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [ ] Write failing test `candle_acquisition_persists_final_cursor_for_resume` that runs one scripted page, saves `final_cursor`, creates a second runner from that checkpoint, and asserts the second request starts at `cursor.next_start`.
- [ ] Run: `rtk cargo test -p fdc-server --test candle_acquisition_contract candle_acquisition_persists_final_cursor_for_resume -- --nocapture` and verify RED.
- [ ] Implement minimal checkpoint store abstraction with an in-memory test implementation and a production-compatible file/tier-backed path decision.
- [ ] Run the test and verify GREEN.
- [ ] Document how to inspect, reset, and resume candle checkpoint state.
- [ ] Commit with message `feat: persist candle acquisition checkpoints`.

### Task 5.2: Candle Acquisition Status API

**Goal:** Expose observable status for the last candle acquisition run and configured task set.

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs` or keep logic in `candle_acquisition.rs` if smaller
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] Write failing test `market_data_candle_acquisition_status_reports_disabled_by_default` for `GET /market-data/candles/acquisition/status`.
- [ ] Write failing test `market_data_candle_acquisition_status_reports_last_run_summary` after a source-injected run.
- [ ] Run the targeted tests and verify RED.
- [ ] Implement DTO/route/service state with fields: `enabled`, `autostart`, `exchange`, `symbols`, `base_intervals`, `last_run`, `last_error`, `tasks_started`, `tasks_completed`, `pages_fetched`, `storage_records_written`, `final_cursors_count`.
- [ ] Run the targeted tests and verify GREEN.
- [ ] Commit with message `feat: expose candle acquisition status`.

### Task 5.3: Verify Intervals and Official Candle Cross-check

**Goal:** Make `verify_intervals` operational by optionally collecting official exchange candles for reference and comparing against locally maintained base data.

**Files:**
- Modify: `crates/fdc-server/src/market_data/candle_acquisition.rs`
- Test: `crates/fdc-server/tests/candle_acquisition_contract.rs`
- Modify: `docs/runbooks/market-data-production-runbook.md`

- [ ] Write failing test `verify_intervals_expand_as_reference_tasks_not_canonical_tasks`.
- [ ] Write failing test `official_verify_candle_mismatch_is_reported_without_blocking_base_write`.
- [ ] Run targeted tests and verify RED.
- [ ] Implement verify task expansion and mismatch reporting in the run status.
- [ ] Run targeted tests and verify GREEN.
- [ ] Document verification policy and limitations.
- [ ] Commit with message `feat: add candle verify interval checks`.

### Task 5.4: Higher-Interval Candle Aggregation

**Goal:** Generate derived intervals such as `5m`, `15m`, `30m`, `1h`, `4h`, and `1d` from canonical base candles.

**Files:**
- Create: `crates/fdc-server/src/market_data/candle_aggregation.rs` or move to a lower crate if storage-independent logic is preferred
- Test: `crates/fdc-server/tests/candle_aggregation_contract.rs`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [ ] Write failing test `aggregates_five_one_minute_candles_into_one_five_minute_candle` asserting open/high/low/close/volume/trade_count/quote_volume/open_time/close_time.
- [ ] Write failing test `aggregation_skips_incomplete_windows_and_reports_gap`.
- [ ] Run targeted tests and verify RED.
- [ ] Implement deterministic aggregation over queried canonical candles.
- [ ] Run targeted tests and verify GREEN.
- [ ] Decide and document whether derived candles share `candles` collection with interval tags or use a separate derived marker.
- [ ] Commit with message `feat: aggregate derived candle intervals`.

### Task 5.5: General Historical Market Data Maintenance Framework

**Goal:** Extend maintenance beyond candles to supported historical market data types before analytics depends on them.

**Scope candidates:**
- Spot historical trades.
- Futures funding rate.
- Futures open interest.
- Futures mark price.
- Futures index price.

**Files:**
- Modify or generalize: `crates/fdc-server/src/market_data/candle_acquisition.rs` into a broader `historical_acquisition` module.
- Test: new or existing server contract tests for each kind.
- Modify: `docs/runbooks/market-data-production-runbook.md`.

- [ ] Write failing test `historical_acquisition_config_expands_trade_and_derivative_tasks`.
- [ ] Write failing test `historical_acquisition_runner_writes_funding_open_interest_mark_and_index_records`.
- [ ] Run targeted tests and verify RED.
- [ ] Generalize config and runner while preserving candle-specific defaults and tests.
- [ ] Run all Stage 5 tests and verify GREEN.
- [ ] Commit with message `feat: generalize historical market data acquisition`.

### Exit Criteria for Stage 5.x Gate

- All Stage 5.1-5.5 tasks are either `validated` or deliberately re-scoped with a committed roadmap note.
- Final verification includes:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract candle_acquisition_config -- --nocapture
rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test candle_aggregation_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract market_data_candle_acquisition_status -- --nocapture
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract -- --nocapture
rtk cargo check -p fdc-server
```

- `docs/roadmaps/factor-data-stage-status.md` records the validation commits and unresolved risks.
