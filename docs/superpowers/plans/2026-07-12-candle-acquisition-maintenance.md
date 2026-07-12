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
