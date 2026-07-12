# Binance Futures Contract Candle Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Phase 1 Binance Futures USD contract candle/OHLCV acquisition maintenance with storage-backed checkpoint, audit, status API, docs, and bounded validation.

**Architecture:** Follow the existing project architecture. The server owns runtime config, task expansion, checkpoint/audit metadata, and status. `fdc-barter` fetches Binance Futures USD historical pages, `fdc-orchestrator` maps envelopes to canonical storage records, and `fdc-storage` persists records in the existing generic storage boundary. Do not use sidecar files or bypass canonical storage mapping.

**Tech Stack:** Rust, Tokio, Axum, fdc-barter historical fetchers, fdc-orchestrator storage pipeline, fdc-storage `StorageWriteSink`/`QueryableStorage`, serde JSON.

---

## File Structure

- Create: `crates/fdc-server/src/market_data/contract_acquisition.rs`
  - Contract candle task expansion, runner status, checkpoint store, audit writer, Binance Futures USD runner wiring.
- Modify: `crates/fdc-server/src/market_data/mod.rs`
  - Export `contract_acquisition` module.
- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Add `MarketDataContractAcquisitionRuntimeConfig` and env parsing.
- Modify: `crates/fdc-server/src/runtime/app.rs`
  - Add last-run/last-error state and source-injected/autostart methods.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add contract acquisition status response DTOs.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add status response construction.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add `GET /market-data/contracts/acquisition/status`.
- Modify: `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`
  - Add `BinanceFuturesUsdOhlcvHistoricalPageFetcher` wrapper around `execute_binance_futures_usd_ohlcv_rest`.
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs` and `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export the new fetcher.
- Test: `crates/fdc-server/tests/runtime_config_contract.rs`
- Test: `crates/fdc-server/tests/contract_acquisition_contract.rs`
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`
- Docs: `docs/runbooks/market-data-production-runbook.md`
- Docs: `docs/roadmaps/factor-data-stage-status.md`

---

### Task 1: Runtime config for contract candle acquisition

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Test: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Write failing default config test**

Add to `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
#[test]
fn contract_acquisition_config_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("runtime config should parse");

    assert!(!config.market_data_contract_acquisition.enabled);
    assert!(!config.market_data_contract_acquisition.autostart);
    assert_eq!(config.market_data_contract_acquisition.exchange, "binance_futures_usd");
    assert!(config.market_data_contract_acquisition.symbols.is_empty());
    assert_eq!(config.market_data_contract_acquisition.kinds, vec!["candle".to_string()]);
    assert!(config.market_data_contract_acquisition.intervals.is_empty());
    assert_eq!(config.market_data_contract_acquisition.limit_per_page, 1000);
    assert_eq!(config.market_data_contract_acquisition.max_pages_per_run, 1);
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config_is_disabled_by_default -- --nocapture
```

Expected: compile failure because `market_data_contract_acquisition` does not exist.

- [ ] **Step 3: Add config type and field**

In `crates/fdc-server/src/runtime/config.rs`, add after `MarketDataCandleAcquisitionRuntimeConfig`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataContractAcquisitionRuntimeConfig {
    pub enabled: bool,
    pub autostart: bool,
    pub exchange: String,
    pub symbols: Vec<String>,
    pub kinds: Vec<String>,
    pub intervals: Vec<String>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit_per_page: usize,
    pub max_pages_per_run: usize,
}

impl Default for MarketDataContractAcquisitionRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            autostart: false,
            exchange: "binance_futures_usd".to_string(),
            symbols: Vec::new(),
            kinds: vec!["candle".to_string()],
            intervals: Vec::new(),
            start_ns: None,
            end_ns: None,
            limit_per_page: 1000,
            max_pages_per_run: 1,
        }
    }
}
```

Add to `ServerRuntimeConfig`:

```rust
pub market_data_contract_acquisition: MarketDataContractAcquisitionRuntimeConfig,
```

Initialize `let mut market_data_contract_acquisition = MarketDataContractAcquisitionRuntimeConfig::default();` in `from_env_pairs`, validate it before `Ok(Self { ... })`, and include it in the returned struct.

- [ ] **Step 4: Run default test**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config_is_disabled_by_default -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Write env override and validation tests**

Add to `runtime_config_contract.rs`:

```rust
#[test]
fn contract_acquisition_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_AUTOSTART", "yes"),
        ("FDC_MARKET_DATA_CONTRACTS_EXCHANGE", "binance_futures_usd"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "btcusdt,ETHUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_KINDS", "candle"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m,5m"),
        ("FDC_MARKET_DATA_CONTRACTS_START_NS", "1000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_END_NS", "2000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE", "500"),
        ("FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN", "3"),
    ])
    .expect("runtime config should parse");

    assert!(config.market_data_contract_acquisition.enabled);
    assert!(config.market_data_contract_acquisition.autostart);
    assert_eq!(config.market_data_contract_acquisition.exchange, "binance_futures_usd");
    assert_eq!(config.market_data_contract_acquisition.symbols, vec!["BTCUSDT", "ETHUSDT"]);
    assert_eq!(config.market_data_contract_acquisition.kinds, vec!["candle"]);
    assert_eq!(config.market_data_contract_acquisition.intervals, vec!["1m", "5m"]);
    assert_eq!(config.market_data_contract_acquisition.start_ns, Some(1_000_000_000));
    assert_eq!(config.market_data_contract_acquisition.end_ns, Some(2_000_000_000));
    assert_eq!(config.market_data_contract_acquisition.limit_per_page, 500);
    assert_eq!(config.market_data_contract_acquisition.max_pages_per_run, 3);
}

#[test]
fn contract_acquisition_config_rejects_invalid_phase1_values() {
    let missing_symbols = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("enabled contract acquisition requires symbols");
    assert!(missing_symbols.to_string().contains("FDC_MARKET_DATA_CONTRACTS_SYMBOLS"));

    let missing_intervals = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
    ])
    .expect_err("enabled contract candle acquisition requires intervals");
    assert!(missing_intervals.to_string().contains("FDC_MARKET_DATA_CONTRACTS_INTERVALS"));

    let unsupported_exchange = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_EXCHANGE", "binance_spot"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("phase1 supports only binance_futures_usd");
    assert!(unsupported_exchange.to_string().contains("binance_futures_usd"));

    let unsupported_kind = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_KINDS", "funding_rate"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("phase1 supports only candle kind");
    assert!(unsupported_kind.to_string().contains("FDC_MARKET_DATA_CONTRACTS_KINDS"));
}
```

- [ ] **Step 6: Implement env parsing and validation**

Add match arms in `from_env_pairs`:

```rust
"FDC_MARKET_DATA_CONTRACTS_ENABLED" => {
    market_data_contract_acquisition.enabled = parse_bool(value.as_ref());
}
"FDC_MARKET_DATA_CONTRACTS_AUTOSTART" => {
    market_data_contract_acquisition.autostart = parse_bool(value.as_ref());
}
"FDC_MARKET_DATA_CONTRACTS_EXCHANGE" => {
    market_data_contract_acquisition.exchange = parse_non_empty_string(
        "FDC_MARKET_DATA_CONTRACTS_EXCHANGE",
        value.as_ref(),
    )?;
}
"FDC_MARKET_DATA_CONTRACTS_SYMBOLS" => {
    market_data_contract_acquisition.symbols =
        parse_comma_list_uppercase("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", value.as_ref())?;
}
"FDC_MARKET_DATA_CONTRACTS_KINDS" => {
    market_data_contract_acquisition.kinds =
        parse_comma_list("FDC_MARKET_DATA_CONTRACTS_KINDS", value.as_ref())?;
}
"FDC_MARKET_DATA_CONTRACTS_INTERVALS" => {
    market_data_contract_acquisition.intervals =
        parse_comma_list("FDC_MARKET_DATA_CONTRACTS_INTERVALS", value.as_ref())?;
}
"FDC_MARKET_DATA_CONTRACTS_START_NS" => {
    market_data_contract_acquisition.start_ns = Some(parse_i64(
        "FDC_MARKET_DATA_CONTRACTS_START_NS",
        value.as_ref(),
    )?);
}
"FDC_MARKET_DATA_CONTRACTS_END_NS" => {
    market_data_contract_acquisition.end_ns = Some(parse_i64(
        "FDC_MARKET_DATA_CONTRACTS_END_NS",
        value.as_ref(),
    )?);
}
"FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE" => {
    market_data_contract_acquisition.limit_per_page = parse_usize_range(
        "FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE",
        value.as_ref(),
        1,
        1000,
    )?;
}
"FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN" => {
    market_data_contract_acquisition.max_pages_per_run = parse_usize_range(
        "FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN",
        value.as_ref(),
        1,
        10000,
    )?;
}
```

Add validation function:

```rust
fn validate_contract_acquisition_config(
    config: &MarketDataContractAcquisitionRuntimeConfig,
) -> Result<()> {
    if config.exchange != "binance_futures_usd" {
        return Err(Error::config(
            "FDC_MARKET_DATA_CONTRACTS_EXCHANGE must be binance_futures_usd in phase 1",
        ));
    }
    if config.kinds != ["candle".to_string()] {
        return Err(Error::config(
            "FDC_MARKET_DATA_CONTRACTS_KINDS must be candle in phase 1",
        ));
    }
    if config.enabled {
        if config.symbols.is_empty() {
            return Err(Error::config(
                "FDC_MARKET_DATA_CONTRACTS_SYMBOLS must not be empty when contract acquisition is enabled",
            ));
        }
        if config.intervals.is_empty() {
            return Err(Error::config(
                "FDC_MARKET_DATA_CONTRACTS_INTERVALS must not be empty when contract candle acquisition is enabled",
            ));
        }
    }
    if let (Some(start), Some(end)) = (config.start_ns, config.end_ns) {
        if end <= start {
            return Err(Error::config(
                "FDC_MARKET_DATA_CONTRACTS_END_NS must be greater than FDC_MARKET_DATA_CONTRACTS_START_NS",
            ));
        }
    }
    Ok(())
}
```

Call `validate_contract_acquisition_config(&market_data_contract_acquisition)?;` after candle validation.

- [ ] **Step 7: Run config tests and commit**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config -- --nocapture
```

Expected: 3 contract acquisition config tests pass.

Commit:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat: add contract acquisition runtime config"
```

---

### Task 2: Barter futures candle page fetcher export

**Files:**
- Modify: `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Test: `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_execution_contract.rs`

- [ ] **Step 1: Write failing fetcher contract test**

Add to `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_execution_contract.rs`:

```rust
#[tokio::test]
async fn binance_futures_ohlcv_page_fetcher_delegates_to_rest_executor() {
    use fdc_barter::{BinanceFuturesUsdOhlcvHistoricalPageFetcher, HistoricalPageFetcher};

    let fetcher = BinanceFuturesUsdOhlcvHistoricalPageFetcher::new(&FakeFuturesExecutor);
    let page = fetcher
        .fetch_page(request(BarterMarketDataKind::Candle))
        .await
        .expect("futures candle page should fetch");

    assert_eq!(page.envelopes.len(), 1);
    assert_eq!(page.envelopes[0].event.exchange, "binance_futures_usd");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Candle);
}
```


- [ ] **Step 2: Run failing fetcher test**

Run:

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract binance_futures_ohlcv_page_fetcher_delegates_to_rest_executor -- --nocapture
```

Expected: compile failure because `BinanceFuturesUsdOhlcvHistoricalPageFetcher` is not exported.

- [ ] **Step 3: Implement fetcher wrapper**

In `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`, add after `BinanceSpotOhlcvHistoricalPageFetcher`:

```rust
#[derive(Clone, Copy)]
pub struct BinanceFuturesUsdOhlcvHistoricalPageFetcher<'a> {
    executor: &'a dyn HistoricalRestExecutor,
}

impl<'a> BinanceFuturesUsdOhlcvHistoricalPageFetcher<'a> {
    pub fn new(executor: &'a dyn HistoricalRestExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl HistoricalPageFetcher for BinanceFuturesUsdOhlcvHistoricalPageFetcher<'_> {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        execute_binance_futures_usd_ohlcv_rest(self.executor, request).await
    }
}
```

Ensure the file imports `execute_binance_futures_usd_ohlcv_rest` alongside the existing spot executor import.

- [ ] **Step 4: Re-export fetcher**

In `crates/fdc-adapter/barter/src/ingestion/mod.rs`, add `BinanceFuturesUsdOhlcvHistoricalPageFetcher` to the `pub use acquisition::{ ... }` list.

In `crates/fdc-adapter/barter/src/lib.rs`, add it to the `pub use ingestion::{ ... }` list.

- [ ] **Step 5: Run fetcher test and commit**

Run:

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract binance_futures_ohlcv_page_fetcher_delegates_to_rest_executor -- --nocapture
```

Expected: pass.

Commit:

```bash
git add crates/fdc-adapter/barter/src/ingestion/acquisition.rs crates/fdc-adapter/barter/src/ingestion/mod.rs crates/fdc-adapter/barter/src/lib.rs crates/fdc-adapter/barter/tests/binance_futures_historical_rest_execution_contract.rs
git commit -m "feat: expose futures candle page fetcher"
```

---

### Task 3: Contract candle acquisition runner with checkpoint and audit

**Files:**
- Create: `crates/fdc-server/src/market_data/contract_acquisition.rs`
- Modify: `crates/fdc-server/src/market_data/mod.rs`
- Test: `crates/fdc-server/tests/contract_acquisition_contract.rs`

- [ ] **Step 1: Write failing task expansion test**

Create `crates/fdc-server/tests/contract_acquisition_contract.rs` with:

```rust
use fdc_barter::{
    BarterMarketDataKind, BarterMarketType, HistoricalBackfillPage, HistoricalBackfillRequest,
    HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::types::TimestampNs;
use fdc_server::{
    MarketDataContractAcquisitionRuntimeConfig,
    market_data::contract_acquisition::expand_contract_backfill_requests,
};

fn config() -> MarketDataContractAcquisitionRuntimeConfig {
    MarketDataContractAcquisitionRuntimeConfig {
        enabled: true,
        autostart: false,
        exchange: "binance_futures_usd".to_string(),
        symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        kinds: vec!["candle".to_string()],
        intervals: vec!["1m".to_string(), "5m".to_string()],
        start_ns: Some(1_700_000_000_000_000_000),
        end_ns: Some(1_700_000_060_000_000_000),
        limit_per_page: 500,
        max_pages_per_run: 2,
    }
}

#[test]
fn contract_candle_requests_expand_for_symbols_and_intervals() {
    let requests = expand_contract_backfill_requests(&config()).expect("requests should expand");

    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|request| request.exchange == "binance_futures_usd"));
    assert!(requests.iter().all(|request| request.market_type == BarterMarketType::Perpetual));
    assert!(requests.iter().all(|request| request.kind == BarterMarketDataKind::Candle));
    assert!(requests.iter().any(|request| request.symbol == "BTCUSDT" && request.interval.as_deref() == Some("1m")));
    assert!(requests.iter().any(|request| request.symbol == "BTCUSDT" && request.interval.as_deref() == Some("5m")));
    assert!(requests.iter().any(|request| request.symbol == "ETHUSDT" && request.interval.as_deref() == Some("1m")));
    assert!(requests.iter().any(|request| request.symbol == "ETHUSDT" && request.interval.as_deref() == Some("5m")));
}
```

- [ ] **Step 2: Run failing task expansion test**

Run:

```bash
rtk cargo test -p fdc-server --test contract_acquisition_contract contract_candle_requests_expand_for_symbols_and_intervals -- --nocapture
```

Expected: compile failure because `contract_acquisition` and `MarketDataContractAcquisitionRuntimeConfig` are not exported yet.

- [ ] **Step 3: Add module export and minimal request expansion**

In `crates/fdc-server/src/market_data/mod.rs`, add:

```rust
pub mod contract_acquisition;
```

In `crates/fdc-server/src/market_data/contract_acquisition.rs`, add:

```rust
use std::sync::Mutex;

use chrono::Utc;
use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketType, BinanceFuturesUsdOhlcvHistoricalPageFetcher, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::{error::Error, types::TimestampNs, Result};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{
    QueryableStorage, StorageQuery, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
    StorageWriteSink,
};
use serde::{Deserialize, Serialize};

use crate::runtime::config::MarketDataContractAcquisitionRuntimeConfig;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContractAcquisitionRunStatus {
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
    pub final_cursors: Vec<HistoricalCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContractCheckpointKey {
    pub exchange: String,
    pub symbol: String,
    pub kind: String,
    pub interval: Option<String>,
}

impl ContractCheckpointKey {
    fn from_request(request: &HistoricalBackfillRequest) -> Self {
        Self {
            exchange: request.exchange.clone(),
            symbol: request.symbol.clone(),
            kind: "candle".to_string(),
            interval: request.interval.clone(),
        }
    }
}

pub fn expand_contract_backfill_requests(
    config: &MarketDataContractAcquisitionRuntimeConfig,
) -> Result<Vec<HistoricalBackfillRequest>> {
    let start = TimestampNs::from_nanos(config.start_ns.unwrap_or(0));
    let end = TimestampNs::from_nanos(config.end_ns.unwrap_or(i64::MAX));
    let source_id = format!("barter:{}:historical:contract:candle", config.exchange);
    let mut requests = Vec::new();

    for symbol in &config.symbols {
        for interval in &config.intervals {
            requests.push(HistoricalBackfillRequest {
                source_id: source_id.clone(),
                exchange: config.exchange.clone(),
                market_type: BarterMarketType::Perpetual,
                symbol: symbol.clone(),
                kind: BarterMarketDataKind::Candle,
                interval: Some(interval.clone()),
                start,
                end,
                limit: Some(config.limit_per_page),
                cursor: None,
            });
        }
    }

    Ok(requests)
}
```

Ensure `crates/fdc-server/src/lib.rs` publicly exports `MarketDataContractAcquisitionRuntimeConfig` next to other runtime config exports if required by the test.

- [ ] **Step 4: Run expansion test**

Run:

```bash
rtk cargo test -p fdc-server --test contract_acquisition_contract contract_candle_requests_expand_for_symbols_and_intervals -- --nocapture
```

Expected: pass.

- [ ] **Step 5: Write checkpoint roundtrip test**

Append to `contract_acquisition_contract.rs`:

```rust
use fdc_server::market_data::contract_acquisition::{
    ContractCheckpointKey, ContractCheckpointStore, StorageBackedContractCheckpointStore,
};
use fdc_storage::QueryableMarketDataStore;

#[test]
fn storage_backed_contract_checkpoint_store_round_trips_cursor() {
    let store = QueryableMarketDataStore::new();
    let checkpoint_store = StorageBackedContractCheckpointStore::new(&store);
    let key = ContractCheckpointKey {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: "candle".to_string(),
        interval: Some("1m".to_string()),
    };
    let cursor = HistoricalCursor {
        exchange: "binance_futures_usd".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        next_start: Some(TimestampNs::from_nanos(1_700_000_060_000_000_001)),
        page_token: Some("resume-token".to_string()),
        last_seen_exchange_id: Some("futures-kline-1".to_string()),
    };

    checkpoint_store.save(key.clone(), cursor.clone()).expect("save should work");
    let loaded = checkpoint_store.load(&key).expect("load should work").expect("cursor should exist");

    assert_eq!(loaded, cursor);
    let records = store.all_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].collection, "contract_checkpoints");
    assert_eq!(records[0].metadata.tags.get("kind").map(String::as_str), Some("contract_checkpoint"));
}
```

- [ ] **Step 6: Implement checkpoint store**

Add to `contract_acquisition.rs`:

```rust
pub trait ContractCheckpointStore: Send + Sync {
    fn load(&self, key: &ContractCheckpointKey) -> Result<Option<HistoricalCursor>>;
    fn save(&self, key: ContractCheckpointKey, cursor: HistoricalCursor) -> Result<()>;
}

const CONTRACT_CHECKPOINT_COLLECTION: &str = "contract_checkpoints";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContractCheckpointStorageValue {
    cursor: HistoricalCursor,
    updated_at_ns: i64,
}

pub struct StorageBackedContractCheckpointStore<'a, S> {
    storage: &'a S,
}

impl<'a, S> StorageBackedContractCheckpointStore<'a, S> {
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }
}

impl<S> ContractCheckpointStore for StorageBackedContractCheckpointStore<'_, S>
where
    S: StorageWriteSink + QueryableStorage,
{
    fn load(&self, key: &ContractCheckpointKey) -> Result<Option<HistoricalCursor>> {
        let records = futures::executor::block_on(self.storage.query_storage(
            &StorageQuery::new("market_data")
                .with_collection(CONTRACT_CHECKPOINT_COLLECTION)
                .with_key_prefix(contract_checkpoint_storage_key(key)),
        ))?;
        let Some(record) = records.last() else {
            return Ok(None);
        };
        let value: ContractCheckpointStorageValue = serde_json::from_slice(&record.value)
            .map_err(|error| Error::internal(error.to_string()))?;
        Ok(Some(value.cursor))
    }

    fn save(&self, key: ContractCheckpointKey, cursor: HistoricalCursor) -> Result<()> {
        let storage_key = contract_checkpoint_storage_key(&key);
        let value = ContractCheckpointStorageValue {
            cursor,
            updated_at_ns: TimestampNs::now().as_nanos(),
        };
        let mut metadata = StorageWriteMetadata::default();
        metadata.content_type = Some("application/json".to_string());
        metadata.schema = Some("contract_checkpoint".to_string());
        metadata.schema_version = Some("1".to_string());
        metadata.source = Some("fdc-server:contract_acquisition".to_string());
        metadata.tags.insert("kind".to_string(), "contract_checkpoint".to_string());
        metadata.tags.insert("exchange".to_string(), key.exchange);
        metadata.tags.insert("symbol".to_string(), key.symbol);
        metadata.tags.insert("data_kind".to_string(), key.kind);
        metadata.tags.insert(
            "interval".to_string(),
            key.interval.unwrap_or_else(|| "none".to_string()),
        );
        let record = StorageWriteRecord::new(
            "market_data",
            CONTRACT_CHECKPOINT_COLLECTION,
            storage_key,
            serde_json::to_vec(&value).map_err(|error| Error::internal(error.to_string()))?,
        )
        .with_timestamp(Utc::now())
        .with_metadata(metadata);
        futures::executor::block_on(self.storage.write_batch(StorageWriteBatch::new(vec![record])))?;
        Ok(())
    }
}

fn contract_checkpoint_storage_key(key: &ContractCheckpointKey) -> Vec<u8> {
    format!(
        "{}:{}:{}:{}",
        key.exchange,
        key.symbol,
        key.kind,
        key.interval.clone().unwrap_or_else(|| "none".to_string())
    )
    .into_bytes()
}
```

- [ ] **Step 7: Write source-injected runner test with audit**

Append a source-injected test with a local scripted source and local candle-envelope helpers. The test must assert:

```rust
assert_eq!(status.tasks_started, 1);
assert_eq!(status.tasks_completed, 1);
assert_eq!(status.pages_fetched, 1);
assert_eq!(status.envelopes_received, 1);
assert_eq!(status.storage_records_written, 1);
assert_eq!(status.audit_records_written, 1);
assert_eq!(store.query(&fdc_storage::MarketDataQuery::for_candles().with_symbol("BTCUSDT")).len(), 1);
assert!(store.all_records().iter().any(|record| record.collection == "contract_checkpoints"));
assert!(store.all_records().iter().any(|record| record.collection == "contract_acquisition_audits"));
```

Use this local source in `contract_acquisition_contract.rs`:

```rust
#[derive(Debug)]
struct ScriptedContractCandleSource {
    pages: std::sync::Mutex<Vec<HistoricalBackfillPage>>,
}

impl ScriptedContractCandleSource {
    fn new(mut pages: Vec<HistoricalBackfillPage>) -> Self {
        pages.reverse();
        Self { pages: std::sync::Mutex::new(pages) }
    }
}

#[async_trait::async_trait]
impl HistoricalPageFetcher for ScriptedContractCandleSource {
    async fn fetch_page(&self, _request: HistoricalBackfillRequest) -> fdc_barter::Result<HistoricalBackfillPage> {
        self.pages
            .lock()
            .expect("scripted source lock")
            .pop()
            .ok_or_else(|| fdc_barter::BarterAdapterError::InvalidHistoricalRequest("no scripted page".to_string()))
    }
}
```

Add local candle payload/envelope builders equivalent to the existing candle acquisition contract helpers: construct a `CandlePayload` with interval/open_time/close_time/OHLCV, wrap it in `BarterMarketPayload::Candle`, and build a `BarterIngestionEnvelope` with `exchange="binance_futures_usd"`, `market_type=BarterMarketType::Perpetual`, `kind=BarterMarketDataKind::Candle`, and `quality.is_backfill=true`.

- [ ] **Step 8: Implement audit writer and runner**

Add to `contract_acquisition.rs`:

```rust
const CONTRACT_ACQUISITION_AUDIT_COLLECTION: &str = "contract_acquisition_audits";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContractAcquisitionAuditStorageValue {
    run_id: String,
    exchange: String,
    symbol: String,
    kind: String,
    interval: Option<String>,
    pages_fetched: usize,
    envelopes_received: usize,
    storage_records_written: usize,
    final_cursor: Option<HistoricalCursor>,
    updated_at_ns: i64,
}
```

Implement `write_contract_acquisition_audit(...) -> Result<()>` using collection `contract_acquisition_audits`, metadata kind `contract_acquisition_audit`, and key `run_id:symbol:kind:interval_or_none:updated_at_ns`.

Implement:

```rust
pub async fn run_contract_acquisition_once_with_checkpoints<S, W, C>(
    config: &MarketDataContractAcquisitionRuntimeConfig,
    source: &S,
    storage_sink: &W,
    checkpoint_store: &C,
) -> Result<ContractAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
    W: StorageWriteSink,
    C: ContractCheckpointStore,
```

Runner behavior:

1. Expand requests.
2. For each request, load checkpoint and apply `request.start = cursor.next_start` when available.
3. Run `run_historical_backfill_pages` with `max_pages_per_run`.
4. Write pages through `run_barter_envelopes_to_storage_once`.
5. Save final cursor to checkpoint store when present.
6. Write one audit record per task.
7. Update status counts.

Also add:

```rust
pub async fn run_binance_futures_usd_contract_candle_acquisition_once<W>(
    config: &MarketDataContractAcquisitionRuntimeConfig,
    storage_sink: &W,
) -> Result<ContractAcquisitionRunStatus>
where
    W: StorageWriteSink + QueryableStorage,
```

This function should build `BarterIntegrationHistoricalRestExecutor::binance_futures_usd()`, `BinanceFuturesUsdOhlcvHistoricalPageFetcher::new(&executor)`, and `StorageBackedContractCheckpointStore::new(storage_sink)`.

- [ ] **Step 9: Run contract acquisition tests and commit**

Run:

```bash
rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
```

Expected: all contract acquisition tests pass.

Commit:

```bash
git add crates/fdc-server/src/market_data/contract_acquisition.rs crates/fdc-server/src/market_data/mod.rs crates/fdc-server/tests/contract_acquisition_contract.rs
git commit -m "feat: add contract candle acquisition runner"
```

---

### Task 4: Production state and contract acquisition status API

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing disabled status API test**

Add to `production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn market_data_contract_acquisition_status_reports_disabled_defaults() {
    let state = ProductionServerState::new(ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap());
    let app = build_production_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/market-data/contracts/acquisition/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["enabled"], false);
    assert_eq!(json["data"]["autostart"], false);
    assert_eq!(json["data"]["exchange"], "binance_futures_usd");
    assert_eq!(json["data"]["kinds"], serde_json::json!(["candle"]));
    assert!(json["data"]["last_run"].is_null());
    assert!(json["data"]["last_error"].is_null());
}
```

- [ ] **Step 2: Run failing status API test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition_status_reports_disabled_defaults -- --nocapture
```

Expected: 404 or compile failure because endpoint/DTO does not exist.

- [ ] **Step 3: Add DTOs**

In `crates/fdc-server/src/market_data/model.rs`, add near candle acquisition DTOs:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataContractAcquisitionRunStatusResponse {
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
    pub final_cursors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataContractAcquisitionStatusResponse {
    pub enabled: bool,
    pub autostart: bool,
    pub exchange: String,
    pub symbols: Vec<String>,
    pub kinds: Vec<String>,
    pub intervals: Vec<String>,
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit_per_page: usize,
    pub max_pages_per_run: usize,
    pub last_run: Option<MarketDataContractAcquisitionRunStatusResponse>,
    pub last_error: Option<String>,
}
```

- [ ] **Step 4: Add ProductionServerState fields and methods**

In `crates/fdc-server/src/runtime/app.rs`, add fields:

```rust
market_data_contract_acquisition_last_run:
    Arc<Mutex<Option<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus>>>,
market_data_contract_acquisition_last_error: Arc<Mutex<Option<String>>>,
```

Initialize both in `new`, `try_new`, and `with_market_data_store`.

Add methods mirroring candle methods:

```rust
pub fn market_data_contract_acquisition_last_run(
    &self,
) -> Option<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus> {
    self.market_data_contract_acquisition_last_run
        .lock()
        .expect("contract acquisition status lock")
        .clone()
}

pub fn market_data_contract_acquisition_last_error(&self) -> Option<String> {
    self.market_data_contract_acquisition_last_error
        .lock()
        .expect("contract acquisition error lock")
        .clone()
}

fn record_contract_acquisition_result(
    &self,
    result: &Result<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus>,
) {
    match result {
        Ok(status) => {
            *self.market_data_contract_acquisition_last_run.lock().expect("contract acquisition status lock") = Some(status.clone());
            *self.market_data_contract_acquisition_last_error.lock().expect("contract acquisition error lock") = None;
        }
        Err(error) => {
            *self.market_data_contract_acquisition_last_error.lock().expect("contract acquisition error lock") = Some(error.to_string());
        }
    }
}
```

Add source-injected runner and autostart method:

```rust
pub async fn run_contract_acquisition_once_with_source<S>(
    &self,
    source: &S,
) -> Result<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus>
where
    S: HistoricalPageFetcher + ?Sized,
{
    let checkpoint_store = crate::market_data::contract_acquisition::StorageBackedContractCheckpointStore::new(
        self.market_data_store.as_ref(),
    );
    let result = crate::market_data::contract_acquisition::run_contract_acquisition_once_with_checkpoints(
        &self.config.market_data_contract_acquisition,
        source,
        self.market_data_store.as_ref(),
        &checkpoint_store,
    )
    .await;
    self.record_contract_acquisition_result(&result);
    result
}

pub async fn start_contract_acquisition_autostart_if_enabled(
    &self,
) -> Result<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus> {
    if !(self.config.market_data_contract_acquisition.enabled
        && self.config.market_data_contract_acquisition.autostart)
    {
        let result = Ok(crate::market_data::contract_acquisition::ContractAcquisitionRunStatus::default());
        self.record_contract_acquisition_result(&result);
        return result;
    }

    let result = crate::market_data::contract_acquisition::run_binance_futures_usd_contract_candle_acquisition_once(
        &self.config.market_data_contract_acquisition,
        self.market_data_store.as_ref(),
    )
    .await;
    self.record_contract_acquisition_result(&result);
    result
}
```

- [ ] **Step 5: Add service status function**

In `crates/fdc-server/src/market_data/service.rs`, import the new DTOs and add:

```rust
pub fn contract_acquisition_status(
    state: &ProductionServerState,
) -> MarketDataContractAcquisitionStatusResponse {
    let config = &state.config().market_data_contract_acquisition;
    let last_run = state
        .market_data_contract_acquisition_last_run()
        .map(|status| MarketDataContractAcquisitionRunStatusResponse {
            tasks_started: status.tasks_started,
            tasks_completed: status.tasks_completed,
            pages_fetched: status.pages_fetched,
            envelopes_received: status.envelopes_received,
            storage_records_written: status.storage_records_written,
            audit_records_written: status.audit_records_written,
            final_cursors: status.final_cursors.len(),
        });

    MarketDataContractAcquisitionStatusResponse {
        enabled: config.enabled,
        autostart: config.autostart,
        exchange: config.exchange.clone(),
        symbols: config.symbols.clone(),
        kinds: config.kinds.clone(),
        intervals: config.intervals.clone(),
        start_ns: config.start_ns,
        end_ns: config.end_ns,
        limit_per_page: config.limit_per_page,
        max_pages_per_run: config.max_pages_per_run,
        last_run,
        last_error: state.market_data_contract_acquisition_last_error(),
    }
}
```

- [ ] **Step 6: Add router endpoint**

In `crates/fdc-server/src/market_data/router.rs`, import `MarketDataContractAcquisitionStatusResponse` and `contract_acquisition_status`.

Add route:

```rust
.route(
    "/market-data/contracts/acquisition/status",
    get(contract_acquisition_status_handler),
)
```

Add handler:

```rust
async fn contract_acquisition_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataContractAcquisitionStatusResponse>> {
    Json(ServerApiResponse::success(contract_acquisition_status(&state)))
}
```

- [ ] **Step 7: Write last-run status API test**

Add to `production_server_router_contract.rs` a source-injected run, then request status. Assert:

```rust
assert_eq!(json["data"]["last_run"]["tasks_started"], 1);
assert_eq!(json["data"]["last_run"]["tasks_completed"], 1);
assert_eq!(json["data"]["last_run"]["storage_records_written"], 1);
assert_eq!(json["data"]["last_run"]["audit_records_written"], 1);
```

Use the same scripted futures candle source helpers from `contract_acquisition_contract.rs` copied into this router test file.

- [ ] **Step 8: Run status API tests and commit**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition_status_reports_ -- --nocapture
```

Expected: disabled and last-run tests pass.

Commit:

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat: expose contract acquisition status"
```

---

### Task 5: Binary autostart wiring and docs

**Files:**
- Modify: `crates/fdc-server/src/bin/fdc_server.rs`
- Modify: `docs/runbooks/market-data-production-runbook.md`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [ ] **Step 1: Wire contract autostart after candle autostart**

In `crates/fdc-server/src/bin/fdc_server.rs`, replace the startup sequence:

```rust
state.start_live_autostart_if_enabled().await?;
state.start_candle_acquisition_autostart_if_enabled().await?;
```

with:

```rust
state.start_live_autostart_if_enabled().await?;
state.start_candle_acquisition_autostart_if_enabled().await?;
state.start_contract_acquisition_autostart_if_enabled().await?;
```

- [ ] **Step 2: Update runbook**

In `docs/runbooks/market-data-production-runbook.md`, insert the section below immediately after `## Candle acquisition operator run`:

```bash
set -a
. ./.env.p39.local
export FDC_MARKET_DATA_CONTRACTS_ENABLED=1
export FDC_MARKET_DATA_CONTRACTS_AUTOSTART=1
export FDC_MARKET_DATA_CONTRACTS_EXCHANGE=binance_futures_usd
export FDC_MARKET_DATA_CONTRACTS_SYMBOLS=BTCUSDT
export FDC_MARKET_DATA_CONTRACTS_KINDS=candle
export FDC_MARKET_DATA_CONTRACTS_INTERVALS=1m
export FDC_MARKET_DATA_CONTRACTS_START_NS=1700000000000000000
export FDC_MARKET_DATA_CONTRACTS_END_NS=1700000060000000000
export FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE=100
export FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN=1
set +a
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo run -p fdc-server
```

Document expected checks:

```bash
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/contracts/acquisition/status'
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/candles?symbol=BTCUSDT&limit=10'
```

State that `contract_checkpoints` and `contract_acquisition_audits` are maintenance metadata collections and that Phase 1 only enables futures candle/OHLCV.

- [ ] **Step 3: Update roadmap**

In `docs/roadmaps/factor-data-stage-status.md`, add a Stage 5.6 row:

```markdown
| Stage 5.6 | Binance Futures USD contract candle maintenance Phase 1 | `validated` after implementation | yes before derivatives expansion | Start with one contract data kind, then expand funding/open-interest/mark/index using the same pattern. |
```

Add notes that Phase 1 does not yet enable all derivative kinds.

- [ ] **Step 4: Commit docs/autostart**

Run:

```bash
rtk cargo check -p fdc-server
```

Expected: 0 errors.

Commit:

```bash
git add crates/fdc-server/src/bin/fdc_server.rs docs/runbooks/market-data-production-runbook.md docs/roadmaps/factor-data-stage-status.md
git commit -m "docs: add contract candle acquisition runbook"
```

---

### Task 6: Final validation

**Files:**
- No code changes unless validation exposes a bug.

- [ ] **Step 1: Run focused test suite**

Run:

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract binance_futures_ohlcv_page_fetcher_delegates_to_rest_executor -- --nocapture
rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config -- --nocapture
rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition_status_reports_ -- --nocapture
rtk cargo check -p fdc-server
```

Expected:

```text
futures fetcher test: passed
runtime config contract acquisition tests: passed
contract acquisition contract tests: passed
contract acquisition status API tests: passed
fdc-server cargo check: 0 errors, existing warnings only
```

- [ ] **Step 2: Update plan checkboxes**

Mark completed steps in this file with `[x]` for tasks that were executed.

- [ ] **Step 3: Commit validation record if plan/docs changed**

If only this plan file changed, commit it:

```bash
git add docs/superpowers/plans/2026-07-12-binance-futures-contract-candle-maintenance.md
git commit -m "docs: plan contract candle maintenance"
```

- [ ] **Step 4: Confirm clean worktree**

Run:

```bash
rtk git status --short
```

Expected:

```text
ok
```
