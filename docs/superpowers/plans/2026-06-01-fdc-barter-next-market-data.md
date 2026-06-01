# fdc-barter Next Market Data Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the next `fdc-barter` market-data slices for candles, Binance Futures USD live acquisition, historical backfill boundaries, and adapter quality metadata while preserving crate boundaries.

**Architecture:** Keep Barter-rs integration and exchange-specific models inside `fdc-barter`. Add offline contract tests before implementation. Keep production runtime, storage, SQL, and API behavior out of this module; downstream crates consume only adapter-owned envelopes and public model types.

**Tech Stack:** Rust, Cargo, Barter-rs local path crates, `chrono`, `rust_decimal`, `serde`, `tokio`, ignored live smoke tests, markdown development docs.

---

## File Structure

### Modify

- `crates/fdc-adapter/barter/src/model/event.rs`
  - Extend `CandlePayload` with `interval`, `trade_count`, and `quote_volume`.
- `crates/fdc-adapter/barter/src/model/mod.rs`
  - Re-export any new historical or metrics model modules.
- `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export new public APIs.
- `crates/fdc-adapter/barter/src/mapper/event.rs`
  - Map `DataKind::Candle` into `BarterMarketPayload::Candle`.
- `crates/fdc-adapter/barter/src/ingestion/live.rs`
  - Add Binance Futures USD subscription defaults and initializer.
- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Export any new historical modules.
- `crates/fdc-adapter/barter/src/ingestion/envelope.rs`
  - Add helper constructors for backfill/replay quality flags if needed.
- `crates/fdc-adapter/barter/src/error.rs`
  - Add validation/backfill errors if needed.
- `docs/DEVELOPMENT_STATUS.md`
  - Append checkpoints after each completed task.

### Create

- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
  - Historical page request/result/provider boundary.
- `crates/fdc-adapter/barter/src/model/quality.rs`
  - Adapter quality/runtime observation types if they outgrow `envelope.rs`.
- `crates/fdc-adapter/barter/tests/candle_mapper_contract.rs`
  - Offline candle payload and mapper tests.
- `crates/fdc-adapter/barter/tests/binance_futures_live_contract.rs`
  - Offline initializer validation and ignored smoke compile coverage.
- `crates/fdc-adapter/barter/tests/historical_ohlcv_contract.rs`
  - Offline historical OHLCV page/checkpoint contracts.
- `crates/fdc-adapter/barter/tests/historical_trades_contract.rs`
  - Offline historical trades page/dedupe/checkpoint contracts.
- `crates/fdc-adapter/barter/tests/quality_metadata_contract.rs`
  - Offline quality/runtime metadata contracts.

---

## Task 1: Candle/OHLCV Payload and Mapper Contract

**Files:**
- Create: `crates/fdc-adapter/barter/tests/candle_mapper_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/model/event.rs`
- Modify: `crates/fdc-adapter/barter/src/mapper/event.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Write failing candle model and mapper tests**

Create `crates/fdc-adapter/barter/tests/candle_mapper_contract.rs` with tests that assert:

```rust
use fdc_barter::{BarterMarketDataKind, BarterMarketPayload};

#[test]
fn candle_payload_exposes_research_fields() {
    let payload = fdc_barter::CandlePayload {
        interval: Some("1m".to_string()),
        open_time: fdc_core::types::TimestampNs::from_nanos(1_700_000_000_000_000_000),
        close_time: fdc_core::types::TimestampNs::from_nanos(1_700_000_060_000_000_000),
        open: fdc_core::types::Price::from_f64(100.0).unwrap(),
        high: fdc_core::types::Price::from_f64(110.0).unwrap(),
        low: fdc_core::types::Price::from_f64(90.0).unwrap(),
        close: fdc_core::types::Price::from_f64(105.0).unwrap(),
        volume: rust_decimal::Decimal::new(12345, 2),
        trade_count: Some(42),
        quote_volume: Some(rust_decimal::Decimal::new(129_622_500, 2)),
    };

    assert_eq!(payload.interval.as_deref(), Some("1m"));
    assert_eq!(payload.trade_count, Some(42));
    assert_eq!(payload.quote_volume.unwrap().to_string(), "1296225.00");
    assert_eq!(BarterMarketPayload::Candle(payload).kind(), BarterMarketDataKind::Candle);
}
```

Also add one mapper test using Barter-rs `DataKind::Candle` if the local Barter-rs candle type is constructible. If it is not constructible with public fields, document that in the test as a compile-only model contract and leave live candle mapping out of this task.

- [ ] **Step 2: Run failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test candle_mapper_contract
```

Expected: compile failure because `CandlePayload` does not expose `interval`, `trade_count`, or `quote_volume`.

- [ ] **Step 3: Extend `CandlePayload`**

Modify `crates/fdc-adapter/barter/src/model/event.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandlePayload {
    pub interval: Option<String>,
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: DecimalQuantity,
    pub trade_count: Option<u64>,
    pub quote_volume: Option<DecimalQuantity>,
}
```

Update any existing `CandlePayload` construction sites to include the new optional fields.

- [ ] **Step 4: Implement candle mapping if Barter-rs type is public**

In `crates/fdc-adapter/barter/src/mapper/event.rs`, replace the raw candle fallback only if the Barter-rs candle fields are accessible:

```rust
DataKind::Candle(candle) => BarterMarketPayload::Candle(CandlePayload {
    interval: None,
    open_time: timestamp,
    close_time: candle
        .close_time
        .timestamp_nanos_opt()
        .map(TimestampNs::from_nanos)
        .ok_or(BarterAdapterError::InvalidTimestamp)?,
    open: price_from_f64("candle.open", candle.open)?,
    high: price_from_f64("candle.high", candle.high)?,
    low: price_from_f64("candle.low", candle.low)?,
    close: price_from_f64("candle.close", candle.close)?,
    volume: decimal_from_f64("candle.volume", candle.volume)?,
    trade_count: Some(candle.trade_count),
    quote_volume: None,
}),
```

If field names differ, inspect the local Barter-rs candle type and adjust exactly to those public names.

- [ ] **Step 5: Verify candle and existing mapper tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test candle_mapper_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test mapper_market_data_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/model/event.rs \
  crates/fdc-adapter/barter/src/mapper/event.rs \
  crates/fdc-adapter/barter/tests/candle_mapper_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: complete barter candle payload boundary"
```

---

## Task 2: Binance Futures USD Live Market Data Initializer

**Files:**
- Create: `crates/fdc-adapter/barter/tests/binance_futures_live_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Write failing futures subscription tests**

Create `crates/fdc-adapter/barter/tests/binance_futures_live_contract.rs` with tests that assert:

```rust
use fdc_barter::{
    default_binance_futures_usd_market_data_subscriptions, BarterMarketDataKind, LiveExchange,
};

#[test]
fn default_binance_futures_usd_subscriptions_include_derivatives_targets() {
    let subscriptions = default_binance_futures_usd_market_data_subscriptions();

    assert!(subscriptions.iter().any(|sub| {
        sub.exchange == LiveExchange::BinanceFuturesUsd
            && sub.base == "btc"
            && sub.quote == "usdt"
            && sub.kind == BarterMarketDataKind::Trade
    }));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::OrderBookL1));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::OrderBook));
    assert!(subscriptions.iter().any(|sub| sub.kind == BarterMarketDataKind::Liquidation));
}
```

Add an ignored compile/smoke test:

```rust
#[tokio::test]
#[ignore]
async fn ignored_live_smoke_can_initialize_binance_futures_usd_market_data() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = fdc_barter::init_binance_futures_usd_market_data(
        fdc_barter::default_binance_futures_usd_market_data_subscriptions(),
    )
    .await
    .expect("Binance Futures USD stream should initialize");

    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        fdc_barter::collect_live_market_data_envelopes("barter-binance-futures-live", streams.select_all(), 1),
    )
    .await
    .expect("should receive one live futures event within timeout")
    .expect("live collection should succeed");

    assert_eq!(envelopes.len(), 1);
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_futures_live_contract
```

Expected: compile failure because `LiveExchange::BinanceFuturesUsd`, `default_binance_futures_usd_market_data_subscriptions`, and `init_binance_futures_usd_market_data` do not exist.

- [ ] **Step 3: Extend live exchange and defaults**

Modify `crates/fdc-adapter/barter/src/ingestion/live.rs`:

```rust
pub enum LiveExchange {
    BinanceSpot,
    BinanceFuturesUsd,
}
```

Add:

```rust
pub fn default_binance_futures_usd_market_data_subscriptions() -> Vec<LiveMarketDataSubscription> {
    ["btc", "eth"]
        .into_iter()
        .flat_map(|base| {
            [
                BarterMarketDataKind::Trade,
                BarterMarketDataKind::OrderBookL1,
                BarterMarketDataKind::OrderBook,
                BarterMarketDataKind::Liquidation,
            ]
            .into_iter()
            .map(move |kind| {
                LiveMarketDataSubscription::new(
                    LiveExchange::BinanceFuturesUsd,
                    base,
                    "usdt",
                    MarketDataInstrumentKind::Perpetual,
                    kind,
                )
            })
        })
        .collect()
}
```

- [ ] **Step 4: Implement initializer using Barter-rs futures exchange types**

Inspect the local Barter-rs exchange module for the exact Binance Futures USD type name. Use the existing Binance Spot `init_binance_spot_market_data` structure and add a futures version that groups trade, L1, L2, and liquidation subscriptions. Unsupported kinds must return `BarterAdapterError::UnsupportedLiveSubscription`.

- [ ] **Step 5: Export futures APIs**

Modify `crates/fdc-adapter/barter/src/lib.rs` to re-export:

```rust
default_binance_futures_usd_market_data_subscriptions,
init_binance_futures_usd_market_data,
```

- [ ] **Step 6: Verify normal and ignored compile tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_futures_live_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_futures_live_contract -- --ignored --list
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test live_acquisition_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: normal tests pass; ignored test lists without network connection.

- [ ] **Step 7: Commit**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/live.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/binance_futures_live_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add binance futures market data live initializer"
```

---

## Task 3: Historical OHLCV Backfill Boundary

**Files:**
- Create: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Create: `crates/fdc-adapter/barter/tests/historical_ohlcv_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/src/error.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Write failing OHLCV boundary tests**

Create `crates/fdc-adapter/barter/tests/historical_ohlcv_contract.rs` with tests that assert a historical OHLCV request can be validated, an offline fake page can return candle envelopes, and quality flags mark backfill records.

Use the public API names planned for this task:

```rust
use fdc_barter::{
    HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalBackfillSource,
    HistoricalPageOutcome, BarterMarketDataKind, BarterMarketType,
};
```

The test should construct a `HistoricalBackfillRequest` for `binance_spot`, `BTCUSDT`, `BarterMarketDataKind::Candle`, `BarterMarketType::Spot`, start/end timestamps, and interval `1m`.

- [ ] **Step 2: Run failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_ohlcv_contract
```

Expected: compile failure because historical backfill boundary types do not exist.

- [ ] **Step 3: Implement historical boundary types**

Create `crates/fdc-adapter/barter/src/ingestion/historical.rs` with:

```rust
use async_trait::async_trait;
use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    model::{BarterMarketDataKind, BarterMarketType, HistoricalCursor},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalBackfillRequest {
    pub source_id: String,
    pub exchange: String,
    pub market_type: BarterMarketType,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub interval: Option<String>,
    pub start: TimestampNs,
    pub end: TimestampNs,
    pub limit: Option<usize>,
    pub cursor: Option<HistoricalCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBackfillPage {
    pub request: HistoricalBackfillRequest,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPageOutcome {
    pub records_received: usize,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

#[async_trait]
pub trait HistoricalBackfillSource: Send + Sync {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage>;
}

pub fn validate_historical_backfill_request(request: &HistoricalBackfillRequest) -> Result<()> {
    if request.symbol.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest("symbol is empty".to_string()));
    }
    if request.start.as_nanos() >= request.end.as_nanos() {
        return Err(BarterAdapterError::InvalidHistoricalRequest("start must be before end".to_string()));
    }
    if request.kind == BarterMarketDataKind::Candle && request.interval.as_deref().unwrap_or("").is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest("candle interval is required".to_string()));
    }
    Ok(())
}
```

Add `InvalidHistoricalRequest(String)` to `BarterAdapterError`.

- [ ] **Step 4: Add backfill envelope helper**

Add a helper to `BarterIngestionEnvelope`:

```rust
pub fn from_backfill_event(source_id: impl Into<String>, event: BarterMarketEvent) -> Self {
    let mut envelope = Self::from_event(source_id, event);
    envelope.quality.is_backfill = true;
    envelope
}
```

- [ ] **Step 5: Export historical types**

Update `ingestion/mod.rs` and `lib.rs` to export historical boundary types and validation helper.

- [ ] **Step 6: Verify**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_ohlcv_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass.

- [ ] **Step 7: Commit**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/ingestion/envelope.rs \
  crates/fdc-adapter/barter/src/error.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/historical_ohlcv_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add barter historical ohlcv backfill boundary"
```

---

## Task 4: Historical Trades Boundary

**Files:**
- Create: `crates/fdc-adapter/barter/tests/historical_trades_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Write failing historical trades tests**

Create `crates/fdc-adapter/barter/tests/historical_trades_contract.rs` with tests that assert:

- `HistoricalBackfillRequest` accepts `BarterMarketDataKind::Trade` without an interval.
- A dedupe key helper returns `exchange:symbol:trade_id` when trade id exists.
- A dedupe key helper falls back to `exchange:symbol:event_time:price:quantity` when trade id is missing.

- [ ] **Step 2: Run failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_trades_contract
```

Expected: compile failure because dedupe key helper does not exist.

- [ ] **Step 3: Add dedupe key helper**

Add to `historical.rs`:

```rust
pub fn historical_trade_dedupe_key(event: &crate::model::BarterMarketEvent) -> Option<String> {
    let crate::model::BarterMarketPayload::Trade(trade) = &event.payload else {
        return None;
    };

    Some(match &trade.trade_id {
        Some(trade_id) => format!("{}:{}:{}", event.exchange, event.symbol.as_str(), trade_id),
        None => format!(
            "{}:{}:{}:{}:{}",
            event.exchange,
            event.symbol.as_str(),
            event.timestamp.as_nanos(),
            trade.price.to_f64(),
            trade.quantity
        ),
    })
}
```

If `Symbol` does not expose `as_str()`, use the existing symbol display/string conversion method from `fdc-core`.

- [ ] **Step 4: Verify**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_trades_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_ohlcv_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/tests/historical_trades_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add barter historical trades boundary"
```

---

## Task 5: Adapter Quality and Runtime Metadata Boundary

**Files:**
- Create: `crates/fdc-adapter/barter/src/model/quality.rs`
- Create: `crates/fdc-adapter/barter/tests/quality_metadata_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/model/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Write failing quality metadata tests**

Create `crates/fdc-adapter/barter/tests/quality_metadata_contract.rs` with tests that assert:

- `BarterRuntimeObservation::Reconnect` can represent reconnect visibility without emitting an envelope.
- `BarterKindCounters` increments counts by `BarterMarketDataKind`.
- `event_latency_ns(event)` returns `received_at - timestamp` when non-negative.

- [ ] **Step 2: Run failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test quality_metadata_contract
```

Expected: compile failure because quality metadata types do not exist.

- [ ] **Step 3: Add quality model types**

Create `crates/fdc-adapter/barter/src/model/quality.rs`:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{BarterMarketDataKind, BarterMarketEvent};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterRuntimeObservation {
    Reconnect { exchange: String },
    StreamItemError { message: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterKindCounters {
    pub emitted_by_kind: BTreeMap<BarterMarketDataKind, u64>,
}

impl BarterKindCounters {
    pub fn record_event(&mut self, event: &BarterMarketEvent) {
        *self.emitted_by_kind.entry(event.kind).or_insert(0) += 1;
    }

    pub fn count(&self, kind: BarterMarketDataKind) -> u64 {
        self.emitted_by_kind.get(&kind).copied().unwrap_or(0)
    }
}

pub fn event_latency_ns(event: &BarterMarketEvent) -> Option<i128> {
    let latency = event.received_at.as_nanos() as i128 - event.timestamp.as_nanos() as i128;
    (latency >= 0).then_some(latency)
}
```

If `BarterMarketDataKind` lacks `Ord`, derive `PartialOrd, Ord` in `event.rs` or use `HashMap` instead.

- [ ] **Step 4: Export quality types**

Update `model/mod.rs` and `lib.rs` to export `BarterRuntimeObservation`, `BarterKindCounters`, and `event_latency_ns`.

- [ ] **Step 5: Preserve live envelope behavior**

Keep `map_live_market_data_result()` returning `Ok(None)` for reconnect events. Do not change downstream envelope behavior in this task; this task only adds the observation boundary for future supervisor integration.

- [ ] **Step 6: Verify**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test quality_metadata_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass.

- [ ] **Step 7: Commit**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/model/quality.rs \
  crates/fdc-adapter/barter/src/model/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/quality_metadata_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add barter quality metadata boundary"
```

---

## Final Verification

After all tasks are complete, run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
grep -R "barter_data\|barter-instrument\|barter_instrument" -n crates \
  | grep -v "crates/fdc-adapter/barter" \
  | grep -v "target" || true
```

Expected:

- formatting passes.
- all non-ignored `fdc-barter` tests pass.
- dependency grep has no output.

Optional live smoke after default verification:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-barter --test binance_futures_live_contract ignored_live_smoke_can_initialize_binance_futures_usd_market_data -- --ignored --nocapture
```

Expected: smoke test collects at least one real Binance Futures USD market-data envelope or reports an exchange/network error that is documented in `docs/DEVELOPMENT_STATUS.md`.

## Self-Review

- Spec coverage: covers candle completion, Binance Futures USD live expansion, historical OHLCV boundary, historical trades boundary, and adapter quality metadata.
- Boundary consistency: no task adds Barter-rs dependencies outside `fdc-barter`.
- Type consistency: public names introduced in tests are introduced and exported in implementation steps.
- Testability: every runtime/network behavior has offline tests; live smoke is ignored and environment-gated.
