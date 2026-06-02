# fdc-barter Acquisition Completion and Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete `fdc-barter` internal bounded data acquisition for supported live and historical data, then add examples for every supported acquisition type.

**Architecture:** Add a focused `ingestion/acquisition.rs` layer that orchestrates bounded live collection and historical multi-page backfills while reusing existing live mappers and historical REST page helpers. Keep Barter-rs networking and exchange-specific details inside `fdc-barter`, keep default tests offline, and provide environment-gated examples for real exchange calls.

**Tech Stack:** Rust, `async-trait`, `futures`, `barter-data`, `barter-instrument`, `barter-integration`, `tokio`, offline contract tests, Cargo examples.

---

## File Structure

### Create

- `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`
  - Bounded live acquisition request/outcome.
  - Historical multi-page runner request/outcome/stop reason.
  - `HistoricalPageFetcher` trait.
  - Binance Spot OHLCV/trades fetcher adapters.
- `crates/fdc-adapter/barter/tests/acquisition_contract.rs`
  - Offline tests for live summary and historical runner behavior.
- `crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs`
- `crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs`
- `crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs`
- `crates/fdc-adapter/barter/examples/historical_binance_spot_ohlcv.rs`
- `crates/fdc-adapter/barter/examples/historical_binance_spot_trades.rs`

### Modify

- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Add `pub mod acquisition;` and re-export acquisition APIs.
- `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export acquisition APIs from crate root.
- `crates/fdc-adapter/barter/src/capability/exchange.rs`
  - Advertise Binance Spot historical candle/trade support.
- `docs/DEVELOPMENT_STATUS.md`
  - Append completion checkpoint after verification.

---

## Task 1: Bounded Live Acquisition Summary

**Files:**
- Create: `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`
- Create: `crates/fdc-adapter/barter/tests/acquisition_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write failing live acquisition tests**

Create `crates/fdc-adapter/barter/tests/acquisition_contract.rs` with the tests below. Define helper functions `fake_trade_result(sequence: u64)` and `fake_historical_envelope()` in this same test file before the tests. Copy the concrete Barter fake event construction from `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs` so the helper returns `MarketStreamResult<MarketDataInstrument, DataKind>` that maps into a trade envelope. The historical helper must construct a `BarterMarketEvent` with `BarterMarketPayload::Trade` and wrap it using `BarterIngestionEnvelope::from_backfill_event()`.

```rust
use fdc_barter::{collect_live_envelopes_with_summary, LiveCollectionRequest};

#[tokio::test]
async fn live_collection_rejects_zero_limit() {
    let stream = futures::stream::empty();
    let error = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "barter-test-live".to_string(),
            limit: 0,
        },
        stream,
    )
    .await
    .expect_err("zero limit must be rejected");

    assert!(error.to_string().contains("limit"));
}
```

Append two more tests after copying/importing the existing fake live event helpers from current tests:

```rust
#[tokio::test]
async fn live_collection_returns_summary_when_limit_reached() {
    let stream = futures::stream::iter(vec![fake_trade_result(1), fake_trade_result(2)]);

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "barter-test-live".to_string(),
            limit: 2,
        },
        stream,
    )
    .await
    .expect("fake stream should collect");

    assert_eq!(outcome.source_id, "barter-test-live");
    assert_eq!(outcome.records_received, 2);
    assert_eq!(outcome.requested_limit, 2);
    assert!(outcome.complete);
    assert_eq!(outcome.envelopes.len(), 2);
    assert_eq!(outcome.skipped_reconnects, 0);
}

#[tokio::test]
async fn live_collection_stops_when_stream_ends_before_limit() {
    let stream = futures::stream::iter(vec![fake_trade_result(1)]);

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "barter-test-live".to_string(),
            limit: 2,
        },
        stream,
    )
    .await
    .expect("fake stream should collect");

    assert_eq!(outcome.records_received, 1);
    assert_eq!(outcome.requested_limit, 2);
    assert!(!outcome.complete);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test acquisition_contract
```

Expected: compile failure because `collect_live_envelopes_with_summary` and `LiveCollectionRequest` do not exist.

- [ ] **Step 3: Implement live acquisition API**

Create `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`:

```rust
use async_trait::async_trait;
use barter_data::{
    event::DataKind,
    streams::{consumer::MarketStreamResult, reconnect},
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::{live::map_live_market_data_result, BarterIngestionEnvelope},
    model::HistoricalCursor,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveCollectionRequest {
    pub source_id: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveCollectionOutcome {
    pub source_id: String,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub records_received: usize,
    pub requested_limit: usize,
    pub complete: bool,
    pub skipped_reconnects: usize,
}

pub async fn collect_live_envelopes_with_summary<S>(
    request: LiveCollectionRequest,
    mut stream: S,
) -> Result<LiveCollectionOutcome>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    if request.limit == 0 {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "live collection limit must be greater than zero".to_string(),
        ));
    }
    if request.source_id.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "source_id is empty".to_string(),
        ));
    }

    let mut envelopes = Vec::with_capacity(request.limit);
    let mut skipped_reconnects = 0;

    while envelopes.len() < request.limit {
        let Some(result) = stream.next().await else {
            break;
        };

        if matches!(result, reconnect::Event::Reconnecting(_)) {
            skipped_reconnects += 1;
        }

        if let Some(envelope) = map_live_market_data_result(&request.source_id, result)? {
            envelopes.push(envelope);
        }
    }

    let records_received = envelopes.len();
    Ok(LiveCollectionOutcome {
        source_id: request.source_id,
        envelopes,
        records_received,
        requested_limit: request.limit,
        complete: records_received == request.limit,
        skipped_reconnects,
    })
}
```

- [ ] **Step 4: Re-export live acquisition APIs**

In `crates/fdc-adapter/barter/src/ingestion/mod.rs`, add:

```rust
pub mod acquisition;
```

Then add to the public re-export block:

```rust
pub use acquisition::{collect_live_envelopes_with_summary, LiveCollectionOutcome, LiveCollectionRequest};
```

In `crates/fdc-adapter/barter/src/lib.rs`, add these names to the `pub use ingestion::{ ... }` block:

```rust
collect_live_envelopes_with_summary,
LiveCollectionOutcome,
LiveCollectionRequest,
```

- [ ] **Step 5: Verify Task 1**

Run:

```bash
rtk cargo fmt --package fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test acquisition_contract
```

Expected: live acquisition tests pass.

- [ ] **Step 6: Commit Task 1**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/acquisition.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/acquisition_contract.rs
rtk git commit -m "feat: add bounded live acquisition summary"
```

---

## Task 2: Historical Multi-Page Runner and Fetcher Adapters

**Files:**
- Modify: `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`
- Modify: `crates/fdc-adapter/barter/tests/acquisition_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write failing historical runner tests**

Append to `crates/fdc-adapter/barter/tests/acquisition_contract.rs`:

```rust
use async_trait::async_trait;
use fdc_barter::{
    run_historical_backfill_pages, BarterMarketDataKind, BarterMarketType, HistoricalBackfillPage,
    HistoricalBackfillRequest, HistoricalBackfillRunRequest, HistoricalBackfillStopReason,
    HistoricalCursor, HistoricalPageFetcher,
};
use fdc_core::types::TimestampNs;

fn historical_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

struct ScriptedFetcher {
    pages: std::sync::Mutex<Vec<HistoricalBackfillPage>>,
    starts_seen: std::sync::Mutex<Vec<i64>>,
}

impl ScriptedFetcher {
    fn new(pages: Vec<HistoricalBackfillPage>) -> Self {
        Self {
            pages: std::sync::Mutex::new(pages),
            starts_seen: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl HistoricalPageFetcher for ScriptedFetcher {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        self.starts_seen.lock().unwrap().push(request.start.as_nanos());
        let mut pages = self.pages.lock().unwrap();
        assert!(!pages.is_empty(), "test requested more pages than scripted");
        let mut page = pages.remove(0);
        page.request = request;
        Ok(page)
    }
}

fn page(record_count: usize, complete: bool, next_start: Option<i64>) -> HistoricalBackfillPage {
    let request = historical_request();
    HistoricalBackfillPage {
        request: request.clone(),
        envelopes: vec![fake_historical_envelope(); record_count],
        next_cursor: next_start.map(|next| {
            HistoricalCursor::next_start(
                request.exchange.clone(),
                request.symbol.clone(),
                request.kind,
                TimestampNs::from_nanos(next),
            )
        }),
        complete,
    }
}

#[tokio::test]
async fn historical_runner_rejects_zero_max_pages() {
    let fetcher = ScriptedFetcher::new(Vec::new());
    let error = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: historical_request(),
            max_pages: 0,
            max_records: None,
        },
    )
    .await
    .expect_err("zero max pages should be rejected");

    assert!(error.to_string().contains("max_pages"));
}

#[tokio::test]
async fn historical_runner_stops_on_source_complete() {
    let fetcher = ScriptedFetcher::new(vec![page(2, false, Some(1_700_000_001_000_000_000)), page(1, true, None)]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: historical_request(),
            max_pages: 5,
            max_records: None,
        },
    )
    .await
    .expect("scripted pages should run");

    assert_eq!(outcome.pages.len(), 2);
    assert_eq!(outcome.records_received, 3);
    assert!(outcome.complete);
    assert_eq!(outcome.stopped_reason, HistoricalBackfillStopReason::SourceComplete);
    assert_eq!(fetcher.starts_seen.lock().unwrap()[1], 1_700_000_001_000_000_000);
}

#[tokio::test]
async fn historical_runner_stops_on_max_pages() {
    let fetcher = ScriptedFetcher::new(vec![page(2, false, Some(1_700_000_001_000_000_000))]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: historical_request(),
            max_pages: 1,
            max_records: None,
        },
    )
    .await
    .expect("scripted page should run");

    assert_eq!(outcome.pages.len(), 1);
    assert!(!outcome.complete);
    assert_eq!(outcome.stopped_reason, HistoricalBackfillStopReason::MaxPagesReached);
}

#[tokio::test]
async fn historical_runner_stops_on_max_records() {
    let fetcher = ScriptedFetcher::new(vec![page(2, false, Some(1_700_000_001_000_000_000))]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: historical_request(),
            max_pages: 5,
            max_records: Some(2),
        },
    )
    .await
    .expect("scripted page should run");

    assert_eq!(outcome.records_received, 2);
    assert!(!outcome.complete);
    assert_eq!(outcome.stopped_reason, HistoricalBackfillStopReason::MaxRecordsReached);
}
```

The `fake_historical_envelope()` helper used above must be defined in the same test file and return `BarterIngestionEnvelope::from_backfill_event("barter-binance-spot-history", event)` where `event.payload` is `BarterMarketPayload::Trade(TradePayload { trade_id: Some("scripted".to_string()), price: Price::new(Decimal::new(100, 0)), quantity: Decimal::new(1, 0), side: Some(TradeSide::Buy) })`.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test acquisition_contract
```

Expected: compile failure because historical runner types do not exist.

- [ ] **Step 3: Implement historical runner types**

Append to `crates/fdc-adapter/barter/src/ingestion/acquisition.rs`:

```rust
use crate::ingestion::historical::{
    execute_binance_spot_historical_trades_rest, execute_binance_spot_ohlcv_rest,
    HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalRestExecutor,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalBackfillRunRequest {
    pub first_request: HistoricalBackfillRequest,
    pub max_pages: usize,
    pub max_records: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoricalBackfillStopReason {
    SourceComplete,
    MaxPagesReached,
    MaxRecordsReached,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBackfillRunOutcome {
    pub pages: Vec<HistoricalBackfillPage>,
    pub records_received: usize,
    pub final_cursor: Option<HistoricalCursor>,
    pub complete: bool,
    pub stopped_reason: HistoricalBackfillStopReason,
}

#[async_trait]
pub trait HistoricalPageFetcher: Send + Sync {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage>;
}

pub async fn run_historical_backfill_pages(
    fetcher: &dyn HistoricalPageFetcher,
    request: HistoricalBackfillRunRequest,
) -> Result<HistoricalBackfillRunOutcome> {
    if request.max_pages == 0 {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "max_pages must be greater than zero".to_string(),
        ));
    }
    if request.max_records == Some(0) {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "max_records must be greater than zero when provided".to_string(),
        ));
    }

    let mut next_request = request.first_request;
    let mut pages = Vec::new();
    let mut records_received = 0;
    let mut final_cursor = None;

    loop {
        let page = fetcher.fetch_page(next_request.clone()).await?;
        records_received += page.envelopes.len();
        final_cursor = page.next_cursor.clone();

        let page_complete = page.complete;
        let next_cursor = page.next_cursor.clone();
        pages.push(page);

        if page_complete {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: true,
                stopped_reason: HistoricalBackfillStopReason::SourceComplete,
            });
        }

        if request.max_records.is_some_and(|max| records_received >= max) {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: false,
                stopped_reason: HistoricalBackfillStopReason::MaxRecordsReached,
            });
        }

        if pages.len() >= request.max_pages {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: false,
                stopped_reason: HistoricalBackfillStopReason::MaxPagesReached,
            });
        }

        let cursor = next_cursor.ok_or_else(|| {
            BarterAdapterError::HistoricalRest(
                "historical page is incomplete but did not provide next_cursor".to_string(),
            )
        })?;
        let next_start = cursor.next_start.ok_or_else(|| {
            BarterAdapterError::HistoricalRest(
                "historical page cursor did not provide next_start".to_string(),
            )
        })?;
        next_request.start = next_start;
        next_request.cursor = Some(cursor);
    }
}

pub struct BinanceSpotOhlcvHistoricalPageFetcher<'a> {
    pub executor: &'a dyn HistoricalRestExecutor,
}

#[async_trait]
impl HistoricalPageFetcher for BinanceSpotOhlcvHistoricalPageFetcher<'_> {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage> {
        execute_binance_spot_ohlcv_rest(self.executor, request).await
    }
}

pub struct BinanceSpotTradesHistoricalPageFetcher<'a> {
    pub executor: &'a dyn HistoricalRestExecutor,
}

#[async_trait]
impl HistoricalPageFetcher for BinanceSpotTradesHistoricalPageFetcher<'_> {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage> {
        execute_binance_spot_historical_trades_rest(self.executor, request).await
    }
}
```

- [ ] **Step 4: Re-export historical acquisition APIs**

In `crates/fdc-adapter/barter/src/ingestion/mod.rs`, extend acquisition re-exports:

```rust
BinanceSpotOhlcvHistoricalPageFetcher,
BinanceSpotTradesHistoricalPageFetcher,
HistoricalBackfillRunOutcome,
HistoricalBackfillRunRequest,
HistoricalBackfillStopReason,
HistoricalPageFetcher,
run_historical_backfill_pages,
```

In `crates/fdc-adapter/barter/src/lib.rs`, add the same names to the crate root re-export.

- [ ] **Step 5: Verify Task 2**

Run:

```bash
rtk cargo fmt --package fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test acquisition_contract
```

Expected: acquisition contract passes.

- [ ] **Step 6: Commit Task 2**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/acquisition.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/acquisition_contract.rs
rtk git commit -m "feat: add historical acquisition runner"
```

---

## Task 3: Capability Matrix Historical Support

**Files:**
- Modify: `crates/fdc-adapter/barter/src/capability/exchange.rs`
- Modify: `crates/fdc-adapter/barter/tests/capability_matrix_contract.rs`

- [ ] **Step 1: Write failing capability matrix test**

Append to `crates/fdc-adapter/barter/tests/capability_matrix_contract.rs`:

```rust
#[test]
fn binance_spot_advertises_implemented_historical_candle_and_trade_support() {
    let capabilities = fdc_barter::supported_crypto_market_data_capabilities();
    let binance_spot = capabilities
        .iter()
        .find(|capability| capability.exchange == "binance_spot")
        .expect("binance_spot capabilities should exist");

    assert!(binance_spot.supports_historical);
    assert!(binance_spot
        .historical_kinds
        .contains(&fdc_barter::BarterMarketDataKind::Candle));
    assert!(binance_spot
        .historical_kinds
        .contains(&fdc_barter::BarterMarketDataKind::Trade));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test capability_matrix_contract binance_spot_advertises_implemented_historical_candle_and_trade_support
```

Expected: assertion failure because Binance Spot historical kinds are currently empty.

- [ ] **Step 3: Update capability matrix**

In `crates/fdc-adapter/barter/src/capability/exchange.rs`, change the `binance_spot` capability entry to:

```rust
BarterSourceCapabilities::crypto_exchange(
    "binance_spot",
    Spot,
    vec![Trade, OrderBookL1, OrderBook],
    vec![Candle, Trade],
    vec![
        RateLimitRule::new("binance_spot", "/api/v3/klines", 1200, 60_000, 1),
        RateLimitRule::new("binance_spot", "/api/v3/aggTrades", 1200, 60_000, 1),
    ],
),
```

Also update the local import line to include `Candle`:

```rust
use BarterMarketDataKind::{Candle, Liquidation, OrderBook, OrderBookL1, Trade};
```

- [ ] **Step 4: Verify Task 3**

Run:

```bash
rtk cargo fmt --package fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test capability_matrix_contract
```

Expected: capability matrix contract passes.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/capability/exchange.rs \
  crates/fdc-adapter/barter/tests/capability_matrix_contract.rs
rtk git commit -m "feat: advertise binance spot historical capabilities"
```

---

## Task 4: Examples for Supported Acquisition Types

**Files:**
- Create: `crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs`
- Create: `crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs`
- Create: `crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs`
- Create: `crates/fdc-adapter/barter/examples/historical_binance_spot_ohlcv.rs`
- Create: `crates/fdc-adapter/barter/examples/historical_binance_spot_trades.rs`

- [ ] **Step 1: Create live spot trades example**

Write `crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs`:

```rust
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_spot_market_data, BarterMarketDataKind,
    LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if std::env::var("FDC_BARTER_LIVE_EXAMPLE").as_deref() != Ok("1") {
        println!("set FDC_BARTER_LIVE_EXAMPLE=1 to run the live Binance Spot trades example");
        return Ok(());
    }

    let streams = init_binance_spot_market_data([LiveMarketDataSubscription::new(
        LiveExchange::BinanceSpot,
        "btc",
        "usdt",
        MarketDataInstrumentKind::Spot,
        BarterMarketDataKind::Trade,
    )])
    .await?;

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-spot-trades".to_string(),
            limit: 5,
        },
        streams.select_all(),
    )
    .await?;

    println!("records_received={} complete={}", outcome.records_received, outcome.complete);
    for envelope in outcome.envelopes {
        println!(
            "kind={:?} exchange={} symbol={} ts={}",
            envelope.event.kind,
            envelope.event.exchange,
            envelope.event.symbol.as_str(),
            envelope.event.timestamp.as_nanos()
        );
    }
    Ok(())
}
```

- [ ] **Step 2: Create live spot order books example**

Write `crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs` with the same env-gated structure as the live trades example. Use `init_binance_spot_market_data()` with two `LiveMarketDataSubscription::new(...)` entries: one for `BarterMarketDataKind::OrderBookL1` and one for `BarterMarketDataKind::OrderBook`, both using `LiveExchange::BinanceSpot`, base `"btc"`, quote `"usdt"`, and `MarketDataInstrumentKind::Spot`. Use source id `example-binance-spot-order-books`, limit `5`, and print `kind`, `exchange`, `symbol`, `sequence`, and `timestamp` for each envelope.

- [ ] **Step 3: Create live futures market data example**

Write `crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs` with the same env-gated structure as the live trades example. Use `init_binance_futures_usd_market_data()` with four `LiveMarketDataSubscription::new(...)` entries for `Trade`, `OrderBookL1`, `OrderBook`, and `Liquidation`, all using `LiveExchange::BinanceFuturesUsd`, base `"btc"`, quote `"usdt"`, and `MarketDataInstrumentKind::Perpetual`. Use source id `example-binance-futures-usd-market-data`, limit `5`, and print `kind`, `exchange`, `symbol`, `sequence`, and `timestamp` for each envelope.

- [ ] **Step 4: Create historical OHLCV example**

Write `crates/fdc-adapter/barter/examples/historical_binance_spot_ohlcv.rs`:

```rust
use fdc_barter::{
    run_historical_backfill_pages, BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind,
    BarterMarketPayload, BarterMarketType, BinanceSpotOhlcvHistoricalPageFetcher,
    HistoricalBackfillRequest, HistoricalBackfillRunRequest,
};
use fdc_core::types::TimestampNs;

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if std::env::var("FDC_BARTER_HISTORICAL_EXAMPLE").as_deref() != Ok("1") {
        println!("set FDC_BARTER_HISTORICAL_EXAMPLE=1 to run the historical OHLCV example");
        return Ok(());
    }

    let end_ms = chrono::Utc::now().timestamp_millis() - 60_000;
    let start_ms = end_ms - 10 * 60_000;
    let executor = BarterIntegrationHistoricalRestExecutor::binance_spot();
    let fetcher = BinanceSpotOhlcvHistoricalPageFetcher { executor: &executor };

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: HistoricalBackfillRequest {
                source_id: "example-binance-spot-ohlcv".to_string(),
                exchange: "binance_spot".to_string(),
                market_type: BarterMarketType::Spot,
                symbol: "BTCUSDT".to_string(),
                kind: BarterMarketDataKind::Candle,
                interval: Some("1m".to_string()),
                start: TimestampNs::from_nanos(start_ms * 1_000_000),
                end: TimestampNs::from_nanos(end_ms * 1_000_000),
                limit: Some(3),
                cursor: None,
            },
            max_pages: 2,
            max_records: Some(6),
        },
    )
    .await?;

    println!("pages={} records={} complete={}", outcome.pages.len(), outcome.records_received, outcome.complete);
    for envelope in outcome.pages.into_iter().flat_map(|page| page.envelopes).take(5) {
        if let BarterMarketPayload::Candle(candle) = envelope.event.payload {
            println!(
                "{} {} open={} close={} volume={}",
                envelope.event.exchange,
                envelope.event.symbol.as_str(),
                candle.open.to_f64(),
                candle.close.to_f64(),
                candle.volume
            );
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Create historical trades example**

Write `crates/fdc-adapter/barter/examples/historical_binance_spot_trades.rs` with the same env-gated structure as the OHLCV example. Use `BinanceSpotTradesHistoricalPageFetcher { executor: &executor }`, `kind: BarterMarketDataKind::Trade`, `interval: None`, `limit: Some(10)`, `max_pages: 2`, and `max_records: Some(20)`. Iterate over returned envelopes and, for `BarterMarketPayload::Trade(trade)`, print exchange, symbol, trade id, side, price, and quantity.

- [ ] **Step 6: Compile examples without running network**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --examples --no-run
```

Expected: all examples compile and do not run network calls.

- [ ] **Step 7: Commit Task 4**

Run:

```bash
rtk git add crates/fdc-adapter/barter/examples
rtk git commit -m "docs: add barter acquisition examples"
```

---

## Task 5: Final Status, Full Verification, and Commit

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update development status**

Append this checkpoint to `docs/DEVELOPMENT_STATUS.md`:

```markdown
## Worktree Checkpoint: fdc-barter Acquisition Completion and Examples

Last updated: 2026-06-02 task checkpoint
Branch: `main`
Design: `docs/superpowers/specs/2026-06-02-fdc-barter-acquisition-completion-examples-design.md`
Plan: `docs/superpowers/plans/2026-06-02-fdc-barter-acquisition-completion-examples.md`

Completed the internal `fdc-barter` acquisition loop without adding storage/API coupling.

Completed capabilities:

- Added bounded live acquisition summaries for Barter stream envelopes.
- Added multi-page historical backfill runner with source-complete, max-pages, and max-records stop reasons.
- Added Binance Spot OHLCV and historical trades page fetcher adapters around existing REST execution helpers.
- Updated the capability matrix to advertise Binance Spot historical candle and trade support.
- Added environment-gated examples for each currently supported live and historical acquisition type.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --examples --no-run
```
```

- [ ] **Step 2: Run final verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --examples --no-run
rtk git status --short --branch
```

Expected:

- Format check passes.
- `fdc-barter` tests pass offline.
- Examples compile offline.
- Only `docs/DEVELOPMENT_STATUS.md` is uncommitted before the final commit.

- [ ] **Step 3: Commit status update**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: update barter acquisition status"
```

- [ ] **Step 4: Verify clean tree**

Run:

```bash
rtk git status --short --branch
```

Expected: working tree is clean.

---

## Final Verification

Before reporting completion, run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --examples --no-run
rtk git status --short --branch
```

Expected:

- All commands exit 0.
- `fdc-barter` default tests pass offline.
- Examples compile without running network calls.
- Working tree is clean.

## Self-Review

- Spec coverage: live bounded collection, historical runner, fetcher adapters, capability matrix update, examples, and docs are covered.
- Placeholder scan: no placeholders remain in this plan; implementation notes point to exact files and concrete code.
- Type consistency: public type/function names match the approved spec.
- Boundary check: no storage/API/database coupling is introduced; network examples remain env-gated.
