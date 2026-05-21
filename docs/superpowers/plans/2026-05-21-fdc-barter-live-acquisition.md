# fdc-barter Live Acquisition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first real-time exchange acquisition path in `fdc-barter` for Binance Spot BTC/USDT and ETH/USDT public trades, yielding `BarterIngestionEnvelope` values that can flow through the existing source-envelope bridge.

**Architecture:** Add a thin `ingestion::live` adapter around Barter-rs `Streams::<PublicTrades>`. The module owns subscription construction, Barter stream initialization, conversion from Barter `PublicTrade` stream results into `DataKind` when needed, and mapping `MarketStreamResult<MarketDataInstrument, DataKind>` into optional `BarterIngestionEnvelope` values, while delegating reconnect behavior entirely to Barter-rs.

**Tech Stack:** Rust, Tokio, futures/tokio-stream, barter-data, barter-instrument, fdc-barter, fdc-ingestion contract tests.

---

## File Structure

- Create `crates/fdc-adapter/barter/src/ingestion/live.rs`
  - Defines live-only subscription types and helpers.
  - Starts Barter-rs Binance Spot public trade streams.
  - Maps Barter reconnect/item events into envelopes or adapter errors.
  - Provides a bounded collection helper for tests and smoke validation.
- Modify `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Exports the new live module API.
- Modify `crates/fdc-adapter/barter/src/lib.rs`
  - Re-exports the new public live acquisition API.
- Modify `crates/fdc-adapter/barter/src/error.rs`
  - Adds live stream initialization/item/reconnect error variants.
- Modify `crates/fdc-adapter/barter/Cargo.toml`
  - Adds any required async stream utilities already used by the workspace if needed by tests or helpers.
- Create `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`
  - Adds deterministic tests for subscriptions, mapping, bounded collection, bridge compatibility, stream errors, and dependency boundary.
- Optional ignored test inside `live_acquisition_contract.rs`
  - Connects to Binance Spot only when explicitly enabled by `FDC_BARTER_LIVE_SMOKE=1`.

---

## Task 1: Add Failing Live Acquisition Contract Tests

**Files:**
- Create: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`
- Modify: `crates/fdc-adapter/barter/Cargo.toml` if the test requires `futures` imports or Tokio features not already available to integration tests.

- [ ] **Step 1: Write the failing contract test file**

Create `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs` with tests that describe the intended API before it exists:

```rust
use std::process::Command;

use barter_data::{
    error::DataError,
    event::{DataKind, MarketEvent},
    streams::{consumer::MarketStreamResult, reconnect},
    subscription::trade::PublicTrade,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, map_live_trade_result, BarterMarketDataKind,
    BarterMarketDataMode, IntoSourceEnvelope, LiveExchange, LiveTradeSubscription,
};
use fdc_ingestion::SourceType;
use futures::{stream, StreamExt};

const SOURCE_ID: &str = "barter-binance-spot-live-trades";

fn barter_trade_event(base: &str, quote: &str, trade_id: &str) -> MarketStreamResult<MarketDataInstrument, DataKind> {
    reconnect::Event::Item(Ok(MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new(base, quote, MarketDataInstrumentKind::Spot),
        kind: DataKind::Trade(PublicTrade {
            id: trade_id.to_string(),
            price: 65_000.25,
            amount: 0.5,
            side: Side::Buy,
        }),
    }))
}

#[test]
fn default_binance_spot_trade_subscriptions_are_btc_and_eth_usdt() {
    let subscriptions = default_binance_spot_trade_subscriptions();

    assert_eq!(
        subscriptions,
        vec![
            LiveTradeSubscription::new(LiveExchange::BinanceSpot, "btc", "usdt"),
            LiveTradeSubscription::new(LiveExchange::BinanceSpot, "eth", "usdt"),
        ]
    );
}

#[test]
fn live_trade_result_maps_to_ingestion_envelope() {
    let envelope = map_live_trade_result(SOURCE_ID, barter_trade_event("btc", "usdt", "trade-1"))
        .expect("trade result should map")
        .expect("trade result should emit an envelope");

    assert_eq!(envelope.source_id, SOURCE_ID);
    assert_eq!(envelope.event.source, "barter-rs");
    assert_eq!(envelope.event.mode, BarterMarketDataMode::Live);
    assert_eq!(envelope.event.exchange, "binance_spot");
    assert_eq!(envelope.event.symbol.to_string(), "BTCUSDT");
    assert_eq!(envelope.event.kind, BarterMarketDataKind::Trade);
    assert_eq!(envelope.checkpoint, None);
    assert!(!envelope.quality.is_replay);
    assert!(!envelope.quality.is_backfill);
}

#[test]
fn mapped_live_trade_envelope_bridges_to_market_data_source_envelope() {
    let envelope = map_live_trade_result(SOURCE_ID, barter_trade_event("eth", "usdt", "trade-2"))
        .unwrap()
        .unwrap();

    let source = envelope.into_source_envelope();

    assert_eq!(source.source_type, SourceType::MarketData);
    assert_eq!(source.source_id, SOURCE_ID);
    assert_eq!(source.payload.mode, BarterMarketDataMode::Live);
    assert_eq!(source.payload.exchange, "binance_spot");
    assert_eq!(source.payload.symbol.to_string(), "ETHUSDT");
    assert_eq!(source.payload.kind, BarterMarketDataKind::Trade);
    assert_eq!(source.metadata.adapter.as_deref(), Some("barter-rs"));
    assert_eq!(source.metadata.exchange.as_deref(), Some("binance_spot"));
}

#[tokio::test]
async fn bounded_collection_returns_requested_number_of_envelopes() {
    let input = stream::iter(vec![
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("eth", "usdt", "trade-2"),
        barter_trade_event("btc", "usdt", "trade-3"),
    ]);

    let envelopes = collect_live_trade_envelopes(SOURCE_ID, input, 2)
        .await
        .expect("bounded collection should succeed");

    assert_eq!(envelopes.len(), 2);
    assert_eq!(envelopes[0].event.symbol.to_string(), "BTCUSDT");
    assert_eq!(envelopes[1].event.symbol.to_string(), "ETHUSDT");
}

#[test]
fn reconnect_event_is_observable_but_does_not_emit_envelope() {
    let mapped = map_live_trade_result(
        SOURCE_ID,
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
    )
    .expect("reconnect events should not fail mapping");

    assert_eq!(mapped, None);
}

#[tokio::test]
async fn stream_item_error_is_returned_instead_of_panicking() {
    let input = stream::iter(vec![reconnect::Event::Item(Err(DataError::SubscriptionsEmpty))]);

    let error = collect_live_trade_envelopes(SOURCE_ID, input, 1)
        .await
        .expect_err("stream item error should be returned");

    assert!(error.to_string().contains("live stream item error"));
}

#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_can_collect_one_binance_spot_trade() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .expect("live Binance Spot stream should initialize");
    let stream = streams.select_all().map(fdc_barter::public_trade_result_to_data_kind);
    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        collect_live_trade_envelopes(SOURCE_ID, stream, 1),
    )
    .await
    .expect("should receive one live trade within timeout")
    .expect("live collection should succeed");

    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].event.exchange, "binance_spot");
    assert_eq!(envelopes[0].event.kind, BarterMarketDataKind::Trade);
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter() {
    let output = Command::new("sh")
        .arg("-c")
        .arg("grep -R \"fdc-barter\\|fdc_barter\" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml")
        .output()
        .expect("dependency guard command should run");

    assert_eq!(
        output.status.code(),
        Some(1),
        "fdc-ingestion must not reference fdc-barter; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
```

- [ ] **Step 2: Add dev dependency if needed**

If `futures::stream` is not already available to `fdc-barter` integration tests, modify `crates/fdc-adapter/barter/Cargo.toml`:

```toml
[dev-dependencies]
async-trait = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt", "rt-multi-thread", "sync", "time"] }
futures = { workspace = true }
```

Keep existing dev dependencies and only add missing feature flags/dependencies.

- [ ] **Step 3: Run the new test and verify it fails for missing API**

Run:

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: FAIL to compile with missing items such as `collect_live_trade_envelopes`, `default_binance_spot_trade_subscriptions`, `init_binance_spot_public_trades`, `map_live_trade_result`, `LiveExchange`, and `LiveTradeSubscription`.

- [ ] **Step 4: Commit the failing tests**

Run:

```bash
git add crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/tests/live_acquisition_contract.rs
git commit -m "test: cover fdc-barter live acquisition contracts"
```

---

## Task 2: Implement Thin Live Mapping and Bounded Collection API

**Files:**
- Create: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/src/error.rs`

- [ ] **Step 1: Add live error variants**

Modify `crates/fdc-adapter/barter/src/error.rs` to include live stream errors:

```rust
/// Result type used by the Barter adapter.
pub type Result<T> = std::result::Result<T, BarterAdapterError>;

/// Errors produced while adapting Barter data into mdb data.
#[derive(Debug, thiserror::Error)]
pub enum BarterAdapterError {
    /// The Barter event kind is not supported by the current adapter stage.
    #[error("unsupported Barter market data kind: {0}")]
    UnsupportedKind(&'static str),

    /// A Barter numeric value cannot be represented by the target mdb type.
    #[error("invalid numeric value for field {field}: {value}")]
    InvalidNumericValue { field: &'static str, value: f64 },

    /// A timestamp cannot be represented as nanoseconds.
    #[error("timestamp cannot be represented as nanoseconds")]
    InvalidTimestamp,

    /// Barter-rs failed to initialise a live stream.
    #[error("live stream initialization error: {0}")]
    LiveStreamInit(String),

    /// Barter-rs yielded an error item from a live stream.
    #[error("live stream item error: {0}")]
    LiveStreamItem(String),

    /// A requested live subscription is not supported by this adapter slice.
    #[error("unsupported live subscription: {0}")]
    UnsupportedLiveSubscription(String),
}
```

- [ ] **Step 2: Create the live module**

Create `crates/fdc-adapter/barter/src/ingestion/live.rs`:

```rust
use barter_data::{
    event::{DataKind, MarketEvent},
    exchange::binance::spot::BinanceSpot,
    streams::{consumer::MarketStreamResult, Streams},
    subscription::{trade::{PublicTrade, PublicTrades}, Subscription},
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
};
use futures::{Stream, StreamExt};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    mapper::event::map_market_event,
};

/// Live exchange variants supported by the first acquisition slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveExchange {
    BinanceSpot,
}

/// Public-trade subscription accepted by the live acquisition adapter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveTradeSubscription {
    pub exchange: LiveExchange,
    pub base: String,
    pub quote: String,
}

impl LiveTradeSubscription {
    pub fn new(exchange: LiveExchange, base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            exchange,
            base: base.into(),
            quote: quote.into(),
        }
    }
}

/// Default first-slice live subscriptions: Binance Spot BTC/USDT and ETH/USDT public trades.
pub fn default_binance_spot_trade_subscriptions() -> Vec<LiveTradeSubscription> {
    vec![
        LiveTradeSubscription::new(LiveExchange::BinanceSpot, "btc", "usdt"),
        LiveTradeSubscription::new(LiveExchange::BinanceSpot, "eth", "usdt"),
    ]
}

/// Start Barter-rs Binance Spot public trade streams.
///
/// Reconnect behavior is owned by Barter-rs. This adapter only builds the subscription list.
pub async fn init_binance_spot_public_trades(
    subscriptions: impl IntoIterator<Item = LiveTradeSubscription>,
) -> Result<Streams<MarketStreamResult<MarketDataInstrument, PublicTrade>>> {
    let mut tuples = Vec::new();

    for subscription in subscriptions {
        if subscription.exchange != LiveExchange::BinanceSpot {
            return Err(BarterAdapterError::UnsupportedLiveSubscription(format!(
                "{:?}:{}{}",
                subscription.exchange, subscription.base, subscription.quote
            )));
        }

        tuples.push((
            BinanceSpot::default(),
            subscription.base,
            subscription.quote,
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        ));
    }

    Streams::<PublicTrades>::builder()
        .subscribe(tuples)
        .init()
        .await
        .map_err(|error| BarterAdapterError::LiveStreamInit(error.to_string()))
}

/// Convert a Barter PublicTrade stream event to the DataKind shape used by the existing mapper.
pub fn public_trade_result_to_data_kind(
    item: MarketStreamResult<MarketDataInstrument, PublicTrade>,
) -> MarketStreamResult<MarketDataInstrument, DataKind> {
    MarketStreamResult::from(item)
}

/// Map one Barter live stream result into an optional ingestion envelope.
///
/// Reconnect notifications are observable stream events from Barter-rs, not market data, so they
/// do not emit envelopes and are not treated as adapter failures.
pub fn map_live_trade_result(
    source_id: &str,
    item: MarketStreamResult<MarketDataInstrument, DataKind>,
) -> Result<Option<BarterIngestionEnvelope>> {
    match item {
        barter_data::streams::reconnect::Event::Reconnecting(_origin) => Ok(None),
        barter_data::streams::reconnect::Event::Item(Ok(event)) => {
            let event = map_market_event(event)?;
            Ok(Some(BarterIngestionEnvelope::from_event(source_id, event)))
        }
        barter_data::streams::reconnect::Event::Item(Err(error)) => {
            Err(BarterAdapterError::LiveStreamItem(error.to_string()))
        }
    }
}

/// Collect up to `limit` live trade envelopes from a stream.
///
/// This helper is intentionally bounded and does not own source lifecycle or persistence.
pub async fn collect_live_trade_envelopes<S>(
    source_id: &str,
    mut stream: S,
    limit: usize,
) -> Result<Vec<BarterIngestionEnvelope>>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    let mut envelopes = Vec::with_capacity(limit);

    while envelopes.len() < limit {
        let Some(item) = stream.next().await else {
            break;
        };

        if let Some(envelope) = map_live_trade_result(source_id, item)? {
            envelopes.push(envelope);
        }
    }

    Ok(envelopes)
}
```

If type inference for `.subscribe(tuples)` fails, make `tuples` explicit:

```rust
let mut tuples: Vec<(
    BinanceSpot,
    String,
    String,
    MarketDataInstrumentKind,
    PublicTrades,
)> = Vec::new();
```

If the compiler requires concrete `Subscription` values, convert before subscribe:

```rust
let subscriptions = tuples
    .into_iter()
    .map(Subscription::from)
    .collect::<Vec<Subscription<BinanceSpot, MarketDataInstrument, PublicTrades>>>();

Streams::<PublicTrades>::builder()
    .subscribe(subscriptions)
    .init()
    .await
    .map_err(|error| BarterAdapterError::LiveStreamInit(error.to_string()))
```

Do not introduce any custom reconnect or lifecycle framework.

- [ ] **Step 3: Export the live module API**

Modify `crates/fdc-adapter/barter/src/ingestion/mod.rs`:

```rust
pub mod envelope;
pub mod live;
pub mod source_bridge;

pub use envelope::{BarterIngestionEnvelope, DataQualityFlags};
pub use live::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, map_live_trade_result, public_trade_result_to_data_kind,
    LiveExchange, LiveTradeSubscription,
};
pub use source_bridge::IntoSourceEnvelope;
```

Modify `crates/fdc-adapter/barter/src/lib.rs` re-exports:

```rust
pub use ingestion::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, map_live_trade_result, public_trade_result_to_data_kind,
    BarterIngestionEnvelope, DataQualityFlags, IntoSourceEnvelope, LiveExchange,
    LiveTradeSubscription,
};
```

Keep the rest of `lib.rs` unchanged.

- [ ] **Step 4: Run the live acquisition contract tests**

Run:

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: tests compile and pass except ignored smoke test is skipped.

- [ ] **Step 5: Commit the implementation**

Run:

```bash
git add crates/fdc-adapter/barter/src/error.rs crates/fdc-adapter/barter/src/ingestion/live.rs crates/fdc-adapter/barter/src/ingestion/mod.rs crates/fdc-adapter/barter/src/lib.rs
git commit -m "feat: add fdc-barter live trade acquisition"
```

---

## Task 3: Stabilize Real Barter Stream Initialization and Smoke Validation

**Files:**
- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`
- Modify: `crates/fdc-adapter/barter/Cargo.toml` only if compile errors identify missing async utilities.

- [ ] **Step 1: Run the full fdc-barter test suite**

Run:

```bash
rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass. If compile errors occur around the concrete Barter `Streams` return type, keep `init_binance_spot_public_trades` aligned with Barter's native `Streams<MarketStreamResult<MarketDataInstrument, PublicTrade>>` type and adapt only the ignored smoke test to call `.map(public_trade_result_to_data_kind)` before `collect_live_trade_envelopes`.

- [ ] **Step 2: Confirm the ignored smoke test is discoverable but not run by default**

Run:

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract -- --ignored --list
```

Expected: output lists `ignored_live_smoke_can_collect_one_binance_spot_trade`.

- [ ] **Step 3: Optionally run the live smoke test only when network is acceptable**

Run only if public internet access is available and the environment should contact Binance:

```bash
FDC_BARTER_LIVE_SMOKE=1 rtk cargo test -p fdc-barter --test live_acquisition_contract ignored_live_smoke_can_collect_one_binance_spot_trade -- --ignored --nocapture
```

Expected: collects one Binance Spot trade within 30 seconds. If Binance blocks the network or the test times out, do not weaken deterministic tests. Record the network failure as an optional smoke-test limitation.

- [ ] **Step 4: Run formatting**

Run:

```bash
rtk cargo fmt --package fdc-barter
```

Expected: formatting succeeds.

- [ ] **Step 5: Commit stabilization changes if any files changed**

Run:

```bash
git status --short
```

If files changed, commit them:

```bash
git add crates/fdc-adapter/barter/src/ingestion/live.rs crates/fdc-adapter/barter/tests/live_acquisition_contract.rs crates/fdc-adapter/barter/Cargo.toml
git commit -m "test: stabilize fdc-barter live acquisition"
```

If no files changed, do not create an empty commit.

---

## Task 4: Verify Integration Boundary and Update Development Status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run package integration tests**

Run:

```bash
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Expected: all tests pass and the live smoke test remains ignored by default.

- [ ] **Step 2: Run dependency guard**

Run:

```bash
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: command exits 0 with no matches from `crates/fdc-ingestion`.

- [ ] **Step 3: Update development status**

Modify `docs/DEVELOPMENT_STATUS.md` to add a completed slice for fdc-barter live acquisition:

```markdown
### B3: fdc-barter Live Exchange Acquisition - Complete

- Added a thin live acquisition path for Binance Spot public trades using Barter-rs `Streams::<PublicTrades>`.
- Default first-slice subscriptions are BTC/USDT and ETH/USDT.
- Live Barter market events map into `BarterIngestionEnvelope` and remain compatible with the B2 `IntoSourceEnvelope` bridge.
- Reconnect behavior is delegated to Barter-rs. `fdc-barter` does not implement a custom reconnect or lifecycle framework.
- Historical data, persistence, transform sinks, and storage remain deferred.

Verification:
- `rtk cargo test -p fdc-barter --test live_acquisition_contract`
- `rtk cargo test -p fdc-barter -p fdc-ingestion`
- dependency guard: no `fdc-barter` or `fdc_barter` references inside `crates/fdc-ingestion`
```

Preserve existing document style and update the latest checkpoint commit hash after the final code commit is known.

- [ ] **Step 4: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: update live acquisition status"
```

---

## Task 5: Final Verification and Completion Review

**Files:**
- No code changes expected.

- [ ] **Step 1: Run formatting check**

Run:

```bash
rtk cargo fmt --package fdc-barter
```

Expected: command succeeds and leaves no diff.

- [ ] **Step 2: Run targeted live acquisition tests**

Run:

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: all non-ignored tests pass.

- [ ] **Step 3: Run integration package tests**

Run:

```bash
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Expected: all tests pass.

- [ ] **Step 4: Run dependency guard**

Run:

```bash
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: exits 0 with no matches.

- [ ] **Step 5: Confirm git state**

Run:

```bash
rtk git status --short --branch
```

Expected: working tree is clean and branch is ahead by the new live acquisition commits.

---

## Self-Review

- Spec coverage:
  - FR-1 covered by Task 1 subscription test and Task 2 default subscription implementation.
  - FR-2 covered by Task 2 `init_binance_spot_public_trades` and Task 3 smoke validation.
  - FR-3 and FR-4 covered by mapping tests and implementation.
  - FR-5 covered by bridge compatibility test.
  - FR-6 covered by bounded collection test and helper.
  - FR-7 covered by stream item error test and live error variants.
  - Dependency boundary covered by Task 1 and Task 4 guards.
- Placeholder scan: no TBD/TODO placeholders remain. Optional live smoke is explicit and gated.
- Type consistency: public API names match the approved spec except `map_live_trade_result` returns `Result<Option<BarterIngestionEnvelope>>` so Barter reconnect notifications can be skipped without being treated as market data or adapter failures.
