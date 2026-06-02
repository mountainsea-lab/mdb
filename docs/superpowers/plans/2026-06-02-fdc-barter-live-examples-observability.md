# fdc-barter Live Examples Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make fdc-barter live examples emit immediate, useful `tracing` diagnostics for manual `cargo run --example ...` and IDE main-function runs.

**Architecture:** Keep changes scoped to the three live example binaries and the `fdc-barter` manifest. Each example initializes its own `tracing_subscriber`, logs the env gate, logs subscription and collection boundaries, and logs each collected envelope summary. The existing safety gate and 30 second bounded collection stay intact.

**Tech Stack:** Rust 2021, `tracing`, `tracing-subscriber`, Tokio, existing `fdc_barter` live acquisition APIs.

---

## File Structure

Modify:

- `crates/fdc-adapter/barter/Cargo.toml`
  - Add `tracing` and `tracing-subscriber` to `dev-dependencies` for examples.
- `crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs`
  - Replace `println!` diagnostics with `tracing` logs, add local `init_tracing()` and `live_example_enabled()` helpers.
- `crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs`
  - Same diagnostic pattern for Spot L1/L2 order book subscriptions.
- `crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs`
  - Same diagnostic pattern for Futures USD trade, L1, L2, liquidation subscriptions.

No new production library API should be added.

---

## Task 1: Add example logging dependencies

**Files:**

- Modify: `crates/fdc-adapter/barter/Cargo.toml`

- [ ] **Step 1: Write the failing build check command**

Run before adding dependencies:

```bash
cd crates/fdc-adapter/barter
rtk cargo check --example live_binance_spot_trades
```

Expected before code changes: PASS, because the example has not imported `tracing` yet. This establishes the baseline.

- [ ] **Step 2: Add dev dependencies**

Edit `crates/fdc-adapter/barter/Cargo.toml` so `[dev-dependencies]` becomes:

```toml
[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt", "rt-multi-thread", "sync", "time"] }
tracing = { workspace = true }
tracing-subscriber = { workspace = true, features = ["env-filter", "fmt"] }
```

Keep the existing production `tokio = { workspace = true, features = ["time"] }` dependency unchanged.

- [ ] **Step 3: Verify manifest compiles**

Run:

```bash
cd crates/fdc-adapter/barter
rtk cargo check --example live_binance_spot_trades
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-adapter/barter/Cargo.toml
git commit -m "Add tracing dependencies for barter examples"
```

---

## Task 2: Convert Spot trades example to tracing diagnostics

**Files:**

- Modify: `crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs`

- [ ] **Step 1: Replace the file with tracing-based implementation**

Use this complete file content:

```rust
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_spot_market_data, BarterMarketDataKind,
    LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

const EXAMPLE_NAME: &str = "live_binance_spot_trades";
const ENABLE_ENV: &str = "FDC_BARTER_LIVE_EXAMPLE";
const LIMIT: usize = 5;
const TIMEOUT_SECS: u64 = 30;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn live_example_enabled() -> bool {
    std::env::var(ENABLE_ENV).as_deref() == Ok("1")
}

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    init_tracing();

    info!(example = EXAMPLE_NAME, env = ENABLE_ENV, enabled = live_example_enabled(), "starting live example");

    if !live_example_enabled() {
        info!(
            example = EXAMPLE_NAME,
            env = ENABLE_ENV,
            command = "FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades",
            "live network access disabled; set env to run this example"
        );
        return Ok(());
    }

    let subscription = LiveMarketDataSubscription::new(
        LiveExchange::BinanceSpot,
        "btc",
        "usdt",
        MarketDataInstrumentKind::Spot,
        BarterMarketDataKind::Trade,
    );

    info!(
        example = EXAMPLE_NAME,
        exchange = ?subscription.exchange,
        base = %subscription.base,
        quote = %subscription.quote,
        instrument_kind = ?subscription.instrument_kind,
        kind = ?subscription.kind,
        "initializing live stream"
    );
    debug!(example = EXAMPLE_NAME, subscription = ?subscription, "subscription detail");

    let streams = match init_binance_spot_market_data([subscription]).await {
        Ok(streams) => streams,
        Err(error) => {
            error!(example = EXAMPLE_NAME, %error, "failed to initialize live stream");
            return Err(error);
        }
    };

    info!(example = EXAMPLE_NAME, limit = LIMIT, timeout_secs = TIMEOUT_SECS, "stream initialized; collecting records");

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-spot-trades".to_string(),
            limit: LIMIT,
            timeout: Some(std::time::Duration::from_secs(TIMEOUT_SECS)),
        },
        streams.select_all(),
    )
    .await?;

    info!(
        example = EXAMPLE_NAME,
        records_received = outcome.records_received,
        requested_limit = outcome.requested_limit,
        complete = outcome.complete,
        skipped_reconnects = outcome.skipped_reconnects,
        "collection finished"
    );

    for (index, envelope) in outcome.envelopes.into_iter().enumerate() {
        info!(
            example = EXAMPLE_NAME,
            index,
            kind = ?envelope.event.kind,
            exchange = %envelope.event.exchange,
            symbol = %envelope.event.symbol.as_str(),
            timestamp_ns = envelope.event.timestamp.as_nanos(),
            "received live envelope"
        );
    }

    Ok(())
}
```

- [ ] **Step 2: Verify ungated manual run output**

Run:

```bash
cd crates/fdc-adapter/barter
cargo run --example live_binance_spot_trades
```

Expected output includes both strings:

```text
starting live example
live network access disabled; set env to run this example
```

It must exit successfully without connecting to Binance.

- [ ] **Step 3: Verify gated live run output**

Run:

```bash
cd crates/fdc-adapter/barter
perl -e 'alarm shift; exec @ARGV' 20 env FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades
```

Expected output includes:

```text
initializing live stream
stream initialized; collecting records
received live envelope
collection finished
```

If Binance/network is unavailable, expected output includes `failed to initialize live stream` or the process exits with the existing adapter error. Do not mask the error.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs
git commit -m "Add tracing diagnostics to spot trades example"
```

---

## Task 3: Convert Spot order books example to tracing diagnostics

**Files:**

- Modify: `crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs`

- [ ] **Step 1: Replace the file with tracing-based implementation**

Use this complete file content:

```rust
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_spot_market_data, BarterMarketDataKind,
    LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

const EXAMPLE_NAME: &str = "live_binance_spot_order_books";
const ENABLE_ENV: &str = "FDC_BARTER_LIVE_EXAMPLE";
const LIMIT: usize = 5;
const TIMEOUT_SECS: u64 = 30;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn live_example_enabled() -> bool {
    std::env::var(ENABLE_ENV).as_deref() == Ok("1")
}

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    init_tracing();

    info!(example = EXAMPLE_NAME, env = ENABLE_ENV, enabled = live_example_enabled(), "starting live example");

    if !live_example_enabled() {
        info!(
            example = EXAMPLE_NAME,
            env = ENABLE_ENV,
            command = "FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_order_books",
            "live network access disabled; set env to run this example"
        );
        return Ok(());
    }

    let subscriptions = [
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceSpot,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            BarterMarketDataKind::OrderBookL1,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceSpot,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Spot,
            BarterMarketDataKind::OrderBook,
        ),
    ];

    for subscription in &subscriptions {
        info!(
            example = EXAMPLE_NAME,
            exchange = ?subscription.exchange,
            base = %subscription.base,
            quote = %subscription.quote,
            instrument_kind = ?subscription.instrument_kind,
            kind = ?subscription.kind,
            "configured live subscription"
        );
    }
    debug!(example = EXAMPLE_NAME, subscriptions = ?subscriptions, "subscription detail");

    info!(example = EXAMPLE_NAME, "initializing live streams");
    let streams = match init_binance_spot_market_data(subscriptions).await {
        Ok(streams) => streams,
        Err(error) => {
            error!(example = EXAMPLE_NAME, %error, "failed to initialize live streams");
            return Err(error);
        }
    };

    info!(example = EXAMPLE_NAME, limit = LIMIT, timeout_secs = TIMEOUT_SECS, "streams initialized; collecting records");

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-spot-order-books".to_string(),
            limit: LIMIT,
            timeout: Some(std::time::Duration::from_secs(TIMEOUT_SECS)),
        },
        streams.select_all(),
    )
    .await?;

    info!(
        example = EXAMPLE_NAME,
        records_received = outcome.records_received,
        requested_limit = outcome.requested_limit,
        complete = outcome.complete,
        skipped_reconnects = outcome.skipped_reconnects,
        "collection finished"
    );

    for (index, envelope) in outcome.envelopes.into_iter().enumerate() {
        info!(
            example = EXAMPLE_NAME,
            index,
            kind = ?envelope.event.kind,
            exchange = %envelope.event.exchange,
            symbol = %envelope.event.symbol.as_str(),
            sequence = ?envelope.event.sequence,
            timestamp_ns = envelope.event.timestamp.as_nanos(),
            "received live envelope"
        );
    }

    Ok(())
}
```

- [ ] **Step 2: Verify ungated manual run output**

Run:

```bash
cd crates/fdc-adapter/barter
cargo run --example live_binance_spot_order_books
```

Expected output includes:

```text
starting live example
live network access disabled; set env to run this example
```

- [ ] **Step 3: Verify compile for gated path**

Run:

```bash
cd crates/fdc-adapter/barter
rtk cargo check --example live_binance_spot_order_books
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs
git commit -m "Add tracing diagnostics to spot order book example"
```

---

## Task 4: Convert Futures USD market-data example to tracing diagnostics

**Files:**

- Modify: `crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs`

- [ ] **Step 1: Replace the file with tracing-based implementation**

Use this complete file content:

```rust
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_futures_usd_market_data,
    BarterMarketDataKind, LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

const EXAMPLE_NAME: &str = "live_binance_futures_usd_market_data";
const ENABLE_ENV: &str = "FDC_BARTER_LIVE_EXAMPLE";
const LIMIT: usize = 5;
const TIMEOUT_SECS: u64 = 30;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn live_example_enabled() -> bool {
    std::env::var(ENABLE_ENV).as_deref() == Ok("1")
}

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    init_tracing();

    info!(example = EXAMPLE_NAME, env = ENABLE_ENV, enabled = live_example_enabled(), "starting live example");

    if !live_example_enabled() {
        info!(
            example = EXAMPLE_NAME,
            env = ENABLE_ENV,
            command = "FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_futures_usd_market_data",
            "live network access disabled; set env to run this example"
        );
        return Ok(());
    }

    let subscriptions = [
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Trade,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBookL1,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBook,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Liquidation,
        ),
    ];

    for subscription in &subscriptions {
        info!(
            example = EXAMPLE_NAME,
            exchange = ?subscription.exchange,
            base = %subscription.base,
            quote = %subscription.quote,
            instrument_kind = ?subscription.instrument_kind,
            kind = ?subscription.kind,
            "configured live subscription"
        );
    }
    debug!(example = EXAMPLE_NAME, subscriptions = ?subscriptions, "subscription detail");

    info!(example = EXAMPLE_NAME, "initializing live streams");
    let streams = match init_binance_futures_usd_market_data(subscriptions).await {
        Ok(streams) => streams,
        Err(error) => {
            error!(example = EXAMPLE_NAME, %error, "failed to initialize live streams");
            return Err(error);
        }
    };

    info!(example = EXAMPLE_NAME, limit = LIMIT, timeout_secs = TIMEOUT_SECS, "streams initialized; collecting records");

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-futures-usd-market-data".to_string(),
            limit: LIMIT,
            timeout: Some(std::time::Duration::from_secs(TIMEOUT_SECS)),
        },
        streams.select_all(),
    )
    .await?;

    info!(
        example = EXAMPLE_NAME,
        records_received = outcome.records_received,
        requested_limit = outcome.requested_limit,
        complete = outcome.complete,
        skipped_reconnects = outcome.skipped_reconnects,
        "collection finished"
    );

    for (index, envelope) in outcome.envelopes.into_iter().enumerate() {
        info!(
            example = EXAMPLE_NAME,
            index,
            kind = ?envelope.event.kind,
            exchange = %envelope.event.exchange,
            symbol = %envelope.event.symbol.as_str(),
            sequence = ?envelope.event.sequence,
            timestamp_ns = envelope.event.timestamp.as_nanos(),
            "received live envelope"
        );
    }

    Ok(())
}
```

- [ ] **Step 2: Verify ungated manual run output**

Run:

```bash
cd crates/fdc-adapter/barter
cargo run --example live_binance_futures_usd_market_data
```

Expected output includes:

```text
starting live example
live network access disabled; set env to run this example
```

- [ ] **Step 3: Verify compile for gated path**

Run:

```bash
cd crates/fdc-adapter/barter
rtk cargo check --example live_binance_futures_usd_market_data
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs
git commit -m "Add tracing diagnostics to futures market data example"
```

---

## Task 5: Final formatting, tests, and live verification

**Files:**

- Verify all files changed in Tasks 1-4.

- [ ] **Step 1: Format and run tests**

Run:

```bash
cd crates/fdc-adapter/barter
rtk cargo fmt
rtk cargo test
```

Expected: all non-ignored tests pass, with the existing ignored live smoke tests still ignored.

- [ ] **Step 2: Verify all ungated examples are useful when IDE-style launched**

Run:

```bash
cd crates/fdc-adapter/barter
cargo run --example live_binance_spot_trades
cargo run --example live_binance_spot_order_books
cargo run --example live_binance_futures_usd_market_data
```

Expected each run exits successfully and includes:

```text
starting live example
live network access disabled; set env to run this example
```

- [ ] **Step 3: Verify one gated live example emits data summaries**

Run:

```bash
cd crates/fdc-adapter/barter
perl -e 'alarm shift; exec @ARGV' 20 env FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades
```

Expected on normal network:

```text
initializing live stream
stream initialized; collecting records
received live envelope
collection finished
```

If the network is unavailable, record the actual error output and do not claim live data verification passed.

- [ ] **Step 4: Verify debug filter is accepted**

Run:

```bash
cd crates/fdc-adapter/barter
perl -e 'alarm shift; exec @ARGV' 20 env RUST_LOG=debug FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades
```

Expected output includes `subscription detail` when the stream path starts, or an initialization error if network fails before collection.

- [ ] **Step 5: Review diff**

Run:

```bash
rtk git diff -- crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs
```

Expected: only the manifest dev-dependency additions and example logging conversions are present.

- [ ] **Step 6: Commit final verification adjustments if any**

If formatting changed files or previous tasks were not committed, run:

```bash
git add crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/examples/live_binance_spot_trades.rs crates/fdc-adapter/barter/examples/live_binance_spot_order_books.rs crates/fdc-adapter/barter/examples/live_binance_futures_usd_market_data.rs
git commit -m "Verify barter live example observability"
```

If there are no changes, do not create an empty commit.

---

## Self-Review

Spec coverage:

- Manual `cargo run --example ...` and IDE main-function output: Tasks 2-5.
- `tracing`/`tracing-subscriber` usage: Tasks 1-4.
- Safety gate retained with clear output: Tasks 2-5.
- Subscription, initialization, collection, per-record, final summary logs: Tasks 2-4.
- Existing bounded 30 second timeout preserved: Tasks 2-4.
- Verification and existing tests: Task 5.

Placeholder scan: the plan contains no unresolved markers or incomplete instructions.

Type consistency:

- `LiveMarketDataSubscription::new`, `LiveCollectionRequest`, `init_binance_*_market_data`, and `collect_live_envelopes_with_summary` names match current code.
- `tracing_subscriber::{fmt, EnvFilter}` matches the planned dependency features.
