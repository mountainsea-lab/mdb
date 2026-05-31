# fdc-barter Market Data Collection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `fdc-barter` from trade-only realtime acquisition toward structured crypto exchange market data collection for trades, L1, L2, liquidation, and precise capability declarations.

**Architecture:** Keep Barter-rs integration inside `fdc-barter`. Add structured module models and offline mapper tests before expanding live stream initialization. Do not add Barter-rs dependencies to ingestion, transform, storage, orchestrator, or server crates in this slice.

**Tech Stack:** Rust, Barter-rs local path crates, `fdc-core`, `rust_decimal`, `chrono`, `serde`, `tokio`, cargo tests.

---

## 0. Scope and sequencing

This plan implements the first development slice from `market-data-collection-requirements.md`:

1. Precise capability matrix.
2. Market type metadata.
3. Structured L1, L2, liquidation payloads.
4. Offline mapper tests for trade/L1/L2/liquidation.
5. Generic live envelope naming boundary.
6. Expanded live stream initializer behind ignored smoke tests.

This plan intentionally does not implement historical REST backfill, durable order-book storage, server route changes, factor engine, strategy engine, or cross-module runtime supervisor work.

## 1. File structure

### Modify

- `crates/fdc-adapter/barter/src/model/event.rs`
  - Add `BarterMarketType`.
  - Add `OrderBookUpdateKind`, `OrderBookLevelPayload`, `OrderBookPayload`, `LiquidationPayload`.
  - Change `BarterMarketPayload::OrderBookDelta(RawPayload)` to `OrderBook(OrderBookPayload)`.
  - Change `BarterMarketPayload::Liquidation(RawPayload)` to `Liquidation(LiquidationPayload)`.
  - Add `market_type: BarterMarketType` to `BarterMarketEvent`.

- `crates/fdc-adapter/barter/src/model/mod.rs`
  - Export new model types.

- `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export new public model types.

- `crates/fdc-adapter/barter/src/capability/exchange.rs`
  - Add market-type-aware capability declarations.
  - Add static supported combinations mirroring Barter-rs `exchange_supports_instrument_kind_sub_kind` for the first mdb slice.

- `crates/fdc-adapter/barter/src/mapper/event.rs`
  - Map `MarketDataInstrumentKind` to `BarterMarketType`.
  - Map `DataKind::OrderBookL1` to `OrderBookL1Payload`.
  - Map `DataKind::OrderBook` snapshot/update to `OrderBookPayload`.
  - Map `DataKind::Liquidation` to `LiquidationPayload`.
  - Keep trade mapping behavior stable.

- `crates/fdc-adapter/barter/src/ingestion/live.rs`
  - Rename generic boundary functions from trade-only names where safe.
  - Add wrapper aliases to preserve existing public API during migration.
  - Add live subscription model for multiple data kinds if needed.

- `crates/fdc-adapter/barter/docs/README.md`
  - Link this implementation plan.

### Create

- `crates/fdc-adapter/barter/tests/capability_matrix_contract.rs`
  - Tests the declared support matrix.

- `crates/fdc-adapter/barter/tests/mapper_market_data_contract.rs`
  - Tests L1, L2 snapshot, L2 update, liquidation, and market type mapping using synthetic Barter-rs events.

### Existing tests to keep green

- `crates/fdc-adapter/barter/tests/adapter_contract.rs`
- `crates/fdc-adapter/barter/tests/checkpoint_contract.rs`
- `crates/fdc-adapter/barter/tests/envelope_contract.rs`
- `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`
- `crates/fdc-adapter/barter/tests/model_contract.rs`

---

## Task 1: Add capability matrix contract tests

**Files:**

- Create: `crates/fdc-adapter/barter/tests/capability_matrix_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/capability/exchange.rs`
- Modify: `crates/fdc-adapter/barter/src/capability/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write failing capability matrix tests**

Create `crates/fdc-adapter/barter/tests/capability_matrix_contract.rs`:

```rust
use fdc_barter::{
    supported_crypto_market_data_capabilities, BarterMarketDataKind, BarterMarketType,
};

#[test]
fn capability_matrix_contains_first_slice_realtime_targets() {
    let capabilities = supported_crypto_market_data_capabilities();

    let binance_spot = capabilities
        .iter()
        .find(|capability| {
            capability.exchange == "binance_spot"
                && capability.market_type == BarterMarketType::Spot
        })
        .expect("binance spot capability should exist");
    assert!(binance_spot.supports_live);
    assert!(!binance_spot.supports_historical);
    assert_eq!(
        binance_spot.kinds,
        vec![
            BarterMarketDataKind::Trade,
            BarterMarketDataKind::OrderBookL1,
            BarterMarketDataKind::OrderBook,
        ]
    );

    let binance_futures = capabilities
        .iter()
        .find(|capability| {
            capability.exchange == "binance_futures_usd"
                && capability.market_type == BarterMarketType::Perpetual
        })
        .expect("binance futures usd capability should exist");
    assert_eq!(
        binance_futures.kinds,
        vec![
            BarterMarketDataKind::Trade,
            BarterMarketDataKind::OrderBookL1,
            BarterMarketDataKind::OrderBook,
            BarterMarketDataKind::Liquidation,
        ]
    );
}

#[test]
fn capability_matrix_marks_historical_as_future_work_for_now() {
    let capabilities = supported_crypto_market_data_capabilities();

    assert!(capabilities.iter().all(|capability| !capability.supports_historical));
    assert!(capabilities
        .iter()
        .all(|capability| capability.historical_kinds.is_empty()));
}

#[test]
fn capability_matrix_includes_known_barter_trade_only_exchanges() {
    let capabilities = supported_crypto_market_data_capabilities();

    for exchange in [
        "coinbase",
        "bitfinex",
        "bitmex",
        "gateio_spot",
        "okx",
    ] {
        let capability = capabilities
            .iter()
            .find(|capability| capability.exchange == exchange)
            .unwrap_or_else(|| panic!("{exchange} capability should exist"));
        assert!(capability.kinds.contains(&BarterMarketDataKind::Trade));
    }
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
rtk cargo test -p fdc-barter --test capability_matrix_contract
```

Expected: compile failure because `supported_crypto_market_data_capabilities` and `BarterMarketType` do not exist or are not exported.

- [ ] **Step 3: Add market type and capability declarations**

Modify `crates/fdc-adapter/barter/src/model/event.rs` near `BarterMarketDataMode`:

```rust
/// Market/instrument class associated with a Barter market event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarterMarketType {
    Spot,
    Future,
    Perpetual,
    Option,
}
```

Modify `crates/fdc-adapter/barter/src/capability/exchange.rs`:

```rust
use serde::{Deserialize, Serialize};

use crate::model::{BarterMarketDataKind, BarterMarketType};

/// Exchange endpoint rate limit rule used by historical REST implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitRule {
    pub exchange: String,
    pub endpoint: String,
    pub max_requests: u32,
    pub window_ms: u64,
    pub weight: u32,
}

impl RateLimitRule {
    pub fn new(
        exchange: impl Into<String>,
        endpoint: impl Into<String>,
        max_requests: u32,
        window_ms: u64,
        weight: u32,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            endpoint: endpoint.into(),
            max_requests,
            window_ms,
            weight,
        }
    }
}

/// Capability declaration for a Barter-backed crypto exchange source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterSourceCapabilities {
    pub exchange: String,
    pub market_type: BarterMarketType,
    pub supports_live: bool,
    pub supports_historical: bool,
    pub kinds: Vec<BarterMarketDataKind>,
    pub historical_kinds: Vec<BarterMarketDataKind>,
    pub rate_limits: Vec<RateLimitRule>,
}

impl BarterSourceCapabilities {
    pub fn crypto_exchange(
        exchange: impl Into<String>,
        market_type: BarterMarketType,
        live_kinds: Vec<BarterMarketDataKind>,
        historical_kinds: Vec<BarterMarketDataKind>,
        rate_limits: Vec<RateLimitRule>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            market_type,
            supports_live: !live_kinds.is_empty(),
            supports_historical: !historical_kinds.is_empty(),
            kinds: live_kinds,
            historical_kinds,
            rate_limits,
        }
    }
}

pub fn supported_crypto_market_data_capabilities() -> Vec<BarterSourceCapabilities> {
    use BarterMarketDataKind::{Liquidation, OrderBook, OrderBookL1, Trade};
    use BarterMarketType::{Future, Option, Perpetual, Spot};

    vec![
        BarterSourceCapabilities::crypto_exchange(
            "binance_spot",
            Spot,
            vec![Trade, OrderBookL1, OrderBook],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "binance_futures_usd",
            Perpetual,
            vec![Trade, OrderBookL1, OrderBook, Liquidation],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "bybit_spot",
            Spot,
            vec![Trade, OrderBookL1, OrderBook],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "bybit_perpetuals_usd",
            Perpetual,
            vec![Trade, OrderBookL1, OrderBook],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange("kraken", Spot, vec![Trade, OrderBookL1], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("coinbase", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("bitfinex", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("bitmex", Perpetual, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_spot", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_futures_usd", Future, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_futures_btc", Future, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_perpetuals_usd", Perpetual, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_perpetuals_btc", Perpetual, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_options", Option, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("okx", Spot, vec![Trade], vec![], vec![]),
    ]
}
```

Modify `crates/fdc-adapter/barter/src/capability/mod.rs`:

```rust
pub mod exchange;

pub use exchange::{
    supported_crypto_market_data_capabilities, BarterSourceCapabilities, RateLimitRule,
};
```

Modify `crates/fdc-adapter/barter/src/model/mod.rs` export list:

```rust
pub use event::{
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    BarterMarketType, CandlePayload, DecimalQuantity, OrderBookL1Payload, RawPayload,
    TradePayload, TradeSide,
};
```

Modify `crates/fdc-adapter/barter/src/lib.rs` export list:

```rust
pub use capability::{
    supported_crypto_market_data_capabilities, BarterSourceCapabilities, RateLimitRule,
};
pub use model::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterMarketEvent, BarterMarketPayload, BarterMarketType, BarterSourceState,
    BarterSourceStatus, CandlePayload, DecimalQuantity, HistoricalCursor, HistoricalPageRequest,
    OrderBookL1Payload, RawPayload, TradePayload, TradeSide,
};
```

- [ ] **Step 4: Update existing capability constructor tests**

Modify `crates/fdc-adapter/barter/tests/model_contract.rs` capability construction:

```rust
let capabilities = BarterSourceCapabilities::crypto_exchange(
    "binance_spot",
    fdc_barter::BarterMarketType::Spot,
    vec![BarterMarketDataKind::Trade, BarterMarketDataKind::Candle],
    vec![BarterMarketDataKind::Trade],
    vec![RateLimitRule::new(
        "binance_spot",
        "klines",
        1200,
        60_000,
        1,
    )],
);
```

Add assertion:

```rust
assert_eq!(capabilities.market_type, fdc_barter::BarterMarketType::Spot);
```

- [ ] **Step 5: Run tests**

Run:

```bash
rtk cargo test -p fdc-barter --test capability_matrix_contract
rtk cargo test -p fdc-barter --test model_contract
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-adapter/barter/src/model/event.rs \
  crates/fdc-adapter/barter/src/model/mod.rs \
  crates/fdc-adapter/barter/src/capability/exchange.rs \
  crates/fdc-adapter/barter/src/capability/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/model_contract.rs \
  crates/fdc-adapter/barter/tests/capability_matrix_contract.rs
git commit -m "feat: declare barter market data capabilities"
```

---
## Task 2: Add structured payload model contracts

**Files:**

- Modify: `crates/fdc-adapter/barter/src/model/event.rs`
- Modify: `crates/fdc-adapter/barter/src/model/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/model_contract.rs`

- [ ] **Step 1: Write failing model assertions**

Append to `crates/fdc-adapter/barter/tests/model_contract.rs`:

```rust
use fdc_barter::{
    LiquidationPayload, OrderBookLevelPayload, OrderBookPayload, OrderBookUpdateKind,
    TradeSide,
};
use fdc_core::types::{Price, TimestampNs};
use rust_decimal::Decimal;

#[test]
fn structured_order_book_payload_reports_order_book_kind() {
    let payload = fdc_barter::BarterMarketPayload::OrderBook(OrderBookPayload {
        update_kind: OrderBookUpdateKind::Snapshot,
        bids: vec![OrderBookLevelPayload {
            price: Price::from_f64(100.0).unwrap(),
            quantity: Decimal::new(15, 1),
        }],
        asks: vec![OrderBookLevelPayload {
            price: Price::from_f64(101.0).unwrap(),
            quantity: Decimal::new(20, 1),
        }],
        sequence: Some("42".to_string()),
    });

    assert_eq!(payload.kind(), BarterMarketDataKind::OrderBook);
}

#[test]
fn structured_liquidation_payload_reports_liquidation_kind() {
    let payload = fdc_barter::BarterMarketPayload::Liquidation(LiquidationPayload {
        side: TradeSide::Sell,
        price: Price::from_f64(64000.0).unwrap(),
        quantity: Decimal::new(25, 1),
        liquidation_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
    });

    assert_eq!(payload.kind(), BarterMarketDataKind::Liquidation);
}
```

- [ ] **Step 2: Run failing model tests**

```bash
rtk cargo test -p fdc-barter --test model_contract
```

Expected: compile failure because the structured payload types and enum variants are not available yet.

- [ ] **Step 3: Add model types and payload variants**

Modify `crates/fdc-adapter/barter/src/model/event.rs` after `OrderBookL1Payload`:

```rust
/// Order-book event shape emitted by Barter-rs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderBookUpdateKind {
    Snapshot,
    Update,
}

/// One price level in a normalized order book payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevelPayload {
    pub price: Price,
    pub quantity: DecimalQuantity,
}

/// Structured L2 order book payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookPayload {
    pub update_kind: OrderBookUpdateKind,
    pub bids: Vec<OrderBookLevelPayload>,
    pub asks: Vec<OrderBookLevelPayload>,
    pub sequence: Option<String>,
}

/// Structured liquidation payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiquidationPayload {
    pub side: TradeSide,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub liquidation_time: TimestampNs,
}
```

Change `BarterMarketPayload` to:

```rust
pub enum BarterMarketPayload {
    Trade(TradePayload),
    OrderBookL1(OrderBookL1Payload),
    OrderBook(OrderBookPayload),
    Candle(CandlePayload),
    Liquidation(LiquidationPayload),
    Raw(RawPayload),
}
```

Change `kind()` match arms:

```rust
Self::OrderBook(_) => BarterMarketDataKind::OrderBook,
Self::Liquidation(_) => BarterMarketDataKind::Liquidation,
```

- [ ] **Step 4: Export new types**

Update `crates/fdc-adapter/barter/src/model/mod.rs`:

```rust
pub use event::{
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    BarterMarketType, CandlePayload, DecimalQuantity, LiquidationPayload, OrderBookL1Payload,
    OrderBookLevelPayload, OrderBookPayload, OrderBookUpdateKind, RawPayload, TradePayload,
    TradeSide,
};
```

Update `crates/fdc-adapter/barter/src/lib.rs` similarly:

```rust
pub use model::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterMarketEvent, BarterMarketPayload, BarterMarketType, BarterSourceState,
    BarterSourceStatus, CandlePayload, DecimalQuantity, HistoricalCursor, HistoricalPageRequest,
    LiquidationPayload, OrderBookL1Payload, OrderBookLevelPayload, OrderBookPayload,
    OrderBookUpdateKind, RawPayload, TradePayload, TradeSide,
};
```

- [ ] **Step 5: Run model tests**

```bash
rtk cargo test -p fdc-barter --test model_contract
```

Expected: compile failure in mapper because old enum variants were removed. Continue immediately to Task 3 before committing Task 2.

---

## Task 3: Add offline mapper tests for L1, L2, liquidation, and market type

**Files:**

- Create: `crates/fdc-adapter/barter/tests/mapper_market_data_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/mapper/event.rs`

- [ ] **Step 1: Write mapper tests**

Create `crates/fdc-adapter/barter/tests/mapper_market_data_contract.rs`:

```rust
use barter_data::{
    books::{Level, OrderBook},
    event::{DataKind, MarketEvent},
    subscription::{
        book::{OrderBookEvent, OrderBookL1},
        liquidation::Liquidation,
    },
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType, OrderBookUpdateKind,
    TradeSide,
};

fn market_event(kind: DataKind, instrument_kind: MarketDataInstrumentKind) -> MarketEvent<MarketDataInstrument, DataKind> {
    MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: MarketDataInstrument::new("btc", "usdt", instrument_kind),
        kind,
    }
}

#[test]
fn l1_event_maps_to_structured_payload() {
    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time: Utc.timestamp_nanos(1_700_000_000_000_000_500),
            best_bid: Some(Level::new(65000, 2)),
            best_ask: Some(Level::new(65001, 3)),
        }),
        MarketDataInstrumentKind::Perpetual,
    ))
    .expect("l1 should map");

    assert_eq!(event.market_type, BarterMarketType::Perpetual);
    assert_eq!(event.kind, BarterMarketDataKind::OrderBookL1);
    match event.payload {
        BarterMarketPayload::OrderBookL1(l1) => {
            assert_eq!(l1.bid_price.unwrap().to_f64(), 65000.0);
            assert_eq!(l1.bid_quantity.unwrap().to_string(), "2");
            assert_eq!(l1.ask_price.unwrap().to_f64(), 65001.0);
            assert_eq!(l1.ask_quantity.unwrap().to_string(), "3");
        }
        other => panic!("expected l1 payload, got {other:?}"),
    }
}

#[test]
fn l2_snapshot_maps_to_structured_payload() {
    let book = OrderBook::new(
        42,
        Some(Utc.timestamp_nanos(1_700_000_000_000_000_500)),
        vec![Level::new(65000, 2), Level::new(64999, 4)],
        vec![Level::new(65001, 3)],
    );

    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBook(OrderBookEvent::Snapshot(book)),
        MarketDataInstrumentKind::Spot,
    ))
    .expect("l2 snapshot should map");

    assert_eq!(event.market_type, BarterMarketType::Spot);
    assert_eq!(event.sequence.as_deref(), Some("42"));
    match event.payload {
        BarterMarketPayload::OrderBook(book) => {
            assert_eq!(book.update_kind, OrderBookUpdateKind::Snapshot);
            assert_eq!(book.sequence.as_deref(), Some("42"));
            assert_eq!(book.bids.len(), 2);
            assert_eq!(book.asks.len(), 1);
            assert_eq!(book.bids[0].price.to_f64(), 65000.0);
            assert_eq!(book.asks[0].quantity.to_string(), "3");
        }
        other => panic!("expected order book payload, got {other:?}"),
    }
}

#[test]
fn l2_update_maps_to_structured_payload() {
    let update = OrderBook::new(
        43,
        None,
        vec![Level::new(65002, 1)],
        vec![Level::new(65003, 5)],
    );

    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBook(OrderBookEvent::Update(update)),
        MarketDataInstrumentKind::Spot,
    ))
    .expect("l2 update should map");

    match event.payload {
        BarterMarketPayload::OrderBook(book) => {
            assert_eq!(book.update_kind, OrderBookUpdateKind::Update);
            assert_eq!(book.sequence.as_deref(), Some("43"));
        }
        other => panic!("expected order book payload, got {other:?}"),
    }
}

#[test]
fn liquidation_event_maps_to_structured_payload() {
    let liquidation_time = Utc.timestamp_nanos(1_700_000_000_000_000_777);
    let event = fdc_barter::map_market_event(market_event(
        DataKind::Liquidation(Liquidation {
            side: Side::Sell,
            price: 64000.5,
            quantity: 2.25,
            time: liquidation_time,
        }),
        MarketDataInstrumentKind::Perpetual,
    ))
    .expect("liquidation should map");

    assert_eq!(event.kind, BarterMarketDataKind::Liquidation);
    assert_eq!(event.market_type, BarterMarketType::Perpetual);
    match event.payload {
        BarterMarketPayload::Liquidation(liquidation) => {
            assert_eq!(liquidation.side, TradeSide::Sell);
            assert_eq!(liquidation.price.to_f64(), 64000.5);
            assert_eq!(liquidation.quantity.to_string(), "2.25");
            assert_eq!(liquidation.liquidation_time.as_nanos(), 1_700_000_000_000_000_777);
        }
        other => panic!("expected liquidation payload, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run failing mapper tests**

```bash
rtk cargo test -p fdc-barter --test mapper_market_data_contract
```

Expected: compile failure or test failure because mapper still emits raw fallback payloads and `BarterMarketEvent` has no `market_type` until implemented.

- [ ] **Step 3: Implement mapper helpers**

Modify imports in `crates/fdc-adapter/barter/src/mapper/event.rs`:

```rust
use barter_data::{
    books::{Level, OrderBook},
    event::{DataKind, MarketEvent},
    subscription::book::OrderBookEvent,
};
use barter_instrument::{
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
```

Update model imports:

```rust
BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload, BarterMarketType,
LiquidationPayload, OrderBookL1Payload, OrderBookLevelPayload, OrderBookPayload,
OrderBookUpdateKind, RawPayload, TradePayload, TradeSide,
```

Add helpers:

```rust
fn market_type_from_instrument(kind: &MarketDataInstrumentKind) -> BarterMarketType {
    match kind {
        MarketDataInstrumentKind::Spot => BarterMarketType::Spot,
        MarketDataInstrumentKind::Future { .. } => BarterMarketType::Future,
        MarketDataInstrumentKind::Perpetual => BarterMarketType::Perpetual,
        MarketDataInstrumentKind::Option { .. } => BarterMarketType::Option,
    }
}

fn side_from_barter(side: Side) -> TradeSide {
    match side {
        Side::Buy => TradeSide::Buy,
        Side::Sell => TradeSide::Sell,
    }
}

fn level_to_payload(level: Level) -> Result<OrderBookLevelPayload> {
    Ok(OrderBookLevelPayload {
        price: Price::new(level.price),
        quantity: level.amount,
    })
}

fn order_book_levels(book: &OrderBook) -> Result<(Vec<OrderBookLevelPayload>, Vec<OrderBookLevelPayload>)> {
    let bids = book
        .bids()
        .levels()
        .iter()
        .copied()
        .map(level_to_payload)
        .collect::<Result<Vec<_>>>()?;
    let asks = book
        .asks()
        .levels()
        .iter()
        .copied()
        .map(level_to_payload)
        .collect::<Result<Vec<_>>>()?;
    Ok((bids, asks))
}

fn map_order_book(event: OrderBookEvent) -> Result<(OrderBookPayload, Option<String>)> {
    let (update_kind, book) = match event {
        OrderBookEvent::Snapshot(book) => (OrderBookUpdateKind::Snapshot, book),
        OrderBookEvent::Update(book) => (OrderBookUpdateKind::Update, book),
    };
    let sequence = Some(book.sequence().to_string());
    let (bids, asks) = order_book_levels(&book)?;
    Ok((
        OrderBookPayload {
            update_kind,
            bids,
            asks,
            sequence: sequence.clone(),
        },
        sequence,
    ))
}
```

- [ ] **Step 4: Implement mapper match arms**

Inside `TryFrom<MarketEvent<MarketDataInstrument, DataKind>> for BarterMarketEvent`, set:

```rust
let market_type = market_type_from_instrument(&event.instrument.kind);
let mut sequence = None;
let payload = match event.kind {
    DataKind::Trade(trade) => BarterMarketPayload::Trade(TradePayload {
        trade_id: Some(trade.id),
        price: price_from_f64("price", trade.price)?,
        quantity: decimal_from_f64("amount", trade.amount)?,
        side: Some(side_from_barter(trade.side)),
    }),
    DataKind::OrderBookL1(book) => BarterMarketPayload::OrderBookL1(OrderBookL1Payload {
        bid_price: book.best_bid.map(|level| Price::new(level.price)),
        bid_quantity: book.best_bid.map(|level| level.amount),
        ask_price: book.best_ask.map(|level| Price::new(level.price)),
        ask_quantity: book.best_ask.map(|level| level.amount),
    }),
    DataKind::OrderBook(book) => {
        let (payload, mapped_sequence) = map_order_book(book)?;
        sequence = mapped_sequence;
        BarterMarketPayload::OrderBook(payload)
    }
    DataKind::Candle(_) => BarterMarketPayload::Raw(RawPayload {
        description: "candle".to_string(),
    }),
    DataKind::Liquidation(liquidation) => BarterMarketPayload::Liquidation(LiquidationPayload {
        side: side_from_barter(liquidation.side),
        price: price_from_f64("liquidation.price", liquidation.price)?,
        quantity: decimal_from_f64("liquidation.quantity", liquidation.quantity)?,
        liquidation_time: liquidation
            .time
            .timestamp_nanos_opt()
            .map(TimestampNs::from_nanos)
            .ok_or(BarterAdapterError::InvalidTimestamp)?,
    }),
};
```

In the returned struct include:

```rust
market_type,
sequence,
```

instead of hard-coded `sequence: None`.

- [ ] **Step 5: Run mapper tests**

```bash
rtk cargo test -p fdc-barter --test mapper_market_data_contract
```

Expected: pass.

- [ ] **Step 6: Run existing adapter tests**

```bash
rtk cargo test -p fdc-barter --test adapter_contract
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: pass after adding `market_type` expectations if required.

- [ ] **Step 7: Commit**

```bash
git add crates/fdc-adapter/barter/src/model/event.rs \
  crates/fdc-adapter/barter/src/model/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/src/mapper/event.rs \
  crates/fdc-adapter/barter/tests/model_contract.rs \
  crates/fdc-adapter/barter/tests/mapper_market_data_contract.rs \
  crates/fdc-adapter/barter/tests/adapter_contract.rs \
  crates/fdc-adapter/barter/tests/live_acquisition_contract.rs
git commit -m "feat: map structured barter market data payloads"
```

---
## Task 4: Rename live mapping boundary from trade-only to market-data generic

**Files:**

- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`

- [ ] **Step 1: Add tests for generic names while preserving old wrappers**

Modify import list in `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs` to include new names:

```rust
collect_live_market_data_envelopes, map_live_market_data_result,
```

Add this test after `live_trade_result_maps_to_ingestion_envelope`:

```rust
#[test]
fn generic_live_market_data_result_mapper_preserves_trade_behavior() {
    let envelope = map_live_market_data_result(SOURCE_ID, barter_trade_event("btc", "usdt", "trade-1"))
        .expect("trade result should map")
        .expect("trade result should emit an envelope");

    assert_eq!(envelope.source_id, SOURCE_ID);
    assert_eq!(envelope.event.exchange, "binance_spot");
    assert_eq!(envelope.event.kind, BarterMarketDataKind::Trade);
}
```

Add this async test:

```rust
#[tokio::test]
async fn generic_live_market_data_collection_preserves_trade_behavior() {
    let input = stream::iter(vec![
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("eth", "usdt", "trade-2"),
    ]);

    let envelopes = collect_live_market_data_envelopes(SOURCE_ID, input, 2)
        .await
        .expect("bounded collection should succeed");

    assert_eq!(envelopes.len(), 2);
    assert_eq!(envelopes[0].event.symbol.to_string(), "BTCUSDT");
    assert_eq!(envelopes[1].event.symbol.to_string(), "ETHUSDT");
}
```

- [ ] **Step 2: Run failing live acquisition tests**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: compile failure because generic live names are not exported.

- [ ] **Step 3: Add generic functions and wrapper aliases**

In `crates/fdc-adapter/barter/src/ingestion/live.rs`, rename implementation function body by adding new generic names:

```rust
/// Map one live Barter stream result into an optional ingestion envelope.
///
/// Reconnect notifications are observable at this boundary but do not emit envelopes.
pub fn map_live_market_data_result(
    source_id: &str,
    result: MarketStreamResult<MarketDataInstrument, DataKind>,
) -> Result<Option<BarterIngestionEnvelope>> {
    match result {
        reconnect::Event::Reconnecting(_exchange) => Ok(None),
        reconnect::Event::Item(Ok(event)) => {
            let event = map_market_event(event)?;
            Ok(Some(BarterIngestionEnvelope::from_event(source_id, event)))
        }
        reconnect::Event::Item(Err(error)) => {
            Err(BarterAdapterError::LiveStreamItem(error.to_string()))
        }
    }
}

/// Backward-compatible wrapper for the original trade-only name.
pub fn map_live_trade_result(
    source_id: &str,
    result: MarketStreamResult<MarketDataInstrument, DataKind>,
) -> Result<Option<BarterIngestionEnvelope>> {
    map_live_market_data_result(source_id, result)
}

/// Collect the next `limit` emitted live market-data envelopes from a stream.
pub async fn collect_live_market_data_envelopes<S>(
    source_id: &str,
    mut stream: S,
    limit: usize,
) -> Result<Vec<BarterIngestionEnvelope>>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    let mut envelopes = Vec::with_capacity(limit);

    while envelopes.len() < limit {
        let Some(result) = stream.next().await else {
            break;
        };

        if let Some(envelope) = map_live_market_data_result(source_id, result)? {
            envelopes.push(envelope);
        }
    }

    Ok(envelopes)
}

/// Backward-compatible wrapper for the original trade-only name.
pub async fn collect_live_trade_envelopes<S>(
    source_id: &str,
    stream: S,
    limit: usize,
) -> Result<Vec<BarterIngestionEnvelope>>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    collect_live_market_data_envelopes(source_id, stream, limit).await
}
```

Remove the old duplicate implementations of `map_live_trade_result` and `collect_live_trade_envelopes` so each function exists once.

Update `crates/fdc-adapter/barter/src/lib.rs` ingestion exports:

```rust
collect_live_market_data_envelopes, collect_live_trade_envelopes,
default_binance_spot_trade_subscriptions, init_binance_spot_public_trades,
map_live_market_data_result, map_live_trade_result, public_trade_result_to_data_kind,
```

- [ ] **Step 4: Run live tests**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/live.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/live_acquisition_contract.rs
git commit -m "refactor: generalize barter live market data mapping"
```

---

## Task 5: Add multi-kind live subscription model without changing production startup

**Files:**

- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`

- [ ] **Step 1: Add tests for multi-kind subscription model**

Append to `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`:

```rust
#[test]
fn live_market_data_subscription_accepts_multiple_data_kinds() {
    let subscription = fdc_barter::LiveMarketDataSubscription::new(
        LiveExchange::BinanceSpot,
        "btc",
        "usdt",
        barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind::Spot,
        BarterMarketDataKind::OrderBookL1,
    );

    assert_eq!(subscription.exchange, LiveExchange::BinanceSpot);
    assert_eq!(subscription.base, "btc");
    assert_eq!(subscription.quote, "usdt");
    assert_eq!(subscription.kind, BarterMarketDataKind::OrderBookL1);
}

#[test]
fn default_binance_spot_market_data_subscriptions_include_trade_l1_and_l2() {
    let subscriptions = fdc_barter::default_binance_spot_market_data_subscriptions();

    assert!(subscriptions.iter().any(|subscription| subscription.kind == BarterMarketDataKind::Trade));
    assert!(subscriptions.iter().any(|subscription| subscription.kind == BarterMarketDataKind::OrderBookL1));
    assert!(subscriptions.iter().any(|subscription| subscription.kind == BarterMarketDataKind::OrderBook));
}
```

- [ ] **Step 2: Run failing test**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: compile failure because `LiveMarketDataSubscription` and `default_binance_spot_market_data_subscriptions` do not exist.

- [ ] **Step 3: Implement subscription model**

In `crates/fdc-adapter/barter/src/ingestion/live.rs`, add imports:

```rust
use crate::model::BarterMarketDataKind;
```

Add after `LiveTradeSubscription`:

```rust
/// Generic market-data subscription accepted by expanded live acquisition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveMarketDataSubscription {
    pub exchange: LiveExchange,
    pub base: String,
    pub quote: String,
    pub instrument_kind: MarketDataInstrumentKind,
    pub kind: BarterMarketDataKind,
}

impl LiveMarketDataSubscription {
    pub fn new(
        exchange: LiveExchange,
        base: impl Into<String>,
        quote: impl Into<String>,
        instrument_kind: MarketDataInstrumentKind,
        kind: BarterMarketDataKind,
    ) -> Self {
        Self {
            exchange,
            base: base.into(),
            quote: quote.into(),
            instrument_kind,
            kind,
        }
    }
}

/// Default first expanded subscriptions for Binance Spot BTC/USDT and ETH/USDT.
pub fn default_binance_spot_market_data_subscriptions() -> Vec<LiveMarketDataSubscription> {
    ["btc", "eth"]
        .into_iter()
        .flat_map(|base| {
            [
                BarterMarketDataKind::Trade,
                BarterMarketDataKind::OrderBookL1,
                BarterMarketDataKind::OrderBook,
            ]
            .into_iter()
            .map(move |kind| {
                LiveMarketDataSubscription::new(
                    LiveExchange::BinanceSpot,
                    base,
                    "usdt",
                    MarketDataInstrumentKind::Spot,
                    kind,
                )
            })
        })
        .collect()
}
```

Update `crates/fdc-adapter/barter/src/lib.rs` ingestion exports:

```rust
LiveExchange, LiveMarketDataSubscription, LiveTradeSubscription,
default_binance_spot_market_data_subscriptions,
```

- [ ] **Step 4: Run live acquisition tests**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/live.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/live_acquisition_contract.rs
git commit -m "feat: add barter live market data subscriptions"
```

---
## Task 6: Add ignored live smoke boundary for expanded Binance Spot market data

**Files:**

- Modify: `crates/fdc-adapter/barter/src/ingestion/live.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`

- [ ] **Step 1: Add ignored smoke test for expanded initializer**

Append to `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`:

```rust
#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_can_initialize_expanded_binance_spot_market_data() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = fdc_barter::init_binance_spot_market_data(
        fdc_barter::default_binance_spot_market_data_subscriptions(),
    )
    .await
    .expect("expanded Binance Spot stream should initialize");

    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        collect_live_market_data_envelopes(SOURCE_ID, streams.select_all(), 1),
    )
    .await
    .expect("should receive one live event within timeout")
    .expect("live collection should succeed");

    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].event.exchange, "binance_spot");
}
```

- [ ] **Step 2: Run ignored-test compile check**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract -- --ignored --list
```

Expected: compile failure because `init_binance_spot_market_data` does not exist.

- [ ] **Step 3: Implement initializer using Barter-rs MultiStreamBuilder**

In `crates/fdc-adapter/barter/src/ingestion/live.rs`, update imports:

```rust
use barter_data::{
    event::{DataKind, MarketEvent},
    exchange::binance::spot::BinanceSpot,
    streams::{builder::multi::MultiStreamBuilder, consumer::MarketStreamResult, reconnect, Streams},
    subscription::{
        book::{OrderBooksL1, OrderBooksL2},
        trade::{PublicTrade, PublicTrades},
    },
};
```

Add function:

```rust
/// Start expanded Barter-rs Binance Spot market-data streams for trades, L1, and L2.
pub async fn init_binance_spot_market_data(
    subscriptions: impl IntoIterator<Item = LiveMarketDataSubscription>,
) -> Result<Streams<MarketStreamResult<MarketDataInstrument, DataKind>>> {
    let mut trade_subscriptions = Vec::new();
    let mut l1_subscriptions = Vec::new();
    let mut l2_subscriptions = Vec::new();

    for subscription in subscriptions {
        if subscription.exchange != LiveExchange::BinanceSpot {
            return Err(BarterAdapterError::UnsupportedLiveSubscription(format!(
                "{:?}:{}{}:{:?}",
                subscription.exchange, subscription.base, subscription.quote, subscription.kind
            )));
        }

        match subscription.kind {
            BarterMarketDataKind::Trade => trade_subscriptions.push((
                BinanceSpot::default(),
                subscription.base,
                subscription.quote,
                subscription.instrument_kind,
                PublicTrades,
            )),
            BarterMarketDataKind::OrderBookL1 => l1_subscriptions.push((
                BinanceSpot::default(),
                subscription.base,
                subscription.quote,
                subscription.instrument_kind,
                OrderBooksL1,
            )),
            BarterMarketDataKind::OrderBook => l2_subscriptions.push((
                BinanceSpot::default(),
                subscription.base,
                subscription.quote,
                subscription.instrument_kind,
                OrderBooksL2,
            )),
            unsupported => {
                return Err(BarterAdapterError::UnsupportedLiveSubscription(format!(
                    "binance_spot:{unsupported:?}"
                )));
            }
        }
    }

    let mut builder = MultiStreamBuilder::<MarketStreamResult<MarketDataInstrument, DataKind>>::new();

    if !trade_subscriptions.is_empty() {
        builder = builder.add(Streams::<PublicTrades>::builder().subscribe(trade_subscriptions));
    }
    if !l1_subscriptions.is_empty() {
        builder = builder.add(Streams::<OrderBooksL1>::builder().subscribe(l1_subscriptions));
    }
    if !l2_subscriptions.is_empty() {
        builder = builder.add(Streams::<OrderBooksL2>::builder().subscribe(l2_subscriptions));
    }

    builder
        .init()
        .await
        .map_err(|error| BarterAdapterError::LiveStreamInit(error.to_string()))
}
```

Use `barter_data::streams::builder::multi::MultiStreamBuilder`, which exists in the local Barter-rs crate used by this workspace.

Update `crates/fdc-adapter/barter/src/lib.rs` exports:

```rust
init_binance_spot_market_data,
```

- [ ] **Step 4: Compile ignored tests without running network**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract -- --ignored --list
```

Expected: command lists ignored tests and does not connect to network.

- [ ] **Step 5: Run normal live acquisition tests**

```bash
rtk cargo test -p fdc-barter --test live_acquisition_contract
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/live.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/live_acquisition_contract.rs
git commit -m "feat: initialize expanded binance spot market data streams"
```

---

## Task 7: Full verification and module docs update

**Files:**

- Modify: `crates/fdc-adapter/barter/docs/README.md`

- [ ] **Step 1: Update module docs index**

Modify `crates/fdc-adapter/barter/docs/README.md`:

```markdown
# fdc-barter Module Docs

This directory contains module-level design notes for `crates/fdc-adapter/barter`.

Documents:

- [Market Data Collection Requirements Design](./market-data-collection-requirements.md)
- [Market Data Collection Implementation Plan](./market-data-collection-implementation-plan.md)

Guideline:

- Keep Barter-specific acquisition, mapping, capability, and historical-data designs here.
- Keep cross-module platform architecture under repository-level `docs/`.
- Preserve crate boundaries: this module may depend on Barter-rs, but generic ingestion, transform, storage, and server layers should not embed Barter-rs details.
```

- [ ] **Step 2: Run full fdc-barter tests**

```bash
rtk cargo test -p fdc-barter
```

Expected: all non-ignored tests pass.

- [ ] **Step 3: Run workspace check if time allows**

```bash
rtk cargo test --workspace
```

Expected: workspace tests pass. If unrelated tests fail, record exact failures and verify `fdc-barter` package tests pass.

- [ ] **Step 4: Inspect dependency boundary**

Run:

```bash
grep -R "barter_data\|barter-instrument\|barter_instrument" -n crates \
  | grep -v "crates/fdc-adapter/barter" \
  | grep -v "target" || true
```

Expected: no output. This confirms Barter-rs dependencies remain inside `fdc-barter`.

- [ ] **Step 5: Commit docs and final verification notes**

```bash
git add crates/fdc-adapter/barter/docs/README.md \
  crates/fdc-adapter/barter/docs/market-data-collection-implementation-plan.md
git commit -m "docs: plan barter market data implementation"
```

Create a normal docs commit for the plan and README update.

---

## Self-review checklist for the implementer

Before claiming completion:

- [ ] `supported_crypto_market_data_capabilities()` exists and returns the planned first-slice matrix.
- [ ] `BarterMarketType` is present on `BarterMarketEvent`.
- [ ] Trade mapping behavior is unchanged.
- [ ] L1 maps to `BarterMarketPayload::OrderBookL1` with bid/ask price and quantity.
- [ ] L2 snapshot/update maps to `BarterMarketPayload::OrderBook` with update kind, levels, and sequence.
- [ ] Liquidation maps to `BarterMarketPayload::Liquidation` with side, price, quantity, and liquidation time.
- [ ] Reconnect events still do not emit envelopes.
- [ ] Old public trade-only live functions still exist as compatibility wrappers.
- [ ] Expanded live stream initialization is covered by ignored smoke tests and normal compile tests.
- [ ] No non-barter crate imports Barter-rs crates.
- [ ] `rtk cargo test -p fdc-barter` passes.

## Execution recommendation

Use subagent-driven development for Tasks 1 to 7, one task per subagent, with review after each task. If executing inline, use `superpowers:executing-plans` and commit after each task exactly as listed.
