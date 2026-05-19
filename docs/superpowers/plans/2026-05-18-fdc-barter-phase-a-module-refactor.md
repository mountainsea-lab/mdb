# fdc-barter Phase A 分步实施计划：业务分包与核心模型

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `crates/fdc-adapter/barter` 从单文件骨架重构为按业务划分的模块结构，补齐实时/历史数据获取前必须存在的核心模型，但不接真实网络。

**Review Input:** 代码实现前先 review `docs/architecture/fdc-barter-phase-a-pseudocode-review.md`。该文档用伪代码梳理实时映射、历史分页/checkpoint、ingestion envelope、source 状态机和 capability 业务逻辑。

**Architecture:** Phase A 只处理 `fdc-barter` crate 内部边界。先用伪代码梳理业务流程和数据结构，再用 TDD 落地 `model / mapper / ingestion / capability` 模块。`live` 和 `historical` 只创建类型边界，不启动 Barter WebSocket 或 REST。

**Tech Stack:** Rust 2021, `fdc-core`, `barter-data`, `barter-instrument`, `chrono`, `serde`, `thiserror`, `rust_decimal`.

---

## Scope

### In Scope

- 拆分 `src/lib.rs`。
- 新增业务模块：
  - `config.rs`
  - `error.rs`
  - `model/event.rs`
  - `model/request.rs`
  - `model/source.rs`
  - `model/checkpoint.rs`
  - `mapper/event.rs`
  - `mapper/instrument.rs`
  - `mapper/exchange.rs`
  - `ingestion/envelope.rs`
  - `capability/exchange.rs`
- 将当前 `BarterMarketEvent` 从 `price/volume` 简化模型调整为 `payload` 模型。
- 增加 `BarterIngestionEnvelope`。
- 增加 `BarterMarketDataRequest`。
- 增加 `BarterCheckpoint` / `HistoricalCursor`。
- 增加 `BarterSourceStatus` / `BarterSourceState`。
- 增加交易所能力声明基础类型。
- 保持现有 public trade mapper 能力。
- 使用伪代码文档作为代码实现前 review 输入。

### Out of Scope

- 不连接真实 WebSocket。
- 不实现 Binance/OKX REST 历史数据。
- 不修改 `fdc-ingestion`。
- 不接 `fdc-transform`。
- 不新增 `fdc-market-data-core`。
- 不处理 workspace 既有 warnings。

## Requirements

### Functional Requirements

- FR-1: `fdc-barter` 必须继续导出当前外部使用的核心类型名，避免无意义破坏。
- FR-2: public trade Barter `MarketEvent` 必须能映射为 `BarterMarketEvent { payload: Trade(...) }`。
- FR-3: crypto 数量必须保留小数精度，不能再强制转换为 `Volume(u64)`。
- FR-4: 历史请求必须能表达时间范围、分页 cursor、limit。
- FR-5: checkpoint 必须能表达 source、exchange、symbol、kind、last_event_time 和 cursor。
- FR-6: ingestion envelope 必须能携带 event、checkpoint、质量标记。
- FR-7: source 状态必须区分 live running、historical backfilling、reconnecting、rate limited、failed。
- FR-8: capability 必须能描述交易所支持的 live/historical kinds 和 rate limit。

### Non-functional Requirements

- NFR-1: 单元测试不访问真实网络。
- NFR-2: 每个模块职责单一，文件保持小而清晰。
- NFR-3: `cargo test -p fdc-barter` 必须通过。
- NFR-4: `cargo check -p fdc-barter --all-targets` 必须通过。

## Business Flow Pseudocode

### 1. Real-time mapper flow, no network in Phase A

```text
Given Barter MarketEvent<MarketDataInstrument, DataKind>
When mapper::event::map_market_event receives it
Then mapper::instrument converts Barter instrument to mdb Symbol
And mapper::exchange converts Barter exchange to snake_case string
And mapper::event converts DataKind::Trade into BarterMarketPayload::Trade
And BarterMarketEvent is returned with mode = Live
```

Pseudo-Rust:

```rust
fn map_market_event(event: MarketEvent<MarketDataInstrument, DataKind>) -> Result<BarterMarketEvent> {
    let symbol = mapper::instrument::to_symbol(&event.instrument);
    let exchange = mapper::exchange::to_exchange_code(event.exchange);
    let payload = match event.kind {
        DataKind::Trade(trade) => BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade.id),
            price: DecimalPrice::from_f64(trade.price)?,
            quantity: DecimalQuantity::from_f64(trade.amount)?,
            side: Some(map_side(trade.side)),
        }),
        DataKind::OrderBookL1(book) => BarterMarketPayload::OrderBookL1(map_l1(book)?),
        DataKind::Candle(candle) => BarterMarketPayload::Candle(map_candle(candle)?),
        other => BarterMarketPayload::Raw(map_raw(other)?),
    };

    Ok(BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange,
        symbol,
        kind: payload.kind(),
        timestamp: TimestampNs::from_nanos(event.time_exchange.timestamp_nanos_opt()?),
        received_at: TimestampNs::from_nanos(event.time_received.timestamp_nanos_opt()?),
        payload,
        sequence: None,
        checkpoint: None,
    })
}
```

### 2. Ingestion envelope flow

```text
Given BarterMarketEvent
When ingestion::envelope::BarterIngestionEnvelope::from_event creates envelope
Then envelope_id is generated
And source_id is copied from event/source config
And quality flags default to false
And checkpoint is attached if present
```

Pseudo-Rust:

```rust
fn envelope_from_event(source_id: String, event: BarterMarketEvent) -> BarterIngestionEnvelope {
    BarterIngestionEnvelope {
        envelope_id: Uuid::new_v4().to_string(),
        source_id,
        emitted_at: TimestampNs::now(),
        checkpoint: event.checkpoint.clone(),
        quality: DataQualityFlags::default(),
        event,
    }
}
```

### 3. Historical checkpoint flow, no REST in Phase A

```text
Given HistoricalPageRequest and last emitted event
When checkpoint is updated
Then checkpoint stores last_event_time and next cursor
And can be attached to subsequent envelopes
```

Pseudo-Rust:

```rust
fn update_checkpoint(
    request: &HistoricalPageRequest,
    last_event_time: TimestampNs,
    cursor: HistoricalCursor,
) -> BarterCheckpoint {
    BarterCheckpoint {
        source_id: request.source_id.clone(),
        exchange: request.exchange.clone(),
        symbol: request.symbol.clone(),
        kind: request.kind,
        mode: BarterMarketDataMode::Historical { start: request.start, end: request.end },
        last_event_time,
        cursor: Some(cursor),
        updated_at: TimestampNs::now(),
    }
}
```

## File Structure

### Create

- `crates/fdc-adapter/barter/src/config.rs`
- `crates/fdc-adapter/barter/src/error.rs`
- `crates/fdc-adapter/barter/src/model/mod.rs`
- `crates/fdc-adapter/barter/src/model/event.rs`
- `crates/fdc-adapter/barter/src/model/request.rs`
- `crates/fdc-adapter/barter/src/model/source.rs`
- `crates/fdc-adapter/barter/src/model/checkpoint.rs`
- `crates/fdc-adapter/barter/src/mapper/mod.rs`
- `crates/fdc-adapter/barter/src/mapper/event.rs`
- `crates/fdc-adapter/barter/src/mapper/instrument.rs`
- `crates/fdc-adapter/barter/src/mapper/exchange.rs`
- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- `crates/fdc-adapter/barter/src/ingestion/envelope.rs`
- `crates/fdc-adapter/barter/src/capability/mod.rs`
- `crates/fdc-adapter/barter/src/capability/exchange.rs`
- `crates/fdc-adapter/barter/tests/model_contract.rs`
- `crates/fdc-adapter/barter/tests/envelope_contract.rs`
- `crates/fdc-adapter/barter/tests/checkpoint_contract.rs`

### Modify

- `crates/fdc-adapter/barter/src/lib.rs`
- `crates/fdc-adapter/barter/tests/adapter_contract.rs`
- `crates/fdc-adapter/barter/Cargo.toml` if extra dependencies are required

## Task 1: Module Shell and Public Re-exports

**Files:**
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Create: module files listed above

- [ ] Step 1: Write failing compile-focused tests in `tests/model_contract.rs`

```rust
use fdc_barter::{
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterSourceStatus, BarterSourceState,
};

#[test]
fn request_and_source_state_types_are_public() {
    let request = BarterMarketDataRequest::live(
        "binance_spot",
        vec!["BTCUSDT"],
        vec![BarterMarketDataKind::Trade],
    );

    assert_eq!(request.exchange, "binance_spot");
    assert_eq!(request.symbols, vec!["BTCUSDT".to_string()]);
    assert_eq!(request.mode, BarterMarketDataMode::Live);

    let state = BarterSourceState::new("barter-binance-live", BarterMarketDataMode::Live);
    assert_eq!(state.source_id, "barter-binance-live");
    assert_eq!(state.status, BarterSourceStatus::Created);
}
```

- [ ] Step 2: Run failing test

Run:

```bash
cargo test -p fdc-barter --test model_contract
```

Expected: FAIL because `BarterMarketDataRequest`, `BarterSourceStatus`, and `BarterSourceState` are not defined.

- [ ] Step 3: Create module files and move existing types into them

Implementation outline:

```rust
// src/lib.rs
pub mod capability;
pub mod config;
pub mod error;
pub mod ingestion;
pub mod mapper;
pub mod model;

pub use config::BarterAdapterConfig;
pub use error::{BarterAdapterError, Result};
pub use ingestion::{BarterIngestionEnvelope, DataQualityFlags};
pub use mapper::event::map_market_event;
pub use model::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterMarketEvent, BarterMarketPayload, BarterSourceState, BarterSourceStatus,
    HistoricalCursor, HistoricalPageRequest,
};
```

- [ ] Step 4: Run test and crate tests

```bash
cargo test -p fdc-barter --test model_contract
cargo test -p fdc-barter
```

Expected: PASS.

- [ ] Step 5: Commit

```bash
git add crates/fdc-adapter/barter
 git commit -m "refactor: split fdc-barter module shell"
```

## Task 2: Payload Model with Decimal Crypto Quantity

**Files:**
- Modify: `crates/fdc-adapter/barter/src/model/event.rs`
- Modify: `crates/fdc-adapter/barter/src/mapper/event.rs`
- Modify: `crates/fdc-adapter/barter/tests/adapter_contract.rs`

- [ ] Step 1: Update failing trade mapper test

Replace price/volume assertions with payload assertions:

```rust
use fdc_barter::{BarterMarketDataKind, BarterMarketEvent, BarterMarketPayload, TradeSide};

match event.payload {
    BarterMarketPayload::Trade(trade) => {
        assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
        assert_eq!(trade.price.to_f64(), 65000.25);
        assert_eq!(trade.quantity.to_string(), "2.5");
        assert_eq!(trade.side, Some(TradeSide::Buy));
    }
    other => panic!("expected trade payload, got {other:?}"),
}
assert_eq!(event.kind, BarterMarketDataKind::Trade);
```

- [ ] Step 2: Run failing test

```bash
cargo test -p fdc-barter --test adapter_contract
```

Expected: FAIL because `payload`, `BarterMarketPayload`, and `TradeSide` are not implemented.

- [ ] Step 3: Implement payload types

Use `rust_decimal::Decimal` for crypto quantity:

```rust
pub type DecimalQuantity = rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradePayload {
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub side: Option<TradeSide>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BarterMarketPayload {
    Trade(TradePayload),
    OrderBookL1(OrderBookL1Payload),
    OrderBookDelta(OrderBookDeltaPayload),
    Candle(CandlePayload),
    Liquidation(LiquidationPayload),
    Raw(RawPayload),
}
```

- [ ] Step 4: Update mapper

`PublicTrade.amount` must become `Decimal`, not `Volume(u64)`.

- [ ] Step 5: Run tests

```bash
cargo test -p fdc-barter --test adapter_contract
cargo test -p fdc-barter
```

Expected: PASS.

- [ ] Step 6: Commit

```bash
git add crates/fdc-adapter/barter
 git commit -m "feat: add fdc-barter payload model"
```

## Task 3: Historical Request, Cursor, and Checkpoint

**Files:**
- Create/Modify: `src/model/checkpoint.rs`
- Create/Modify: `src/model/request.rs`
- Test: `tests/checkpoint_contract.rs`

- [ ] Step 1: Write failing checkpoint test

```rust
use fdc_barter::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, HistoricalCursor,
    HistoricalPageRequest,
};
use fdc_core::types::TimestampNs;

#[test]
fn historical_checkpoint_captures_resume_cursor() {
    let request = HistoricalPageRequest::new(
        "barter-binance-history",
        "binance_spot",
        "BTCUSDT",
        BarterMarketDataKind::Candle,
        TimestampNs::from_nanos(1_000),
        Some(TimestampNs::from_nanos(2_000)),
        Some(1000),
    );

    let cursor = HistoricalCursor::next_start(
        "binance_spot",
        "BTCUSDT",
        BarterMarketDataKind::Candle,
        TimestampNs::from_nanos(1_500),
    );

    let checkpoint = BarterCheckpoint::from_historical_page(
        &request,
        TimestampNs::from_nanos(1_499),
        cursor.clone(),
    );

    assert_eq!(checkpoint.source_id, "barter-binance-history");
    assert_eq!(checkpoint.exchange, "binance_spot");
    assert_eq!(checkpoint.symbol, "BTCUSDT");
    assert_eq!(checkpoint.kind, BarterMarketDataKind::Candle);
    assert_eq!(checkpoint.cursor, Some(cursor));
}
```

- [ ] Step 2: Run failing test

```bash
cargo test -p fdc-barter --test checkpoint_contract
```

Expected: FAIL because historical request/checkpoint types do not exist.

- [ ] Step 3: Implement types and constructors

Implement exactly what the test uses.

- [ ] Step 4: Run tests

```bash
cargo test -p fdc-barter --test checkpoint_contract
cargo test -p fdc-barter
```

Expected: PASS.

- [ ] Step 5: Commit

```bash
git add crates/fdc-adapter/barter
 git commit -m "feat: add fdc-barter historical checkpoint model"
```

## Task 4: Ingestion Envelope and Data Quality Flags

**Files:**
- Create/Modify: `src/ingestion/envelope.rs`
- Test: `tests/envelope_contract.rs`

- [ ] Step 1: Write failing envelope test

```rust
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode,
    BarterMarketEvent, BarterMarketPayload, DataQualityFlags, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;

#[test]
fn envelope_wraps_event_with_default_quality_flags() {
    let event = BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_000),
        received_at: TimestampNs::from_nanos(1_100),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("t1".to_string()),
            price: Price::from_f64(100.0).unwrap(),
            quantity: Decimal::new(25, 1),
            side: Some(TradeSide::Buy),
        }),
        sequence: None,
        checkpoint: None,
    };

    let envelope = BarterIngestionEnvelope::from_event("source-1", event);

    assert_eq!(envelope.source_id, "source-1");
    assert!(!envelope.envelope_id.is_empty());
    assert_eq!(envelope.quality, DataQualityFlags::default());
    assert_eq!(envelope.event.exchange, "binance_spot");
}
```

- [ ] Step 2: Run failing test

```bash
cargo test -p fdc-barter --test envelope_contract
```

Expected: FAIL because envelope types do not exist.

- [ ] Step 3: Implement envelope and quality flags

- [ ] Step 4: Run tests

```bash
cargo test -p fdc-barter --test envelope_contract
cargo test -p fdc-barter
```

Expected: PASS.

- [ ] Step 5: Commit

```bash
git add crates/fdc-adapter/barter
 git commit -m "feat: add fdc-barter ingestion envelope"
```

## Task 5: Capability and Source State

**Files:**
- Create/Modify: `src/model/source.rs`
- Create/Modify: `src/capability/exchange.rs`
- Test: `tests/model_contract.rs`

- [ ] Step 1: Extend model contract test

```rust
use fdc_barter::{
    BarterMarketDataKind, BarterSourceCapabilities, BarterSourceStatus, RateLimitRule,
};

#[test]
fn capabilities_describe_live_and_historical_support() {
    let capabilities = BarterSourceCapabilities::crypto_exchange(
        "binance_spot",
        vec![BarterMarketDataKind::Trade, BarterMarketDataKind::Candle],
        vec![BarterMarketDataKind::Trade],
        vec![RateLimitRule::new("binance_spot", "klines", 1200, 60_000, 1)],
    );

    assert!(capabilities.supports_live);
    assert!(capabilities.supports_historical);
    assert!(capabilities.kinds.contains(&BarterMarketDataKind::Trade));
}
```

- [ ] Step 2: Run failing test

```bash
cargo test -p fdc-barter --test model_contract
```

Expected: FAIL because capability types do not exist.

- [ ] Step 3: Implement capability and rate limit types

- [ ] Step 4: Run tests

```bash
cargo test -p fdc-barter --test model_contract
cargo test -p fdc-barter
```

Expected: PASS.

- [ ] Step 5: Commit

```bash
git add crates/fdc-adapter/barter
 git commit -m "feat: add fdc-barter source capabilities"
```

## Task 6: Final Verification

- [ ] Step 1: Run fdc-barter tests

```bash
cargo test -p fdc-barter
```

Expected: all fdc-barter tests pass.

- [ ] Step 2: Run fdc-barter check

```bash
cargo check -p fdc-barter --all-targets
```

Expected: command exits 0.

- [ ] Step 3: Run workspace check

```bash
cargo check --workspace --all-targets
```

Expected: command exits 0. Existing warnings may remain.

- [ ] Step 4: Confirm no real network code was added

```bash
grep -R "DynamicStreams::init\|connect_async\|reqwest::Client" -n crates/fdc-adapter/barter/src || true
```

Expected: no live network startup code in Phase A. Type references in docs/tests are acceptable only outside `src`.

- [ ] Step 5: Commit any final cleanup

```bash
git status --short
```

Expected: clean after commits.

## Review Checklist

- [ ] Requirements covered: request, payload, checkpoint, envelope, source status, capability.
- [ ] No real network I/O.
- [ ] Crypto quantity keeps decimal precision.
- [ ] `fdc-barter` remains independent from `fdc-ingestion` implementation.
- [ ] `fdc-transform` does not depend on Barter types.
- [ ] Existing tests updated rather than deleted.
- [ ] Commands in Task 6 pass.
