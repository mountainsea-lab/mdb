# fdc-data market Detailed Data Structure Design

本文档详细设计 `fdc_data::market` 市场数据结构。

它是 `docs/modules/fdc-data.md` 的 market 细化文档。本文档只做数据结构设计，不做实现任务拆解。用户审查通过后，再进入实现计划。

设计优先级：

1. Spot / futures 通用市场数据；
2. 支持高频行情 ingestion、storage、query、transform；
3. 保留 derivatives 常用字段；
4. 不引入 adapter-specific raw schema；
5. 暂不完整设计 portfolio、factor、feature、analytics。

---

## 1. Design scope

`fdc_data::market` 负责 FDC 内部 canonical market data model。

第一版覆盖：

```text
Trade
Quote
Bar
OrderBookSnapshot
OrderBookDelta
FundingRate
OpenInterest
MarkPrice
IndexPrice
Liquidation
MarketEvent
MarketDataSelector
```

第一版不覆盖：

```text
full order lifecycle
private account order update
portfolio position
strategy signal
feature vector
factor value
vendor raw payload
exchange-specific schema
```

这些分别属于 `fdc-data::portfolio`、`fdc-feature`、`fdc-factor`、`fdc-adapter` 或其他模块。

---

## 2. Type imports and aliases

第一阶段为了减少迁移成本，可以复用现有基础类型。

```rust
use fdc_core::types::TimestampNs;
use fdc_core::types::Price;
use fdc_core::types::Volume;
use fdc_core::types::SequenceNumber;
use fdc_data::common::{DataQuality, Lineage, SourceId};
use fdc_data::reference::InstrumentId;
```

第一阶段数量类型：

```rust
pub type Quantity = Volume;
```

长期目标：

```rust
pub struct Quantity(pub Decimal);
```

审查点：如果市场模型一开始就要支持 crypto 小数数量，则实现阶段不应继续使用 `Volume(u64)`，应直接引入 decimal-backed `Quantity`。

---

## 3. Module layout

建议 market module 文件结构：

```text
crates/fdc-data/src/market/
  mod.rs
  kind.rs
  side.rs
  trade.rs
  quote.rs
  bar.rs
  orderbook.rs
  derivatives.rs
  price.rs
  event.rs
  selector.rs
  validation.rs
```

职责：

| File | Responsibility |
| --- | --- |
| `kind.rs` | `MarketDataKind` and stream classification |
| `side.rs` | `AggressorSide`, `BookSide`, side semantics |
| `trade.rs` | public trade data |
| `quote.rs` | top-of-book quote data |
| `bar.rs` | OHLCV and bar interval semantics |
| `orderbook.rs` | snapshot/delta/price level |
| `derivatives.rs` | funding, open interest, mark/index price, liquidation |
| `price.rs` | market price semantic enum, not value type |
| `event.rs` | `MarketEvent` enum and shared accessors |
| `selector.rs` | market query selector contracts |
| `validation.rs` | lightweight data model validation helpers |

---

## 4. Shared market enums

### 4.1 MarketDataKind

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketDataKind {
    Trade,
    Quote,
    Bar,
    OrderBookSnapshot,
    OrderBookDelta,
    FundingRate,
    OpenInterest,
    MarkPrice,
    IndexPrice,
    Liquidation,
}
```

Semantics:

| Variant | Meaning | Primary time field |
| --- | --- | --- |
| `Trade` | Public execution print | `event_time` |
| `Quote` | Top-of-book bid/ask | `event_time` |
| `Bar` | OHLCV aggregate | `open_time`, `close_time` |
| `OrderBookSnapshot` | Full or partial book snapshot | `event_time` |
| `OrderBookDelta` | Incremental book changes | `event_time` |
| `FundingRate` | Perpetual/futures funding rate | `funding_time`, `event_time` |
| `OpenInterest` | Outstanding contracts/positions | `event_time` |
| `MarkPrice` | Venue mark price | `event_time` |
| `IndexPrice` | Venue/index basket price | `event_time` |
| `Liquidation` | Public liquidation print/order | `event_time` |

Usage:

- stream subscription;
- storage partition;
- query filter;
- metrics;
- event routing.

### 4.2 AggressorSide

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}
```

Semantics:

| Variant | Meaning |
| --- | --- |
| `Buy` | Buyer was aggressor / taker buy |
| `Sell` | Seller was aggressor / taker sell |
| `Unknown` | Source did not provide enough information |

Rules:

- Do not use `Bid`/`Ask` for trade aggressor side.
- Adapter must normalize source-specific flags such as Binance `isBuyerMaker` into this enum.
- If a source exposes maker side only, adapter must invert it correctly or use `Unknown`.

### 4.3 BookSide

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}
```

### 4.4 BookAction

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookAction {
    Upsert,
    Delete,
}
```

Rules:

- Exchange payloads often encode deletion as `quantity = 0`.
- Canonical `OrderBookDelta` should map that to `BookAction::Delete`.
- Consumers should not need to know source-specific delete conventions.

### 4.5 MarketPriceType

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketPriceType {
    Last,
    Bid,
    Ask,
    Mid,
    Mark,
    Index,
    Settlement,
}
```

This is market price semantics. It must not be confused with `fdc-types::PriceType`, which describes a field's type definition.

---

## 5. Shared event metadata pattern

Most market objects share these fields:

```rust
pub instrument_id: InstrumentId,
pub event_time: TimestampNs,
pub received_time: Option<TimestampNs>,
pub source_sequence: Option<SequenceNumber>,
pub quality: Option<DataQuality>,
pub lineage: Option<Lineage>,
```

Meaning:

| Field | Required | Owner | Meaning |
| --- | --- | --- | --- |
| `instrument_id` | yes | adapter/reference mapping | canonical instrument id |
| `event_time` | yes | source/adapter | exchange or source event timestamp |
| `received_time` | no | adapter/ingestion | local receive timestamp |
| `source_sequence` | no | source/adapter | source-provided update sequence |
| `quality` | no | adapter/ingestion/validation | standardized quality flags |
| `lineage` | no | adapter/transform | source and parent data provenance |

Design choice:

- Do not create a generic embedded `MarketMetadata` struct in the first version.
- Keep fields explicit in each struct for Arrow/serde/schema clarity.
- Helper traits can provide common accessors.

Possible future trait:

```rust
pub trait MarketRecord {
    fn kind(&self) -> MarketDataKind;
    fn instrument_id(&self) -> &InstrumentId;
    fn event_time(&self) -> TimestampNs;
    fn received_time(&self) -> Option<TimestampNs>;
    fn quality(&self) -> Option<&DataQuality>;
    fn lineage(&self) -> Option<&Lineage>;
}
```

---

## 6. Trade

### 6.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub instrument_id: InstrumentId,
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: Quantity,
    pub side: AggressorSide,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub source_sequence: Option<SequenceNumber>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 6.2 Field semantics

| Field | Required | Cardinality | Semantics |
| --- | --- | --- | --- |
| `instrument_id` | yes | one | canonical instrument id |
| `trade_id` | no | one | source trade id, not globally unique |
| `price` | yes | one | execution price |
| `quantity` | yes | one | executed base/contract quantity |
| `side` | yes | one | taker/aggressor side |
| `event_time` | yes | one | source event timestamp |
| `received_time` | no | one | local receive timestamp |
| `source_sequence` | no | one | source sequence or update id |
| `quality` | no | one | quality metadata |
| `lineage` | no | one | source lineage |

### 6.3 Validation rules

Required validation:

```text
instrument_id is not empty
price > 0
quantity > 0
event_time is valid timestamp
received_time >= event_time is not required, because clock skew exists
```

Recommended validation:

```text
if received_time exists and received_time < event_time by large threshold -> quality flag ClockSkewCandidate or Late/OutOfOrder diagnostic
if trade_id is empty string -> normalize to None
```

### 6.4 Identity and deduplication

`Trade` does not define a global primary key.

Suggested deduplication key candidates:

```text
source_id + instrument_id + trade_id
source_id + instrument_id + source_sequence
source_id + instrument_id + event_time + price + quantity + side
```

Owner:

- Adapter can provide source identity via `Lineage.source`.
- Ingestion/storage decides dedup policy.
- `fdc-data` only exposes fields needed by those policies.

### 6.5 Source mapping examples

Binance aggTrade style:

```text
symbol       -> InstrumentRegistry.resolve_symbol(...)
aggTradeId   -> trade_id
price        -> price
quantity     -> quantity
isBuyerMaker -> side = Sell if buyer is maker, Buy otherwise
eventTime    -> event_time
receive time -> received_time
```

Generic trade print:

```text
symbol
price
size
side or aggressor flag
timestamp
trade id optional
```

---

## 7. Quote

### 7.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub instrument_id: InstrumentId,
    pub bid_price: Option<Price>,
    pub bid_quantity: Option<Quantity>,
    pub ask_price: Option<Price>,
    pub ask_quantity: Option<Quantity>,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub source_sequence: Option<SequenceNumber>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 7.2 Semantics

`Quote` represents top-of-book quote data.

Rules:

- Missing side uses `None`.
- Do not represent missing price or quantity as zero.
- Full depth belongs to `OrderBookSnapshot`.
- Incremental depth belongs to `OrderBookDelta`.

### 7.3 Validation rules

```text
instrument_id is not empty
event_time is valid
at least one of bid_price or ask_price exists
if bid_price and ask_price both exist, bid_price <= ask_price unless source explicitly allows crossed book
if bid_quantity exists, bid_price should exist
if ask_quantity exists, ask_price should exist
quantity values > 0 when present
```

If a source reports crossed quotes, do not silently fix them. Mark quality flag and preserve canonical values.

---

## 8. Bar

### 8.1 BarInterval

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarInterval {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Weeks(u32),
    Months(u32),
    Custom { label: String, duration_nanos: i64 },
}
```

Rules:

- `Seconds(60)` and `Minutes(1)` are semantically equivalent but should be normalized by transform/query conventions.
- Month bars cannot be reduced to fixed nanoseconds.
- `Custom` must have non-empty label and positive duration.

Recommended canonical labels:

```text
1s, 5s, 15s, 30s
1m, 3m, 5m, 15m, 30m
1h, 4h
1d, 1w, 1M
```

### 8.2 BarKind

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarKind {
    Time,
    Tick,
    Volume,
    Dollar,
    Imbalance,
    Range,
    Custom(String),
}
```

First implementation target: `BarKind::Time`.

### 8.3 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub instrument_id: InstrumentId,
    pub interval: BarInterval,
    pub kind: BarKind,
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub quote_volume: Option<Quantity>,
    pub trade_count: Option<u64>,
    pub vwap: Option<Price>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 8.4 Time semantics

```text
open_time inclusive
close_time exclusive
```

Example:

```text
1m bar from 10:00:00 to 10:01:00 includes trades where:
  event_time >= 10:00:00
  event_time <  10:01:00
```

### 8.5 Field semantics

| Field | Required | Meaning |
| --- | --- | --- |
| `open` | yes | first valid trade price in interval |
| `high` | yes | max trade price in interval |
| `low` | yes | min trade price in interval |
| `close` | yes | last valid trade price in interval |
| `volume` | yes | base/contract volume |
| `quote_volume` | no | quote/notional traded value |
| `trade_count` | no | number of contributing trades |
| `vwap` | no | volume-weighted average price |

### 8.6 Validation rules

```text
instrument_id is not empty
open_time < close_time
high >= low
high >= open and high >= close
low <= open and low <= close
volume >= 0
quote_volume >= 0 when present
trade_count > 0 for normal non-empty trade-derived bars
vwap between low and high when present, unless source-specific reason is recorded
```

### 8.7 Empty bars

First version should not encode empty bars with zero prices.

Recommended policy:

```text
No trade in interval -> no Bar emitted
```

Future extension can add:

```rust
pub enum BarCompleteness {
    Complete,
    Partial,
    Empty,
}
```

But first version should avoid adding this until storage/query semantics require it.

### 8.8 Derived bar lineage

For transform-generated bars:

```text
lineage.source = transform source or aggregated provider
lineage.parent_data_ids = trade ids or source batch id if individual ids are too expensive
lineage.transform_id = trade_to_bar:<version>
quality.level = Derived
```

---

## 9. OrderBookSnapshot

### 9.1 PriceLevel

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: Option<u64>,
}
```

Validation:

```text
price > 0
quantity > 0
order_count > 0 when present
```

### 9.2 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub instrument_id: InstrumentId,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub depth: Option<u32>,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub source_sequence: Option<SequenceNumber>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 9.3 Semantics

- `bids` are sorted descending by price.
- `asks` are sorted ascending by price.
- `depth` means intended source depth, not necessarily vector length if source sends partial book.
- Empty bid or ask side is allowed only if source genuinely reports one-sided book; quality should indicate partial if appropriate.

### 9.4 Validation rules

```text
instrument_id is not empty
event_time is valid
bids prices strictly descending
asks prices strictly ascending
all price > 0
all quantity > 0
if best bid and best ask exist, best bid <= best ask unless crossed source book is preserved with quality flag
if depth exists, depth > 0
```

---

## 10. OrderBookDelta

### 10.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub instrument_id: InstrumentId,
    pub changes: Vec<BookChange>,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub first_update_id: Option<SequenceNumber>,
    pub last_update_id: Option<SequenceNumber>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookChange {
    pub side: BookSide,
    pub price: Price,
    pub quantity: Quantity,
    pub action: BookAction,
}
```

### 10.2 Semantics

| Field | Meaning |
| --- | --- |
| `changes` | all level changes in this update |
| `first_update_id` | first source sequence covered by this delta |
| `last_update_id` | last source sequence covered by this delta |
| `BookAction::Upsert` | set level quantity at price |
| `BookAction::Delete` | remove level at price |

Rules:

- For `Delete`, `quantity` should be normalized to zero only if implementation requires a value. Prefer preserving source-free explicit action.
- If using decimal `Quantity`, delete can use `Quantity::ZERO` plus `BookAction::Delete`.
- If first implementation aliases `Quantity = Volume`, define a zero value policy in implementation notes.

### 10.3 Validation rules

```text
instrument_id is not empty
changes is not empty
all price > 0
Upsert quantity > 0
Delete action does not require quantity > 0
if first_update_id and last_update_id both exist, first_update_id <= last_update_id
```

Sequence continuity is not a pure data-model validation. It belongs to ingestion/order-book builder.

### 10.4 Order book reconstruction boundary

`fdc-data` defines snapshot/delta structures only.

Not in scope:

```text
apply_delta(snapshot, delta)
resync after gap
book checksum validation
local order book state machine
```

Those belong to `fdc-transform`, `fdc-ingestion`, or a future market microstructure utility module.

---

## 11. FundingRate

### 11.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRate {
    pub instrument_id: InstrumentId,
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 11.2 Semantics

| Field | Meaning |
| --- | --- |
| `funding_rate` | rate applied for the funding interval |
| `funding_time` | settlement/application time |
| `event_time` | source update time |

Rules:

- Funding rate may be negative.
- `funding_time` can be in the future for predicted/next funding rate.
- If source distinguishes predicted vs realized funding, use `quality` or future `FundingRateKind` extension.

Future possible extension:

```rust
pub enum FundingRateKind {
    Predicted,
    Realized,
}
```

Not included in first version unless required by source mapping.

---

## 12. OpenInterest

### 12.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenInterest {
    pub instrument_id: InstrumentId,
    pub open_interest: Quantity,
    pub notional_value: Option<Quantity>,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 12.2 Semantics

- `open_interest` is contract/base quantity depending on instrument specification.
- `notional_value` is optional quote/notional amount.
- Unit interpretation comes from `reference::Instrument.contract_spec` and `quantity_spec`.

Validation:

```text
open_interest >= 0
notional_value >= 0 when present
event_time valid
```

---

## 13. MarkPrice and IndexPrice

### 13.1 Structures

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPrice {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexPrice {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 13.2 Semantics

| Type | Meaning |
| --- | --- |
| `MarkPrice` | venue-defined fair price used for margin/liquidation/PnL |
| `IndexPrice` | index basket or external reference price |

Validation:

```text
price > 0
event_time valid
```

Design note:

- Do not combine them into generic `PriceUpdate` in the first version.
- Separate structs make storage partition and query semantics clearer.

---

## 14. Liquidation

### 14.1 LiquidationSide decision

Liquidation side semantics differ between venues. Using `AggressorSide` may be ambiguous.

Recommended first-version design:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LiquidationSide {
    LongLiquidated,
    ShortLiquidated,
    BuyOrder,
    SellOrder,
    Unknown,
}
```

Rationale:

- `AggressorSide::Buy/Sell` describes trade aggressor side.
- Liquidation payloads often describe liquidated position side or liquidation order side.
- Explicit enum prevents semantic confusion.

### 14.2 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Liquidation {
    pub instrument_id: InstrumentId,
    pub side: LiquidationSide,
    pub price: Price,
    pub quantity: Quantity,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub quality: Option<DataQuality>,
    pub lineage: Option<Lineage>,
}
```

### 14.3 Validation rules

```text
price > 0
quantity > 0
event_time valid
side != Unknown when source provides enough information
```

Adapter must document how source-specific liquidation side maps into `LiquidationSide`.

---

## 15. MarketEvent

### 15.1 Structure

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    Bar(Bar),
    OrderBookSnapshot(OrderBookSnapshot),
    OrderBookDelta(OrderBookDelta),
    FundingRate(FundingRate),
    OpenInterest(OpenInterest),
    MarkPrice(MarkPrice),
    IndexPrice(IndexPrice),
    Liquidation(Liquidation),
}
```

### 15.2 Common accessors

```rust
impl MarketEvent {
    pub fn kind(&self) -> MarketDataKind;
    pub fn instrument_id(&self) -> &InstrumentId;
    pub fn event_time(&self) -> TimestampNs;
    pub fn received_time(&self) -> Option<TimestampNs>;
    pub fn quality(&self) -> Option<&DataQuality>;
    pub fn lineage(&self) -> Option<&Lineage>;
}
```

### 15.3 Event design rules

- `MarketEvent` is the canonical market stream boundary.
- It does not include adapter raw payload.
- It does not include private account updates.
- It does not include factor/feature/analytics results.
- All variants must map to exactly one `MarketDataKind`.

---

## 16. MarketDataSelector

`fdc-data` does not implement query execution, but it should define shared selector semantics.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataSelector {
    pub instrument_ids: Vec<InstrumentId>,
    pub kinds: Vec<MarketDataKind>,
    pub time_range: TimeRange,
    pub source: Option<SourceId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: TimestampNs,
    pub end: TimestampNs,
    pub bound: TimeRangeBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeRangeBound {
    ClosedOpen,
    ClosedClosed,
}
```

Default query convention:

```text
[start, end)
```

Validation:

```text
instrument_ids not empty unless query layer explicitly supports wildcard
kinds not empty unless query layer explicitly supports all kinds
start < end
default bound = ClosedOpen
```

---

## 17. Serialization and schema expectations

All market structs should support:

```rust
Serialize
Deserialize
Debug
Clone
PartialEq
```

IDs and enums should additionally support:

```rust
Eq
Hash
```

Schema expectations:

- optional fields serialize as nullable fields;
- enum values should have stable string or numeric representation;
- storage should record `SchemaVersion` outside or alongside records;
- high-frequency storage may use Arrow/native representation, but semantic model remains this Rust contract.

Do not optimize the design around a single storage engine.

---

## 18. Validation ownership

`fdc-data::market::validation` may provide lightweight checks.

```rust
pub trait ValidateMarketRecord {
    fn validate(&self) -> Result<(), DataModelError>;
}
```

Validation split:

| Check | Owner |
| --- | --- |
| price > 0 | `fdc-data` helper |
| quantity > 0 | `fdc-data` helper |
| bar OHLC consistency | `fdc-data` helper |
| bid/ask ordering | `fdc-data` helper |
| source sequence continuity | ingestion/orderbook builder |
| duplicate detection | ingestion/storage |
| instrument exists in registry | adapter/ingestion with reference registry |
| storage schema compatibility | storage/query/schema registry |

---

## 19. Error model

Market validation errors should be data-model level only.

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketDataError {
    MissingInstrumentId,
    InvalidTimestamp { field: &'static str },
    InvalidPrice { field: &'static str, reason: String },
    InvalidQuantity { field: &'static str, reason: String },
    InvalidBarTimeRange,
    InvalidOhlc { reason: String },
    InvalidBookOrdering { side: BookSide },
    InvalidUpdateRange,
    EmptyChanges,
}
```

Not included:

```text
ConnectionError
SubscriptionError
DecodeError
StorageWriteError
QueryExecutionError
```

Those belong to adapter/ingestion/storage/query.

---

## 20. Compatibility mapping

### 20.1 From fdc_core::TickData

Current `fdc_core::types::TickData` can be mapped to market events.

```text
MessageType::Trade -> Trade -> MarketEvent::Trade
MessageType::Quote -> Quote -> MarketEvent::Quote
MessageType::OrderBook -> OrderBookSnapshot or OrderBookDelta depending on payload availability
```

Rules:

- This is compatibility only.
- Do not extend `TickData` as the new canonical model.
- Missing fields should become `None` or quality flags, not fake defaults.

### 20.2 From adapter raw payload

Adapter mapping rule:

```text
Raw payload -> source raw struct -> canonical market struct -> MarketEvent
```

Never:

```text
Raw payload -> storage directly
Raw payload -> analytics directly
Raw payload -> transform-specific market type
```

---

## 21. Open review questions

These are intentional review points before implementation:

1. Should first implementation introduce decimal-backed `Quantity` immediately instead of aliasing `Volume`?
2. Should `Trade` include both `exchange_trade_id` and `source_trade_id`, or is `trade_id + lineage.source` enough?
3. Should `FundingRate` include `FundingRateKind::{Predicted, Realized}` in v1?
4. Should `Bar` include a `completeness` field in v1, or should empty/partial bars be handled via `quality` first?
5. Should `OrderBookDelta::Delete` require a zero quantity value, or should `BookChange.quantity` become `Option<Quantity>` for delete actions?
6. Should `MarketDataSelector` live in `fdc-data`, or should it move to `fdc-query` after storage/query detailed design?

Recommended defaults for v1:

```text
1. Use decimal-backed Quantity if feasible; otherwise alias Volume with explicit limitation.
2. trade_id + lineage.source is enough.
3. Defer FundingRateKind unless needed by first source.
4. Use quality first; defer BarCompleteness.
5. Keep quantity required for layout simplicity, but document delete-zero policy.
6. Keep selector in fdc-data as shared semantic contract.
```

---

## 22. Acceptance criteria for this design

This document is ready for implementation planning when:

- market structs and enum semantics are approved;
- liquidation side semantics are approved;
- Quantity decision is made or explicitly deferred;
- order book delete quantity policy is approved;
- selector ownership is approved;
- no adapter-specific raw fields are required in canonical structs.
