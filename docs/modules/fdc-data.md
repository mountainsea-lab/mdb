# fdc-data Module Design

本文档定义 `fdc-data` 模块的设计。它是 FDC canonical financial data model 的核心 crate。

当前优先级：

1. 优先设计 `fdc_data::market`，建立标准市场数据模型；
2. 同步设计支撑 market 的 `fdc_data::common` 与 `fdc_data::reference`；
3. 其他数据域只定义边界、命名和后续设计入口，不进入完整字段设计；
4. 本文档只做设计，不包含实现任务拆解。

相关文档：

- `docs/FDC_architecture_design.md`
- `docs/FDC_development_roadmap.md`

---

## 1. 设计目标

`fdc-data` 的目标是提供 FDC 内部统一、稳定、低依赖的标准金融数据模型。

它解决的问题：

- adapter、ingestion、storage、query、transform、analytics 各自定义 market data 的重复问题；
- 市场数据、参考数据、新闻、宏观、基本面、链上、组合等数据域缺少统一 metadata 的问题；
- 外部数据源 schema 直接污染内部模型的问题；
- 后续 factor、feature、strategy、analytics 缺少共同数据语言的问题。

第一阶段不追求覆盖所有金融数据域。第一阶段只要求 market vertical slice 所需模型足够清晰。

---

## 2. 非目标

`fdc-data` 不负责：

- 数据接入协议、WebSocket、REST、认证、重连；
- adapter-specific raw payload；
- ingestion pipeline、buffer、checkpoint、backpressure；
- storage engine、索引、压缩、冷热分层；
- SQL/query execution；
- indicator、factor、feature 计算；
- WASM runtime；
- API DTO 或 protobuf wire schema。

这些能力应由 `fdc-adapter`、`fdc-ingestion`、`fdc-storage`、`fdc-query`、`fdc-transform`、`fdc-feature`、`fdc-factor`、`fdc-wasm`、`fdc-proto` 分别承担。

---

## 3. crate 边界

### 3.1 依赖方向

`fdc-data` 可以依赖：

```text
fdc-core
fdc-types
serde
rust_decimal / chrono / uuid 等基础序列化和标识依赖
```

`fdc-data` 不应依赖：

```text
fdc-adapter
fdc-ingestion
fdc-storage
fdc-query
fdc-transform
fdc-feature
fdc-factor
fdc-analytics
fdc-api
fdc-proto
fdc-wasm
```

设计含义：

- `fdc-data` 是被下游消费的模型层；
- 它不能知道数据如何接入、存储、查询或计算；
- `fdc-proto` 可以和 `fdc-data` 做转换，但 protobuf DTO 不是 canonical Rust model。

### 3.2 与 fdc-core 的边界

当前 `fdc-core::types` 已包含一些基础类型：

```text
TimestampNs
Symbol
Price
Volume
ExchangeId
SequenceNumber
MessageType
TickData
Value
```

长期边界建议：

| 类型 | 短期归属 | 长期归属 | 说明 |
| --- | --- | --- | --- |
| `TimestampNs` | `fdc-core` | `fdc-core` 或 `fdc-types` | 第一阶段 `fdc-data` 可直接使用 |
| `Value` | `fdc-core` | `fdc-types` | 动态值系统最终应靠近 type system |
| `Price` | `fdc-core` | `fdc-types` | 基础金融 value type，不属于 market object |
| `Volume` | `fdc-core` | `fdc-types` | 建议后续泛化为 `Quantity` |
| `Symbol` | `fdc-core` | `fdc-types` 或 `fdc-data::reference` | 若只代表字符串类型，归 `fdc-types`；若带交易所语义，归 reference |
| `ExchangeId` | `fdc-core` | `fdc-data::reference` | 交易所是 reference/master data |
| `MessageType` | `fdc-core` | `fdc-data::market` | 属于 market event classification |
| `TickData` | `fdc-core` | `fdc-data::market` compatibility only | 不应作为长期 canonical model |

第一阶段实现时可以复用 `fdc-core` 的 `TimestampNs`、`Price`、`Volume`、`Symbol` 以降低迁移成本，但文档和 API 应明确它们不是最终 market object 的语义归属。

### 3.3 与 fdc-types 的边界

`fdc-types` 负责“字段是什么类型”：

```text
PriceType
VolumeType
CurrencyType
OptionContractType
FutureContractType
TypeDefinition
TypeRegistry
TypeValidator
```

`fdc-data` 负责“字段组成什么金融数据对象”：

```text
fdc_data::market::Trade
fdc_data::market::Bar
fdc_data::market::OrderBook
fdc_data::reference::Instrument
fdc_data::reference::Exchange
```

`fdc-data` 可以引用 `fdc-types` 的基础 value/type definition，但不能反向让 `fdc-types` 依赖 `fdc-data`。

---

## 4. crate 内部模块结构

推荐结构：

```text
crates/fdc-data/src/
  lib.rs
  common.rs
  quality.rs
  schema.rs
  reference/
    mod.rs
    exchange.rs
    instrument.rs
    registry.rs
    calendar.rs
    entity.rs
  market/
    mod.rs
    identifiers.rs
    instrument.rs
    trade.rs
    quote.rs
    bar.rs
    orderbook.rs
    derivatives.rs
    event.rs
    query.rs
  news/
    mod.rs
  macro_data/
    mod.rs
  fundamental/
    mod.rs
  onchain/
    mod.rs
  altdata/
    mod.rs
  portfolio/
    mod.rs
```

说明：

- `common.rs` 放跨数据域元数据；
- `quality.rs` 放通用质量标记与质量等级；
- `schema.rs` 放 domain/schema version 描述；
- `reference` 是 instrument、exchange、calendar、entity graph 等基础主数据；
- `market` 是第一阶段重点；
- 其他 domain module 第一阶段只定义 module boundary，不实现完整对象。

---

## 5. Public API 原则

### 5.1 推荐访问路径

推荐所有领域对象通过 domain module 访问：

```rust
fdc_data::common::DataId
fdc_data::common::DataDomain
fdc_data::reference::Instrument
fdc_data::reference::InstrumentId
fdc_data::reference::Exchange
fdc_data::market::Trade
fdc_data::market::Bar
fdc_data::market::MarketEvent
fdc_data::market::OrderBook
```

不推荐长期暴露：

```rust
fdc_data::Trade
fdc_data::Bar
fdc_data::Position
```

原因：

- 多数据域会出现同名对象，例如 market position、portfolio position、onchain position；
- root 级 re-export 容易造成 namespace 污染；
- domain path 能表达语义上下文。

### 5.2 允许 root re-export 的类型

crate root 可以 re-export 少量跨域基础类型：

```rust
fdc_data::DataId
fdc_data::DataDomain
fdc_data::SourceId
fdc_data::SchemaVersion
```

领域对象仍应通过 domain path 访问。

---

## 6. 第一阶段基础类型约定

本文档中的 Rust-like 结构用于定义 contract，不表示最终代码必须逐字实现。为避免第一阶段重复造基础 value type，采用以下约定。

### 6.1 第一阶段可复用的基础类型

```rust
use fdc_core::types::TimestampNs;
use fdc_core::types::Price;
use fdc_core::types::Volume;
use fdc_core::types::SequenceNumber;
```

第一阶段 market model 可以临时复用这些类型，以便尽快建立 canonical object 边界。

### 6.2 fdc-data 内部新定义的标识类型

以下类型应由 `fdc-data` 定义，因为它们具有 domain identity 语义：

```rust
pub struct InstrumentId(pub String);
pub struct ExchangeId(pub String);
pub struct VenueId(pub String);
pub struct AssetId(pub String);
pub struct EntityId(pub String);
```

说明：

- 不建议继续使用 `fdc_core::types::ExchangeId(u16)` 表达长期 exchange identity；
- 交易所、venue、asset、entity 是 reference/master data，不是 core primitive；
- 使用 string newtype 是为了支持 crypto venue、传统交易所、OTC、聚合源等多种命名空间。

### 6.3 Quantity 命名约定

当前 `fdc-core` 中已有 `Volume(u64)`。但 market/fundamental/portfolio 中需要表达的数量不一定是整数，也不一定只代表成交量。

第一阶段设计上使用：

```rust
pub type Quantity = Volume;
```

长期建议迁移为 decimal-backed newtype：

```rust
pub struct Quantity(pub Decimal);
```

原因：

- crypto quantity 经常是小数；
- notional、quote volume、open interest 不总是整数；
- `Volume` 更像具体字段含义，`Quantity` 更适合作为基础数量 value type。

第一阶段实现如果继续复用 `Volume(u64)`，必须在文档和 compatibility mapper 中标记精度限制，不应假装已经支持所有小数资产数量。

### 6.4 reference 辅助类型

以下类型由 `fdc_data::reference` 定义：

```rust
pub enum TradingStatus {
    Active,
    Halted,
    Suspended,
    Delisted,
    Expired,
    Unknown,
}

pub struct ContractSpec {
    pub contract_size: Option<Decimal>,
    pub expiry_time: Option<TimestampNs>,
    pub strike_price: Option<Price>,
    pub option_kind: Option<OptionKind>,
}

pub enum OptionKind {
    Call,
    Put,
}

pub struct InstrumentLifecycle {
    pub listed_time: Option<TimestampNs>,
    pub delisted_time: Option<TimestampNs>,
    pub first_trade_time: Option<TimestampNs>,
    pub last_trade_time: Option<TimestampNs>,
}

pub struct SymbolAlias {
    pub source: SourceId,
    pub symbol: String,
    pub valid_from: Option<TimestampNs>,
    pub valid_to: Option<TimestampNs>,
}
```

这些类型只表达 reference/master data 语义，不处理 adapter-specific parsing。

### 6.5 命名冲突处理

- `fdc-types::PriceType` 是字段类型定义；
- `fdc_data::market::MarketPriceType` 是市场价格语义，例如 Last/Bid/Ask/Mark；
- `fdc-core::MessageType` 不应继续作为 market event canonical classification；
- `fdc_data::market::MarketDataKind` 是新的 canonical classification。

---

## 7. common 设计

`fdc_data::common` 提供跨数据域共享 metadata。

### 7.1 DataDomain

```rust
pub enum DataDomain {
    Market,
    Reference,
    News,
    MacroData,
    Fundamental,
    OnChain,
    Alternative,
    Portfolio,
    Feature,
    Factor,
    Analytics,
}
```

设计说明：

- `DataDomain` 用于 metadata、storage partition、query routing、schema registry；
- `Feature`、`Factor`、`Analytics` 虽不一定由 `fdc-data` 完整建模，但可以作为数据产物 domain 出现在 metadata 中。

### 7.2 DataId

```rust
pub struct DataId {
    pub domain: DataDomain,
    pub namespace: String,
    pub key: String,
    pub version: Option<SchemaVersion>,
}
```

设计说明：

- `domain` 表示数据域；
- `namespace` 表示来源或业务命名空间，例如 `binance.spot.trade`、`sec.filing`；
- `key` 是稳定业务 key，不应使用临时内存地址；
- `version` 可选，用于 schema 或 object version。

### 7.3 SourceId

```rust
pub struct SourceId {
    pub provider: String,
    pub venue: Option<String>,
    pub dataset: Option<String>,
}
```

示例：

```text
provider = "binance"
venue = "binance_spot"
dataset = "agg_trade"
```

```text
provider = "sec"
venue = None
dataset = "10-k"
```

### 7.4 SchemaVersion

```rust
pub struct SchemaVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}
```

规则：

- `major` 改变表示 breaking schema change；
- `minor` 改变表示向后兼容字段增加；
- `patch` 改变表示文档、validation 或非结构性修正。

### 7.5 Time semantics

跨域时间字段统一使用纳秒级 UTC 时间戳。

基础时间语义：

```text
event_time      事件在来源系统中发生的时间
received_time   FDC 或 adapter 接收到数据的时间
ingested_time   ingestion pipeline 接收/确认处理时间
emitted_time    FDC 向下游输出时间
```

market 领域还会增加：

```text
open_time
close_time
exchange_time
```

非 market 领域可能使用：

```text
published_time
period_start
period_end
filing_time
revision_time
```

### 7.6 DataQuality

```rust
pub struct DataQuality {
    pub level: QualityLevel,
    pub flags: Vec<QualityFlag>,
    pub source_confidence: Option<f32>,
}

pub enum QualityLevel {
    Raw,
    Normalized,
    Validated,
    Corrected,
    Derived,
}

pub enum QualityFlag {
    Replay,
    Backfill,
    DuplicateCandidate,
    OutOfOrder,
    GapBefore,
    GapAfter,
    LateArrival,
    Partial,
    Estimated,
    VendorCorrected,
}
```

设计原则：

- quality 是数据本身的标准质量描述；
- ingestion 细节如 buffer id、retry count、checkpoint offset 不进入 `fdc-data`；
- 详细 ingestion diagnostics 由 `fdc-ingestion` 自己维护。

### 7.7 Lineage

```rust
pub struct Lineage {
    pub source: SourceId,
    pub source_record_id: Option<String>,
    pub parent_data_ids: Vec<DataId>,
    pub transform_id: Option<String>,
}
```

用途：

- raw trade -> corrected trade；
- trade stream -> bar；
- financial statements -> normalized fundamental metrics；
- news article -> extracted event。

### 7.8 EntityLink

```rust
pub struct EntityLink {
    pub entity_id: String,
    pub entity_kind: EntityKind,
    pub relation: EntityRelation,
    pub confidence: Option<f32>,
}

pub enum EntityKind {
    Instrument,
    Asset,
    Issuer,
    Exchange,
    Country,
    Sector,
    Protocol,
    Wallet,
    Strategy,
}

pub enum EntityRelation {
    Primary,
    Underlying,
    QuoteAsset,
    IssuerOf,
    ListedOn,
    Mentions,
    Affects,
    Holds,
    Trades,
}
```

设计说明：

- 跨域 join 不应通过 domain object 互相嵌套；
- news、fundamental、macro、onchain 等数据通过 `EntityLink` 与 reference/market 关联；
- `EntityLink` 是防止 domain cycles 的核心机制。

---

## 8. reference 设计

`fdc_data::reference` 是 market 和其他数据域共享的主数据层。

### 8.1 Reference 的职责

负责：

- instrument identity；
- exchange / venue；
- trading calendar；
- asset / issuer / entity mapping；
- symbol alias and source mapping；
- contract specification。

不负责：

- trade/bar/orderbook 等事件数据；
- adapter raw symbol parsing；
- storage index；
- strategy portfolio state。

### 8.2 InstrumentId

```rust
pub struct InstrumentId(pub String);
```

设计要求：

- 稳定；
- 不包含临时 source-specific symbol；
- 可以被 storage/query/transform/factor 长期引用；
- 可支持多资产类别。

建议 key 形式：

```text
<venue>:<market_type>:<symbol_or_contract_id>
```

示例：

```text
binance_spot:spot:BTCUSDT
binance_um_futures:perpetual:BTCUSDT
nasdaq:equity:AAPL
cme:futures:ESZ2026
```

注意：key 形式是建议，不应成为唯一 API。实际实现可封装构造器和 parser。

### 8.3 Exchange / Venue

```rust
pub struct Exchange {
    pub id: ExchangeId,
    pub code: String,
    pub name: String,
    pub country: Option<String>,
    pub timezone: Option<String>,
}

pub struct Venue {
    pub id: VenueId,
    pub exchange_id: ExchangeId,
    pub code: String,
    pub venue_type: VenueType,
}
```

```rust
pub enum VenueType {
    Spot,
    Margin,
    Futures,
    Options,
    DarkPool,
    Otc,
    Aggregated,
}
```

设计说明：

- `Exchange` 是组织或交易所层面；
- `Venue` 是具体交易场所或市场分区，例如 Binance Spot、Binance USD-M Futures；
- 高频 market event 中只引用 `InstrumentId`，不重复携带 exchange 全量信息。

### 8.4 Instrument

```rust
pub struct Instrument {
    pub id: InstrumentId,
    pub venue_id: VenueId,
    pub symbol: String,
    pub display_symbol: Option<String>,
    pub instrument_type: InstrumentType,
    pub base_asset: Option<AssetId>,
    pub quote_asset: Option<AssetId>,
    pub settlement_asset: Option<AssetId>,
    pub status: TradingStatus,
    pub price_spec: PriceSpec,
    pub quantity_spec: QuantitySpec,
    pub contract_spec: Option<ContractSpec>,
    pub lifecycle: InstrumentLifecycle,
}
```

```rust
pub enum InstrumentType {
    Equity,
    Etf,
    Spot,
    MarginPair,
    Future,
    PerpetualFuture,
    Option,
    Index,
    Rate,
    Fund,
    Bond,
    CryptoToken,
    Synthetic,
}
```

### 8.5 PriceSpec / QuantitySpec

```rust
pub struct PriceSpec {
    pub currency: Option<String>,
    pub precision: u8,
    pub tick_size: Option<Decimal>,
}

pub struct QuantitySpec {
    pub precision: u8,
    pub lot_size: Option<Decimal>,
    pub min_quantity: Option<Decimal>,
}
```

设计说明：

- `fdc-types::PriceType` 和 `VolumeType` 描述类型规则；
- `reference::PriceSpec` 和 `QuantitySpec` 是具体 instrument 的规则实例；
- market event 中的 `price` 和 `quantity` 不重复携带 precision metadata。

### 8.6 InstrumentRegistry

```rust
pub trait InstrumentRegistry {
    fn get(&self, id: &InstrumentId) -> Option<Instrument>;
    fn resolve_symbol(&self, venue: &VenueId, symbol: &str) -> Option<InstrumentId>;
    fn aliases(&self, id: &InstrumentId) -> Vec<SymbolAlias>;
}
```

设计说明：

- trait 只定义查询 contract；
- registry 的存储、缓存、加载策略不属于 `fdc-data`；
- adapter 可以使用 registry 做 raw symbol -> canonical instrument id 映射。

### 8.7 Calendar

```rust
pub struct TradingCalendar {
    pub calendar_id: String,
    pub timezone: String,
    pub sessions: Vec<TradingSession>,
    pub holidays: Vec<TradingHoliday>,
}
```

第一阶段可以只定义结构，不实现完整日历规则引擎。

---

## 9. market 设计总览

`fdc_data::market` 是第一阶段重点。

核心对象：

```text
Instrument reference
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
```

基础原则：

- 高频事件只携带 `InstrumentId`，不重复嵌入 `Instrument`；
- 所有 market event 必须有明确时间语义；
- 所有 source-specific 字段要么进入 `Lineage`，要么进入受控 extension，不污染核心字段；
- market event 是 adapter、ingestion、storage、query、transform 的共同语言。

---

## 10. market identifiers and enums

### 10.1 MarketDataKind

```rust
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

用途：

- storage partition；
- query filter；
- metrics；
- stream subscription。

### 10.2 AggressorSide

```rust
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}
```

语义：

- `Buy` 表示主动买成交；
- `Sell` 表示主动卖成交；
- `Unknown` 用于来源未提供或无法判断。

不要用 `Bid/Ask` 代替 aggressor side。

### 10.3 BookSide

```rust
pub enum BookSide {
    Bid,
    Ask,
}
```

### 10.4 PriceType

```rust
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

避免和 `fdc-types::PriceType` 命名冲突，market 语义中使用 `MarketPriceType`。

---

## 11. Trade

### 11.1 结构

```rust
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

### 11.2 字段语义

| Field | Required | Meaning |
| --- | --- | --- |
| `instrument_id` | yes | canonical instrument id |
| `trade_id` | no | source trade id, may be absent or non-unique across venues |
| `price` | yes | execution price |
| `quantity` | yes | executed quantity |
| `side` | yes | aggressor side if known |
| `event_time` | yes | exchange/source event time |
| `received_time` | no | local receive time, may be added by adapter/ingestion |
| `source_sequence` | no | source-provided sequence number |
| `quality` | no | normalized data quality flags |
| `lineage` | no | source and transform provenance |

### 11.3 设计约束

- 不在 `Trade` 中嵌入 exchange raw payload；
- 不在 `Trade` 中嵌入 `Instrument` 全对象；
- `trade_id` 不作为全局唯一主键；
- 去重应基于 `instrument_id + source + trade_id/sequence/time/price/quantity` 的策略，由 ingestion/storage 层决定。

---

## 12. Quote

### 12.1 结构

```rust
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

### 12.2 设计约束

- quote 表示 top-of-book 或 NBBO 类数据；
- 完整 depth 使用 `OrderBookSnapshot` 或 `OrderBookDelta`；
- bid/ask 任一侧缺失时用 `None`，不要用 0 表示缺失。

---

## 13. Bar

### 13.1 BarInterval

```rust
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

设计说明：

- 高频内部计算可以使用 duration nanos；
- 对外语义保留 `1m`、`5m`、`1d` 等 interval 表达；
- 月线不是固定纳秒长度，不能简单等价为固定 duration。

### 13.2 BarKind

```rust
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

第一阶段主要支持 `Time`。

### 13.3 Bar 结构

```rust
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

### 13.4 设计约束

- `open_time` inclusive，`close_time` exclusive；
- `high >= low` 应由 validation 检查；
- `open/high/low/close` 不允许缺失；
- 对空 bar 的表达需后续单独设计，不用 `0` price 表示空；
- derived bar 的 lineage 应引用 parent trades 或 parent bars 的 data ids。

---

## 14. OrderBook

### 14.1 PriceLevel

```rust
pub struct PriceLevel {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: Option<u64>,
}
```

### 14.2 OrderBookSnapshot

```rust
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

### 14.3 OrderBookDelta

```rust
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

pub struct BookChange {
    pub side: BookSide,
    pub price: Price,
    pub quantity: Quantity,
    pub action: BookAction,
}

pub enum BookAction {
    Upsert,
    Delete,
}
```

### 14.4 设计约束

- `quantity = 0` 在某些交易所表示删除，但 canonical model 应显式映射为 `BookAction::Delete`；
- bids 应按价格降序，asks 应按价格升序，validation 可检查；
- snapshot/delta 的 sequence continuity 由 ingestion 或 order book builder 检查，结果映射到 quality flags。

---

## 15. Derivatives market data

第一阶段只定义常用衍生品市场数据对象。

### 15.1 FundingRate

```rust
pub struct FundingRate {
    pub instrument_id: InstrumentId,
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub lineage: Option<Lineage>,
}
```

### 15.2 OpenInterest

```rust
pub struct OpenInterest {
    pub instrument_id: InstrumentId,
    pub open_interest: Quantity,
    pub notional_value: Option<Quantity>,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub lineage: Option<Lineage>,
}
```

### 15.3 MarkPrice / IndexPrice

```rust
pub struct MarkPrice {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub lineage: Option<Lineage>,
}

pub struct IndexPrice {
    pub instrument_id: InstrumentId,
    pub price: Price,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub lineage: Option<Lineage>,
}
```

### 15.4 Liquidation

```rust
pub struct Liquidation {
    pub instrument_id: InstrumentId,
    pub side: AggressorSide,
    pub price: Price,
    pub quantity: Quantity,
    pub event_time: TimestampNs,
    pub received_time: Option<TimestampNs>,
    pub lineage: Option<Lineage>,
}
```

设计说明：

- `side` 对 liquidation 的含义必须在 adapter mapper 中统一成 canonical 语义；
- 不同交易所 liquidation payload 差异较大，source-specific 字段不进入核心结构。

---

## 16. MarketEvent

### 16.1 结构

```rust
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

### 16.2 Required common behavior

所有 `MarketEvent` 应能提供：

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

这不是实现要求，只是 API contract 方向。

### 16.3 设计约束

- `MarketEvent` 是 FDC 内部 market stream 的标准枚举；
- adapter 输出可以直接是 `MarketEvent`，也可以是带 envelope 的 `SourceEnvelope<MarketEvent>`；
- storage/query/transform 不应知道 adapter raw event 类型；
- `MarketEvent` 不包含 feature/factor/analytics 结果。

---

## 17. Market query contract

`fdc-data` 不实现查询引擎，但应定义 market query 的语义结构，供 `fdc-query` 和 `fdc-storage` 对齐。

```rust
pub struct MarketDataSelector {
    pub instrument_ids: Vec<InstrumentId>,
    pub kinds: Vec<MarketDataKind>,
    pub time_range: TimeRange,
    pub source: Option<SourceId>,
}

pub struct TimeRange {
    pub start: TimestampNs,
    pub end: TimestampNs,
    pub bound: TimeRangeBound,
}

pub enum TimeRangeBound {
    ClosedOpen,
    ClosedClosed,
}
```

默认时间范围语义：

```text
[start, end)
```

设计说明：

- query execution 属于 `fdc-query`；
- selector 是跨 storage/query/api 的 shared contract；
- `TimeRangeBound` 避免不同模块对区间边界理解不一致。

---

## 18. Adapter mapping boundary

`fdc-adapter` 的职责是：

```text
source raw payload -> source raw model -> fdc_data::market::MarketEvent
```

示例：

```text
Binance aggTrade payload
  -> BinanceAggTradeRaw
  -> Trade {
       instrument_id,
       trade_id,
       price,
       quantity,
       side,
       event_time,
       received_time,
       source_sequence,
       lineage,
     }
```

设计约束：

- source raw model 不进入 `fdc-data`；
- `fdc-data` 不提供 Binance/OKX/Bybit 专用字段；
- adapter 可以参考 barter-rs 的 connector/stream/normalized event 分层，但最终输出必须服从 FDC canonical model；
- source-specific diagnostics 可放在 adapter/ingestion 日志或 diagnostics，不放入 canonical core fields。

---

## 19. Storage and query boundary

storage 应以 canonical model 为输入：

```text
MarketEvent
Bar
Trade
OrderBookSnapshot
OrderBookDelta
```

storage 不应要求 adapter raw model。

query 应以 semantic selector 表达：

```text
instrument_id + market_data_kind + time_range + optional source
```

而不是：

```text
exchange raw stream name + raw table name + vendor-specific symbol
```

---

## 20. Transform boundary

`fdc-transform` 使用 `fdc_data::market` 类型作为输入输出。

第一条转换链：

```text
Vec<fdc_data::market::Trade>
  -> TradeToBar
  -> fdc_data::market::Bar
```

`fdc-transform` 不定义：

```text
TransformTrade
TransformBar
AnalyticsMarketData
```

如需 transform-specific metadata，应放在 transform result envelope，而不是复制 canonical market object。

---

## 21. Analytics boundary

`fdc-analytics` 当前已有 `models::MarketData`。长期应迁移为消费：

```rust
fdc_data::market::Bar
fdc_data::market::Trade
fdc_feature::FeatureSet
```

保留在 analytics 的模型应是结果类型：

```text
AnalyticsResult
RiskMetrics
PredictionResult
BacktestReport
```

不是 canonical market data。

---

## 22. Other data domains

其他数据域在第一阶段只定义边界和依赖方向。

### 22.1 news

职责：

- news article；
- announcement；
- corporate event；
- extracted event；
- sentiment annotation。

依赖：

```text
common
reference
```

通过 `EntityLink` 关联 instrument、issuer、sector、country。

### 22.2 macro_data

职责：

- macro indicator；
- release calendar；
- actual/forecast/previous；
- revision history。

依赖：

```text
common
reference
```

通过 country、region、currency、rate index 等 entity 关联。

### 22.3 fundamental

职责：

- financial statement；
- filing metadata；
- normalized metric；
- period and revision semantics。

依赖：

```text
common
reference
```

通过 issuer/instrument/entity 关联。

### 22.4 onchain

职责：

- transaction；
- block；
- wallet/address；
- protocol metric；
- token flow。

依赖：

```text
common
reference
```

通过 protocol、wallet、asset、token entity 关联。

### 22.5 altdata

职责：

- sentiment score；
- web/social metric；
- alternative vendor metric；
- crowding or attention signal。

依赖：

```text
common
reference
```

### 22.6 portfolio

职责：

- order；
- fill；
- position；
- PnL；
- strategy run metadata。

依赖：

```text
common
reference
market
```

注意：portfolio 可以引用 market instrument 和 market price，但 market 不能反向引用 portfolio。

---

## 23. Validation strategy

`fdc-data` 可以提供 lightweight validation helpers，但不实现完整 pipeline validation。

建议 validation 层级：

```text
struct-level validation
  required fields, enum constraints, time order, price/quantity sanity

cross-field validation
  bar high >= low, open_time < close_time, bid <= ask where applicable

cross-event validation
  sequence continuity, duplicate detection, order book reconstruction
```

归属：

| Validation | Owner |
| --- | --- |
| Required fields | `fdc-data` helper |
| OHLC sanity | `fdc-data` helper |
| Instrument exists | adapter/ingestion with registry |
| Sequence continuity | ingestion/orderbook builder |
| Duplicate detection | ingestion/storage |
| Schema compatibility | fdc-types/schema registry |

---

## 24. Error model

`fdc-data` 应定义模型级错误，不定义接入或存储错误。

```rust
pub enum DataModelError {
    MissingRequiredField { field: &'static str },
    InvalidTimeRange { start: TimestampNs, end: TimestampNs },
    InvalidPriceRelation { reason: String },
    InvalidQuantity { reason: String },
    InvalidInstrumentId { value: String },
    UnsupportedSchemaVersion { version: SchemaVersion },
}
```

非目标错误：

```text
ConnectionError
SubscriptionError
StorageWriteError
QueryExecutionError
TransformRuntimeError
```

这些属于其他 crate。

---

## 25. Compatibility and migration

### 25.1 From fdc-core::TickData

当前 `fdc-core::types::TickData` 不应长期作为 canonical market model。

迁移方向：

```text
fdc_core::types::TickData
  -> compatibility mapper
  -> fdc_data::market::Trade / Quote / MarketEvent
```

不建议让新模块继续扩展 `TickData`。

### 25.2 From fdc-analytics::MarketData

迁移方向：

```text
fdc_analytics::models::MarketData
  -> deprecated compatibility input
  -> fdc_data::market::Bar
```

analytics 后续只保留分析结果模型。

---

## 26. Versioning policy

`fdc-data` 需要明确 schema version，而不仅是 crate version。

建议：

```text
crate version
  Rust package/API version

schema version
  serialized data compatibility version
```

规则：

- 增加 optional field：minor schema change；
- 改变字段语义或 required field：major schema change；
- 修正文档或 validation：patch schema change；
- storage/query 必须记录 schema version，避免历史数据不可解释。

---

## 27. First design acceptance criteria

本文档完成后，`fdc-data` 可从 `Designing` 推进到 `Contract Ready` 的条件：

- `common`、`reference`、`market` 的职责清晰；
- `market` 第一版核心对象和字段语义清晰；
- 与 `fdc-core` / `fdc-types` 的边界清晰；
- 与 adapter、ingestion、storage、query、transform、analytics 的边界清晰；
- 其他数据域有明确后续设计入口；
- 用户审查通过。

---

## 28. Recommended next design documents

用户审查通过后，推荐顺序：

```text
1. docs/modules/fdc-data.md review and revise
2. docs/modules/fdc-data-implementation-notes.md or implementation plan
3. docs/modules/market-vertical-slice.md
4. docs/modules/fdc-adapter.md
5. docs/modules/fdc-storage-market.md
6. docs/modules/fdc-query-market.md
```

在用户审查通过前，不进入代码实现。
