# 金融数据中心（FDC）标准市场模型与模块架构设计

## 1. 文档定位

本文档不是替代 `README.md` 中的 FDC v3.0 初始设计，而是在 README 已定义的系统能力之上，补充一层更严格的金融市场领域架构。

README 定义的是 FDC 的总体能力边界：

- 高性能金融级高频交易数据中心；
- Rust + WASM 插件系统；
- 自定义类型系统；
- 多引擎、多层级存储；
- 查询、接入、分析、API、CLI、Server 等模块化架构。

本文档定义的是市场数据语义边界：

- 统一金融市场数据模型；
- 各模块围绕该模型的职责划分；
- `fdc-types` 与 `fdc-market` 的边界；
- 后续从实验分支迁移 adapter、orchestrator、market data pipeline 的方式。

核心原则：

> FDC 不应首先被设计成一个数据库，而应首先拥有一个稳定的 Canonical Market Data Model，然后围绕该模型构建采集、转换、存储、查询、因子、分析和策略能力。

---

## 2. 与 README 初始设计的关系

README 中 FDC v3.0 的核心定位是：

> 高性能金融级高频交易数据中心，基于 Rust + WASM 插件系统的可扩展架构。

本文档与 README 的关系如下：

```text
README.md
  定义系统级目标：性能、WASM、类型系统、存储、查询、API、分析。

FDC_architecture_design.md
  定义金融市场领域模型：Instrument、Trade、Bar、OrderBook、MarketEvent 等。
```

二者并不冲突：

- README 中的 `fdc-types` 仍负责自定义类型系统；
- README 中的 `fdc-wasm` 仍负责横向扩展能力；
- README 中的 `fdc-storage` 仍负责多层存储；
- 本文新增的 `fdc-market` 负责标准市场数据模型；
- 后续新增的 `fdc-factor`、`fdc-feature` 用于量化研究和特征体系。

推荐将 README 项目结构扩展为：

```text
crates/
  fdc-core/          基础设施
  fdc-common/        通用工具
  fdc-types/         自定义类型系统与金融基础类型
  fdc-market/        标准金融市场数据模型
  fdc-wasm/          WASM 插件系统
  fdc-ingestion/     数据接入管线
  fdc-transform/     数据转换与派生
  fdc-storage/       多层存储引擎
  fdc-query/         查询引擎
  fdc-factor/        因子定义与计算
  fdc-feature/       特征模型与特征存储接口
  fdc-analytics/     分析、风控、回测统计
  fdc-proto/         Protobuf/gRPC wire schema
  fdc-api/           API 层
  fdc-server/        服务组装与启动
  fdc-cli/           命令行工具
  fdc-orchestrator/  管线编排与运行协调
  fdc-adapter/*      交易所和第三方数据源适配器
```

---

## 3. 核心设计思想

统一市场数据模型是系统中心。

不同交易所和数据源会提供不同格式：

- Binance、OKX、Bybit 的成交、订单簿、资金费率字段不同；
- WebSocket 和 REST 的返回结构不同；
- 实时数据、历史回补、重放数据的元信息不同；
- 策略、因子、回测、查询希望使用统一输入。

因此内部系统不应长期传播交易所原始结构，而应转换为统一模型：

```text
外部数据源
  -> fdc-adapter / fdc-ingestion
  -> fdc-market
  -> fdc-transform
  -> fdc-storage
  -> fdc-query
  -> fdc-factor / fdc-feature / fdc-analytics / Strategy
```

其中：

```text
fdc-market 是标准市场语义层。
fdc-types 是基础类型系统层。
fdc-storage 是持久化层。
fdc-query 是查询层。
fdc-transform 是派生计算层。
```

---

## 4. 总体架构

```text
External Sources
  Binance / OKX / Bybit / Third-party Data
        |
        v
fdc-adapter/*
  exchange SDK, REST/WebSocket client, raw exchange model, source mapper
        |
        v
fdc-ingestion
  receiver, parser, validator, buffer, backpressure, checkpoint, source envelope
        |
        v
fdc-market
  Instrument, Trade, Bar, OrderBook, FundingRate, OpenInterest, MarketEvent
        |
        +--------------------+
        |                    |
        v                    v
fdc-transform          fdc-storage
  tick-to-bar            L1 memory/ring buffer
  resample               L2 redb/Arrow
  trade flow             L3 DuckDB/Parquet
  indicators             L4 RocksDB/archive
  derived events
        |                    |
        +---------+----------+
                  v
              fdc-query
                  |
        +---------+----------+
        |                    |
        v                    v
   fdc-factor          fdc-analytics
   fdc-feature         risk/backtest/reports
        |
        v
   Strategy / API / CLI / WASM plugins
```

`fdc-wasm` 是横向扩展能力，可以服务于：

- 自定义类型转换；
- 自定义数据转换；
- 查询 UDF；
- 自定义因子；
- 自定义分析逻辑。

但 `fdc-market` 不依赖 `fdc-wasm`，以保持 canonical model 稳定、低依赖、可测试。

---

## 5. fdc-types 与 fdc-market 的边界

这是新方案中最重要的边界。

### 5.1 fdc-types 的职责

`fdc-types` 负责“字段是什么类型”。

它是类型系统和基础金融类型层，适合包含：

- `Value`；
- `Price`；
- `Quantity`；
- `Volume`；
- `Symbol`；
- `Currency`；
- `DecimalScale`；
- `TypeId`；
- `TypeDefinition`；
- `FieldDefinition`；
- `TypeConstraint`；
- `TypeRegistry`；
- `TypeValidator`；
- `TypeConverter`；
- `Schema`；
- WASM 类型转换桥接。

它关心的是：

```text
price 字段如何表达，精度是多少，是否合法，如何序列化，如何做 schema validation。
```

它不关心：

```text
这个 price 是一笔 Trade 的成交价，还是一个 Bar 的收盘价。
```

### 5.2 fdc-market 的职责

`fdc-market` 负责“这些字段组成什么市场对象”。

它是金融市场领域模型层，适合包含：

- `InstrumentId`；
- `Instrument`；
- `Exchange`；
- `MarketType`；
- `AssetClass`；
- `TradingStatus`；
- `Trade`；
- `AggressorSide`；
- `Bar`；
- `BarInterval`；
- `OrderBook`；
- `OrderBookLevel`；
- `OrderBookDelta`；
- `FundingRate`；
- `OpenInterest`；
- `MarkPrice`；
- `IndexPrice`；
- `Liquidation`；
- `MarketEvent`；
- `MarketDataKind`；
- `MarketDataQuality`；
- `MarketTimestamp`；
- `InstrumentRegistry`；
- `SchemaVersion`。

它关心的是：

```text
Trade、Bar、OrderBook、FundingRate、OpenInterest 等市场对象如何标准表达。
```

### 5.3 推荐依赖关系

```text
fdc-core
  -> fdc-types
  -> fdc-market
```

严格避免：

```text
fdc-types -> fdc-market
fdc-market -> fdc-storage
fdc-market -> fdc-query
fdc-market -> fdc-transform
fdc-market -> fdc-wasm
```

### 5.4 当前代码中的重叠

当前 `fdc-core::types` 已包含：

- `TimestampNs`；
- `Symbol`；
- `Price`；
- `Volume`；
- `ExchangeId`；
- `MessageType`；
- `SequenceNumber`；
- `Value`；
- `TickData`。

当前 `fdc-types` 已包含：

- `FinancialType`；
- `PriceType`；
- `VolumeType`；
- `CurrencyType`；
- `OptionContractType`；
- `FutureContractType`；
- type definition、schema、validation、conversion。

短期策略：

- 不立即删除旧类型；
- `fdc-market` 第一版可临时使用 `fdc-core::TimestampNs`；
- `Price`、`Symbol`、`Volume` 的长期归属应是 `fdc-types`；
- `TickData`、`MessageType` 的长期归属应是 `fdc-market`；
- 通过 re-export 和 compatibility module 渐进迁移。

---

## 6. 模块职责定义

### 6.1 fdc-core

定位：底层基础设施。

职责：

- `Error` / `Result`；
- 时间基础类型，例如 `TimestampNs`；
- 配置基础设施；
- metrics 基础设施；
- memory utilities；
- 最小通用 trait。

不应继续新增：

- `Trade`；
- `Bar`；
- `OrderBook`；
- `Instrument`；
- `Factor`；
- 交易所适配逻辑。

当前代码中 `fdc-core::types` 已承担较多类型系统职责，后续应逐步收敛。

### 6.2 fdc-common

定位：跨 crate 通用工具。

职责：

- retry/backoff；
- tracing helpers；
- serde helpers；
- async utilities；
- test utilities。

不应放领域模型。

### 6.3 fdc-types

定位：自定义类型系统和基础金融类型。

职责：

- 基础值类型；
- 类型定义；
- schema；
- validation；
- conversion；
- serialization；
- introspection；
- WASM 类型转换集成。

它可以被 `fdc-market` 使用，但不依赖 `fdc-market`。

### 6.4 fdc-market

定位：标准金融市场数据模型。

第一版目标：

- 建立 `Instrument` / `InstrumentId` / `InstrumentRegistry`；
- 建立 `Trade` / `Bar` / `OrderBook` 等基础市场对象；
- 建立 `MarketEvent` 统一事件枚举；
- 建立 `MarketDataQuality` 和时间语义；
- 建立 schema version 机制。

设计原则：

- 高频数据中避免重复保存 `exchange string` 和 `symbol string`；
- 使用 `instrument_id` 关联 `InstrumentRegistry`；
- 所有交易所差异在进入该层前或该层边界处被消化；
- 不依赖存储、查询、转换、WASM、API。

### 6.5 fdc-ingestion

定位：数据接入管线。

职责：

- receiver；
- parser；
- validator；
- buffer；
- batch；
- backpressure；
- recovery；
- source envelope；
- checkpoint；
- quality metadata。

未来输出应转向：

```text
SourceEnvelope<fdc_market::MarketEvent>
```

或：

```text
SourceEnvelope<RawExchangeEvent> -> Mapper -> fdc_market::MarketEvent
```

不负责：

- tick-to-bar；
- indicators；
- factor；
- storage tier routing。

### 6.6 fdc-adapter/*

定位：具体数据源适配器。

职责：

- Binance/OKX/Bybit/Barter 等具体 API 对接；
- REST/WebSocket client；
- exchange-specific raw model；
- exchange-specific parser；
- raw event 到 `fdc-market` 的 mapper。

与 `fdc-ingestion` 的区别：

```text
fdc-adapter 负责具体来源。
fdc-ingestion 负责统一接入管线。
```

实验分支中的 `fdc-adapter/barter` 可作为后续迁移参考。

### 6.7 fdc-transform

定位：标准市场数据加工层。

职责：

- Tick/Trade -> Bar；
- resample；
- aggregation；
- order flow；
- TradeFlow；
- CVD；
- indicators；
- derived market events；
- transform pipeline。

输入：

```text
fdc_market::Trade
fdc_market::OrderBook
fdc_market::MarketEvent
```

输出：

```text
fdc_market::Bar
fdc_market::DerivedEvent
fdc_feature::Feature
```

不应拥有 canonical market model。

### 6.8 fdc-storage

定位：多层存储引擎。

职责：

- L1 超热缓存；
- L2 redb/Arrow 热数据；
- L3 DuckDB/Parquet 温数据；
- L4 RocksDB/archive 冷数据；
- engine abstraction；
- tier manager；
- shard；
- index；
- cache；
- compression；
- replication；
- backup；
- retention。

它不关心交易所差异，只保存标准模型和派生数据。

建议新增 market storage adapter：

```text
fdc-storage::market
  MarketDataWriter
  MarketDataReader
  MarketStorageKey
  MarketPartitionStrategy
  MarketStorageSchema
```

底层仍使用 `StorageEngine`。

### 6.9 fdc-query

定位：查询引擎。

职责：

- SQL parser；
- optimizer；
- planner；
- executor；
- cache；
- built-in functions；
- aggregates；
- storage tier routing；
- market-aware query API。

除 SQL 外，应增加类型安全市场查询：

```text
MarketDataQuery {
  instrument_id,
  kind,
  time_range,
  interval,
  limit,
  schema_version,
}
```

查询语义应围绕：

```text
instrument_id + market_data_kind + time_range
```

而不是只围绕 `symbol string`。

### 6.10 fdc-factor

定位：因子定义和计算。

职责：

- factor trait；
- factor registry；
- factor metadata；
- factor versioning；
- factor compute graph；
- batch/stream factor execution；
- momentum；
- volatility；
- liquidity；
- order flow；
- market regime。

依赖建议：

```text
fdc-factor -> fdc-market
fdc-factor -> fdc-feature
fdc-factor -> fdc-transform
fdc-factor -> fdc-wasm 可选
```

### 6.11 fdc-feature

定位：特征模型和特征存储接口。

职责：

- `Feature`；
- `FeatureValue`；
- `FeatureVector`；
- `FeatureSet`；
- `FeatureSchema`；
- feature metadata；
- feature version；
- feature lineage；
- feature store interface。

与 `fdc-factor` 的区别：

```text
fdc-factor 负责怎么算。
fdc-feature 负责算出来的数据如何表达、组织和存取。
```

### 6.12 fdc-analytics

定位：分析、回测统计、风控和报告。

职责：

- stream analytics；
- batch analytics；
- technical analysis；
- risk metrics；
- ML/feature analysis；
- backtest reports；
- portfolio metrics。

当前 `fdc-analytics::models::MarketData` 与未来 `fdc_market::Bar` 重复。

长期策略：

- `fdc-analytics` 不定义 canonical market data；
- 使用 `fdc_market::Bar` / `fdc_market::Trade` 作为输入；
- 只保留 `AnalyticsResult`、`RiskMetrics`、`PredictionResult` 等分析结果模型。

### 6.13 fdc-wasm

定位：插件和扩展执行层。

职责：

- WASM runtime；
- plugin lifecycle；
- plugin registry；
- sandbox；
- security policy；
- host function bridge；
- event bridge；
- WASM UDF；
- custom transform；
- custom factor；
- custom type conversion。

不应成为 `fdc-market` 的依赖。

### 6.14 fdc-api

定位：对外 API 层。

职责：

- REST；
- gRPC；
- GraphQL；
- WebSocket；
- auth；
- middleware；
- request/response DTO；
- metrics endpoint；
- query endpoint；
- market data endpoint；
- ingestion control endpoint。

不负责：

- 查询执行；
- 存储实现；
- canonical model 定义。

### 6.15 fdc-proto

定位：wire protocol。

职责：

- Protobuf message；
- gRPC service；
- cross-language DTO；
- protocol compatibility。

边界：

```text
fdc-market 是 Rust 内部 canonical model。
fdc-proto 是外部 wire schema。
```

二者通过 conversion 层互转。

### 6.16 fdc-server

定位：服务组装层。

职责：

- load config；
- init storage；
- init query engine；
- init ingestion runtime；
- init plugin runtime；
- init API server；
- start orchestrator；
- graceful shutdown。

与 `fdc-api` 区别：

```text
fdc-api    负责 API crate、routes、handlers、DTO。
fdc-server 负责 application composition 和 binary/service startup。
```

### 6.17 fdc-cli

定位：命令行工具。

职责：

- start server；
- run live ingestion；
- run historical backfill；
- query；
- inspect storage；
- manage instruments；
- manage plugins；
- run benchmarks；
- development utilities。

### 6.18 fdc-orchestrator

定位：运行时编排层。

职责：

- pipeline orchestration；
- live runner；
- backfill runner；
- replay runner；
- source-to-storage wiring；
- checkpoint coordination；
- task scheduling；
- failure recovery；
- health supervision。

不应定义市场模型，也不应定义存储模型。

实验分支中的 orchestrator 可作为后续迁移参考。

---

## 7. 推荐依赖方向

总体依赖方向：

```text
fdc-core
  ↓
fdc-common
  ↓
fdc-types
  ↓
fdc-market
  ↓
fdc-ingestion / fdc-transform / fdc-storage / fdc-query
  ↓
fdc-factor / fdc-feature / fdc-analytics
  ↓
fdc-api / fdc-server / fdc-cli / fdc-orchestrator
```

更精确地说：

```text
fdc-core       <- almost everyone
fdc-types      -> fdc-core, optional fdc-wasm integration
fdc-market     -> fdc-core, fdc-types
fdc-ingestion  -> fdc-core, fdc-types, fdc-market
fdc-transform  -> fdc-core, fdc-types, fdc-market, optional fdc-wasm
fdc-storage    -> fdc-core, fdc-types, fdc-market, optional fdc-wasm
fdc-query      -> fdc-core, fdc-types, fdc-market, fdc-storage, optional fdc-wasm
fdc-factor     -> fdc-core, fdc-types, fdc-market, fdc-feature, optional fdc-wasm
fdc-feature    -> fdc-core, fdc-types, fdc-market, optional fdc-storage
fdc-analytics  -> fdc-core, fdc-types, fdc-market, fdc-query, fdc-feature
fdc-api        -> fdc-core, fdc-market, fdc-query, fdc-ingestion
fdc-server     -> fdc-api, fdc-storage, fdc-query, fdc-ingestion, fdc-wasm, fdc-orchestrator
fdc-cli        -> fdc-api client or direct service crates for admin commands
fdc-proto      -> wire DTO, conversion to/from fdc-market
```

禁止形成反向依赖：

```text
fdc-market -> fdc-storage
fdc-market -> fdc-query
fdc-market -> fdc-transform
fdc-market -> fdc-api
fdc-market -> fdc-wasm
fdc-types  -> fdc-market
fdc-core   -> fdc-market
```

---

## 8. 标准市场数据模型初版范围

`fdc-market` 第一版应聚焦最小可用 canonical model。

建议模块结构：

```text
crates/fdc-market/src/
  lib.rs
  instrument.rs
  exchange.rs
  time.rs
  quality.rs
  trade.rs
  bar.rs
  orderbook.rs
  derivatives.rs
  event.rs
  registry.rs
  schema.rs
```

### 8.1 Instrument

```rust
pub struct Instrument {
    pub id: InstrumentId,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub market_type: MarketType,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub price_scale: u8,
    pub quantity_scale: u8,
    pub status: TradingStatus,
}
```

高频事件中只保存：

```text
instrument_id
```

通过 registry 查询：

```text
instrument_id -> Instrument -> exchange/symbol/market_type
```

### 8.2 Trade

```rust
pub struct Trade {
    pub instrument_id: InstrumentId,
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: Quantity,
    pub side: AggressorSide,
    pub event_time: TimestampNs,
    pub received_time: TimestampNs,
    pub source_sequence: Option<String>,
}
```

### 8.3 Bar

```rust
pub struct Bar {
    pub instrument_id: InstrumentId,
    pub interval: BarInterval,
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub trade_count: Option<u64>,
}
```

### 8.4 MarketEvent

```rust
pub enum MarketEvent {
    Trade(Trade),
    Bar(Bar),
    OrderBook(OrderBook),
    OrderBookDelta(OrderBookDelta),
    FundingRate(FundingRate),
    OpenInterest(OpenInterest),
    MarkPrice(MarkPrice),
    IndexPrice(IndexPrice),
    Liquidation(Liquidation),
}
```

---

## 9. 时间语义

金融市场数据必须明确多种时间：

```text
event_time       交易所事件时间
received_time    本地接收时间
ingested_time    接入管线处理时间
emitted_time     系统向下游发送时间
open_time        Bar 开始时间
close_time       Bar 结束时间
```

设计原则：

- `event_time` 是回测和重放主时间；
- `received_time` 用于延迟分析；
- `ingested_time` 用于管线监控；
- `emitted_time` 用于下游端到端延迟；
- 所有内部时间统一使用纳秒级 UTC 时间戳；
- 不在核心模型中混入本地时区。

---

## 10. 数据质量模型

`fdc-market` 应包含基础质量标记，但不要把 ingestion 实现细节放进去。

建议：

```rust
pub struct MarketDataQuality {
    pub is_replay: bool,
    pub is_backfill: bool,
    pub is_duplicate_candidate: bool,
    pub has_gap_before: bool,
    pub is_out_of_order: bool,
    pub source_confidence: Option<f32>,
}
```

接入层可产生更详细的质量报告，最终映射到标准质量字段。

---

## 11. 存储分层与标准模型

存储层只关心标准模型，不关心 Binance/OKX/Bybit 原始格式。

推荐分层：

| 层级 | 技术 | 用途 |
|---|---|---|
| L1 | RingBuffer / memory cache | 实盘策略、高频最近窗口 |
| L2 | redb / Arrow | 热数据、低延迟查询 |
| L3 | DuckDB / Parquet | 回测、分析、批量扫描 |
| L4 | RocksDB / archive / object storage | 历史归档、低频访问 |

存储 key 应围绕：

```text
namespace / market_data_kind / instrument_id / event_time / sequence
```

而不是：

```text
exchange string / symbol string / raw source format
```

---

## 12. 查询语义

查询层应同时支持：

1. SQL 查询；
2. 类型安全市场数据查询；
3. 策略低延迟查询；
4. API 查询。

推荐 market query：

```rust
pub struct MarketDataQuery {
    pub instrument_id: InstrumentId,
    pub kind: MarketDataKind,
    pub time_range: TimeRange,
    pub interval: Option<BarInterval>,
    pub limit: Option<usize>,
}
```

示例：

```text
获取 BTCUSDT 最近 500 根 1m K 线：

instrument_id = registry.resolve("Binance", "BTCUSDT")
kind = Bar
interval = 1m
limit = 500
```

返回：

```text
Vec<fdc_market::Bar>
```

---

## 13. 当前 upstream/main 模块实现盘点

当前项目中：

- `fdc-core` 已有基础类型、配置、错误、指标、内存等实现；
- `fdc-types` 已有自定义类型系统和金融类型定义；
- `fdc-wasm` 已有 runtime/plugin/security/bridge/types 框架；
- `fdc-storage` 已有多引擎抽象和 L1-L4 结构；
- `fdc-query` 已有 SQL parser/optimizer/planner/executor/cache 框架；
- `fdc-ingestion` 已有 receiver/parser/validator/buffer/backpressure/recovery 框架；
- `fdc-analytics` 已有分析、风险、ML、指标、聚合框架；
- `fdc-api` 已有 API DTO 和 server scaffold；
- `fdc-transform`、`fdc-common`、`fdc-proto`、`fdc-cli`、`fdc-server` 目前基本是占位。

主要重叠点：

1. `fdc-core::types` 与 `fdc-types` 重叠；
2. `fdc-analytics::models::MarketData` 与未来 `fdc-market::Bar` 重叠；
3. `fdc-wasm::WasmValue` 与 `fdc_core::Value` 有转换关系，长期应转向 `fdc_types::Value`；
4. 当前缺少独立 `fdc-market`，导致市场数据语义尚无统一归属。

---

## 14. 实验分支迁移策略

当前新分支从干净 `main` 开始。实验分支 `mdb-mqdev` 中已有一些可参考成果：

- `fdc-adapter/barter`；
- market data DTO；
- ingestion envelope；
- orchestrator mapping；
- storage market queryable 实验；
- Binance spot/futures 示例和测试。

迁移原则：

```text
只迁移成熟思想，不直接延续临时边界。
```

迁移时必须改为围绕 `fdc-market`：

```text
adapter raw event -> fdc_market::MarketEvent -> storage/query/transform
```

不再把 canonical model 放在 `fdc-transform` 或 `fdc-orchestrator` 中。

---

## 15. 推荐开发阶段

### Phase 0：文档和边界确认

目标：

- 明确 README 与本文档关系；
- 明确各 crate 职责；
- 明确 `fdc-types` / `fdc-market` 边界；
- 明确依赖方向；
- 明确实验分支迁移策略。

### Phase 1：新增 fdc-market

目标：

- 新增 `crates/fdc-market`；
- 定义最小 canonical market model；
- 增加单元测试；
- 不连接 storage/query/ingestion。

### Phase 2：类型边界整理

目标：

- 明确 `Price`、`Symbol`、`Quantity` 的短期使用和长期归属；
- 提供 re-export 或 compatibility module；
- 避免一次性破坏现有代码。

### Phase 3：接入 transform 和 analytics

目标：

- `fdc-transform` 输入输出改为使用 `fdc-market`；
- `fdc-analytics::MarketData` 逐步替换为 `fdc_market::Bar`；
- 建立 `Trade -> Bar` 第一条转换链。

### Phase 4：接入 storage 和 query

目标：

- 新增 market storage writer/reader；
- 新增 market query model；
- 支持 `instrument_id + time_range + kind` 查询。

### Phase 5：迁移 adapter 和 orchestrator

目标：

- 从实验分支迁移 `fdc-adapter/barter`；
- 新增或迁移 `fdc-orchestrator`；
- 打通 live/historical -> market event -> storage 的闭环。

### Phase 6：因子和特征

目标：

- 新增 `fdc-factor`；
- 新增 `fdc-feature`；
- 支持 batch/stream 因子计算；
- 支持 WASM 自定义因子。

---

## 16. 最终定位

FDC 不只是一个数据库。

它应成为：

> Rust 实现的高性能量化金融数据基础设施。

它结合：

- kdb+ 的时序能力；
- QuestDB 的 SQL 能力；
- Arrow/DataFusion/DuckDB 的数据处理能力；
- WASM 的可扩展能力；
- Factor Factory 的研究能力；
- Canonical Market Data Model 的标准化能力。

核心建设顺序：

```text
基础设施
  -> 类型系统
  -> 标准市场模型
  -> 数据采集
  -> 数据转换
  -> 数据存储
  -> 数据查询
  -> 因子与特征
  -> 分析与策略
```

其中：

> `fdc-market` 是市场数据语义的核心，`fdc-types` 是基础类型系统的核心，二者必须协作但不能重复。
