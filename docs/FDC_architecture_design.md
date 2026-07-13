# 金融数据中心（FDC）标准金融数据模型与模块架构设计

## 1. 文档定位

本文档不是替代 `README.md` 中的 FDC v3.0 初始设计，而是在 README 已定义的系统能力之上，补充一层更严格的金融市场领域架构。

README 定义的是 FDC 的总体能力边界：

- 高性能金融级高频交易数据中心；
- Rust + WASM 插件系统；
- 自定义类型系统；
- 多引擎、多层级存储；
- 查询、接入、分析、API、CLI、Server 等模块化架构。

本文档定义的是金融数据语义边界：

- 统一金融市场数据模型；
- 面向因子、策略、分析的数据支撑体系；
- 行情、新闻、公告、宏观、基本面、链上、舆情、另类数据等数据域边界；
- 各模块围绕标准数据模型的职责划分；
- `fdc-types` 与 `fdc-data` 的边界；
- 后续从实验分支迁移 adapter、orchestrator、market data pipeline 的方式。

核心原则：

> FDC 不应首先被设计成一个数据库，也不应只被设计成行情库，而应首先拥有稳定的 Canonical Financial Data Model，然后围绕这些标准数据域构建采集、转换、存储、查询、因子、分析和策略能力。

---

## 2. 与 README 初始设计的关系

README 中 FDC v3.0 的核心定位是：

> 高性能金融级高频交易数据中心，基于 Rust + WASM 插件系统的可扩展架构。

本文档与 README 的关系如下：

```text
README.md
  定义系统级目标：性能、WASM、类型系统、存储、查询、API、分析。

FDC_architecture_design.md
  定义金融数据领域模型：Market Data、News、Macro、Fundamentals、On-chain、Alternative Data 等。
```

二者并不冲突：

- README 中的 `fdc-types` 仍负责自定义类型系统；
- README 中的 `fdc-wasm` 仍负责横向扩展能力；
- README 中的 `fdc-storage` 仍负责多层存储；
- 本文新增的 `fdc-data` 负责统一标准金融数据模型，并在 crate 内部按 market、reference、news、macro_data、fundamental、onchain、altdata、portfolio 等领域分模块；
- 后续新增的 `fdc-factor`、`fdc-feature` 用于量化研究和特征体系。

推荐将 README 项目结构扩展为：

```text
crates/
  fdc-core/          基础设施
  fdc-common/        通用工具
  fdc-types/         自定义类型系统与金融基础类型
  fdc-data/          统一标准金融数据模型，内部按领域分模块
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
  -> fdc-data
  -> fdc-transform
  -> fdc-storage
  -> fdc-query
  -> fdc-factor / fdc-feature / fdc-analytics / Strategy
```

其中：

```text
fdc-data 是统一标准金融数据模型层，其中 market 子模块负责标准市场语义。
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
fdc-data
  market::{Instrument, Trade, Bar, OrderBook}
  reference::{InstrumentRegistry, Calendar}
  news::{NewsArticle, Announcement}
  macro_data::{MacroIndicator, MacroRelease}
  fundamental::{FinancialStatement, FundamentalMetric}
  onchain::{OnChainTransaction, ProtocolMetric}
  altdata::{SentimentScore, AlternativeMetric}
  portfolio::{Order, Fill, Position}
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

但 `fdc-data` 不依赖 `fdc-wasm`，以保持 canonical model 稳定、低依赖、可测试。

---

## 5. 数据支撑目标与金融数据域

FDC 的目标不是只采集行情，而是为后续因子计算、策略执行、回测分析、风险管理和研究平台提供统一、可追溯、可查询的数据支撑。

因此系统应从一开始区分两层概念：

```text
Canonical Market Data Model
  行情和市场微观结构：Trade、Bar、OrderBook、FundingRate、OpenInterest。

Canonical Financial Data Domains
  面向量化研究的完整数据域：行情、新闻、公告、宏观、基本面、链上、舆情、另类数据等。
```

`fdc-data::market` 是第一阶段最重要的数据域，但它不是 FDC 的全部数据模型。长期应在同一个 `fdc-data` crate 内形成多个标准数据域，共同服务 `fdc-factor`、`fdc-feature`、`fdc-analytics` 和 Strategy。

### 5.1 行情与市场微观结构数据

这是 `fdc-data::market` 的核心范围，也是实盘和高频策略的基础。

包括：

- Instrument / Symbol / Contract Spec；
- Trade / Tick；
- Bar / OHLCV / VWAP；
- OrderBook L1/L2/L3；
- OrderBookDelta；
- FundingRate；
- OpenInterest；
- MarkPrice / IndexPrice；
- Liquidation；
- Futures Basis；
- Perpetual Swap Premium；
- Option Chain；
- Implied Volatility；
- Greeks；
- Spread / Depth / Liquidity metrics。

典型用途：

- 高频策略；
- 趋势、动量、反转因子；
- 波动率因子；
- 流动性因子；
- 订单流因子；
- 回测撮合；
- 风险监控。

### 5.2 参考数据与主数据

参考数据是所有数据域对齐的基础。没有稳定主数据，行情、新闻、基本面和策略持仓无法可靠关联。

包括：

- Instrument Registry；
- exchange / venue；
- trading calendar；
- session hours；
- contract specification；
- tick size / lot size；
- margin rules；
- symbol mapping；
- listing / delisting；
- corporate actions；
- asset metadata；
- sector / industry classification。

典型用途：

- 将不同来源的 symbol 映射到统一 `instrument_id`；
- 回测时处理合约换月、股票复权、交易日历；
- 新闻和公告与资产关联；
- 因子横截面分组和行业中性化。

### 5.3 新闻、公告与事件数据

新闻和事件数据是中低频策略、事件驱动策略和风险管理的重要输入。

包括：

- 新闻文章；
- 交易所公告；
- 项目公告；
- 监管公告；
- 上市 / 下架 / 停牌 / 复牌；
- 财报发布事件；
- 宏观数据发布时间表；
- 央行会议；
- 突发事件；
- 事件标签；
- NLP 情绪分数；
- entity extraction；
- topic classification。

建议标准对象：

```text
NewsArticle
Announcement
CalendarEvent
CorporateEvent
RegulatoryEvent
EventEntityLink
SentimentScore
```

典型用途：

- 新闻情绪因子；
- 事件驱动策略；
- 公告前后收益分析；
- 风险预警；
- 策略交易过滤器。

### 5.4 宏观经济数据

宏观数据为资产配置、风险因子、跨品种策略和 regime 判断提供支撑。

包括：

- CPI / PPI；
- GDP；
- unemployment / NFP；
- interest rate；
- yield curve；
- central bank decision；
- money supply；
- PMI；
- FX rates；
- commodity inventory；
- economic calendar；
- actual / forecast / previous。

建议标准对象：

```text
MacroIndicator
MacroRelease
EconomicCalendarEvent
RateCurve
YieldCurvePoint
```

典型用途：

- 宏观 regime 因子；
- 利率敏感性分析；
- 跨资产配置；
- 事件窗口回测；
- 风险因子暴露分析。

### 5.5 基本面与财务数据

基本面数据主要服务股票、债券、基金、项目研究和中低频策略。

包括：

- income statement；
- balance sheet；
- cashflow statement；
- valuation ratios；
- earnings estimate；
- analyst rating；
- revenue / profit / margin；
- shares outstanding；
- dividend；
- buyback；
- tokenomics / protocol revenue。

建议标准对象：

```text
FinancialStatement
FundamentalMetric
EarningsEvent
AnalystRating
ValuationSnapshot
```

典型用途：

- 价值因子；
- 成长因子；
- 质量因子；
- 财报事件策略；
- 横截面选股。

### 5.6 链上与加密原生数据

对于 crypto 市场，链上数据是区别于传统金融的重要数据域。

包括：

- block / transaction；
- address balance；
- exchange inflow/outflow；
- whale transfer；
- stablecoin supply；
- DeFi TVL；
- protocol revenue；
- staking；
- gas fee；
- active address；
- holder distribution；
- liquidation on lending protocols。

建议标准对象：

```text
OnChainTransaction
AddressBalanceSnapshot
ExchangeFlow
ProtocolMetric
StablecoinSupply
DefiTvlSnapshot
```

典型用途：

- 链上资金流因子；
- 稳定币流动性因子；
- DeFi 风险监控；
- 大户行为分析；
- crypto regime 判断。

### 5.7 舆情与另类数据

另类数据用于捕捉传统行情以外的信息优势。

包括：

- social media posts；
- forum / community discussions；
- search trends；
- app ranking；
- web traffic；
- GitHub activity；
- developer activity；
- sentiment score；
- topic trend；
- crowd attention。

建议标准对象：

```text
SocialPost
SentimentScore
TrendMetric
AttentionMetric
DeveloperActivity
AlternativeMetric
```

典型用途：

- 热度因子；
- 情绪反转因子；
- 项目活跃度分析；
- 风险事件监控；
- 新闻确认信号。

### 5.8 策略、交易与组合数据

除了外部数据，FDC 还应保存内部策略运行数据，便于归因和复盘。

包括：

- orders；
- fills；
- positions；
- portfolio snapshot；
- cash balance；
- PnL；
- risk exposure；
- signal snapshot；
- decision log；
- execution report。

建议标准对象：

```text
Order
Fill
Position
PortfolioSnapshot
SignalSnapshot
DecisionLog
ExecutionReport
RiskSnapshot
```

典型用途：

- 策略归因；
- 交易成本分析；
- 风控；
- 复盘；
- 训练 execution model。

### 5.9 跨数据域通用元数据

所有数据域都应有统一元数据，以支持追溯、质量控制和回测一致性。

建议通用字段：

```text
data_id
source_id
source_type
domain
schema_version
event_time
received_time
ingested_time
emitted_time
quality_flags
lineage
raw_reference
entity_links
```

其中：

- `domain` 区分 market/news/macro/fundamental/onchain/alternative/portfolio；
- `schema_version` 支持模型演进；
- `lineage` 记录数据来源和转换链；
- `entity_links` 将新闻、宏观、链上、基本面数据关联到 instrument、asset、issuer、protocol、country 等实体。

### 5.10 对模块设计的影响

为避免 crates 过早膨胀，后续不建议为每个数据域单独新建 crate。更合理的长期方向是新增一个统一标准数据定义模块：

```text
fdc-data
  market/        行情与市场微观结构
  reference/     主数据、交易日历、合约规格、实体映射
  news/          新闻、公告、事件
  macro_data/    宏观经济数据
  fundamental/   基本面和财务数据
  onchain/       链上数据
  altdata/       舆情和另类数据
  portfolio/     订单、成交、持仓、组合、策略运行数据
  common/        跨数据域通用元数据、实体链接、schema version、lineage
```

第一阶段建议先实现 `fdc-data::market` 和 `fdc-data::reference`，但模块结构和通用元数据必须预留多数据域扩展能力，避免系统被设计成只能处理 Trade/Bar/OrderBook 的行情库。

如果未来某个数据域变得足够复杂或需要独立发布，再考虑从 `fdc-data` 内部模块拆分为独立 crate。拆分应是后期演进结果，而不是第一阶段默认设计。

### 5.11 fdc-data 内部模块化原则

`fdc-data` 不是一个大杂烩 crate，而是一个集中发布、内部边界清晰的标准数据定义 crate。它应遵循以下原则：

1. **一个 crate，多个 domain modules**：第一阶段不创建 `fdc-news`、`fdc-macro`、`fdc-fundamental` 等独立 crate。所有标准金融数据模型先进入 `fdc-data`。
2. **common 只放跨域元数据**：`common` 放 `DataId`、`SourceId`、`Domain`、`SchemaVersion`、时间戳语义、质量标记、lineage、entity links 等跨域结构，不放具体业务对象。
3. **reference 是跨域基础设施**：交易日历、交易所、合约规格、实体映射、标的关系等放 `reference`，供 market、fundamental、news 等领域引用。
4. **domain modules 保持低耦合**：`market` 可以引用 `common` 和 `reference` 的基础标识，但不应直接依赖 `news`、`macro_data`、`portfolio` 等兄弟模块的具体对象。跨域关系通过 `common::EntityLink`、`common::DataId` 或明确的 join key 表达。
5. **拆 crate 是例外，不是默认**：只有当某个领域需要独立版本、独立 feature flags、独立依赖树或独立维护团队时，才考虑从 `fdc-data` 中拆出。拆出时必须保持原路径的 compatibility re-export。

推荐内部依赖方向：

```text
fdc_data::common
  -> no fdc_data sibling dependency

fdc_data::reference
  -> common

fdc_data::market
  -> common, reference

fdc_data::{news, macro_data, fundamental, onchain, altdata, portfolio}
  -> common, reference
```

禁止内部依赖形成领域环：

```text
market -> news -> market
fundamental -> portfolio -> fundamental
common -> any domain module
reference -> market/news/macro_data/...
```

## 6. fdc-types 与 fdc-data 的边界

这是新方案中最重要的边界。`fdc-data` 是统一标准金融数据模型 crate，`fdc-data::market` 是其中的行情与市场微观结构子模块。

### 6.1 fdc-types 的职责

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

### 6.2 fdc-data 的职责

`fdc-data` 负责“这些字段组成什么标准金融数据对象”。

它是统一标准金融数据模型层，内部按领域分模块。第一阶段 `market` 子模块适合包含：

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

### 6.3 推荐依赖关系

```text
fdc-core
  -> fdc-types
  -> fdc-data
```

严格避免：

```text
fdc-types -> fdc-data
fdc-data -> fdc-storage
fdc-data -> fdc-query
fdc-data -> fdc-transform
fdc-data -> fdc-wasm
```

### 6.4 当前代码中的重叠

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
- `fdc-data` 第一版可临时使用 `fdc-core::TimestampNs`；
- `Price`、`Symbol`、`Volume` 的长期归属应是 `fdc-types`；
- `TickData`、`MessageType` 的长期归属应是 `fdc-data::market`；
- 通过 re-export 和 compatibility module 渐进迁移。

---

## 7. 模块职责定义

### 7.1 fdc-core

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

### 7.2 fdc-common

定位：跨 crate 通用工具。

职责：

- retry/backoff；
- tracing helpers；
- serde helpers；
- async utilities；
- test utilities。

不应放领域模型。

### 7.3 fdc-types

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

它可以被 `fdc-data` 使用，但不依赖 `fdc-data`。

### 7.4 fdc-data

定位：统一标准金融数据模型，内部按数据域细分模块。第一阶段重点实现 market 和 reference。

第一版目标：

- 建立 `market::Instrument` / `InstrumentId` / `reference::InstrumentRegistry`；
- 建立 `Trade` / `Bar` / `OrderBook` 等基础市场对象；
- 建立 `MarketEvent` 统一事件枚举；
- 建立 `MarketDataQuality` 和时间语义；
- 建立 schema version 机制。

设计原则：

- 高频数据中避免重复保存 `exchange string` 和 `symbol string`；
- 使用 `instrument_id` 关联 `InstrumentRegistry`；
- 所有交易所差异在进入该层前或该层边界处被消化；
- 不依赖存储、查询、转换、WASM、API。

### 7.5 fdc-ingestion

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
SourceEnvelope<fdc_data::market::MarketEvent>
```

或：

```text
SourceEnvelope<RawExchangeEvent> -> Mapper -> fdc_data::market::MarketEvent
```

不负责：

- tick-to-bar；
- indicators；
- factor；
- storage tier routing。

### 7.6 fdc-adapter/*

定位：具体数据源适配器。

职责：

- Binance/OKX/Bybit/Barter 等具体 API 对接；
- REST/WebSocket client；
- exchange-specific raw model；
- exchange-specific parser；
- raw event 到 `fdc_data::market::MarketEvent` 的 mapper。

与 `fdc-ingestion` 的区别：

```text
fdc-adapter 负责具体来源。
fdc-ingestion 负责统一接入管线。
```

实验分支中的 `fdc-adapter/barter` 可作为后续迁移参考。

后续具体设计实现可重点参考 `barter-rs` 的 adapter 分层思想：

- exchange-specific connector 只处理交易所协议、认证、订阅和原始 payload；
- stream layer 负责 websocket/rest stream 生命周期、重连、心跳和 backpressure 入口；
- normalized market event 作为 adapter 输出边界，进入 FDC 时应映射为 `fdc_data::market::MarketEvent`；
- 不把 barter-rs 的内部类型直接作为 FDC canonical model，避免外部库结构污染 `fdc-data`；
- 可借鉴其 exchange/product/subscription 抽象，但最终命名、错误模型、时间语义和质量标记应服从 FDC canonical architecture。

### 7.7 fdc-transform

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
fdc_data::market::Trade
fdc_data::market::OrderBook
fdc_data::market::MarketEvent
```

输出：

```text
fdc_data::market::Bar
fdc_data::market::DerivedEvent
fdc_feature::Feature
```

不应拥有 canonical market model。

### 7.8 fdc-storage

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

### 7.9 fdc-query

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

### 7.10 fdc-factor

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
fdc-factor -> fdc-data
fdc-factor -> fdc-feature
fdc-factor -> fdc-transform
fdc-factor -> fdc-wasm 可选
```

### 7.11 fdc-feature

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

### 7.12 fdc-analytics

定位：分析、回测统计、风控和报告。

职责：

- stream analytics；
- batch analytics；
- technical analysis；
- risk metrics；
- ML/feature analysis；
- backtest reports；
- portfolio metrics。

当前 `fdc-analytics::models::MarketData` 与未来 `fdc_data::market::Bar` 重复。

长期策略：

- `fdc-analytics` 不定义 canonical market data；
- 使用 `fdc_data::market::Bar` / `fdc_data::market::Trade` 作为输入；
- 只保留 `AnalyticsResult`、`RiskMetrics`、`PredictionResult` 等分析结果模型。

### 7.13 fdc-wasm

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

不应成为 `fdc-data` 的依赖。

### 7.14 fdc-api

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

### 7.15 fdc-proto

定位：wire protocol。

职责：

- Protobuf message；
- gRPC service；
- cross-language DTO；
- protocol compatibility。

边界：

```text
fdc-data 是 Rust 内部 canonical model。
fdc-proto 是外部 wire schema。
```

二者通过 conversion 层互转。

### 7.16 fdc-server

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

### 7.17 fdc-cli

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

### 7.18 fdc-orchestrator

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

## 8. 推荐依赖方向

总体依赖方向：

```text
fdc-core
  ↓
fdc-common
  ↓
fdc-types
  ↓
fdc-data
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
fdc-data       -> fdc-core, fdc-types
fdc-ingestion  -> fdc-core, fdc-types, fdc-data
fdc-transform  -> fdc-core, fdc-types, fdc-data, optional fdc-wasm
fdc-storage    -> fdc-core, fdc-types, fdc-data, optional fdc-wasm
fdc-query      -> fdc-core, fdc-types, fdc-data, fdc-storage, optional fdc-wasm
fdc-factor     -> fdc-core, fdc-types, fdc-data, fdc-feature, optional fdc-wasm
fdc-feature    -> fdc-core, fdc-types, fdc-data, optional fdc-storage
fdc-analytics  -> fdc-core, fdc-types, fdc-data, fdc-query, fdc-feature
fdc-api        -> fdc-core, fdc-data, fdc-query, fdc-ingestion
fdc-server     -> fdc-api, fdc-storage, fdc-query, fdc-ingestion, fdc-wasm, fdc-orchestrator
fdc-cli        -> fdc-api client or direct service crates for admin commands
fdc-proto      -> wire DTO, conversion to/from fdc-data
```

禁止形成反向依赖：

```text
fdc-data -> fdc-storage
fdc-data -> fdc-query
fdc-data -> fdc-transform
fdc-data -> fdc-api
fdc-data -> fdc-wasm
fdc-types  -> fdc-data
fdc-core   -> fdc-data
```

---

## 9. 标准市场数据模型初版范围

`fdc-data` 第一版应聚焦最小可用 canonical financial data model。实现范围以 `market` 和 `reference` 子模块为主，同时预留 news、macro_data、fundamental、onchain、altdata、portfolio 等内部模块边界。

建议模块结构：

```text
crates/fdc-data/src/
  lib.rs
  common.rs
  market/
    mod.rs
    instrument.rs
    exchange.rs
    trade.rs
    bar.rs
    orderbook.rs
    derivatives.rs
    event.rs
  reference/
    mod.rs
    registry.rs
    calendar.rs
    entity.rs
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
  schema.rs
  quality.rs
```

对外公开路径应稳定、明确：

```rust
fdc_data::common::DataId
fdc_data::reference::InstrumentRegistry
fdc_data::market::Trade
fdc_data::market::Bar
fdc_data::market::MarketEvent
fdc_data::fundamental::FinancialStatement
fdc_data::portfolio::Position
```

不建议在 crate root 大量平铺 re-export 所有领域对象，避免 `fdc_data::Bar`、`fdc_data::Position` 这类路径在长期演进中产生命名冲突。crate root 可以 re-export 最基础的 `common` 类型，领域对象应通过 domain module 访问。

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

## 10. 时间语义

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

## 11. 数据质量模型

`fdc-data::common` 应包含跨数据域基础质量标记，`fdc-data::market` 可扩展市场数据质量字段，但不要把 ingestion 实现细节放进去。

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

## 12. 存储分层与标准模型

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

## 13. 查询语义

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
Vec<fdc_data::market::Bar>
```

---

## 14. 当前 upstream/main 模块实现盘点

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
2. `fdc-analytics::models::MarketData` 与未来 `fdc-data::market::Bar` 重叠；
3. `fdc-wasm::WasmValue` 与 `fdc_core::Value` 有转换关系，长期应转向 `fdc_types::Value`；
4. 当前缺少独立 `fdc-data`，导致金融数据语义尚无统一归属。

---

## 15. 实验分支迁移策略

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

迁移时必须改为围绕 `fdc-data`：

```text
adapter raw event -> fdc_data::market::MarketEvent -> storage/query/transform
```

不再把 canonical model 放在 `fdc-transform` 或 `fdc-orchestrator` 中。

---

## 16. 推荐开发阶段

### Phase 0：文档和边界确认

目标：

- 明确 README 与本文档关系；
- 明确各 crate 职责；
- 明确 `fdc-types` / `fdc-data` 边界；
- 明确依赖方向；
- 明确实验分支迁移策略。

### Phase 1：新增 fdc-data

目标：

- 新增 `crates/fdc-data`；
- 定义统一标准金融数据模型 crate；
- 第一阶段实现 `market` 与 `reference` 子模块；
- 预留 news/macro_data/fundamental/onchain/altdata/portfolio 内部模块边界；
- 增加单元测试；
- 不连接 storage/query/ingestion。

### Phase 2：类型边界整理

目标：

- 明确 `Price`、`Symbol`、`Quantity` 的短期使用和长期归属；
- 提供 re-export 或 compatibility module；
- 避免一次性破坏现有代码。

### Phase 3：接入 transform 和 analytics

目标：

- `fdc-transform` 输入输出改为使用 `fdc_data::market`；
- `fdc-analytics::MarketData` 逐步替换为 `fdc_data::market::Bar`；
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

## 17. 最终定位

FDC 不只是一个数据库。

它应成为：

> Rust 实现的高性能量化金融数据基础设施，而不是单一行情数据库。

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
  -> 标准金融数据域
  -> 数据采集
  -> 数据转换
  -> 数据存储
  -> 数据查询
  -> 因子与特征
  -> 分析与策略
```

其中：

> `fdc-data` 是统一标准金融数据模型的核心 crate，`fdc-data::market` 是行情与市场微观结构语义的核心子模块，`fdc-types` 是基础类型系统的核心。FDC 必须在一个集中数据标准模块内为新闻、公告、宏观、基本面、链上、舆情、组合与策略运行数据预留扩展能力，避免 crates 过早碎片化。
