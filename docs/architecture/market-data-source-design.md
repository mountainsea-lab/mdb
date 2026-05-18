# Market Data Source 顶层设计

## 设计状态

- 状态：已确认方向，后续持续维护
- 适用范围：加密交易所市场数据接入，包括实时数据、历史数据和后续回放能力
- 关联文档：`docs/architecture/barter-rs-integration.md`
- 关联模块：`fdc-barter`、`fdc-ingestion`、`fdc-transform`、`fdc-storage`

## 核心约定

`fdc-barter` 负责加密交易所实时数据和历史数据的获取。它的目的不是重新实现交易所接入生态，而是尽量复用 `/Volumes/wdata/mountainsea-lab/barter-rs` 中的 Barter 生态完成需求。

职责约定：

- `fdc-barter`：加密交易所数据源适配器，优先复用 `barter-data`、`barter-integration`、`barter-instrument`。
- `fdc-ingestion`：mdb 接入中枢，负责 source 生命周期、缓冲、背压、批处理、恢复、指标。
- `fdc-transform`：数据标准化、格式转换、类型转换、schema 转换。
- `fdc-storage`：数据写入、多层存储、缓存和后续查询支撑。

## 顶层目标

实时、历史和回放数据进入 mdb 后必须走同一条下游管道，避免为实时和历史维护两套模型。

```text
fdc-barter / future data source adapters
        ↓
fdc-ingestion
        ↓
fdc-transform
        ↓
fdc-storage / fdc-query / fdc-analytics
```

`fdc-barter` 是数据源插件，不绕过 `fdc-ingestion` 直接进入 `fdc-transform` 或 `fdc-storage`。

## 推荐模块边界

### fdc-market-data-core，建议新增

定义市场数据源的通用抽象，避免未来所有数据源都依赖 `fdc-barter` 或 barter-rs。

建议包含：

- `MarketDataSource`
- `MarketDataMode`
- `MarketDataRequest`
- `MarketDataEvent`
- `MarketDataKind`
- `MarketDataPayload`
- `SourceCapabilities`
- `SourceCheckpoint`
- `MarketDataSourceError`

如果短期不新增 crate，也可以先放入 `fdc-ingestion` 的 `source` 模块，但长期建议抽为独立 crate。

### fdc-barter

负责使用 Barter 生态实现加密交易所数据源。

建议包含：

- `BarterLiveSource`：基于 `barter-data` WebSocket 实时行情。
- `BarterHistoricalSource`：优先复用 Barter 历史抽象；如果 Barter 当前能力不足，则在 `fdc-barter` 内通过交易所 REST 补齐。
- `BarterEventMapper`：将 Barter 事件映射为 mdb 统一 `MarketDataEvent`。
- `BarterSubscriptionBuilder`：将 mdb 请求转换为 Barter subscriptions。
- `BarterExchangeCapabilities`：声明每个交易所支持的实时/历史数据种类。

不包含：

- 存储写入逻辑。
- 查询逻辑。
- 策略交易逻辑。
- mdb 全局调度逻辑。

### fdc-ingestion

负责把任意 `MarketDataSource` 纳入 mdb 接入生命周期。

建议包含：

- source 启停管理。
- 多 source 并发运行。
- buffer 管理。
- backpressure。
- batch。
- recovery。
- metrics。
- checkpoint 协调。
- 将 source 输出交给 `fdc-transform`。

### fdc-transform

只处理统一事件，不感知数据来自 Barter、REST、文件还是回放。

建议输入：

```text
MarketDataEvent -> TransformPipeline -> ProcessedData
```

## 统一数据模式

```rust
enum MarketDataMode {
    Live,
    Historical { start: TimestampNs, end: Option<TimestampNs> },
    Replay { dataset_id: String, speed: ReplaySpeed },
}
```

语义：

- `Live`：实时 WebSocket 或类似实时流。
- `Historical`：交易所 REST、文件、对象存储等历史数据源。
- `Replay`：基于已落地数据集的模拟实时回放。

## 统一请求模型

```rust
struct MarketDataRequest {
    exchange: String,
    symbols: Vec<String>,
    kinds: Vec<MarketDataKind>,
    mode: MarketDataMode,
    granularity: Option<Duration>,
    options: MarketDataRequestOptions,
}
```

请求约束：

- `symbols` 使用 mdb 标准符号，例如 `BTCUSDT`。
- `fdc-barter` 负责转换为 Barter 或交易所要求的 base/quote/instrument 表示。
- `granularity` 主要用于 candles/klines。
- 请求中不包含存储目标，存储由下游统一决定。

## 统一事件模型

```rust
struct MarketDataEvent {
    source: String,
    mode: MarketDataMode,
    exchange: String,
    symbol: Symbol,
    kind: MarketDataKind,
    timestamp: TimestampNs,
    received_at: TimestampNs,
    payload: MarketDataPayload,
    sequence: Option<u64>,
    checkpoint: Option<SourceCheckpoint>,
}
```

事件原则：

- 实时和历史输出同一种事件。
- 历史事件必须带有可恢复 checkpoint。
- 实时事件可以带有 exchange sequence 或本地 sequence。
- `payload` 承载 trade、order book、candle、liquidation 等具体数据。

## 数据类型

```rust
enum MarketDataKind {
    Trade,
    OrderBookL1,
    OrderBookL2,
    OrderBookL3,
    Candle,
    Liquidation,
}
```

```rust
enum MarketDataPayload {
    Trade(TradePayload),
    OrderBookL1(OrderBookL1Payload),
    OrderBookDelta(OrderBookDeltaPayload),
    Candle(CandlePayload),
    Liquidation(LiquidationPayload),
    Raw(RawPayload),
}
```

`Raw` 只用于无法立即标准化但需要保留的数据，不作为长期主要模型。

## Source 能力声明

每个数据源需要声明能力，便于 ingestion 在运行前校验请求。

```rust
struct SourceCapabilities {
    supports_live: bool,
    supports_historical: bool,
    supports_replay: bool,
    exchanges: Vec<String>,
    kinds: Vec<MarketDataKind>,
    max_symbols_per_subscription: Option<usize>,
    historical_granularities: Vec<Duration>,
    rate_limits: Vec<RateLimitRule>,
}
```

## fdc-barter 对 Barter 生态的使用原则

优先级：

1. 直接复用 `barter-data` 的实时 WebSocket stream。
2. 复用 `barter-integration` 的协议、stream、retry、REST/WebSocket 基础设施。
3. 复用 `barter-instrument` 的 exchange/instrument 表示。
4. 如果 barter-rs 没有某个交易所历史 REST 客户端，则在 `fdc-barter` 中实现最小历史客户端，并保持接口与 Barter 风格兼容。
5. 当历史能力成熟且可复用时，再考虑反向贡献到 barter-rs 生态。

## 历史数据设计

历史数据是 `MarketDataMode::Historical`，不是旁路。

必须支持的机制：

- 时间范围分页。
- rate limit。
- retry/backoff。
- checkpoint/resume。
- 数据去重。
- gap detection。
- 历史补齐到某一时间点后切换实时流。

推荐 MVP：

1. Binance spot candles/klines。
2. Binance spot trades。
3. OKX spot candles/klines。
4. 历史补齐到 T 后启动实时流。

## 实时与历史衔接

目标流程：

```text
HistoricalSource(start, T)
        ↓
补齐历史数据并记录 checkpoint
        ↓
LiveSource(from T or current)
        ↓
实时持续接入
```

需要处理：

- 历史末尾与实时开头的重叠去重。
- 交易所服务端时间与本地接收时间的差异。
- 实时启动前的短暂数据空窗。
- candles 与 trades 的不同时间边界。

## fdc-ingestion 中枢职责

`fdc-ingestion` 不负责理解 Barter 内部类型，但负责管理 source 输出的统一事件流。

建议数据流：

```text
MarketDataSource::stream(request)
        ↓
DataBuffer
        ↓
BackpressureController
        ↓
BatchProcessor
        ↓
RecoveryManager / checkpoint
        ↓
fdc-transform
```

## 错误模型

错误需要区分可重试和不可重试：

```rust
enum MarketDataSourceError {
    UnsupportedExchange,
    UnsupportedSymbol,
    UnsupportedKind,
    InvalidTimeRange,
    RateLimited { retry_after: Option<Duration> },
    Network,
    Decode,
    ExchangeRejected,
    CheckpointCorrupted,
    Fatal,
}
```

`fdc-ingestion` 根据错误类型决定重试、跳过、降级或停止 source。

## 阶段路线

### Phase 1：顶层抽象设计

只产出设计和接口边界，不接真实网络。

验收标准：

- 文档明确 `fdc-barter`、`fdc-ingestion`、`fdc-transform` 的职责边界。
- 文档明确实时、历史、回放统一事件模型。
- 文档明确历史数据 checkpoint/resume/rate limit/gap detection 要求。

### Phase 2：抽象落地骨架

- 新增或确定 `fdc-market-data-core`。
- 定义统一类型和 trait。
- `fdc-barter` 适配当前 `BarterMarketEvent` 到统一 `MarketDataEvent`。
- `fdc-ingestion` 增加 source 管理骨架。

不接真实网络。

### Phase 3：实时 Barter MVP

- 接入 `barter-data` 实时 public trades。
- 接入 L1 order book。
- 输出统一 `MarketDataEvent`。
- 进入 `fdc-ingestion` buffer/batch。

### Phase 4：历史数据 MVP

- 实现 Binance spot candles/trades 历史拉取。
- 支持分页、rate limit、retry、checkpoint。
- 输出统一 `MarketDataEvent`。

### Phase 5：历史 + 实时衔接

- 历史补齐到 T。
- 实时从 T 附近启动。
- 去重和 gap detection。
- 形成可重复验证的端到端数据流。

## 当前不做

- 不把 storage 写入逻辑放入 `fdc-barter`。
- 不让 `fdc-transform` 依赖 barter-rs 类型。
- 不绕过 `fdc-ingestion`。
- 不在顶层设计阶段实现真实 WebSocket 或 REST。
- 不优先支持所有交易所，先用少量交易所验证抽象。

## 推荐下一步

先实现 Phase 2 的设计计划，但在写代码前应先产出详细 implementation plan。优先决策点是：是否新增独立 `fdc-market-data-core` crate。

推荐：新增 `fdc-market-data-core`，因为它能避免 `fdc-ingestion`、`fdc-transform`、`fdc-barter` 之间产生错误依赖方向。
