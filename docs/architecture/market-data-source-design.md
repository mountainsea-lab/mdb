# Market Data Source 顶层设计

## 设计状态

- 状态：已确认方向，后续持续维护
- 适用范围：加密交易所市场数据接入，包括实时数据、历史数据和后续回放能力
- 关联文档：`docs/architecture/barter-rs-integration.md`
- 设计回顾：`docs/architecture/fdc-barter-design-review.md`
- 关联模块：`crates/fdc-adapter/barter`、`fdc-ingestion`、`fdc-transform`、`fdc-storage`

## 核心约定

`fdc-barter` 位于 `crates/fdc-adapter/barter`，负责加密交易所实时数据和历史数据的获取。它的目的不是重新实现交易所接入生态，而是尽量复用 `/Volumes/wdata/mountainsea-lab/barter-rs` 中的 Barter 生态完成需求。

本阶段 **不新增 `fdc-market-data-core` crate**。市场数据请求、事件、source、checkpoint 等抽象先在 `fdc-adapter/barter` 内按业务模块拆分。未来如果多个 adapter 需要共享这些抽象，再考虑抽取公共 crate。

职责约定：

- `crates/fdc-adapter`：统一存放具体外部数据源适配器，未来新增数据源也放在该目录下。
- `fdc-barter`：加密交易所数据源适配器，优先复用 `barter-data`、`barter-integration`、`barter-instrument`。
- `fdc-ingestion`：mdb 接入中枢，负责 source 生命周期、缓冲、背压、批处理、恢复、指标和 checkpoint 协调。
- `fdc-transform`：数据标准化、格式转换、类型转换、schema 转换。
- `fdc-storage`：数据写入、多层存储、缓存和后续查询支撑。

## 顶层目标

实时、历史和回放数据进入 mdb 后必须走同一条下游管道，避免为实时和历史维护两套模型。

```text
fdc-adapter/barter 或 future data source adapters
        ↓
fdc-ingestion source path
        ↓
fdc-transform
        ↓
fdc-storage / fdc-query / fdc-analytics
```

`fdc-barter` 是数据源插件，不绕过 `fdc-ingestion` 直接进入 `fdc-transform` 或 `fdc-storage`。

## 当前 fdc-ingestion 参考结论

`crates/fdc-ingestion/examples/ingestion_demo.rs` 当前展示的是网络字节流路径：

```text
ReceivedData
        ↓
DataParser
        ↓
DataValidator
        ↓
BatchItem
        ↓
BatchProcessor
        ↓
SimpleStorage
```

这条路径适合 TCP/WebSocket/UDP 等原始字节输入。`fdc-barter` 输出的是已标准化的市场数据事件，不应该强行伪装成 `ReceivedData` 再走 `DataParser`。

因此 `fdc-ingestion` 需要新增 source path：

```text
BarterIngestionEnvelope
        ↓
DataBuffer<BarterIngestionEnvelope>
        ↓
SourceValidator / SourceValidationResult
        ↓
SourceBatchItem
        ↓
SourceBatchProcessor 或现有 BatchProcessor 的泛化版本
        ↓
fdc-transform
```

短期设计目标是先定义交接边界，不急于改造现有 `DataParser` 和 `BatchProcessor`。

## fdc-adapter/barter 推荐分包

```text
crates/fdc-adapter/barter/src/
  lib.rs
  error.rs
  config.rs

  model/
    mod.rs
    event.rs          # BarterMarketEvent / BarterMarketPayload / BarterMarketDataKind
    request.rs        # BarterMarketDataRequest / symbol/kind/mode request
    source.rs         # source id / source status / source trait
    checkpoint.rs     # historical checkpoint / resume cursor

  live/
    mod.rs
    source.rs         # BarterLiveSource
    subscription.rs   # mdb request -> Barter subscription batches
    stream.rs         # Barter DynamicStreams / Streams 包装

  historical/
    mod.rs
    source.rs         # BarterHistoricalSource
    request.rs        # historical request / time range / pagination
    checkpoint.rs     # checkpoint persistence model
    binance.rs        # 第一阶段 Binance REST 历史数据
    okx.rs            # 后续 OKX

  mapper/
    mod.rs
    event.rs          # Barter MarketEvent -> BarterMarketEvent
    instrument.rs     # symbol <-> base/quote/instrument
    exchange.rs       # exchange enum/string 映射

  ingestion/
    mod.rs
    envelope.rs       # 给 fdc-ingestion 的统一输出 envelope
    sink.rs           # 后续对接 ingestion sender/stream

  capability/
    mod.rs
    exchange.rs       # 每个交易所支持 trade/l1/candle/history 等声明
```

分包原则：

- 按业务功能拆分，不按技术层堆大文件。
- 每个模块只暴露必要 public API。
- `live` 只处理实时流，不处理历史分页。
- `historical` 只处理历史拉取、分页、checkpoint，不处理实时 WebSocket。
- `mapper` 是 Barter 类型和 mdb adapter 类型的唯一映射层。
- `ingestion` 只定义给 `fdc-ingestion` 的输出 envelope 和 sink 边界，不实现存储。

## Barter 实时数据参考

`barter-data/examples` 中可直接参考：

- `dynamic_multi_stream_multi_exchange.rs`
- `public_trades_streams.rs`
- `public_trades_streams_multi_exchange.rs`
- `order_books_l1_streams_multi_exchange.rs`

推荐优先使用 `DynamicStreams` 作为 mdb 的实时流基础，因为它可以统一多交易所、多 symbol、多数据类型：

```rust
let streams = DynamicStreams::init(subscription_batches).await?;
let merged = streams
    .select_all::<MarketStreamResult<MarketDataInstrument, DataKind>>()
    .with_error_handler(|error| warn!(?error, "MarketStream generated error"));
```

相比 `Streams::<PublicTrades>` 或 `Streams::<OrderBooksL1>`，`DynamicStreams` 更适合 mdb 统一 source，因为后续需要同时支持 trades、L1、L2、candles、liquidations。

## fdc-barter 内部数据模式

```rust
enum BarterMarketDataMode {
    Live,
    Historical { start: TimestampNs, end: Option<TimestampNs> },
    Replay { dataset_id: String, speed: ReplaySpeed },
}
```

语义：

- `Live`：Barter WebSocket 或类似实时流。
- `Historical`：交易所 REST、Barter 历史抽象、文件或对象存储等历史数据源。
- `Replay`：基于已落地数据集的模拟实时回放。

## fdc-barter 请求模型

```rust
struct BarterMarketDataRequest {
    exchange: String,
    symbols: Vec<String>,
    kinds: Vec<BarterMarketDataKind>,
    mode: BarterMarketDataMode,
    granularity: Option<Duration>,
    options: BarterRequestOptions,
}
```

请求约束：

- `symbols` 使用 mdb 标准符号，例如 `BTCUSDT`。
- `mapper::instrument` 负责转换为 Barter 或交易所要求的 base/quote/instrument 表示。
- `granularity` 主要用于 candles/klines。
- 请求中不包含存储目标，存储由下游统一决定。

## fdc-barter 事件模型

```rust
struct BarterMarketEvent {
    source: String,
    mode: BarterMarketDataMode,
    exchange: String,
    symbol: Symbol,
    kind: BarterMarketDataKind,
    timestamp: TimestampNs,
    received_at: TimestampNs,
    payload: BarterMarketPayload,
    sequence: Option<u64>,
    checkpoint: Option<BarterCheckpoint>,
}
```

事件原则：

- 实时和历史输出同一种事件。
- 历史事件必须带有可恢复 checkpoint。
- 实时事件可以带有 exchange sequence 或本地 sequence。
- `payload` 承载 trade、order book、candle、liquidation 等具体数据。

## 数据类型

```rust
enum BarterMarketDataKind {
    Trade,
    OrderBookL1,
    OrderBookL2,
    OrderBookL3,
    Candle,
    Liquidation,
}
```

```rust
enum BarterMarketPayload {
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
struct BarterSourceCapabilities {
    supports_live: bool,
    supports_historical: bool,
    supports_replay: bool,
    exchanges: Vec<String>,
    kinds: Vec<BarterMarketDataKind>,
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

## fdc-ingestion 对接模型

`fdc-barter` 给 `fdc-ingestion` 的交接对象是 envelope：

```rust
struct BarterIngestionEnvelope {
    source: String,
    mode: BarterMarketDataMode,
    event: BarterMarketEvent,
    checkpoint: Option<BarterCheckpoint>,
    received_at: TimestampNs,
}
```

`fdc-ingestion` 后续应新增 source path 模块：

```text
crates/fdc-ingestion/src/
  source.rs
  source_manager.rs
  source_pipeline.rs
  source_envelope.rs
  source_batch.rs
```

建议职责：

- `source.rs`：定义 ingestion 侧 source 输入边界。
- `source_manager.rs`：管理 source 启停和生命周期。
- `source_pipeline.rs`：连接 source stream、buffer、validation、batch 和 transform。
- `source_envelope.rs`：定义通用 source envelope，避免 ingestion 依赖具体 Barter 内部细节。
- `source_batch.rs`：source 事件批处理项，可复用或泛化现有 `BatchProcessor`。

## 历史数据设计

历史数据是 `BarterMarketDataMode::Historical`，不是旁路。

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

## 错误模型

错误需要区分可重试和不可重试：

```rust
enum BarterMarketDataError {
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
- 文档明确不新增 `fdc-market-data-core`，抽象先留在 `fdc-adapter/barter`。
- 文档明确实时、历史、回放统一事件模型。
- 文档明确 `fdc-ingestion` 新增 source path，而不是强行套旧的 `ReceivedData -> DataParser` 路径。
- 文档明确历史数据 checkpoint/resume/rate limit/gap detection 要求。

### Phase 2：fdc-barter 业务分包落地

- 按 `model/live/historical/mapper/ingestion/capability` 拆分文件。
- 将当前 `lib.rs` 中的事件、配置、错误和 mapper 迁移到对应模块。
- 保持现有测试通过。
- 不接真实网络。

### Phase 3：fdc-ingestion source path 骨架

- 增加 source envelope/source pipeline 设计对应的最小模块。
- 支持接收 `BarterIngestionEnvelope` 或其通用化形式。
- 对接 `DataBuffer<T>`。
- 不改动现有 network receiver path。

### Phase 4：实时 Barter MVP

- 参考 `barter-data/examples/dynamic_multi_stream_multi_exchange.rs`。
- 接入 `barter-data` 实时 public trades。
- 接入 L1 order book。
- 输出 `BarterIngestionEnvelope`。
- 进入 `fdc-ingestion` source buffer/batch。

### Phase 5：历史数据 MVP

- 实现 Binance spot candles/trades 历史拉取。
- 支持分页、rate limit、retry、checkpoint。
- 输出同一个 `BarterIngestionEnvelope`。

### Phase 6：历史 + 实时衔接

- 历史补齐到 T。
- 实时从 T 附近启动。
- 去重和 gap detection。
- 形成可重复验证的端到端数据流。

## 当前不做

- 不新增 `fdc-market-data-core`。
- 不把 storage 写入逻辑放入 `fdc-barter`。
- 不让 `fdc-transform` 依赖 barter-rs 类型。
- 不绕过 `fdc-ingestion`。
- 不强行把 `fdc-barter` 输出伪装成 `ReceivedData`。
- 不在顶层设计阶段实现真实 WebSocket 或 REST。
- 不优先支持所有交易所，先用少量交易所验证抽象。

## 推荐下一步

先写 Phase 2 的 implementation plan：重构 `crates/fdc-adapter/barter` 的模块结构，但不接真实网络。

Phase 2 完成后，再写 Phase 3 的 `fdc-ingestion` source path 计划。
