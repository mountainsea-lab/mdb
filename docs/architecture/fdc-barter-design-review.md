# fdc-barter 设计回顾与缺口分析

## 背景

本文件回顾当前 `fdc-barter` 设计是否足以支撑目标：**加密交易所实时数据和历史数据获取，并衔接 mdb 的 `fdc-ingestion -> fdc-transform -> storage/query/analytics` 数据流程**。

关联文档：

- `docs/architecture/barter-rs-integration.md`
- `docs/architecture/market-data-source-design.md`

当前实现：

- 路径：`crates/fdc-adapter/barter`
- crate 名：`fdc-barter`
- 当前代码只有 `src/lib.rs`、`tests/adapter_contract.rs`
- 已实现最小配置、错误、事件类型和 Barter public trade 到 `BarterMarketEvent` 的映射

## 回顾结论

当前顶层方向正确，但为了满足“实时数据 + 历史数据获取”的完整需求，现有 `fdc-barter` 设计需要进一步细化。主要缺口不是是否接入 Barter，而是缺少支持生产级数据源的边界对象和生命周期模型。

需要补齐的设计点：

1. source 生命周期与运行状态。
2. 实时订阅请求和 Barter subscription batch 规划。
3. 历史分页、游标、checkpoint 和限流模型。
4. payload 完整结构，当前 `price/volume` 过于简化。
5. ingestion envelope 的明确契约。
6. 错误分类和重试策略。
7. 数据质量控制，包括去重、gap detection、时间边界。
8. 测试边界和不接真实网络的 mock/fixture 策略。

## 当前设计已覆盖的内容

| 需求 | 当前覆盖情况 | 说明 |
| --- | --- | --- |
| adapter 放在统一目录 | 已覆盖 | `crates/fdc-adapter/barter` |
| 不新增 `fdc-market-data-core` | 已覆盖 | 抽象先留在 `fdc-barter` 内 |
| 优先复用 Barter 生态 | 已覆盖 | 文档明确使用 `barter-data` / `barter-integration` / `barter-instrument` |
| 实时和历史统一输出模型 | 部分覆盖 | 已有方向，但事件 payload 和 checkpoint 需要细化 |
| 不绕过 `fdc-ingestion` | 已覆盖 | 文档明确 source path |
| 参考 Barter examples | 已覆盖 | 明确优先参考 `dynamic_multi_stream_multi_exchange.rs` |
| `fdc-ingestion` 对接参考 | 已覆盖 | 已分析现有 `ReceivedData -> DataParser` 路径不适合结构化 source |

## 需要补齐的设计缺口

### 1. Source 生命周期模型

实时 WebSocket 和历史 REST 都不是一次性函数调用，需要可观察的生命周期。

建议增加：

```rust
enum BarterSourceStatus {
    Created,
    Starting,
    Running,
    Backfilling,
    Reconnecting,
    RateLimited,
    Stopping,
    Stopped,
    Failed,
}
```

```rust
struct BarterSourceState {
    source_id: String,
    mode: BarterMarketDataMode,
    status: BarterSourceStatus,
    started_at: Option<TimestampNs>,
    last_event_at: Option<TimestampNs>,
    last_error: Option<String>,
    checkpoint: Option<BarterCheckpoint>,
}
```

目的：

- `fdc-ingestion` 可以监控 source 状态。
- 重连、限流、历史回填和失败状态可区分。
- 后续 metrics 和恢复策略有状态依据。

### 2. 实时订阅规划模型

Barter examples 中 `DynamicStreams::init` 接收 subscription batches。mdb 不能只传 symbols，需要先规划批次。

建议增加：

```rust
struct BarterSubscriptionPlan {
    batches: Vec<BarterSubscriptionBatch>,
    expected_streams: usize,
}

struct BarterSubscriptionBatch {
    exchange: String,
    kind: BarterMarketDataKind,
    symbols: Vec<String>,
    high_volume: bool,
}
```

规划原则：

- 高频 symbol 可以单独 WebSocket。
- 低频 symbol 可以共享 WebSocket。
- 不同 exchange 或不同 kind 需要拆分。
- 规划层负责从 mdb 标准 symbol 转换到 Barter base/quote/instrument。

### 3. 历史数据分页和游标模型

历史数据不是简单 `start/end`，必须支持分页、checkpoint 和恢复。

建议增加：

```rust
struct HistoricalPageRequest {
    exchange: String,
    symbol: String,
    kind: BarterMarketDataKind,
    start: TimestampNs,
    end: Option<TimestampNs>,
    limit: Option<usize>,
    cursor: Option<HistoricalCursor>,
}

struct HistoricalCursor {
    exchange: String,
    symbol: String,
    kind: BarterMarketDataKind,
    next_start: Option<TimestampNs>,
    page_token: Option<String>,
    last_seen_exchange_id: Option<String>,
}
```

`BarterCheckpoint` 应至少包含：

```rust
struct BarterCheckpoint {
    source_id: String,
    exchange: String,
    symbol: String,
    kind: BarterMarketDataKind,
    mode: BarterMarketDataMode,
    last_event_time: TimestampNs,
    cursor: Option<HistoricalCursor>,
    updated_at: TimestampNs,
}
```

### 4. Rate limit 与 retry 策略

历史 REST 必须提前建模限流。否则实现 Binance/OKX 历史数据时会反复返工。

建议增加：

```rust
struct RateLimitRule {
    exchange: String,
    endpoint: String,
    max_requests: u32,
    window_ms: u64,
    weight: u32,
}

struct RetryPolicy {
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    jitter: bool,
}
```

错误中需要明确：

```rust
RateLimited { retry_after_ms: Option<u64> }
TransientNetwork
PermanentExchangeRejection
Decode
InvalidResponse
```

### 5. Payload 结构需要从 price/volume 扩展为业务 payload

当前 `BarterMarketEvent` 只有 `price`、`volume`，不足以表达 L1、L2、candle、liquidation，也不足以保留 trade side、trade id 等信息。

建议改为：

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

关键 payload：

```rust
struct TradePayload {
    trade_id: Option<String>,
    price: Price,
    quantity: DecimalLike,
    side: Option<TradeSide>,
}

struct CandlePayload {
    open_time: TimestampNs,
    close_time: TimestampNs,
    open: Price,
    high: Price,
    low: Price,
    close: Price,
    volume: DecimalLike,
}
```

注意：当前 `fdc_core::Volume` 是 `u64`，加密货币成交量经常是小数。不能长期用 `Volume(u64)` 表示 crypto amount。需要在 `fdc-barter` 内保留 decimal amount，后续由 `fdc-transform` 决定如何转成目标 schema。

### 6. Ingestion envelope 契约需要更稳定

建议 `BarterIngestionEnvelope` 不直接暴露过多 Barter 细节。

```rust
struct BarterIngestionEnvelope {
    envelope_id: String,
    source_id: String,
    emitted_at: TimestampNs,
    event: BarterMarketEvent,
    checkpoint: Option<BarterCheckpoint>,
    quality: DataQualityFlags,
}
```

```rust
struct DataQualityFlags {
    is_replay: bool,
    is_backfill: bool,
    is_duplicate_candidate: bool,
    has_gap_before: bool,
    is_out_of_order: bool,
}
```

目的：

- ingestion 可以做 buffer/batch/recovery。
- transform 可以读取质量标记。
- 历史补齐和实时衔接可检测重复和 gap。

### 7. 实时 + 历史衔接需要明确状态机

建议补充状态机：

```text
HistoricalBackfill(start, T)
        ↓
Checkpoint(last_event_time = T)
        ↓
LiveWarmup(overlap_window)
        ↓
Deduplicate(overlap)
        ↓
LiveRunning
```

必须支持：

- overlap window，例如从 T - 30s 开始接实时或对齐最近 trade id。
- duplicate detection key。
- gap detection policy。
- out-of-order handling。

建议定义去重 key：

```rust
struct EventDedupKey {
    exchange: String,
    symbol: String,
    kind: BarterMarketDataKind,
    event_time: TimestampNs,
    exchange_event_id: Option<String>,
    sequence: Option<u64>,
}
```

### 8. Capability 模型需要更细

不同交易所、市场类型、数据类型能力不同。

建议能力维度：

- exchange：Binance、OKX、Bybit 等。
- market：spot、perpetual、future、option。
- live kinds：trades、l1、l2、liquidation。
- historical kinds：candles、trades。
- granularities：1s/1m/5m/1h 等。
- rate limit。
- symbol format。
- max symbols per stream。
- whether Barter native support exists。

### 9. 测试策略需要提前定义

为了避免实现时必须连接真实交易所，建议：

- mapper 使用纯单元测试。
- subscription planner 使用 deterministic table tests。
- historical pagination 使用 fake HTTP client 或 fixture。
- live stream wrapper 使用 mock stream，不在单元测试中连真实 WebSocket。
- integration test 可单独 feature-gate，例如 `--features live-exchange-tests`。

## 对当前 fdc-barter 代码的调整建议

当前 `src/lib.rs` 是单文件骨架，已经不适合继续直接扩展。建议下一步不是接真实 WebSocket，而是先做 **结构性重构**：

```text
src/lib.rs
src/error.rs
src/config.rs
src/model/event.rs
src/model/request.rs
src/model/source.rs
src/model/checkpoint.rs
src/mapper/event.rs
src/mapper/instrument.rs
src/mapper/exchange.rs
src/ingestion/envelope.rs
src/capability/exchange.rs
```

第一轮重构目标：

- 保持现有 public trade mapper 测试通过。
- 把 `BarterMarketEvent` 从 `price/volume` 扩展为 `payload` 模型。
- 增加 `BarterIngestionEnvelope`。
- 增加 `BarterMarketDataRequest`。
- 增加 `BarterCheckpoint`。
- 增加 source status/capability 类型。
- 不接真实网络。

这一步完成后，再分别实现：

1. `live/subscription.rs`：根据 request 生成 Barter subscription batches。
2. `live/stream.rs`：参考 `DynamicStreams` examples 包装实时流。
3. `historical/request.rs` 和 `historical/checkpoint.rs`：历史分页与 checkpoint。
4. `historical/binance.rs`：第一个真实历史 REST MVP。

## 推荐下一步

建议下一步创建详细 implementation plan，目标为：

**“fdc-barter business module refactor without network IO”**

验收标准：

- 文件结构按业务模块拆分。
- 现有测试继续通过。
- 新增测试覆盖 request、payload、checkpoint、envelope、subscription plan 的基础行为。
- 不连接真实交易所。
- `cargo test -p fdc-barter` 通过。
- `cargo check --workspace --all-targets` 通过，既有 warnings 暂不处理。
