# Stage 1：Binance Futures 数据采集验证设计规格

**日期：** 2026-07-11  
**分支：** `mdb-mqdev`  
**关联文档：**

- [`factor-data-collection-roadmap.md`](./factor-data-collection-roadmap.md)
- [`factor-data-development-plan.md`](./factor-data-development-plan.md)
- [`crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`](../../crates/fdc-adapter/barter/docs/market-data-collection-requirements.md)

本文档定义下一步开发的第一个模块级任务：只在 `crates/fdc-adapter/barter` 内完成 Binance Futures USD / Perpetual 公共数据采集验证。该阶段不接 storage、不开放 server query API、不做 analytics 因子计算。

## 1. 目标

Stage 1 的目标是证明 `fdc-barter` 可以稳定采集合约基础数据，并产出后续 storage/query 模块可以消费的标准 adapter envelope。

优先数据类型：

1. Funding rate。
2. Open interest。
3. Mark price / index price。
4. Futures / Perpetual OHLCV Kline。

完成后应得到明确输出：

- Binance Futures REST descriptor 合同。
- Binance Futures response parser / provider 合同。
- Offline fixture tests。
- Explicit opt-in real-network smoke。
- Adapter 输出 payload 与字段语义说明。
- 下一模块需要消费的 tags/metadata 建议。

## 2. 范围

### 2.1 本阶段包含

仅修改或新增 `crates/fdc-adapter/barter` 内部内容：

- `src/ingestion/historical.rs` 或拆分后的 Binance Futures REST provider 文件。
- `src/model/event.rs` 中必要的 adapter payload 扩展。
- `src/lib.rs` 导出新增 public adapter API。
- `tests/*binance_futures*_contract.rs` offline contract tests。
- `examples/*binance_futures*_historical*.rs` 可运行示例，日志必须直观输出 endpoint、symbol、records、关键字段样例。
- `crates/fdc-adapter/barter/docs/*` 下的验证报告或 capability 更新。

### 2.2 本阶段不包含

- 不修改 `fdc-storage`。
- 不修改 `fdc-server` 查询 API。
- 不修改 `fdc-analytics`。
- 不实现因子计算。
- 不实现其他交易所。
- 不实现 L2 reconstruction。
- 不将 adapter 输出直接写入生产 storage。

## 3. 当前基础

`fdc-barter` 已具备以下基础：

- `BarterMarketDataKind` 支持 `Trade`、`OrderBookL1`、`OrderBook`、`Candle`、`Liquidation`。
- `BarterMarketType` 支持 `Spot`、`Future`、`Perpetual`、`Option`。
- `HistoricalBackfillRequest`、`HistoricalBackfillPage`、`HistoricalProviderCapabilities`、`HistoricalRestRequestDescriptor` 已存在。
- Binance Spot OHLCV 与 historical trades 已有 REST descriptor、executor、provider 和 fixture contract。
- Binance Futures USD live market data 已有 live 示例和合同测试。

因此 Stage 1 应复用现有 Binance Spot historical REST 模式，而不是引入新的采集框架。

## 4. 设计原则

1. **模块先行：** 先让 `fdc-barter` 独立完成采集验证，再进入 orchestrator/storage 衔接。
2. **基于 barter-rs 边界：** `fdc-barter` 仍是基于 barter-rs 的 adapter 模块。Barter-rs 已覆盖的 live connector 能力继续复用；Barter-rs 不直接覆盖的公共 REST 数据由 adapter-owned REST descriptor/provider 补齐。
3. **离线优先：** 默认测试不访问公网，全部通过 fixture contract 验证。
4. **真实验证显式启用：** real-network smoke 必须 `#[ignore]` 或环境变量 opt-in。
5. **输出稳定：** 本阶段的最终产物是 adapter envelope，不是 storage record。
6. **可扩展：** Binance Futures 的 provider 命名和 capability 表达要为 Bybit、OKX 等后续交易所保留空间。

## 5. 数据模型设计

### 5.1 新增 market data kind

当前 `BarterMarketDataKind` 没有 derivatives metrics 类型。建议新增：

- `FundingRate`
- `OpenInterest`
- `MarkPrice`
- `IndexPrice`

保留 `Candle` 用于 Binance Futures OHLCV。

### 5.2 新增 payload

建议新增以下 adapter payload：

```rust
pub struct FundingRatePayload {
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub mark_price: Option<Price>,
}

pub struct OpenInterestPayload {
    pub open_interest: Decimal,
    pub timestamp: TimestampNs,
}

pub struct MarkPricePayload {
    pub mark_price: Price,
    pub index_price: Option<Price>,
    pub estimated_settle_price: Option<Price>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_time: Option<TimestampNs>,
}

pub struct IndexPricePayload {
    pub index_price: Price,
    pub timestamp: TimestampNs,
}
```

并扩展：

```rust
pub enum BarterMarketPayload {
    Trade(TradePayload),
    OrderBookL1(OrderBookL1Payload),
    OrderBook(OrderBookPayload),
    Candle(CandlePayload),
    Liquidation(LiquidationPayload),
    FundingRate(FundingRatePayload),
    OpenInterest(OpenInterestPayload),
    MarkPrice(MarkPricePayload),
    IndexPrice(IndexPricePayload),
    Raw(RawPayload),
}
```

说明：

- `FundingRatePayload.mark_price` 为可选字段，因为不同 endpoint 返回结构不同。
- `MarkPricePayload` 可以承载 Binance premium index / mark price endpoint 返回的组合字段。
- `IndexPricePayload` 保留独立 payload，是为了后续支持只返回 index price 的交易所 endpoint。
- 所有数值解析使用 `Decimal` 或已有 `Price`，避免直接用 `f64` 作为采集层输出。

## 6. Binance Futures REST endpoint 设计

### 6.1 Exchange 标识

本阶段统一使用：

```text
exchange = "binance_futures_usd"
market_type = BarterMarketType::Perpetual
```

### 6.2 Endpoint 映射

| 数据 | Binance Futures public endpoint | Adapter kind | 备注 |
|---|---|---|---|
| Funding rate history | `/fapi/v1/fundingRate` | `FundingRate` | 支持 symbol、startTime、endTime、limit |
| Open interest current | `/fapi/v1/openInterest` | `OpenInterest` | current snapshot；historical OI 后续单独扩展 |
| Mark/index price | `/fapi/v1/premiumIndex` | `MarkPrice` 或 `IndexPrice` | 单 symbol 返回 markPrice、indexPrice、lastFundingRate、nextFundingTime |
| Kline/OHLCV | `/fapi/v1/klines` | `Candle` | 与 spot kline schema 类似，路径和 exchange 不同 |

### 6.3 本阶段不做的 endpoint

- `/futures/data/openInterestHist` historical OI 暂不纳入第一版，避免和 Binance data endpoint 权限、周期、limit 规则混在一起。
- `/fapi/v1/ticker/*` 暂不纳入。
- WebSocket futures metrics 暂不纳入。

## 7. Provider/API 设计

建议新增 public functions：

```rust
pub fn binance_futures_usd_funding_rate_capabilities() -> HistoricalProviderCapabilities;
pub fn binance_futures_usd_open_interest_capabilities() -> HistoricalProviderCapabilities;
pub fn binance_futures_usd_mark_price_capabilities() -> HistoricalProviderCapabilities;
pub fn binance_futures_usd_ohlcv_capabilities() -> HistoricalProviderCapabilities;

pub fn binance_futures_usd_funding_rate_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor>;

pub fn binance_futures_usd_open_interest_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor>;

pub fn binance_futures_usd_mark_price_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor>;

pub fn binance_futures_usd_ohlcv_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor>;
```

建议新增 fixture-provider constructors：

```rust
pub fn binance_futures_usd_funding_rate_provider_from_response(
    response: impl Into<String>,
) -> Result<impl HistoricalExchangeProvider>;

pub fn binance_futures_usd_open_interest_provider_from_response(
    response: impl Into<String>,
) -> Result<impl HistoricalExchangeProvider>;

pub fn binance_futures_usd_mark_price_provider_from_response(
    response: impl Into<String>,
) -> Result<impl HistoricalExchangeProvider>;

pub fn binance_futures_usd_ohlcv_provider_from_response(
    response: impl Into<String>,
) -> Result<impl HistoricalExchangeProvider>;
```

如果 implementation 阶段发现 `impl Trait` 对 trait object/export 不便，可以按现有 Binance Spot provider 模式返回具体 provider 类型或 boxed provider。设计目标是保持外部可测试，而不是强制具体签名。

## 8. Request validation

### 8.1 Funding rate

要求：

- `exchange == "binance_futures_usd"`
- `market_type == Perpetual`
- `kind == FundingRate`
- `symbol` 非空
- `limit <= 1000`
- `start < end`

Descriptor query：

```text
symbol=BTCUSDT
startTime=<millis>
endTime=<millis>
limit=<limit or 1000>
```

### 8.2 Open interest

要求：

- `exchange == "binance_futures_usd"`
- `market_type == Perpetual`
- `kind == OpenInterest`
- `symbol` 非空

Descriptor query：

```text
symbol=BTCUSDT
```

说明：current OI endpoint 没有 start/end/limit 分页语义。provider 应返回一条 envelope，并标记 `complete=true`。

### 8.3 Mark/index price

要求：

- `exchange == "binance_futures_usd"`
- `market_type == Perpetual`
- `kind == MarkPrice` 或 `IndexPrice`
- `symbol` 非空

Descriptor query：

```text
symbol=BTCUSDT
```

说明：第一版推荐用 `MarkPrice` payload 承载 mark 与 index 组合字段。如果调用者请求 `IndexPrice`，provider 可以输出 `IndexPricePayload`，但不要求第一版同时开放两个 descriptor。实施计划中应选择一个最小稳定接口，优先 `MarkPrice`。

### 8.4 Futures OHLCV

要求：

- `exchange == "binance_futures_usd"`
- `market_type == Perpetual`
- `kind == Candle`
- `interval` 属于已支持集合
- `limit <= 1500`，第一版可以收敛为 `limit <= 1000` 以复用 spot 行为
- `start < end`

Descriptor path：

```text
/fapi/v1/klines
```

## 9. Envelope 输出语义

所有 provider 输出 `BarterIngestionEnvelope`：

- `source_id` 使用 request 中的 `source_id`。
- `event.source` 使用明确来源，例如 `barter-binance-futures-usd-history`。
- `event.exchange = "binance_futures_usd"`。
- `event.market_type = BarterMarketType::Perpetual`。
- `event.mode = BarterMarketDataMode::Historical`。
- `event.symbol` 使用请求 symbol。
- `event.timestamp` 使用交易所语义最强的时间：
  - funding：`fundingTime`
  - open interest：response time 或 adapter receive time；如果 endpoint 无 timestamp，则使用 `received_at` 并在 validation report 中说明。
  - mark price：response `time`，若缺失则使用 `received_at`。
  - kline：open time。
- `event.received_at` 使用 adapter receive timestamp。
- `event.checkpoint` 对可分页 endpoint 使用最后一条 event timestamp 计算下一页。
- `quality.is_backfill = true`。

## 10. 测试与验证

### 10.1 Offline descriptor tests

新增测试文件建议：

- `tests/binance_futures_historical_rest_contract.rs`

覆盖：

- funding descriptor path/query。
- open interest descriptor path/query。
- mark price descriptor path/query。
- futures kline descriptor path/query。
- unsupported exchange/market/kind/limit 被拒绝。

### 10.2 Offline provider fixture tests

新增测试文件建议：

- `tests/binance_futures_derivatives_provider_contract.rs`

覆盖：

- funding response → `FundingRatePayload`。
- open interest response → `OpenInterestPayload`。
- premium index response → `MarkPricePayload`。
- futures kline response → `CandlePayload`。
- invalid numeric payload 返回错误。
- current snapshot endpoint 返回 `complete=true`。
- paginated endpoint 在返回数量小于 limit 时 `complete=true`。

### 10.3 Runnable examples with visible logs

Stage 1 的验收输出必须包含可运行 example，而不只是测试。example 的目的不是替代 contract tests，而是让开发者和使用者能直观看到 Binance Futures 数据已经被采集、解析并转换为 adapter envelope。

建议新增 example：

- `examples/historical_binance_futures_usd_derivatives.rs`

example 至少支持：

- funding rate。
- open interest。
- mark/index price。
- futures kline。

默认行为：

- 默认不访问公网。
- 未设置真实网络环境变量时，example 使用内置 fixture 或清晰提示如何启用真实网络。
- 设置 `MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1` 后访问 Binance Futures public endpoint。

日志必须包含以下直观字段：

```text
endpoint=/fapi/v1/fundingRate
exchange=binance_futures_usd
market_type=perpetual
symbol=BTCUSDT
kind=funding_rate
records=1
first_event_time=...
first_payload={ funding_rate: ..., funding_time: ..., mark_price: ... }
complete=true
```

对不同数据类型，日志中的 `first_payload` 应展示关键业务字段：

- funding：`funding_rate`、`funding_time`、可选 `mark_price`。
- open interest：`open_interest`、`timestamp`。
- mark price：`mark_price`、`index_price`、`funding_rate`、`next_funding_time`。
- kline：`open_time`、`close_time`、`open`、`high`、`low`、`close`、`volume`。

推荐运行命令：

```bash
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives

MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1 \
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
```

### 10.4 Real-network smoke

新增 ignored smoke：

- `ignored_live_smoke_fetches_binance_futures_funding_rate`
- `ignored_live_smoke_fetches_binance_futures_open_interest`
- `ignored_live_smoke_fetches_binance_futures_mark_price`
- `ignored_live_smoke_fetches_binance_futures_ohlcv`

启用方式建议：

```bash
MDB_BARTER_ENABLE_REAL_NETWORK_TESTS=1 \
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract -- --ignored
```

默认 CI 不运行这些测试。

## 11. 验收门禁

只有满足以下条件后，才进入 Stage 2 orchestrator/storage mapping：

1. `rtk cargo test -p fdc-barter` 默认离线测试通过。
2. Binance Futures descriptor tests 覆盖所有本阶段 endpoint。
3. Binance Futures provider fixture tests 覆盖所有本阶段 payload。
4. `rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives` 可运行，并在默认 fixture 模式下输出可读日志，能直观看到 endpoint、symbol、kind、records 和首条 payload 关键字段。
5. 设置 `MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1` 后，example 可以访问 Binance Futures public endpoint，并输出真实数据摘要。
6. 至少一次手动 real-network smoke 成功，并在 validation report 中记录命令、时间、symbol、endpoint 和结果摘要。
7. `fdc-barter` 文档更新，明确：
   - 已验证 endpoint。
   - 字段语义。
   - example 运行方式和日志样例。
   - 不支持项。
   - 下一阶段 storage tags 建议。
8. 没有修改 `fdc-storage`、`fdc-server`、`fdc-analytics`。

## 12. 下一阶段衔接输出

Stage 1 完成后，交付给 Stage 2 的消费 contract 为：

```text
exchange=binance_futures_usd
market_type=perpetual
symbol=BTCUSDT
kind=funding_rate | open_interest | mark_price | index_price | candle
mode=historical
source_id=barter-binance-futures-usd-history
event_time=<exchange event time>
received_at=<adapter receive time>
checkpoint=<optional historical checkpoint>
```

Stage 2 只消费 adapter envelope，不直接调用 Binance endpoint，也不依赖 Binance response schema。

## 13. 推荐实施切片

为了保持每个环节都有明确输出，Stage 1 建议拆成四个小提交：

1. **Data model commit：** 新增 derivatives market data kinds 与 payload，补充 unit/mapper tests。
2. **Descriptor commit：** 新增 Binance Futures REST descriptor 和 validation tests。
3. **Provider commit：** 新增 fixture parser/provider tests 与实现。
4. **Smoke/docs/examples commit：** 新增可运行 example、ignored real-network smoke、更新 validation report 和 capability 文档。

每个提交都必须能独立通过 `rtk cargo test -p fdc-barter`。
