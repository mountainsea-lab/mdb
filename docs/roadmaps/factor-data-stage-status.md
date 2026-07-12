# 因子数据阶段状态台账

**日期：** 2026-07-12  
**用途：** 记录因子数据路线图各开发阶段的状态、交付物、验收结果和下一阶段交接输入，避免重复开发和跨模块衔接遗漏。

每个模块或阶段完成后，必须更新本文档。未更新本文档的阶段不得视为完成。

## 1. 状态定义

| 状态 | 含义 | 是否允许下游开发 |
|---|---|---|
| `planned` | 已规划，尚未开始实现 | 否 |
| `in_progress` | 正在实现或验证 | 否 |
| `blocked` | 遇到阻塞，需要处理后继续 | 否 |
| `validated` | 当前模块已通过验收，可作为下游输入 | 是 |
| `superseded` | 被新设计替代，不再继续 | 否 |

## 2. 完成标记规则

阶段标记为 `validated` 前，必须满足：

1. 代码或文档交付物已提交。
2. 验收命令已运行并记录结果。
3. example 或 smoke 输出能证明该阶段核心能力可观察。
4. 已记录下游消费 contract。
5. 已记录未完成事项和明确不支持项。
6. 工作区干净。

建议每个阶段完成提交信息使用以下格式之一：

```text
feat: validate <module> <capability>
docs: mark <stage> validated
```

## 3. 阶段状态表

| Stage | 模块 | 范围 | 状态 | 完成提交 | 验收证据 | 下游交接 |
|---|---|---|---|---|---|---|
| Stage 1 | `fdc-adapter/barter` | Binance Futures funding/OI/mark/index/OHLCV 采集验证 | `validated` | `f417ece`, `fa998a0`, `2b09b03`, `7f3e537` | `rtk cargo test -p fdc-barter` → `77 passed, 7 ignored`; example 默认真实网络已返回 Binance `/fapi` 实盘公开数据；fixture 模式需显式 `MDB_BARTER_EXAMPLE_MODE=fixture` | Adapter envelope contract，见完成记录和验证报告 |
| Stage 2 | `fdc-orchestrator` | Adapter envelope → storage write input | `validated` | `dd4e273`, `c6f1fb0` | `rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract -- --nocapture` → `9 passed`; `rtk cargo check -p fdc-orchestrator` → `0 errors` | Structured `MarketDataDto` + `StorageWriteRecord` tags/metadata |
| Stage 3 | `fdc-storage` | Candle generic write/query and derivatives storage mapping | `validated` | `b41d5d1`, `c6f1fb0` | `rtk cargo test -p fdc-server --test realtime_mvp_contract -- --nocapture` → `3 passed`; orchestrator storage mapping contract → `9 passed` | `MarketDataQuery::for_candles()`, collections `candles`, `funding_rates`, `open_interest`, `mark_prices`, `index_prices` |
| Stage 4 | `fdc-server` | Candle 受控查询接口和运行时验证 | `validated` | `b41d5d1` | `rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture` → `1 passed` | `GET /market-data/candles?symbol=<SYMBOL>&limit=<N>` |
| Stage 5 | Spot data | 现货数据补齐 | `planned` | - | 待合约链路 validated | Spot adapter/storage/query contract |
| Stage 6 | `fdc-analytics` | 因子计算 | `planned` | - | 待数据采集、存储、查询闭环 validated | Factor input datasets |

## 4. Stage 完成记录模板

完成一个阶段时，在对应阶段下新增记录。

```markdown
### Stage N：<名称> 完成记录

**状态：** `validated`  
**完成提交：** `<commit>`  
**完成日期：** `YYYY-MM-DD`  
**范围：** <本阶段实际完成范围>  

**验收命令：**

```bash
<command 1>
<command 2>
```

**关键输出摘要：**

```text
<example 或 smoke 日志摘要>
```

**下游消费 contract：**

```text
<字段、tags、payload 或 API contract>
```

**明确未完成 / 不支持：**

- <item 1>
- <item 2>

**下一阶段建议：** <进入哪个 Stage，消费哪些输出>
```

## 5. Stage 1 当前交接 contract 草案

### Stage 1：Binance Futures USD / Perpetual adapter validation 完成记录

**状态：** `validated`  
**完成提交：** `f417ece`, `fa998a0`, `2b09b03`, `7f3e537`  
**完成日期：** `2026-07-11`  
**范围：** 仅 `crates/fdc-adapter/barter`。完成 Binance Futures USD perpetual funding rate、open interest、mark/index price、OHLCV 的 adapter 数据模型、REST descriptor、fixture provider、REST executor helper、ignored real-network smoke 和默认真实网络 example 可见输出。

**验收命令：**

```bash
rtk cargo test -p fdc-barter derivatives_payloads_report_expected_kinds
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract
rtk cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
MDB_BARTER_EXAMPLE_MODE=fixture rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
rtk cargo test -p fdc-barter
```

**关键输出摘要：**

```text
cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
example=historical_binance_futures_usd_derivatives mode=real_network symbol=BTCUSDT
endpoint=/fapi/v1/fundingRate exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=FundingRate records=1 first_payload=Some(FundingRate(...)) complete=false
endpoint=/fapi/v1/openInterest exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=OpenInterest records=1 first_payload=Some(OpenInterest(...)) complete=true
endpoint=/fapi/v1/premiumIndex exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=MarkPrice records=1 first_payload=Some(MarkPrice(...)) complete=true
endpoint=/fapi/v1/klines exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=Candle records=1 first_payload=Some(Candle(...)) complete=false
example=historical_binance_futures_usd_derivatives complete=true

MDB_BARTER_EXAMPLE_MODE=fixture cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
example=historical_binance_futures_usd_derivatives mode=fixture symbol=BTCUSDT
endpoint=/fapi/v1/fundingRate exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=FundingRate records=1 first_event_time=1700000000000000000 first_payload=Some(FundingRate(...)) complete=false
endpoint=/fapi/v1/openInterest exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=OpenInterest records=1 first_payload=Some(OpenInterest(...)) complete=true
endpoint=/fapi/v1/premiumIndex exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=MarkPrice records=1 first_payload=Some(MarkPrice(...)) complete=true
endpoint=/fapi/v1/klines exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=Candle records=1 first_payload=Some(Candle(...)) complete=false
example=historical_binance_futures_usd_derivatives complete=true

rtk cargo test -p fdc-barter
cargo test: 77 passed, 7 ignored
```

**下游消费 contract：**

```text
source_id=barter-binance-futures-usd-history | example-binance-futures-usd-derivatives
exchange=binance_futures_usd
market_type=Perpetual
symbol=BTCUSDT
mode=Historical
kind=FundingRate | OpenInterest | MarkPrice | Candle
quality.is_backfill=true
timestamp=<exchange event time when available; adapter receive time for current snapshots>
received_at=<adapter receive time>
payload=FundingRatePayload | OpenInterestPayload | MarkPricePayload | CandlePayload
```

**明确未完成 / 不支持：**

- 不写入 `fdc-storage`，不提供查询 API。
- 不做因子计算。
- 不支持其他交易所。
- 不支持 Binance `/futures/data/openInterestHist` historical OI time series。
- `IndexPricePayload` 已建模，但 Binance `/fapi/v1/premiumIndex` 当前以 `MarkPricePayload.index_price` 字段交付。

**验证报告：** `crates/fdc-adapter/barter/docs/binance-futures-validation-report.md`

**下一阶段建议：** 进入 Stage 2，在 `fdc-orchestrator` 中消费上述 `BarterIngestionEnvelope` contract，映射为 storage write input，保持 adapter raw response schema 与 storage/query 解耦。

Stage 1 完成后，至少应交付以下 adapter envelope contract：

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

Stage 2 只能依赖该 adapter envelope contract，不得直接依赖 Binance 原始 response schema。


## 6. Stage 2-4：Candle downstream / derivatives DTO completion record

**状态：** `validated`  
**完成提交：** `dd4e273`, `b41d5d1`, `c6f1fb0`  
**完成日期：** `2026-07-12`  
**范围：** 完成 adapter envelope 下游接入、candle DTO/storage/query 闭环，以及 derivatives structured DTO/storage mapping。

**验收命令：**

```bash
rtk cargo test -p fdc-server --test realtime_mvp_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract -- --nocapture
rtk cargo test -p fdc-transform -- --nocapture
rtk cargo check -p fdc-orchestrator
```

**关键输出摘要：**

```text
fdc-server realtime_mvp_contract: 3 passed
fdc-server p39_market_data_candles_query_returns_candle_records: 1 passed, 66 filtered out
fdc-orchestrator orchestrator_boundary_contract: 9 passed
fdc-transform: 2 passed
fdc-orchestrator cargo check: 0 errors, 20 existing warnings
```

**下游消费 contract：**

```text
Candle:
collection=candles
tag kind=candle
tag data.kind=aggregate
tag record.kind=candle
query=MarketDataQuery::for_candles().with_symbol(...).with_limit(...)
HTTP=GET /market-data/candles?symbol=<SYMBOL>&limit=<N>

Derivatives:
MarketDataKind=FundingRate | OpenInterest | MarkPrice | IndexPrice
MarketDataPayload=FundingRateDto | OpenInterestDto | MarkPriceDto | IndexPriceDto
collections=funding_rates | open_interest | mark_prices | index_prices
tag data.kind=derivative
tag record.kind=funding_rate | open_interest | mark_price | index_price
storage durability=Persistent
storage access_pattern=Warm
```

**明确未完成 / 不支持：**

- 本次状态更新未重新运行真实网络 `historical_binance_spot_ohlcv` smoke。
- Derivatives 已有 structured DTO/storage mapping，但尚未提供独立 HTTP 查询 API。
- 多 symbol / 多 interval candle 回补调度、checkpoint 连续性和 production runbook 仍需后续切片。

**下一阶段建议：** 进入 Stage 5 / Stage 6 前，优先补 derivatives 查询 API 或 candle 回补 runbook，并把真实网络 smoke 纳入最终验收。
