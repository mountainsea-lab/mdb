# 因子数据阶段状态台账

**日期：** 2026-07-11  
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
| Stage 1 | `fdc-adapter/barter` | Binance Futures funding/OI/mark/index/OHLCV 采集验证 | `validated` | `f417ece`, `fa998a0`, `2b09b03`, `7f3e537` | `rtk cargo test -p fdc-barter` → `77 passed, 7 ignored`; example 默认 fixture 输出 4 个 futures endpoint 数据 | Adapter envelope contract，见完成记录和验证报告 |
| Stage 2 | `fdc-orchestrator` | Adapter envelope → storage write input | `planned` | - | 待 Stage 1 validated | StorageWriteRecord tags/metadata |
| Stage 3 | `fdc-storage` | Derivatives records generic write/query | `planned` | - | 待 Stage 2 validated | Queryable storage contract |
| Stage 4 | `fdc-server` | 受控查询接口和运行时验证 | `planned` | - | 待 Stage 3 validated | HTTP query API / runbook |
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
**范围：** 仅 `crates/fdc-adapter/barter`。完成 Binance Futures USD perpetual funding rate、open interest、mark/index price、OHLCV 的 adapter 数据模型、REST descriptor、fixture provider、REST executor helper、ignored real-network smoke 和默认 fixture example 可见输出。

**验收命令：**

```bash
rtk cargo test -p fdc-barter derivatives_payloads_report_expected_kinds
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract
rtk cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
rtk cargo test -p fdc-barter
```

**关键输出摘要：**

```text
cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
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
