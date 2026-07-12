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
| Stage 5 | Candle acquisition maintenance | 配置化 candle 采集、维护、checkpoint、runbook | `validated` | `4a3a689`, `b6eacdd`, `f09a2cc`, `28f4d0d` | Stage 2-4 validated; candle storage metadata hardening validated | Config-driven candle backfill + maintenance contract |
| Stage 6 | `fdc-analytics` | 因子计算 | `blocked` | - | 等待 Stage 5.x 历史数据维护 gate 完成或明确 re-scope | Factor input datasets |

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


## 7. Stage 5 planned gate：Candle acquisition maintenance

**状态：** `planned`  
**规划日期：** `2026-07-12`  
**原因：** 当前 candle 下游 DTO/storage/query 闭环已 validated，但启动服务不会自动按币种和周期采集 candle。进入下一阶段因子或更广 spot 开发前，需要先把 candle 采集维护产品化，避免依赖 ad-hoc examples。

**设计决策：**

- 前期通过配置文件指定需要采集的币种和周期。
- 不建议默认采集所有周期。优先采集关键基础周期，推荐 `1m` 作为 canonical base interval。
- 其他周期优先由基础周期聚合生成，例如 `5m`、`15m`、`30m`、`1h`、`4h`、`1d`。
- 可选采集交易所官方 `1h` / `1d` 等低频周期用于校验，不作为主数据源。

**第一版建议配置 contract：**

```toml
[market_data.candles]
enabled = true
autostart = false
exchange = "binance_spot"
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
base_intervals = ["1m"]
verify_intervals = ["1h", "1d"]
start = "2024-01-01T00:00:00Z"
end = null
limit_per_page = 1000
max_pages_per_run = 10
```

**Stage 5 必须交付：**

1. 配置文件解析和校验，覆盖 symbols、base intervals、verify intervals、时间范围、page/record limit。
2. 手动触发或 autostart-gated candle backfill runner。
3. 写入现有 `candles` collection，并可通过 `GET /market-data/candles` 查询。
4. checkpoint / cursor / retry / partial completion 状态可观察。
5. runbook 记录如何启动、验证、暂停和恢复。
6. 验收必须包含 fixture/offline contract；真实网络 smoke 可 opt-in，但需要记录命令和输出摘要。

**进入后续阶段前置条件：** Stage 5 标记为 `validated`，并在本文档记录提交、验收命令、输出摘要和明确未完成项。


## 8. Stage 5 completion record：Candle acquisition maintenance

**状态：** `validated`  
**完成日期：** `2026-07-12`  
**提交：** `4a3a689 feat: add candle acquisition runtime config`，`b6eacdd feat: add candle acquisition runner`  

**完成内容：**

- 新增 candle acquisition runtime config，支持 env 指定 enabled/autostart/exchange/symbols/base intervals/verify intervals/start/end/page limits/max pages。
- 新增 `market_data::candle_acquisition`，将配置扩展为 `HistoricalBackfillRequest`。
- 新增 bounded runner：`fdc-barter` historical pages -> `fdc-orchestrator::run_barter_envelopes_to_storage_once` -> existing `candles` collection。
- `ProductionServerState` 增加 source-injected manual runner 和 startup autostart gate。
- `fdc_server` binary 启动时在 live autostart 后检查 candle acquisition autostart。
- Runbook 记录安全默认值、示例 env、查询验证命令和当前 scope。

**验收命令摘要：**

```bash
rtk cargo test -p fdc-server --test runtime_config_contract candle_acquisition_config -- --nocapture
# 4 passed

rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture
# 4 passed

rtk cargo test -p fdc-server --test realtime_mvp_contract -- --nocapture
# 3 passed

rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture
# 1 passed

rtk cargo check -p fdc-server
# 0 errors, existing warnings only
```

**明确未完成项：**

- Durable checkpoint persistence/status API 尚未实现；当前 runner 返回 per-run `final_cursors`，可作为下一步持久化输入。
- `verify_intervals` 当前仅配置解析/策略记录，未自动执行官方低频校验。
- 高周期 candle 聚合尚未实现。
- 真实网络 Binance smoke 未作为默认 CI 验收运行，仍为 operator opt-in。


## 9. Stage 5.x gate：Historical market data maintenance completion before next stage

**状态：** `candle maintenance validated / Stage 6 decision ready`  
**规划日期：** `2026-07-12`  
**原因：** Stage 5 MVP 只覆盖 Binance Spot historical candle maintenance。按当前路线，先完成 candle 数据采集维护业务，再评估是否进入 analytics 或下一阶段。非 candle historical acquisition 暂缓，不作为本轮 candle gate 的必需项。

**Stage 5.x 任务安排：**

| Slice | Scope | Status | Required before Stage 6? | Plan anchor |
| --- | --- | --- | --- | --- |
| Stage 5.1 | Durable candle checkpoint persistence and resume | `validated` | yes | `d8ef071 feat: persist candle acquisition checkpoints`; storage-backed hardening `f09a2cc feat: persist candle checkpoints in market storage` |
| Stage 5.2 | Candle acquisition status API | `validated` | yes | `83b3102 feat: expose candle acquisition status` |
| Stage 5.3 | `verify_intervals` official candle cross-check | `validated` | yes | `89cdb6c feat: verify official candle intervals`; audit persistence `28f4d0d feat: persist candle verify audits` |
| Stage 5.4 | Higher-interval candle aggregation from base intervals | `validated` | yes | `93ee52b feat: aggregate base candles for verification` |
| Stage 5.5 | General historical acquisition for trades and derivative market-data kinds | `deferred / out of current candle gate` | no | Re-scope: do not go broad until candle acquisition/maintenance is accepted and next-stage scope is chosen. |

**Validation commands:**

```bash
rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract market_data_candle_acquisition_status_reports_ -- --nocapture
rtk cargo check -p fdc-server
```

**Storage metadata hardening:** Candle maintenance now stores base interval resume cursors in `market_data/candle_checkpoints` and verify run summaries in `market_data/candle_verify_audits`. Canonical candle payloads remain in `candles`; official verify candles remain reference-only and are not persisted as production candles.

**Next-stage gate:** The candle acquisition maintenance gate is complete for the current scope. Stage 6 or any broader historical acquisition work should start only after an explicit follow-up decision. Trades, derivatives, and generalized historical acquisition remain intentionally deferred.
