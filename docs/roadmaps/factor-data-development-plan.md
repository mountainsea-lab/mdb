# 因子数据开发计划

**日期：** 2026-07-11  
**分支：** `mdb-mqdev`  
**关联路线图：** [`docs/roadmaps/factor-data-collection-roadmap.md`](./factor-data-collection-roadmap.md)

本文档将因子数据采集路线图拆成可审查、可实施的小切片。目标是避免一次性建设完整因子平台，而是每次只打通一个数据闭环：采集、映射、存储、查询或导出、基础因子验证。

## 1. 背景与目标

当前项目已经具备：

- Binance Spot live trades、historical trades 和 historical OHLCV 基础能力。
- Binance Spot/Futures live L1/L2/liquidation 结构化映射基础。
- `fdc-storage` generic write/query boundary、tiered storage、market-data integration baseline。
- `fdc-server` 生产 runtime、live 控制、storage 状态/健康/maintenance 接口，以及已加固的 `/market-data/trades` 查询。
- `fdc-analytics` 的基础 SMA、RSI 和简化 ML stub。

下一阶段目标：

1. 优先形成可用于 offline factor research 的数据闭环。
2. 固定模块职责边界，避免业务类型污染 `fdc-storage`。
3. 每个阶段都能用 contract tests 或 bounded examples 验证。
4. 先扩大 Binance 主线的数据深度，再扩展交易所广度。

非目标：

- 不在一个阶段内同时实现所有数据类型。
- 不立即建设完整通用 market-data API。
- 不让 `fdc-storage` 依赖 `fdc-adapter/barter`、`fdc-server` 或 `fdc-analytics`。
- 不把 L2 reconstruction 与基础 OHLCV/trade/L1 工作混在同一阶段。

## 2. 模块职责边界

| 模块 | 当前定位 | 后续职责 | 明确不做 |
|---|---|---|---|
| `crates/fdc-adapter/barter` | Barter-rs 接入边界 | 交易所 REST/WebSocket 接入、adapter event、source capability、bounded acquisition helpers | 不写入 storage，不暴露 HTTP API，不做通用因子计算 |
| `crates/fdc-orchestrator` | 跨模块 glue 边界 | adapter event → neutral DTO/storage record 的映射，统一 tags/metadata | 不嵌入 Barter-rs connector 细节 |
| `crates/fdc-storage` | generic storage 边界 | namespace/collection/tag/query、tiered persistence、maintenance、health | 不依赖业务 crate，不理解 Candle/Trade/L1 的 Rust 业务类型 |
| `crates/fdc-server` | 生产 runtime 与 HTTP control | runtime config、受控 API、生产 smoke、operator-visible status | 不实现交易所 connector，不承担 reconstruction 细节 |
| `crates/fdc-analytics` | 因子/指标计算 | 输入模型、离线因子计算、fixture-based tests | 不直接接交易所，不直接操作 server state |
| `crates/fdc-query` | 查询层候选模块 | 后续统一 market-data query abstraction | 短期不作为阻塞项 |
| `crates/fdc-ingestion` | generic ingestion 候选路径 | 后续承接 adapter bounded helpers 到通用 source pipeline | 初期不强制改造所有采集流程 |

## 3. 总体优先级

推荐采用 **纵向小切片 + 模块边界固定** 的推进方式。

| 优先级 | 阶段 | 核心数据 | 主要收益 | 复杂度 |
|---|---|---|---|---|
| P0 | Phase 1 | OHLCV/Candle | 最快支持基础离线因子和回测 | 中低 |
| P0 | Phase 2 | Trade 增强 | 复用当前最成熟 trade 主线，支持 flow 因子 | 中 |
| P0 | Phase 3 | L1 盘口 | 支持 spread、mid、microprice、top imbalance | 中 |
| P1 | Phase 4 | Funding/OI/Mark/Index | 支持 derivatives carry、basis、leverage 因子 | 中 |
| P1 | Phase 5 | Liquidation | 支持 stress/forced-flow 因子 | 中 |
| P2 | Phase 6 | L2 reconstruction | 支持深度微观结构因子 | 高 |
| P3 | Phase 7 | Multi-exchange | 支持 cross-exchange 因子 | 高 |

建议先批准 Phase 1 到 Phase 3，形成基础因子数据闭环；Phase 4 之后根据前面结果再细化。

## 4. Phase 1：OHLCV/Candle 历史数据闭环

### 4.1 目标

打通标准化 Candle/OHLCV 的最小可用闭环：adapter historical backfill → storage record mapping → generic query 验证 → analytics 基础因子。

### 4.2 模块拆分

#### `fdc-adapter/barter`

范围：

- 固化 `HistoricalBackfillRequest` 对 Candle/OHLCV 的字段约束。
- 支持多 symbol、多 interval 的 Binance Spot OHLCV backfill helper。
- 保留 Binance Futures/Perpetual OHLCV 设计入口，但不在第一切片实现。
- 增加 interval、limit、cursor/checkpoint 的离线 contract tests。

验收：

- `binance_spot` Candle request descriptor 可稳定生成。
- interval 不合法时有确定错误。
- limit 超出来源上限时有确定错误。
- backfill outcome 包含 records_received、next_cursor、complete。

#### `fdc-orchestrator`

范围：

- 定义 Candle event 到 `StorageWriteRecord` 的 mapping。
- 统一 tags：
  - `kind=candle`
  - `symbol`
  - `exchange`
  - `market_type`
  - `interval`
- 保留 `event_time`、`close_time`、source mode、checkpoint metadata。

验收：

- fixture candle page 可以转换为 storage batch。
- 不引入 `fdc-storage` 对 Barter 类型的依赖。

#### `fdc-storage`

范围：

- 使用现有 generic query 验证 `collection=candles`。
- 验证 symbol、interval、kind tags 的过滤行为。
- 同时覆盖 in-memory 和 memory-tiered 或 tiered backend。

验收：

- Candle records 可写入、查询、按 limit 返回。
- storage dependency guard 继续通过。

#### `fdc-server`

范围：

- 初期不强制暴露完整 public HTTP API。
- 可先增加 router/server contract，证明 runtime 能持有并查询 candle records。
- 若必须暴露 API，只做最小只读接口：`GET /market-data/candles?symbol=&interval=&limit=`。

验收：

- 生产 runtime 下 candle storage/query contract 通过。
- HTTP API 如存在，需要有参数上限和错误 envelope。

#### `fdc-analytics`

范围：

- 增加 Candle 输入模型或 fixture adapter。
- 基于 close price 计算：
  - return
  - SMA
  - RSI
  - realized volatility 初版

验收：

- 使用 fixture candles 可计算基础因子。
- 不依赖 server、adapter 或 storage runtime。

### 4.3 不做事项

- 不做多交易所 OHLCV。
- 不做完整通用 market-data query API。
- 不做 live candle stream。
- 不做高阶因子库。

## 5. Phase 2：Trade 数据增强与 live/historical parity

### 5.1 目标

将当前最成熟的 trade 主线稳定为因子输入，重点补齐 live/historical schema parity、dedupe 和 flow 因子字段。

### 5.2 模块拆分

#### `fdc-adapter/barter`

范围：

- 对齐 live trade 与 historical trade payload。
- 固化 aggressor side / taker side 语义。
- 定义稳定 dedupe key：优先 exchange + market_type + symbol + trade_id，缺失 trade_id 时使用 event_time + price + quantity + side fallback。
- 增强 historical trade backfill outcome metadata。

验收：

- live/historical trade fixture 输出同构 payload。
- 同一 exchange trade id 生成相同 dedupe key。
- 缺失 side 时有明确质量标记或 `None` 语义。

#### `fdc-orchestrator`

范围：

- Trade event → `StorageWriteRecord` mapping 规范化。
- Tags：
  - `kind=trade`
  - `symbol`
  - `exchange`
  - `market_type`
  - `side`，如果存在
- Metadata：dedupe key、source mode、duplicate candidate、event_time、received_at。

验收：

- duplicate candidate 不破坏 storage write。
- backfill/live tags 一致。

#### `fdc-storage`

范围：

- 保持 generic storage，不新增业务类型依赖。
- 针对 enhanced trade tags 增加 query contract。

验收：

- `MarketDataQuery::for_trades()` 现有行为不回退。
- 新增 side/source mode tags 不破坏现有 `/market-data/trades`。

#### `fdc-server`

范围：

- 保持 `/market-data/trades` 为主要生产 API。
- 增强响应 metadata 时必须保持向后兼容。
- 如支持 side/source mode filter，先加 contract 再开放。

验收：

- P38 query hardening 相关测试继续通过。
- 非法 limit 行为不变。

#### `fdc-analytics`

范围：

- 增加 trade fixture 输入。
- 实现第一批 flow 因子：
  - signed volume
  - buy/sell imbalance
  - trade intensity
  - large trade impact 初版

验收：

- 离线 fixture tests 可复现预期因子值。

## 6. Phase 3：L1 盘口持久化与查询

### 6.1 目标

将 L1 live mapping 变成可持久化、可查询、可用于微观结构因子的稳定数据集。

### 6.2 模块拆分

#### `fdc-adapter/barter`

范围：

- 固化 L1 payload 中 bid/ask 为空时的语义。
- 增加 L1 数据质量 flags：missing_bid、missing_ask、crossed_book、zero_quantity。
- 保持 Binance Spot/Futures USD 作为首批来源。

验收：

- L1 mapper contract 覆盖完整 bid/ask、缺 bid、缺 ask、异常数量。

#### `fdc-orchestrator`

范围：

- L1 event → storage record。
- Tags：
  - `kind=order_book_l1`
  - `symbol`
  - `exchange`
  - `market_type`
- Payload 或 metadata 中保留 bid/ask、spread、mid_price。

验收：

- spread/mid 计算规则确定。
- crossed book 有质量标记。

#### `fdc-storage`

范围：

- 验证 `collection=quotes_l1` 或 `collection=order_book_l1`。
- 支持 symbol/kind/limit 查询。

验收：

- L1 records 可写入 tiered store 并 reopen 后查询。

#### `fdc-server`

范围：

- 可选 API：`GET /market-data/quotes/l1?symbol=&limit=`。
- 初期也可只保留 contract，不立即公开。

验收：

- 参数验证规则与 `/market-data/trades` 保持一致。

#### `fdc-analytics`

范围：

- spread factor。
- mid-price return。
- top-of-book imbalance。
- microprice 初版。

验收：

- fixture L1 序列可生成确定因子结果。

## 7. Phase 4：衍生品指标，Funding / OI / Mark / Index

### 7.1 目标

在 L2 reconstruction 前先补齐高价值、复杂度适中的 derivatives 数据。

### 7.2 模块拆分

#### `fdc-adapter/barter`

范围：

- Binance Futures USD funding rate REST。
- Binance Futures USD open interest REST。
- Mark price / index price REST。
- 明确 current 与 historical endpoint 差异。

验收：

- descriptor generation 离线 contract。
- response parser fixture contract。
- real-network smoke ignored by default。

#### `fdc-orchestrator`

范围：

- Derivatives metric event → storage record。
- Tags：
  - `kind=funding_rate`
  - `kind=open_interest`
  - `kind=mark_price`
  - `kind=index_price`
  - `symbol`
  - `exchange`
  - `market_type=perpetual`

验收：

- 不同 kind 可走同一 generic storage path。

#### `fdc-storage`

范围：

- 验证 derivatives metrics 按 kind/symbol 查询。
- 暂不引入专用 derivatives storage engine。

验收：

- funding/OI/mark/index fixtures 可写入和查询。

#### `fdc-server`

范围：

- 初期可不公开 API。
- 若公开，建议先做只读：`GET /market-data/derivatives/metrics?kind=&symbol=&limit=`。

验收：

- kind allowlist 和 limit 校验明确。

#### `fdc-analytics`

范围：

- funding carry。
- funding mean reversion。
- basis 初版。
- leverage build-up 初版。

验收：

- 使用 OHLCV + funding/OI/mark fixtures 可计算确定结果。

## 8. Phase 5：Liquidation 持久化与聚合

### 8.1 目标

将已有 live liquidation mapping 转化为可持久化、可聚合、可用于压力因子的事件序列。

### 8.2 模块拆分

| 模块 | 范围 | 验收 |
|---|---|---|
| `fdc-adapter/barter` | 补齐 liquidation notional、event_time、side 语义 | mapper contract 覆盖 buy/sell、quantity、price、time |
| `fdc-orchestrator` | liquidation event → storage record，tags 包含 `kind=liquidation` | fixture 可写入 storage batch |
| `fdc-storage` | 按 symbol/kind/limit 查询 liquidation records | tiered backend contract 通过 |
| `fdc-server` | 可选只读查询或 status，不强制第一步公开 | 不影响 live control API |
| `fdc-analytics` | forced-flow pressure、liquidation spike、window aggregation | fixture window 聚合结果确定 |

## 9. Phase 6：L2 reconstruction 专项

### 9.1 目标

单独处理 L2 的高复杂度正确性问题，避免影响前面基础因子数据闭环。

### 9.2 子阶段

| 子阶段 | 范围 | 主要模块 | 验收 |
|---|---|---|---|
| 6A | Raw L2 snapshot/update 持久化 | adapter、orchestrator、storage | raw book events 可写入和查询 |
| 6B | Binance-only reconstruction | 新 reconstruction 边界、analytics 或独立模块 | fixture updates 可重建 N-level book |
| 6C | Gap detection/repair 状态 | adapter、reconstruction、server status | sequence gap 可检测并记录质量状态 |

### 9.3 模块边界建议

- 不建议把 reconstruction engine 放进 `fdc-adapter/barter`，adapter 应只负责来源事件。
- 可新建独立模块或先放在 `fdc-analytics` 的 microstructure 子模块中，但要避免依赖 server runtime。
- `fdc-server` 只暴露状态和查询，不承担 reconstruction 细节。

## 10. Phase 7：多交易所扩展

### 10.1 前置条件

进入多交易所扩展前，应完成：

- OHLCV/trade/L1 的 storage/query contracts 稳定。
- 数据质量 metadata 最小集稳定。
- Binance 主线至少完成一次 operator-visible smoke 或 runbook 更新。

### 10.2 推荐顺序

1. Bybit Perpetuals USD。
2. OKX。
3. Coinbase/Kraken Spot。
4. Gate.io / BitMEX / Bitfinex。

### 10.3 每个交易所必须具备的最小验收

- Adapter capability declaration。
- Offline parser/mapper contracts。
- Bounded acquisition example 或 ignored real smoke。
- Storage mapping contract。
- Query/export contract。
- 数据质量 metadata 显示来源和缺口。

## 11. 每阶段统一验收标准

每个阶段完成前至少满足：

1. **边界清晰：** 没有新增反向依赖，尤其是 `fdc-storage` 不依赖业务 crate。
2. **离线可测：** 有 deterministic contract tests，不依赖公网。
3. **真实来源可选验证：** real-network smoke 必须 ignored 或显式 opt-in。
4. **存储闭环：** 至少一个 fixture 能完成 write → query。
5. **时间语义明确：** 区分 event_time 与 received_at。
6. **质量元数据：** 至少能表达 source mode、dedupe/checkpoint/gap 中与本阶段相关的字段。
7. **文档同步：** README、roadmap 或 runbook 中对应状态更新。

## 12. 当前不做事项

近期阶段不做：

- 完整通用 market-data API 平台。
- 完整多交易所历史数据平台。
- L2 reconstruction 与 OHLCV/trade/L1 同阶段混做。
- 直接把因子计算嵌入 `fdc-server` runtime。
- 让 `fdc-storage` 认识 Barter 或因子业务类型。
- 默认依赖公网的 CI 测试。

## 13. 建议下一步

建议下一次具体开发从 **Phase 1：OHLCV/Candle 历史数据闭环** 开始，并进一步拆成三个实施计划：

1. **Phase 1A adapter historical OHLCV contract：** 只做 request/descriptor/parser/outcome 合同。
2. **Phase 1B storage mapping/query contract：** 只做 Candle → StorageWriteRecord 和 generic query 验证。
3. **Phase 1C analytics candle factors：** 只做 fixture candle 输入和基础因子计算。

这样每个实施计划都足够小，可以独立审查、实现和回滚。
