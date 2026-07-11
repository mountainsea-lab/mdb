# 因子计算数据采集路线图

**日期：** 2026-07-11  
**分支：** `mdb-mqdev`  
**目的：** 梳理后续因子研究、策略回测和实时分析所需的市场数据采集路线图。

本文档是供审查的路线图草案，不直接定义具体实现任务。审查通过后，再将确认的切片拆分为独立设计文档和实施计划。

## 1. 当前基础

当前项目基础状态可参考：

- [`README.md`](../../README.md)
- [`docs/DEVELOPMENT_STATUS.md`](../DEVELOPMENT_STATUS.md)
- [`crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`](../../crates/fdc-adapter/barter/docs/market-data-collection-requirements.md)
- [`docs/runbooks/market-data-production-runbook.md`](../runbooks/market-data-production-runbook.md)

当前市场数据支持最成熟的是 Binance 主线：

| 数据 | 当前状态 | 可支持的因子类型 |
|---|---|---|
| Trade | Binance Spot live 与 Binance Spot historical trades | 收益、成交量、signed flow、成交不平衡 |
| OHLCV / Candle | Binance Spot historical REST | 动量、反转、已实现波动率、成交量因子 |
| OrderBookL1 | Binance Spot 与 Binance Futures USD live mapping | spread、mid-price、top-of-book imbalance |
| OrderBook L2 | Binance Spot 与 Binance Futures USD live mapping | reconstruction 后可支持 depth imbalance、liquidity slope、order-flow imbalance |
| Liquidation | Binance Futures USD live mapping | forced-flow pressure、liquidation stress |

对后续因子开发影响较大的当前限制：

- `fdc-analytics` 当前主要包含 SMA、RSI 等基础指标和简化版 ML stub。
- 生产查询加固当前主要集中在 `GET /market-data/trades`。
- Historical REST 当前以 Binance Spot 为主。
- Historical order-book reconstruction 尚未实现。
- L2 book gap detection、out-of-order repair 和 durable reconstruction 仍是后续工作。
- 外部网络 live/historical smoke 需要显式 opt-in。

## 2. 路线图目标

下一阶段数据路线图应让因子计算在三个层面逐步可用：

1. **研究/回测层面：** 提供干净、对齐、带时间戳的历史数据集，用于特征生成和事件回放。
2. **微观结构层面：** 提供带 event-time 与 receive-time 语义的 L1、L2 和 trade 数据。
3. **衍生品层面：** 为 perpetual/futures 因子补齐 funding、open interest、mark/index price、basis 和 liquidation 数据。

本文档暂不覆盖：

- 不直接给出实施计划。
- 不定义新的 API contract。
- 不做超出当前能力矩阵的逐交易所生产认证声明。

## 3. P0 数据：最快支撑基础因子研究

### 3.1 标准化多周期 OHLCV 历史数据

当前 Binance Spot OHLCV 支持应扩展为系统化历史数据集。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `interval`，例如 `1m`、`5m`、`15m`、`1h`、`1d`
- `open_time`
- `close_time`
- `open`
- `high`
- `low`
- `close`
- `base_volume`
- `quote_volume`
- `trade_count`
- `taker_buy_base_volume`，如果来源支持
- `taker_buy_quote_volume`，如果来源支持
- 源分页和 checkpoint 元数据

优先补充：

1. 多 symbol Binance Spot OHLCV 回补。
2. Binance Futures/Perpetual OHLCV 回补。
3. Candle 的统一 storage/query 形态。
4. interval 校验、limit 处理和 checkpoint 连续性的离线 contract tests。

可支持的因子类型：

- 收益与动量。
- 反转。
- 已实现波动率和振幅类因子。
- 成交量和换手类因子。
- 日内季节性。

### 3.2 Trade 增强与稳定历史成交数据

当前 trade payload 应稳定为一等因子输入。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `trade_id`
- `price`
- `quantity`
- `side` / aggressor side / taker side，如果来源支持
- `event_time`
- `received_at`
- `sequence`，如果来源支持
- duplicate/dedupe key
- source mode：live、historical、replay/backfill

优先补充：

1. 对齐 live trade 与 historical trade schema。
2. 持久化 dedupe key 和重复记录分类。
3. 仅在 storage/query contract 准备好后，再扩展当前生产 `/market-data/trades` 之外的 trade 查询面。
4. 增加批量回补结果元数据：接收记录数、重复数、cursor、完成状态。

可支持的因子类型：

- 买卖不平衡。
- Signed volume。
- 成交强度。
- 大单冲击。
- 短周期收益预测。
- VPIN 类 flow toxicity，前提是后续实现 bucket 边界。

### 3.3 L1 盘口持久化与查询

L1 当前已有 live mapping，但因子研究需要可持久化、可查询的样本。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `bid_price`
- `bid_quantity`
- `ask_price`
- `ask_quantity`
- `spread`
- `mid_price`
- `event_time`
- `received_at`
- source sequence，如果来源支持

优先补充：

1. 将 L1 snapshots 作为一等 market data records 持久化。
2. 增加按 symbol、time range、limit 查询 L1 的查询形态。
3. 增加派生 mid/spread 字段，或定义确定性的派生边界。
4. 增加 bounded live verification，证明 L1 records 经过 storage reopen 后仍可读取。

可支持的因子类型：

- Bid-ask spread。
- Mid-price returns。
- Microprice。
- Top-of-book imbalance。
- Quote pressure。
- Liquidity cost estimation。

## 4. P1 数据：高价值微观结构与衍生品因子

### 4.1 L2 book reconstruction

当前 L2 payload 会在来源支持时保留 update type、levels、timestamps 和 sequence。高质量因子需要 reconstruction、validation 和 gap handling。

必要数据与状态：

- Full 或 partial depth snapshot。
- Incremental depth update。
- Bid/ask levels。
- 交易所 sequence 字段，例如 first update id、final update id。
- Checksum，如果交易所支持。
- Reconstructed book snapshot。
- Gap detection state。
- Out-of-order 和 repair metadata。

优先补充：

1. 持久化 raw L2 snapshot/update。
2. 从 Binance Spot/Futures 开始实现交易所特定 reconstruction rules。
3. 增加 gap detection 和 suppress/repair status。
4. 为因子任务输出 reconstructed N-level book snapshots。

可支持的因子类型：

- Depth imbalance。
- Liquidity slope。
- Liquidity wall detection。
- Price impact curve。
- Order-flow imbalance。
- Queue pressure。
- 短周期 continuation/reversal。

### 4.2 Funding rate

Funding 是 perpetual carry 和拥挤度因子的必要数据。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `funding_rate`
- `predicted_funding_rate`，如果来源支持
- `funding_time`
- `mark_price`，如果 funding endpoint 同时返回
- `index_price`，如果 funding endpoint 同时返回
- source endpoint 和 request timestamp

优先补充：

1. Binance Futures USD funding rate REST backfill/current endpoint。
2. Historical funding series 持久化。
3. 定义与 mark/index price、OHLCV 的 join boundary。

可支持的因子类型：

- Funding carry。
- Funding mean reversion。
- Crowded long/short signal。
- Funding-adjusted basis。

### 4.3 Open interest

Open interest 是杠杆和 crowded-position 因子的必要数据。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `open_interest`
- `open_interest_value`，如果来源支持
- `timestamp`
- source endpoint 和 request timestamp

优先补充：

1. Binance Futures USD open-interest current 和 historical endpoints，如果来源支持。
2. 按 symbol 和 time range 的 storage/query 支持。
3. 与 funding 和 liquidation 数据对齐。

可支持的因子类型：

- Leverage build-up。
- Crowded position。
- Trend confirmation。
- Liquidation risk。
- Volatility regime classification。

### 4.4 Mark price 与 index price

Mark/index 数据用于衍生品估值、basis 和 liquidation 上下文。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `mark_price`
- `index_price`
- `estimated_settle_price`，如果来源支持
- `timestamp`
- source endpoint 和 request timestamp

可支持的因子类型：

- Perpetual basis。
- Futures basis。
- Mark-vs-last dislocation。
- Liquidation risk context。

## 5. P2 数据：跨市场与压力因子

### 5.1 Spot-futures basis 输入

Basis 可由 spot mid/last price、futures/perpetual mark price、index price 和 expiry metadata 计算。

必要字段：

- Spot last/mid price。
- Perpetual mark price。
- Futures price。
- Index price。
- Delivery futures 的 expiry date。
- Perpetuals 的 funding rate。

可支持的因子类型：

- Cash-and-carry。
- Relative value。
- Funding-adjusted spread。
- Basis mean reversion。

### 5.2 Liquidation 历史与增强

当前 Binance Futures USD liquidation 支持偏 live。因子研究需要 durable，并在来源允许时补充 historical/enriched liquidation 数据。

必要字段：

- `exchange`
- `symbol`
- `market_type`
- `side`
- `price`
- `quantity`
- `notional`
- `event_time`
- `received_at`
- aggregation window metadata，用于派生序列

优先补充：

1. 持久化 live liquidation events。
2. 增加 notional 计算。
3. 按 symbol 和 window 增加 aggregated liquidation series。
4. 逐交易所调研 historical liquidation sources。

可支持的因子类型：

- Forced-flow pressure。
- Panic factor。
- Liquidation cascade risk。
- Liquidation spike 后的 contrarian reversal。

### 5.3 多交易所同标的数据覆盖

当前能力矩阵已经声明额外 crypto venues。因子工作应在 Binance 数据模型稳定后再扩展交易所覆盖。

每个交易所候选数据：

- Trade。
- L1 quote。
- Mid price。
- Volume。
- 衍生品交易所的 funding 和 open interest。
- Exchange status 和 latency metadata。

当前 capability map 中的候选交易所：

- Bybit Spot 与 Perpetuals USD。
- Kraken Spot。
- Coinbase Spot。
- Bitfinex Spot。
- BitMEX Perpetual。
- Gate.io Spot、Futures、Perpetuals、Options。
- OKX Spot。

可支持的因子类型：

- Cross-exchange basis。
- Lead-lag。
- Liquidity fragmentation。
- Venue dominance。
- Arbitrage pressure。

## 6. P3 数据：质量、元数据与标的池控制

### 6.1 数据质量元数据

必要质量字段：

- Missing interval count。
- Duplicate count。
- Out-of-order count。
- Late arrival count。
- Exchange disconnect count。
- Reconnect count。
- Gap detected。
- Gap repaired。
- Source latency。
- Receive latency。
- Suppression 或 degraded-mode reason。

为什么重要：

- 过滤坏训练样本。
- 提供因子置信度评分。
- 改善回测质量控制。
- 帮助比较 online 和 offline 因子结果。

### 6.2 交易所与合约/标的元数据

必要元数据：

- Symbol list。
- Trading status。
- Base asset。
- Quote asset。
- Tick size。
- Lot size。
- Minimum notional。
- Contract size。
- Delivery contracts 的 expiry date。
- Listing 和 delisting timestamp，如果来源支持。

为什么重要：

- 归一化价格和数量。
- 过滤不可交易 symbol。
- 定义横截面标的池。
- 避免回测使用当时不可用的 instruments。

## 7. 推荐实施切片

以下是后续 spec 和 implementation plan 的建议顺序。

| 切片 | 范围 | 预期结果 |
|---|---|---|
| F1 | 标准化 Candle/OHLCV 回补与 storage/query 形态 | 基础收益、波动率和成交量因子可用 |
| F2 | Trade schema 对齐、dedupe metadata、historical/live parity | Flow 和成交强度因子可用 |
| F3 | L1 persistence/query 与派生 spread/mid 字段 | Spread、microprice、top imbalance 因子可用 |
| F4 | Binance Futures funding、open interest、mark/index price | Funding carry、leverage、basis 因子可用 |
| F5 | Binance L2 raw persistence 与 reconstruction | Depth 和 order-flow imbalance 因子可用 |
| F6 | Liquidation persistence、notional enrichment、aggregation | Forced-flow 和 stress 因子可用 |
| F7 | Binance 模型稳定后的 multi-exchange expansion | Cross-exchange lead-lag 和 basis 因子可用 |
| F8 | Data quality 与 instrument metadata surfaces | 因子过滤、标的池控制和置信度可用 |

推荐优先推进前三个切片：

1. **F1 Candle/OHLCV backfill：** 最快支持有用的 offline factor research。
2. **F2 Trade parity：** 复用当前 Binance trade 主线优势，增强 flow 因子。
3. **F3 L1 persistence：** 不引入完整 L2 reconstruction 的复杂度，也能解锁简单微观结构因子。

## 8. 审查问题

可用以下问题决定下一批批准的实施切片：

1. 下一阶段应优先优化 **offline factor research**，还是 **realtime factor streaming**？
2. 在数据模型稳定前，Binance 是否继续作为唯一 production-certified source？
3. 第一版因子标的池只覆盖 BTC/USDT 和 ETH/USDT，还是扩展为 top-N universe？
4. 是否应先实现 Candle/OHLCV storage/query，再扩展 `/market-data/trades` 到更通用的查询 API？
5. 在添加 funding/OI/mark price 前，是否需要专门的 `market_data_kind` 查询面？
6. 考虑到 L2 reconstruction 的正确性负担较高，是否应排在 funding/OI/mark price 之后？
7. 数据进入回测前，最低需要哪些 data-quality metadata？

## 9. 后续计划的建议验收标准

一个数据切片在被认为可用于因子工作前，应至少提供：

- 离线 deterministic contract tests。
- Bounded acquisition examples 或 test helpers。
- Storage write path 覆盖。
- Query 或 export path 覆盖。
- 清晰的 event-time 与 receive-time 语义。
- 在相关场景提供 dedupe、checkpoint、gap metadata。
- 面向操作方行为的 runbook 或 README 更新。

## 10. 总结建议

最高杠杆路径是先把 Binance 作为初始认证数据源，优先扩展数据深度，再扩展交易所广度：

1. 多周期 OHLCV historical backfill。
2. Trade enrichment 与 live/historical parity。
3. L1 book persistence 与 query。
4. 衍生品 funding、open interest、mark/index price。
5. 在更简单的因子数据集稳定后，再推进 L2 reconstruction。
6. 在数据 contract 被验证后，再推进 multi-exchange expansion。
