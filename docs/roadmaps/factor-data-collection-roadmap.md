# Factor Data Collection Roadmap

**Date:** 2026-07-11  
**Branch:** `mdb-mqdev`  
**Purpose:** Capture the next market-data collection roadmap required for factor research, backtesting, and future realtime analytics.

This document is a review draft. It does not prescribe implementation tasks yet. After review, split approved items into concrete specs and implementation plans.

## 1. Current baseline

The current project baseline is documented in:

- [`README.md`](../../README.md)
- [`docs/DEVELOPMENT_STATUS.md`](../DEVELOPMENT_STATUS.md)
- [`crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`](../../crates/fdc-adapter/barter/docs/market-data-collection-requirements.md)
- [`docs/runbooks/market-data-production-runbook.md`](../runbooks/market-data-production-runbook.md)

Current market-data support is strongest on the Binance path:

| Data | Current status | Factor families enabled |
|---|---|---|
| Trade | Binance Spot live and Binance Spot historical trades | Return, volume, signed flow, trade imbalance |
| OHLCV / Candle | Binance Spot historical REST | Momentum, reversal, realized volatility, volume factors |
| OrderBookL1 | Binance Spot and Binance Futures USD live mapping | Spread, mid-price, top-of-book imbalance |
| OrderBook L2 | Binance Spot and Binance Futures USD live mapping | Depth imbalance, liquidity slope, order-flow imbalance after reconstruction |
| Liquidation | Binance Futures USD live mapping | Forced-flow pressure, liquidation stress |

Current limitations that matter for factor development:

- `fdc-analytics` currently contains only basic indicators such as SMA and RSI plus a simplified ML stub.
- Production query hardening currently focuses on `GET /market-data/trades`.
- Historical REST is Binance Spot focused.
- Historical order-book reconstruction is not implemented.
- L2 book gap detection, out-of-order repair, and durable reconstruction are future work.
- External-network live and historical smoke tests require explicit opt-in.

## 2. Roadmap goals

The next data roadmap should make factor calculation practical across three horizons:

1. **Research/backtest horizon:** clean, aligned, timestamped historical datasets for feature generation and replay.
2. **Microstructure horizon:** L1/L2/trade datasets with event-time and receive-time semantics.
3. **Derivatives horizon:** funding, open interest, mark/index price, basis, and liquidation data for perpetual/futures factors.

Non-goals for this roadmap draft:

- No implementation plan yet.
- No new API contract definitions yet.
- No exchange-by-exchange certification claim beyond the current capability matrix.

## 3. P0 data: unlock basic factor research fastest

### 3.1 Standardized multi-interval OHLCV history

Current Binance Spot OHLCV support should be expanded into a systematic historical dataset.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `interval`, such as `1m`, `5m`, `15m`, `1h`, `1d`
- `open_time`
- `close_time`
- `open`
- `high`
- `low`
- `close`
- `base_volume`
- `quote_volume`
- `trade_count`
- `taker_buy_base_volume`, if available
- `taker_buy_quote_volume`, if available
- source pagination/checkpoint metadata

Priority additions:

1. Multi-symbol Binance Spot OHLCV backfill.
2. Binance Futures/Perpetual OHLCV backfill.
3. Unified storage/query shape for candles.
4. Offline contract tests for interval validation, limit handling, and checkpoint continuity.

Factor families enabled:

- Return and momentum.
- Reversal.
- Realized volatility and range factors.
- Volume and turnover factors.
- Intraday seasonality.

### 3.2 Trade enrichment and stable trade history

Current trade payload should be stabilized as a first-class factor input.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `trade_id`
- `price`
- `quantity`
- `side` / aggressor side / taker side, where available
- `event_time`
- `received_at`
- `sequence`, if available
- duplicate/dedupe key
- source mode: live, historical, replay/backfill

Priority additions:

1. Align live trade and historical trade schema.
2. Persist dedupe key and duplicate classification.
3. Add trade query surfaces beyond the current production `/market-data/trades` path only if the storage/query contract is ready.
4. Add batch backfill result metadata: records received, duplicates, cursor, completion state.

Factor families enabled:

- Buy/sell imbalance.
- Signed volume.
- Trade intensity.
- Large-trade impact.
- Short-horizon return prediction.
- VPIN-style flow toxicity, once buckets are available.

### 3.3 L1 order book persistence and query

L1 currently has live mapping, but factor research needs durable, queryable samples.

Required fields:

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
- source sequence, if available

Priority additions:

1. Persist L1 snapshots as first-class market data records.
2. Add query shape for L1 by symbol, time range, and limit.
3. Add derived mid/spread fields or define a deterministic derivation boundary.
4. Add bounded live verification that L1 records survive storage reopen.

Factor families enabled:

- Bid-ask spread.
- Mid-price returns.
- Microprice.
- Top-of-book imbalance.
- Quote pressure.
- Liquidity cost estimation.

## 4. P1 data: high-value microstructure and derivatives factors

### 4.1 L2 book reconstruction

Current L2 payloads preserve update type, levels, timestamps, and sequence where available. Factor quality requires reconstruction, validation, and gap handling.

Required data and state:

- Full or partial depth snapshot.
- Incremental depth update.
- Bid/ask levels.
- Exchange sequence fields, such as first update id and final update id where available.
- Checksum, if supported by exchange.
- Reconstructed book snapshot.
- Gap detection state.
- Out-of-order and repair metadata.

Priority additions:

1. Durable raw L2 snapshot/update storage.
2. Exchange-specific reconstruction rules, starting with Binance Spot/Futures.
3. Gap detection and suppress/repair status.
4. Reconstructed N-level book snapshots for factor jobs.

Factor families enabled:

- Depth imbalance.
- Liquidity slope.
- Liquidity wall detection.
- Price impact curve.
- Order-flow imbalance.
- Queue pressure.
- Short-horizon continuation/reversal.

### 4.2 Funding rate

Funding is required for perpetual carry and crowding factors.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `funding_rate`
- `predicted_funding_rate`, if available
- `funding_time`
- `mark_price`, if returned with funding endpoint
- `index_price`, if returned with funding endpoint
- source endpoint and request timestamp

Priority additions:

1. Binance Futures USD funding rate REST backfill/current endpoint.
2. Historical funding series persistence.
3. Join boundary with mark/index price and OHLCV.

Factor families enabled:

- Funding carry.
- Funding mean reversion.
- Crowded long/short signal.
- Funding-adjusted basis.

### 4.3 Open interest

Open interest is required for leverage and crowded-position factors.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `open_interest`
- `open_interest_value`, if available
- `timestamp`
- source endpoint and request timestamp

Priority additions:

1. Binance Futures USD open-interest current and historical endpoints where available.
2. Storage/query support by symbol and time range.
3. Alignment with funding and liquidation data.

Factor families enabled:

- Leverage build-up.
- Crowded position.
- Trend confirmation.
- Liquidation risk.
- Volatility regime classification.

### 4.4 Mark price and index price

Mark/index data is needed for derivatives valuation, basis, and liquidation context.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `mark_price`
- `index_price`
- `estimated_settle_price`, if available
- `timestamp`
- source endpoint and request timestamp

Factor families enabled:

- Perpetual basis.
- Futures basis.
- Mark-vs-last dislocation.
- Liquidation risk context.

## 5. P2 data: cross-market and stress factors

### 5.1 Spot-futures basis inputs

Basis can be computed from spot mid/last price, futures/perpetual mark price, index price, and expiry metadata.

Required fields:

- Spot last/mid price.
- Perpetual mark price.
- Futures price.
- Index price.
- Expiry date for delivery futures.
- Funding rate for perpetuals.

Factor families enabled:

- Cash-and-carry.
- Relative value.
- Funding-adjusted spread.
- Basis mean reversion.

### 5.2 Liquidation history and enrichment

Current Binance Futures USD liquidation support is live-oriented. Factor research needs durable and historical/enriched liquidation data where sources permit.

Required fields:

- `exchange`
- `symbol`
- `market_type`
- `side`
- `price`
- `quantity`
- `notional`
- `event_time`
- `received_at`
- aggregation window metadata for derived series

Priority additions:

1. Persist live liquidation events durably.
2. Add notional calculation.
3. Add aggregated liquidation series by symbol and window.
4. Investigate historical liquidation sources per exchange.

Factor families enabled:

- Forced-flow pressure.
- Panic factor.
- Liquidation cascade risk.
- Contrarian reversal after liquidation spikes.

### 5.3 Multi-exchange same-symbol coverage

The capability matrix declares additional crypto venues. Factor work should expand only after the Binance data model is stable.

Candidate data per exchange:

- Trade.
- L1 quote.
- Mid price.
- Volume.
- Funding and open interest for derivatives venues.
- Exchange status and latency metadata.

Candidate venues from the current capability map:

- Bybit Spot and Perpetuals USD.
- Kraken Spot.
- Coinbase Spot.
- Bitfinex Spot.
- BitMEX Perpetual.
- Gate.io Spot, Futures, Perpetuals, Options.
- OKX Spot.

Factor families enabled:

- Cross-exchange basis.
- Lead-lag.
- Liquidity fragmentation.
- Venue dominance.
- Arbitrage pressure.

## 6. P3 data: quality, metadata, and universe control

### 6.1 Data quality metadata

Required quality fields:

- Missing interval count.
- Duplicate count.
- Out-of-order count.
- Late arrival count.
- Exchange disconnect count.
- Reconnect count.
- Gap detected.
- Gap repaired.
- Source latency.
- Receive latency.
- Suppression or degraded-mode reason.

Why this matters:

- Filters bad training samples.
- Provides factor confidence scores.
- Improves backtest quality control.
- Helps compare online and offline factor results.

### 6.2 Exchange and instrument metadata

Required metadata:

- Symbol list.
- Trading status.
- Base asset.
- Quote asset.
- Tick size.
- Lot size.
- Minimum notional.
- Contract size.
- Expiry date for delivery contracts.
- Listing and delisting timestamps where available.

Why this matters:

- Normalizes price and quantity.
- Filters non-tradable symbols.
- Defines cross-sectional universe.
- Prevents backtests from using unavailable instruments.

## 7. Recommended implementation slicing

This is a proposed sequencing for later specs and implementation plans.

| Slice | Scope | Expected outcome |
|---|---|---|
| F1 | Standardized Candle/OHLCV backfill and storage/query shape | Basic return, volatility, and volume factors become feasible |
| F2 | Trade schema alignment, dedupe metadata, and historical/live parity | Flow and trade-intensity factors become feasible |
| F3 | L1 persistence/query and derived spread/mid fields | Spread, microprice, and top imbalance factors become feasible |
| F4 | Binance Futures funding, open interest, mark/index price | Funding carry, leverage, and basis factors become feasible |
| F5 | L2 raw persistence and reconstruction for Binance | Depth and order-flow imbalance factors become feasible |
| F6 | Liquidation persistence, notional enrichment, and aggregation | Forced-flow and stress factors become feasible |
| F7 | Multi-exchange expansion after Binance model stabilizes | Cross-exchange lead-lag and basis factors become feasible |
| F8 | Data quality and instrument metadata surfaces | Factor filtering, universe control, and confidence become feasible |

Recommended first three slices:

1. **F1 Candle/OHLCV backfill:** fastest path to useful offline factor research.
2. **F2 Trade parity:** reuses current Binance trade strengths and improves flow factors.
3. **F3 L1 persistence:** unlocks simple microstructure factors without full L2 reconstruction complexity.

## 8. Review questions

Use these questions to decide the next approved implementation slice:

1. Should the next phase optimize for **offline factor research** or **realtime factor streaming** first?
2. Should Binance remain the only production-certified source until the data model stabilizes?
3. Which symbols should define the first factor universe: BTC/USDT and ETH/USDT only, or a broader top-N universe?
4. Should Candle/OHLCV storage/query be implemented before expanding `/market-data/trades` into a more generic query API?
5. Do we need a dedicated `market_data_kind` query surface before adding funding/OI/mark price?
6. Should L2 reconstruction wait until after funding/OI/mark price, given its higher correctness burden?
7. What minimum data-quality metadata is required before using data in backtests?

## 9. Suggested acceptance criteria for future plans

Before a data slice is considered ready for factor work, it should provide:

- Offline deterministic contract tests.
- Bounded acquisition examples or test helpers.
- Storage write path coverage.
- Query or export path coverage.
- Clear event-time and receive-time semantics.
- Dedupe/checkpoint/gap metadata where relevant.
- Runbook or README update for operator-facing behavior.

## 10. Summary recommendation

The highest-leverage path is to keep Binance as the initial certified source and expand data depth before expanding exchange breadth:

1. Multi-interval OHLCV historical backfill.
2. Trade enrichment and live/historical parity.
3. L1 book persistence and query.
4. Funding, open interest, mark/index price for derivatives.
5. L2 reconstruction after the simpler factor datasets are stable.
6. Multi-exchange expansion after the data contracts are proven.
