# fdc-barter Market Data Collection Requirements Design

Date: 2026-05-31
Module: `crates/fdc-adapter/barter`
Status: Stage 1 Binance Futures USD derivatives historical validation completed on 2026-07-11

## 1. Purpose

This document defines what exchange market data the `fdc-barter` adapter should collect for factor research, strategy backtesting, and production realtime analytics.

The adapter has moved beyond the original realtime trade proof. It now owns the Barter-rs integration boundary for structured live market data, Binance Spot historical REST acquisition, adapter envelopes, capability declarations, and example binaries. The next module-level work should keep the requirements and capability map accurate while productionizing verification, integration, and operational gaps.

The goal is to make `fdc-barter` a reliable crypto exchange market-data adapter while preserving existing workspace boundaries:

- `fdc-barter` owns Barter-rs integration and Barter-specific event/envelope models.
- `fdc-ingestion` stays generic and adapter-independent.
- `fdc-transform` owns neutral DTOs, not Barter-specific mapping.
- `fdc-storage` owns generic storage writes and queryable stores.
- `fdc-orchestrator` owns cross-layer mapping from adapter events to source envelopes, DTOs, and storage records.
- `fdc-server` owns production runtime assembly and HTTP control, not Barter-rs connector details.

## 2. Current Module State

### 2.1 Implemented in `fdc-barter`

Current models and boundaries:

- `BarterMarketDataKind`
  - `Trade`
  - `OrderBookL1`
  - `OrderBook`
  - `Candle`
  - `Liquidation`
  - `FundingRate`
  - `OpenInterest`
  - `MarkPrice`
  - `IndexPrice`
- `BarterMarketDataMode`
  - `Live`
  - `Historical`
- `BarterMarketType`
  - `Spot`
  - `Future`
  - `Perpetual`
  - `Option`
- `BarterMarketEvent`
- `BarterMarketPayload`
  - structured `TradePayload`
  - structured `OrderBookL1Payload`
  - structured `OrderBookPayload`
  - structured `CandlePayload`
  - structured `LiquidationPayload`
  - structured `FundingRatePayload`
  - structured `OpenInterestPayload`
  - structured `MarkPricePayload`
  - structured `IndexPricePayload`
  - `RawPayload` fallback
- `BarterIngestionEnvelope`
- `DataQualityFlags`
- `HistoricalBackfillRequest`
- `HistoricalBackfillPage`
- `HistoricalBackfillRunRequest`
- `HistoricalBackfillRunOutcome`
- `HistoricalCursor`
- `HistoricalPageRequest`
- `BarterCheckpoint`
- `BarterSourceCapabilities`
- `HistoricalProviderCapabilities`

Current live acquisition functions:

- `default_binance_spot_trade_subscriptions`
- `default_binance_spot_market_data_subscriptions`
- `default_binance_futures_usd_market_data_subscriptions`
- `init_binance_spot_public_trades`
- `init_binance_spot_market_data`
- `init_binance_futures_usd_market_data`
- `public_trade_result_to_data_kind`
- `map_live_trade_result`
- `map_live_market_data_result`
- `collect_live_trade_envelopes`
- `collect_live_market_data_envelopes`
- `collect_live_envelopes_with_summary`

Current historical acquisition functions:

- `binance_spot_ohlcv_capabilities`
- `binance_spot_historical_trades_capabilities`
- `binance_futures_usd_funding_rate_capabilities`
- `binance_futures_usd_open_interest_capabilities`
- `binance_futures_usd_mark_price_capabilities`
- `binance_futures_usd_ohlcv_capabilities`
- `binance_spot_ohlcv_rest_request_descriptor`
- `binance_spot_historical_trades_rest_request_descriptor`
- `binance_futures_usd_funding_rate_rest_request_descriptor`
- `binance_futures_usd_open_interest_rest_request_descriptor`
- `binance_futures_usd_mark_price_rest_request_descriptor`
- `binance_futures_usd_ohlcv_rest_request_descriptor`
- `execute_binance_spot_ohlcv_rest`
- `execute_binance_spot_historical_trades_rest`
- `execute_binance_futures_usd_funding_rate_rest`
- `execute_binance_futures_usd_open_interest_rest`
- `execute_binance_futures_usd_mark_price_rest`
- `execute_binance_futures_usd_ohlcv_rest`
- `run_historical_backfill_pages`
- `validate_historical_backfill_request`
- `historical_trade_dedupe_key`

Current examples:

- `historical_binance_futures_usd_derivatives`
- `historical_binance_spot_ohlcv`
- `historical_binance_spot_trades`
- `live_binance_futures_usd_market_data`
- `live_binance_spot_order_books`
- `live_binance_spot_trades`

Current production behavior:

- Binance Spot public trades can be collected live through Barter-rs.
- Binance Spot public trades, Spot L1 order books, Spot L2 order books, and Binance Futures USD liquidations have structured mapper coverage.
- Binance Spot OHLCV and historical trades can be fetched through adapter-owned REST descriptors/executors.
- Binance Futures USD funding rate, open interest snapshot, premium index / mark price, and OHLCV can be fetched through adapter-owned REST descriptors/executors and validated through offline fixture providers.
- `historical_binance_futures_usd_derivatives` calls real Binance Futures REST by default and prints records; deterministic fixture mode is explicit via `MDB_BARTER_EXAMPLE_MODE=fixture`.
- Bounded live and historical helpers provide finite acquisition outcomes for tests, examples, and future pipeline integration.
- Collected events can be mapped into mdb envelopes and written through the existing orchestrator/storage path where that cross-module path is already wired.
- Production live smoke has already proven real Binance Spot trades are queryable through the production API.

### 2.2 Current gaps

The adapter is no longer trade-only, but several production-level gaps remain:

- Historical REST support now covers Binance Spot OHLCV/trades and Binance Futures USD derivatives basics; multi-exchange historical REST providers are future work.
- Binance Futures historical open-interest time series via `/futures/data/openInterestHist` is not implemented; current OI support is the `/fapi/v1/openInterest` snapshot endpoint.
- L2 order-book payloads preserve snapshot/update, levels, timestamps, and sequence where available, but durable book reconstruction, gap detection, and out-of-order repair are future work.
- Live smoke and historical smoke tests require explicit environment variables and public internet access, so routine CI still relies on offline contract tests by default.
- Cross-module glue from `fdc-barter` bounded helpers to the generic `fdc-ingestion` source pipeline is future work and intentionally outside this document update.

## 3. Downstream Data Consumers

### 3.1 Factor research

Factor research needs clean, aligned, timestamped data that can support feature construction across symbols, exchanges, and market types.

Important factor families:

- Return and momentum factors.
- Realized volatility and range factors.
- Volume and turnover factors.
- Trade imbalance and aggressor-flow factors.
- Spread, mid-price, and microprice factors.
- Order-book imbalance and liquidity slope factors.
- Funding, basis, and carry factors for derivatives.
- Liquidation and forced-flow stress factors.
- Cross-exchange basis and relative-value factors.

### 3.2 Strategy backtesting

Backtesting needs historical data and replayable event streams with clear event-time semantics.

Backtesting requirements:

- Event time and receive time must be distinct.
- Backfilled data must be flagged as replay/backfill.
- Duplicate and out-of-order candidates must be identifiable.
- Historical schemas should match realtime schemas where possible.
- Order-book history must be reconstructable if used for execution simulation.
- Data completeness and checkpoint metadata must be available for long backfills.

### 3.3 Production realtime analytics

Production analytics needs live streams, health/status, and enough microstructure data to power near-realtime signals.

Realtime requirements:

- Continuous trade stream.
- Continuous top-of-book stream.
- Optional L2 stream for depth-aware signals.
- Runtime counters and last-event timestamps.
- Reconnect/gap visibility.
- Ability to stop and restart acquisition safely.

## 4. Barter-rs Capability Summary

Barter-rs already provides the crypto exchange WebSocket connector layer. `fdc-barter` should reuse it rather than reimplement exchange WebSocket clients.

Relevant Barter-rs capabilities:

- `Streams` and `StreamBuilder` for typed market streams.
- `MultiStreamBuilder` for merging several subscription-kind streams.
- `DynamicStreams` for dynamic `ExchangeId + SubKind` subscription batches.
- Reconnection behavior through:
  - `init_market_stream`
  - `init_reconnecting_stream`
  - `with_reconnect_backoff`
  - `with_termination_on_error`
  - `with_reconnection_events`
  - socket-level `init_reconnecting_socket`
- Normalized event model:
  - `MarketEvent { time_exchange, time_received, exchange, instrument, kind }`
  - `DataKind::{Trade, OrderBookL1, OrderBook, Candle, Liquidation}`
- Subscription validation and deduplication.
- Exchange and instrument-kind compatibility checks.

## 5. Barter-rs Supported Data Kinds

### 5.1 Public trades

Barter-rs model:

```rust
pub struct PublicTrade {
    pub id: String,
    pub price: f64,
    pub amount: f64,
    pub side: Side,
}
```

Required mdb payload:

```rust
pub struct TradePayload {
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub side: Option<TradeSide>,
}
```

Current status: structured mapping exists.

### 5.2 Level 1 order book

Barter-rs model:

```rust
pub struct OrderBookL1 {
    pub last_update_time: DateTime<Utc>,
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
}
```

Required mdb payload:

```rust
pub struct OrderBookL1Payload {
    pub bid_price: Option<Price>,
    pub bid_quantity: Option<DecimalQuantity>,
    pub ask_price: Option<Price>,
    pub ask_quantity: Option<DecimalQuantity>,
}
```

Current status: structured model and mapper coverage exist for live Barter L1 events.

### 5.3 Level 2 order book

Barter-rs model:

```rust
pub enum OrderBookEvent {
    Snapshot(OrderBook),
    Update(OrderBook),
}
```

Required mdb direction:

- Distinguish snapshot vs update.
- Preserve bid and ask price levels.
- Preserve exchange and receive timestamps.
- Preserve sequence/update id when Barter-rs or exchange-specific data exposes it.
- Flag gaps and out-of-order updates when detection is added.

Current status: structured L2 payload and mapper coverage exist for snapshots and updates. Durable order-book reconstruction, gap detection, and storage/query semantics remain future work before L2 can fully support execution simulation and production backtesting.

### 5.4 Candles / OHLCV

Barter-rs model:

```rust
pub struct Candle {
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub trade_count: u64,
}
```

Current mdb payload:

```rust
pub struct CandlePayload {
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: DecimalQuantity,
}
```

Current status:

- `CandlePayload` includes `interval`, open/close time, OHLC, volume, optional trade count, and optional quote volume.
- Binance Spot historical OHLCV REST execution exists.
- Barter-rs live candle stream maturity should still be verified before relying on it for live candles.
- mdb can also derive candles from trades.

### 5.5 Liquidations

Barter-rs model:

```rust
pub struct Liquidation {
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub time: DateTime<Utc>,
}
```

Required mdb direction:

- Add structured liquidation payload.
- Preserve side, price, quantity, exchange timestamp, receive timestamp, exchange, market type, and symbol.

Current status: structured liquidation payload and mapper coverage exist for Barter liquidation events. Production use still needs live smoke coverage and downstream storage/query integration.

## 6. Exchange and Kind Support Matrix

Based on the current Barter-rs dynamic subscription support, the first useful matrix is:

| Exchange | Market type | Trades | L1 | L2 | Liquidations | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Binance Spot | Spot | yes | yes | yes | no | Best first expansion target beyond trades |
| Binance Futures USD | Perpetual | yes | yes | yes | yes | Best first derivatives target |
| Bybit Spot | Spot | yes | yes | yes | no | Secondary spot venue |
| Bybit Perpetuals USD | Perpetual | yes | yes | yes | not in current matrix | Secondary derivatives venue |
| Kraken | Spot | yes | yes | no | no | Useful spot venue, limited depth |
| Coinbase | Spot | yes | no | no | no | Useful US spot venue |
| Bitfinex | Spot | yes | no | no | no | Useful spot venue |
| Bitmex | Perpetual | yes | no | no | no | Derivatives trades |
| Gateio Spot | Spot | yes | no | no | no | Additional spot venue |
| Gateio Futures USD/BTC | Future | yes | no | no | no | Futures trades |
| Gateio Perpetuals USD/BTC | Perpetual | yes | no | no | no | Perpetual trades |
| Gateio Options | Option | yes | no | no | no | Options trades |
| OKX | Spot/Future/Perpetual/Option | yes | no | no | no | Multi-market trades |

This matrix should become a module-level capability table exposed by `fdc-barter` tests and docs.

## 7. Required Realtime Data for Factor Analysis

### R0: Public trades

Priority: P0

Required for:

- Price returns.
- Realized volatility.
- Trade intensity.
- Buy/sell imbalance.
- VWAP/TWAP.
- Dollar-volume bars.
- Short-horizon momentum and reversal.
- Tick-level strategy replay.

Required fields:

- `source_id`
- `exchange`
- `market_type`
- `symbol`
- `trade_id`
- `price`
- `quantity`
- `side`
- `event_time`
- `received_at`
- `sequence`, when available
- quality flags

Current status: implemented for Binance Spot public trades.

### R1: Level 1 order book

Priority: P0

Required for:

- Mid price.
- Spread.
- Microprice.
- Top-of-book imbalance.
- Short-horizon liquidity factors.
- Execution simulation using touch prices.

Required fields:

- `best_bid_price`
- `best_bid_quantity`
- `best_ask_price`
- `best_ask_quantity`
- `event_time`
- `received_at`
- `sequence`, when available
- quality flags

Current status: structured model, mapper coverage, and live acquisition wiring exist for the first Binance Spot and Binance Futures USD targets.

### R2: Level 2 order book

Priority: P0 for high-frequency research, P1 for simpler strategies.

Required for:

- Depth imbalance.
- Liquidity slope.
- Depth-weighted spread.
- Market impact modeling.
- Queue-pressure features.
- More realistic execution simulation.

Required fields:

- snapshot/update type
- bid levels
- ask levels
- depth limit
- event_time
- received_at
- sequence/update id if available
- quality flags for gaps/out-of-order updates

Current status: structured L2 payload and mapper coverage exist for Barter snapshots and updates. Binance Spot and Binance Futures USD live initialization exists; durable reconstruction and gap handling remain future work.

### R3: Liquidations

Priority: P1

Required for:

- Forced-flow indicators.
- Stress and crowding factors.
- Derivatives risk signals.
- Volatility regime detection.

Required fields:

- side
- price
- quantity
- liquidation time
- event_time
- received_at
- exchange
- symbol
- market_type

Current status: structured liquidation payload and mapper coverage exist. Binance Futures USD live initialization includes liquidation subscriptions; downstream storage/query integration remains future work.

### R4: Candles / OHLCV

Priority: P1

Required for:

- Low-frequency factor baselines.
- Quick strategy backtests.
- Data sanity checks.
- Aggregation validation against trade-derived bars.

Required fields:

- interval
- open_time
- close_time
- open
- high
- low
- close
- volume
- trade_count
- quote_volume, when available

Current status: mdb has a candle payload but it needs interval/trade_count extensions. Realtime direct candle collection should not block trade-derived candle aggregation.

## 8. Required Historical Data for Backtesting

### H0: Historical OHLCV

Priority: P0

Reason:

- Fastest path to useful research and strategy baseline backtests.
- Smaller storage footprint than tick/order-book history.
- Easier to backfill from exchange REST APIs.

Requirements:

- Query by exchange, market type, symbol, interval, start, and end.
- Support pagination and checkpoint resume.
- Store completeness metadata.
- Flag backfilled records with `is_backfill` and `is_replay` when replayed.
- Match the realtime/derived candle schema.

Likely implementation source:

- Exchange REST APIs or dedicated historical provider.
- Barter-rs does not currently expose a mature exchange historical OHLCV fetcher for this project to use directly.

### H1: Historical public trades

Priority: P0/P1

Reason:

- Needed for tick-level backtests and high-frequency factor research.
- Needed to recreate candles and volume bars exactly.

Requirements:

- Query by exchange, market type, symbol, start, end.
- Paginate by exchange cursor, trade id, or timestamp.
- Dedupe by stable key such as `(exchange, symbol, trade_id)` when available.
- Preserve event time.
- Mark receive time as backfill receive time or synthetic replay receive time.
- Store checkpoint per page.

Likely implementation source:

- Exchange REST APIs.
- Dedicated historical adapter or provider-specific backfill module.

### H2: Historical L1/L2 order book

Priority: P1 for high-frequency strategies, P2 for initial platform.

Reason:

- Needed for realistic execution simulation and order-book factors.
- Hard to source historically from most free exchange APIs.

Recommended split:

1. Recorded history:
   - Start recording realtime L1/L2 from the moment the system is deployed.
   - Persist snapshots and updates.
   - Validate update continuity.
2. Vendor backfill:
   - Later integrate professional historical order-book vendors if required.

Requirements:

- Snapshot/update distinction.
- Sequence/gap tracking.
- Book reconstruction tests.
- Data completeness reports.

### H3: Historical derivatives metrics

Priority: P1 for derivatives strategies.

Includes:

- Funding rate.
- Open interest.
- Mark price.
- Index price.
- Liquidations.

Reason:

- Needed for carry, basis, crowding, liquidation, and risk-regime factors.

Likely implementation source:

- REST/polling adapters, not Barter-rs core streams in the current state.

## 9. Data Quality Requirements

Every collected record should support quality and lineage metadata.

Required metadata:

- adapter name
- exchange
- market type
- symbol
- data kind
- mode: live, historical, replay
- event time
- received time
- emitted time
- source id
- optional checkpoint
- optional sequence/update id

Required quality flags:

- `is_replay`
- `is_backfill`
- `is_duplicate_candidate`
- `has_gap_before`
- `is_out_of_order`

Additional quality metrics to add later:

- event latency: `received_at - event_time`
- processing latency: `emitted_at - received_at`
- reconnect count
- gap count
- duplicate count
- invalid numeric count

## 10. Recommended Collection Tiers

### Tier 0: Current accepted baseline

- Binance Spot public trades, realtime.
- Queryable through production market-data API.

Status: implemented.

### Tier 1: Realtime microstructure base

Implement next:

- Binance Spot trades, L1, L2.
- Binance Futures USD trades, L1, L2, liquidations.
- Structured mappers for L1, L2, and liquidation.
- Capability matrix tests.
- Runtime counters by data kind.

Why:

- Provides enough data for basic microstructure factors.
- Uses Barter-rs capabilities already available.
- Keeps scope focused on crypto exchange data.

### Tier 2: Historical research base

Implement after Tier 1:

- Historical OHLCV backfill.
- Historical trades backfill.
- Checkpointed pagination.
- Backfill quality flags.
- Completeness reports.

Why:

- Enables first meaningful backtesting workflow.
- Does not require solving historical order-book backfill immediately.

### Tier 3: Recorded order-book history

Implement after durable storage design is ready:

- Persist realtime L1/L2 snapshots and updates.
- Reconstruct books for replay.
- Validate sequence continuity.
- Report gaps.

Why:

- Required for high-frequency execution and order-book alpha research.
- Needs durable storage and careful integrity checks.

### Tier 4: Derivatives and advanced metrics

Implement after core market data stabilizes:

- Funding rate.
- Open interest.
- Mark/index price.
- Long-short ratio if needed.
- Exchange metadata and instrument specs.

Why:

- Important for crypto derivatives factors.
- Likely requires REST/polling adapters beyond current Barter stream types.

## 11. Proposed `fdc-barter` Model Extensions

### 11.1 Add market type metadata

Current event has exchange and symbol but no explicit market type.

Recommended addition:

```rust
pub enum BarterMarketType {
    Spot,
    Future,
    Perpetual,
    Option,
}
```

Add to `BarterMarketEvent` or metadata in downstream conversion.

### 11.2 Add structured L2 payload

Recommended shape:

```rust
pub enum OrderBookUpdateKind {
    Snapshot,
    Update,
}

pub struct OrderBookLevelPayload {
    pub price: Price,
    pub quantity: DecimalQuantity,
}

pub struct OrderBookPayload {
    pub update_kind: OrderBookUpdateKind,
    pub bids: Vec<OrderBookLevelPayload>,
    pub asks: Vec<OrderBookLevelPayload>,
    pub sequence: Option<String>,
}
```

### 11.3 Add structured liquidation payload

Recommended shape:

```rust
pub struct LiquidationPayload {
    pub side: TradeSide,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub liquidation_time: TimestampNs,
}
```

### 11.4 Extend candle payload

Recommended additions:

- `interval: Option<String>`
- `trade_count: Option<u64>`
- `quote_volume: Option<DecimalQuantity>`

`interval` may be unknown for some live sources. It should be optional until historical candle requests are implemented.

### 11.5 Add historical capability declarations

Current `BarterSourceCapabilities` has live and historical fields. It should become precise enough to express:

- realtime stream support by exchange/kind/market type
- historical REST support by exchange/kind/interval
- whether source supports pagination
- whether source provides sequence/update id
- whether source can produce replay-safe records

## 12. Implementation Sequence for `fdc-barter`

### B-Data-1: Capability matrix and model contract

- Document and test supported Barter-rs exchange/kind combinations.
- Add module-level capability matrix APIs or static fixtures.
- Add model contract tests for market type and new payload shapes.

### B-Data-2: Structured realtime mappers

- Implement L1 mapper.
- Implement L2 mapper.
- Implement liquidation mapper.
- Extend candle mapper if direct candle stream is validated.
- Keep all tests offline using synthetic Barter-rs events.

### B-Data-3: Live acquisition expansion

- Add live init paths for:
  - Binance Spot L1/L2.
  - Binance Futures USD trades/L1/L2/liquidations.
- Prefer dynamic/multi stream builder if it keeps the implementation smaller and testable.
- Preserve existing Binance Spot trade behavior.

### B-Data-4: Runtime integration

- Integrate expanded data kinds into the production source runtime once the adapter core runtime design is implemented.
- Status should expose counters per data kind.
- Reconnect events should update status instead of disappearing silently.

### B-Data-5: Historical OHLCV design and implementation

- Design historical REST request/response/page/checkpoint semantics.
- Start with OHLCV because it is easiest to validate and most useful for initial backtesting.

### B-Data-6: Historical trades design and implementation

- Add historical public trade backfill.
- Align historical and realtime trade schemas.
- Add dedupe and checkpoint semantics.

## 13. Out of Scope for the Next `fdc-barter` Slice

The next barter module slice should not include:

- A-share, US equity, or non-crypto adapters.
- News, sentiment, or on-chain data.
- Durable storage engine routing.
- SQL query integration.
- Full historical order-book vendor integration.
- Full Barter-rs exchange coverage.
- Production-grade replay engine.
- Strategy engine or factor engine implementation.

These are important future areas, but the next module work should focus on crypto exchange market data requirements and the Barter-rs-backed realtime expansion path.

## 14. Acceptance Criteria

### AC-1: Requirements coverage

Given this document is used to guide development, when the next implementation plan is written, then it identifies which realtime and historical data kinds are in scope and which are out of scope.

### AC-2: Capability clarity

Given a developer opens the `fdc-barter` module docs, when they inspect the exchange/kind matrix, then they can determine which Barter-rs-backed data kinds are first-class targets for mdb.

### AC-3: Mapper gap clarity

Given a developer reviews current mapper status, when they compare it to this document, then they can see that trades are structured and L1/L2/candle/liquidation need structured mapping work.

### AC-4: Downstream alignment

Given factor research and backtesting needs, when data kinds are prioritized, then public trades, L1, L2, OHLCV, liquidations, funding, and open interest are clearly ordered by importance and implementation phase.

### AC-5: Boundary preservation

Given future `fdc-barter` work proceeds, when new data kinds are implemented, then `fdc-ingestion`, `fdc-transform`, and `fdc-storage` remain free of direct Barter-rs dependencies.

## 15. Recommended Next Step

Write an implementation plan for B-Data-1 and B-Data-2:

1. Add precise capability matrix tests for `fdc-barter`.
2. Extend `BarterMarketPayload` with structured L2 and liquidation payloads.
3. Add market type metadata.
4. Implement structured L1, L2, and liquidation mappers using offline synthetic Barter-rs events.
5. Preserve existing trade behavior and tests.

After these mappers are complete, live acquisition expansion can safely add more Barter-rs streams without sending raw placeholder payloads downstream.
