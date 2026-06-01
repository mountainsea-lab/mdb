# fdc-barter Next Market Data Design

Date: 2026-06-01
Module: `crates/fdc-adapter/barter`
Status: design for next implementation slices

## Purpose

This design defines the next `fdc-barter` work after the completed expanded realtime Binance Spot market-data slice. It keeps `fdc-barter` focused on Barter-rs integration, adapter-owned models, acquisition boundaries, and adapter quality metadata. It does not move orchestration, storage, SQL, API, or production supervisor responsibilities into the adapter.

## Current Baseline

Implemented in `fdc-barter`:

- Market-data capability matrix through `supported_crypto_market_data_capabilities()`.
- `BarterMarketType` on `BarterMarketEvent`.
- Structured payloads for:
  - `TradePayload`
  - `OrderBookL1Payload`
  - `OrderBookPayload`
  - `LiquidationPayload`
- Offline mapper coverage for L1, L2 snapshot/update, liquidation, and market type.
- Expanded Binance Spot live subscriptions for trades, L1, and L2 through:
  - `LiveMarketDataSubscription`
  - `default_binance_spot_market_data_subscriptions()`
  - `init_binance_spot_market_data()`
  - `map_live_market_data_result()`
  - `collect_live_market_data_envelopes()`
- Backward-compatible trade-only live wrappers.
- Dependency boundary: Barter-rs crates remain under `crates/fdc-adapter/barter`.

## Scope

The next `fdc-barter` module work is split into narrow, independently testable slices:

1. Candle/OHLCV model completion and mapper contract.
2. Binance Futures USD live market-data subscription initialization for trades, L1, L2, and liquidations.
3. Historical OHLCV backfill design and offline boundary contracts.
4. Historical public trades backfill design and offline boundary contracts.
5. Adapter-side quality/runtime metadata boundary for reconnect, gap, duplicate, and latency visibility.

## Non-goals

These remain outside `fdc-barter`:

- Durable storage routing.
- SQL query execution.
- Factor engine or strategy engine.
- Production live actor/supervisor lifecycle.
- API route behavior.
- `fdc-ingestion`, `fdc-transform`, or `fdc-storage` Barter-rs dependencies.
- Full exchange coverage across every matrix row in one slice.

## Design Principles

- Keep all Barter-rs types and exchange-specific initialization inside `fdc-barter`.
- Keep downstream handoff through `BarterIngestionEnvelope` and adapter-owned model types.
- Use offline synthetic Barter-rs events for mapper and model tests.
- Keep network-dependent smoke tests ignored and environment-gated.
- Preserve existing public trade-only APIs until downstream callers have fully migrated.
- Add historical support through request/page/checkpoint abstractions before real REST calls.

## Slice 1: Candle/OHLCV Model Completion

Extend `CandlePayload` with optional fields needed by research and backtesting:

```rust
pub struct CandlePayload {
    pub interval: Option<String>,
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: DecimalQuantity,
    pub trade_count: Option<u64>,
    pub quote_volume: Option<DecimalQuantity>,
}
```

`interval`, `trade_count`, and `quote_volume` are optional because live Barter-rs candle support and exchange-specific fields may vary. The mapper should convert `DataKind::Candle` into `BarterMarketPayload::Candle` only for fields that Barter-rs exposes. If a required numeric value is invalid, return `BarterAdapterError::InvalidNumericValue`.

## Slice 2: Binance Futures USD Live Expansion

Add futures-specific live subscription helpers without breaking the existing Binance Spot helpers:

- `default_binance_futures_usd_market_data_subscriptions()`
- `init_binance_futures_usd_market_data()`

The default futures USD set should include BTC/USDT and ETH/USDT for:

- trades
- L1
- L2
- liquidations

The function should reject unsupported kinds with `BarterAdapterError::UnsupportedLiveSubscription`. Normal tests should compile ignored smoke tests without connecting to the network. Real live validation must remain gated by `FDC_BARTER_LIVE_SMOKE=1`.

## Slice 3: Historical OHLCV Backfill Boundary

Add adapter-owned historical OHLCV page types before implementing exchange REST calls:

- request validation
- page result shape
- checkpoint progression
- backfill quality flags
- completeness metadata

Start with a single exchange target in design, preferably Binance Spot, because OHLCV is the fastest path to useful research and baseline backtesting. Default tests must use an offline fake provider and must not call the network.

## Slice 4: Historical Public Trades Backfill Boundary

After OHLCV boundary is stable, add historical trades page semantics:

- query by exchange, market type, symbol, start, end
- pagination by cursor, timestamp, or trade id
- dedupe key declaration, preferably `(exchange, symbol, trade_id)` when `trade_id` exists
- event-time preservation
- backfill/replay quality flags

This slice should align historical trade records with existing realtime `TradePayload`.

## Slice 5: Adapter Quality and Runtime Metadata Boundary

Expose adapter-level observations without owning the production supervisor:

- reconnect observations
- last event timestamps by data kind
- per-kind emitted envelope counters
- invalid numeric count
- duplicate candidate count
- gap/out-of-order markers when sequence information is available
- event latency and processing latency helper calculations

`fdc-server` may later aggregate these into production status, but `fdc-barter` should define the adapter-owned event/metric boundary.

## Acceptance Criteria

1. `fdc-barter` tests pass with no network by default.
2. Network smoke tests are ignored unless the required environment variable is set.
3. Barter-rs dependency grep outside `crates/fdc-adapter/barter` has no output.
4. Candle payload extensions do not break existing trade/L1/L2/liquidation mappings.
5. Binance Spot APIs remain source-compatible.
6. Binance Futures USD live initialization compiles and validates unsupported subscriptions offline.
7. Historical OHLCV and trades boundaries are testable offline before real REST calls.
8. Development status is updated after each completed slice.

## Recommended Implementation Order

1. Candle/OHLCV model completion and mapper contract.
2. Binance Futures USD live expansion.
3. Historical OHLCV boundary design and contracts.
4. Historical trades boundary design and contracts.
5. Adapter quality/runtime metadata boundary.

## Self-Review

- Scope is limited to `fdc-barter` adapter responsibilities.
- Cross-layer runtime, storage, SQL, and API work are explicitly excluded.
- Each slice can be tested offline and committed separately.
- Public API compatibility is preserved for existing Binance Spot trade callers.
