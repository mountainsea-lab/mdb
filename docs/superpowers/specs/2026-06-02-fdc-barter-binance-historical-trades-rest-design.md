# fdc-barter Binance Spot Historical Trades REST Design

Date: 2026-06-02
Module: `crates/fdc-adapter/barter`
Status: approved design for implementation

## Purpose

Add a Binance Spot historical public trades REST acquisition slice to `fdc-barter`. This follows the completed Binance Spot OHLCV REST execution pattern and keeps all exchange transport, parsing, and Barter-rs network integration inside the adapter crate.

The slice gives research and backtesting a historical trade source that maps into the same adapter-owned `TradePayload` used by realtime trades.

## Scope

In scope:

1. Binance Spot historical aggregated trade REST descriptor for `/api/v3/aggTrades`.
2. Offline parser/provider for Binance aggregate trade response rows.
3. Adapter-owned execution helper that fetches trades through `HistoricalRestExecutor`.
4. `BarterIntegrationHistoricalRestExecutor` support for the aggregate trades REST request using `barter-integration::RestClient`.
5. Offline contract tests and ignored/env-gated real REST smoke.

Out of scope:

- Orchestrator, storage, SQL, or API integration.
- Full multi-page backfill runner.
- Durable checkpoint persistence.
- Non-Binance exchanges.
- Futures trades.
- Default network tests.

## REST Endpoint Choice

Use Binance Spot `/api/v3/aggTrades` for this slice.

Reasons:

- Supports `symbol`, `startTime`, `endTime`, and `limit` query parameters.
- Does not require API-key authentication for public market data.
- Provides stable aggregate trade id `a`, price `p`, quantity `q`, first/last trade ids `f`/`l`, event time `T`, and buyer-maker flag `m`.
- Time-window query support aligns with existing `HistoricalBackfillRequest` fields better than `/api/v3/historicalTrades`, which is more id-oriented and commonly requires API key headers.

## Public API Shape

Reuse existing generic historical types:

- `HistoricalBackfillRequest`
- `HistoricalBackfillPage`
- `HistoricalRestRequestDescriptor`
- `HistoricalRestExecutor`
- `HistoricalProviderCapabilities`

Add Binance Spot historical trade-specific helpers:

```rust
pub fn binance_spot_historical_trades_capabilities() -> HistoricalProviderCapabilities;

pub fn binance_spot_historical_trades_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor>;

pub fn binance_spot_historical_trades_provider_from_response(
    response_body: &str,
) -> Result<BinanceSpotHistoricalTradesProvider>;

pub async fn execute_binance_spot_historical_trades_rest(
    executor: &dyn HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage>;
```

The provider type remains adapter-owned:

```rust
pub struct BinanceSpotHistoricalTradesProvider { ... }
```

## Mapping Rules

Each Binance aggregate trade row maps to one `BarterMarketEvent`:

```json
{
  "a": 26129,
  "p": "0.01633102",
  "q": "4.70443515",
  "f": 27781,
  "l": 27781,
  "T": 1498793709153,
  "m": true,
  "M": true
}
```

Mapping:

- `a` -> `TradePayload.trade_id = Some(a.to_string())`
- `p` -> `TradePayload.price`
- `q` -> `TradePayload.quantity`
- `T` milliseconds -> event `timestamp` in nanoseconds
- `m = true` means buyer is maker, so aggressor side is sell
- `m = false` means buyer is taker, so aggressor side is buy
- `exchange` -> normalized `binance_spot`
- `market_type` -> `BarterMarketType::Spot`
- `kind` -> `BarterMarketDataKind::Trade`
- `mode` -> `BarterMarketDataMode::Historical`
- envelope quality -> `is_backfill = true`

## Pagination and Cursor Semantics

This slice returns one page. A full runner is deferred.

Provider pagination behavior:

- `complete = true` when response rows are fewer than `request.limit.unwrap_or(default)`.
- `complete = false` when rows fill the requested limit.
- `next_cursor` points to the timestamp after the last trade event time, using `HistoricalCursor::next_start(...)`.
- `sequence` stores the aggregate trade id string.
- `historical_trade_dedupe_key()` remains the stable dedupe helper for `(exchange, symbol, trade_id)`.

## REST Execution

Extend the existing `BarterIntegrationHistoricalRestExecutor` pattern with private request/parser adapters for Binance aggregate trades:

- private `BinanceSpotAggTradesRestRequest` implements `barter_integration::protocol::http::rest::RestRequest`
- private query struct serializes Binance query params
- private parser uses `HttpParser` to deserialize success payloads and Binance API errors
- public helper `execute_binance_spot_historical_trades_rest()` converts the response into a provider-backed `HistoricalBackfillPage`

If `reqwest::Method` and `reqwest::StatusCode` must be named because `barter-integration` exposes them in trait signatures, `fdc-barter` may keep a minimal direct `reqwest` dependency for type references. HTTP execution still goes through `barter-integration::RestClient`.

## Error Handling

Use existing adapter errors:

- `InvalidHistoricalRequest` for malformed source/exchange/symbol/time/interval semantics.
- `UnsupportedHistoricalExchange` for non-Binance Spot exchange descriptors.
- `UnsupportedHistoricalSubscription` for unsupported kind, market type, interval, or limit.
- `HistoricalRest` for parser, transport, HTTP status, or Binance API error payload failures.

For trade requests, `interval` is not required.

## Testing Strategy

Default tests must not call the network.

Add offline tests for:

1. Descriptor shape for `/api/v3/aggTrades`.
2. Descriptor rejects unsupported market type, kind, exchange, and limit.
3. Parser maps aggregate trade rows into `TradePayload` fields and backfill envelopes.
4. Provider marks complete/next cursor correctly.
5. Fake executor feeds a response into `execute_binance_spot_historical_trades_rest()` without network.
6. Dedupe key remains compatible with parsed trade ids.

Add ignored smoke:

```bash
FDC_BARTER_HISTORICAL_SMOKE=1 cargo test -p fdc-barter \
  --test binance_spot_historical_trades_rest_contract \
  ignored_live_smoke_fetches_binance_spot_historical_trades -- --ignored --nocapture
```

The smoke is optional and may fail because of external network/API availability; default verification remains offline.

## Acceptance Criteria

1. `fdc-barter` non-ignored tests pass offline.
2. `barter-integration` remains the real REST networking path.
3. No downstream crate depends on `barter-integration`, `barter-data`, or `barter-instrument` because of this slice.
4. Binance aggregate trade rows map into adapter-owned `TradePayload` and `BarterIngestionEnvelope` values.
5. Historical trade dedupe keys use aggregate trade ids.
6. Development status records the completed slice.

## Self-Review

- Scope is narrow enough for one implementation plan.
- The design does not add storage/API/orchestrator work.
- Default tests remain offline.
- REST networking reuses Barter-rs network infrastructure through `barter-integration`.
- No placeholder requirements remain.
