# fdc-barter Multi-Exchange Historical REST Acquisition Design

Date: 2026-06-01
Module: `crates/fdc-adapter/barter`
Status: design for implementation

## Purpose

Design a historical market-data acquisition boundary for `fdc-barter` that is inspired by Barter-rs and reuses its best network-layer ideas where practical. The goal is to support multiple exchanges over time without leaking Barter-rs or exchange REST types outside `fdc-barter`.

The first implementation slice should stay small: add the multi-exchange provider and registry boundary, prove it offline with contracts, and keep the first concrete runtime target as Binance Spot OHLCV. Later slices can add real REST execution and additional exchanges.

## Barter-rs Reference Points

`barter-data` provides mature multi-exchange live WebSocket structure:

- `Subscription<Exchange, Instrument, Kind>` models exchange + instrument + data kind.
- `Connector` maps Barter subscriptions into exchange-specific channels and markets.
- `StreamSelector` selects the exchange-specific stream implementation for a subscription kind.
- `MultiStreamBuilder` combines multiple exchange stream builders into a common stream output.

`barter-integration` provides the more relevant generic network layer for historical REST:

- `DataArgs<Historical, Subs, Config>` separates mode, subscriptions, and source configuration.
- `RestRequest` describes an HTTP method, path, query params, body, timeout, and response type.
- `RestClient<Strategy, Parser>` executes HTTP requests against a base URL.
- `BuildStrategy` handles public vs signed/private request construction.
- `HttpParser` deserializes success payloads and maps exchange API errors.
- `PublicNoHeaders` is the simplest build strategy for public market-data endpoints.
- `SocketError::{Http, HttpTimeout, HttpResponse}` already models the main HTTP failure classes.

`fdc-barter` should follow these shapes but keep adapter-owned public models.

## Scope

In scope for this design:

1. Multi-exchange historical provider abstraction.
2. Provider registry that routes `HistoricalBackfillRequest` by exchange.
3. Provider capability metadata for exchange, market type, data kind, interval support, and limit bounds.
4. REST-boundary types that mirror Barter-rs `RestRequest` and can later be backed by `barter-integration::protocol::http`.
5. Offline contract tests for routing, capability rejection, request validation, and page outcome semantics.
6. A Binance Spot OHLCV provider boundary as the first concrete target.

Out of scope for the first implementation slice:

- Durable storage writes.
- API routes.
- SQL/query integration.
- Production supervisor behavior.
- Network calls in default tests.
- Adding direct Barter-rs dependencies outside `crates/fdc-adapter/barter`.
- Implementing every exchange listed in Barter-rs.

## Architecture

```text
caller inside fdc-barter/server later
  -> HistoricalProviderRegistry
      -> HistoricalExchangeProvider trait object
          -> exchange-specific provider implementation
              -> adapter-owned REST request descriptor
              -> later: barter-integration RestClient<BuildStrategy, HttpParser>
          -> BarterMarketEvent
          -> BarterIngestionEnvelope
```

The registry owns routing and high-level validation. Providers own exchange-specific pagination, URL/query construction, REST response parsing, and conversion to adapter-owned events.

## Public Adapter-Owned Types

The existing `HistoricalBackfillRequest`, `HistoricalBackfillPage`, `HistoricalPageOutcome`, and `HistoricalBackfillSource` remain source-compatible.

Add the following adapter-owned types:

```rust
pub struct HistoricalProviderCapabilities {
    pub exchange: String,
    pub market_types: Vec<BarterMarketType>,
    pub kinds: Vec<BarterMarketDataKind>,
    pub intervals: Vec<String>,
    pub max_limit: Option<usize>,
}

pub trait HistoricalExchangeProvider: Send + Sync {
    fn capabilities(&self) -> &HistoricalProviderCapabilities;
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage>;
}

pub struct HistoricalProviderRegistry { ... }
```

`HistoricalBackfillSource` can be implemented by `HistoricalProviderRegistry` so existing callers can use the new multi-exchange route without changing call sites.

## REST Boundary Shape

The first implementation should not require network I/O, but it should make later network implementation straightforward. Add an internal or public adapter-owned request descriptor such as:

```rust
pub struct HistoricalRestRequestDescriptor {
    pub exchange: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub timeout_ms: u64,
}
```

This descriptor mirrors Barter-rs `RestRequest` without exposing `reqwest` or `barter-integration` types. A later concrete provider may add private implementation types that implement Barter-rs `RestRequest` and execute them with `RestClient<PublicNoHeaders, Parser>`.

For Binance Spot OHLCV, the descriptor should be able to represent:

- base URL: provider-owned, not required in the descriptor
- method: `GET`
- path: `/api/v3/klines`
- query: `symbol`, `interval`, `startTime`, `endTime`, `limit`

## Routing and Capability Rules

The registry should:

1. Validate the generic historical request before provider I/O.
2. Find a provider by normalized `request.exchange`.
3. Ask provider capabilities whether the requested market type and data kind are supported.
4. Reject unsupported intervals for candle requests.
5. Reject limits above provider `max_limit` when present.
6. Delegate to the provider only after these checks pass.

Errors should use explicit `BarterAdapterError` variants:

- `UnsupportedHistoricalExchange(String)` for missing provider.
- `UnsupportedHistoricalSubscription(String)` for unsupported market type, kind, interval, or limit.
- Existing `InvalidHistoricalRequest(String)` for malformed request fields.
- Later REST execution can use `HistoricalRest(String)` or map Barter `SocketError` into an adapter error without leaking Barter-rs error types.

## Pagination and Checkpoints

Providers return one `HistoricalBackfillPage` at a time.

For OHLCV, page progression should use event time:

- `next_cursor` should point to the next start timestamp after the last returned candle close/open boundary, depending on exchange semantics.
- `complete = true` means the requested range is exhausted.
- Empty pages are allowed only when `complete = true` or when the provider returns a next cursor that guarantees progress.

For historical trades, providers may use trade IDs, event timestamps, or opaque exchange cursors. The existing `HistoricalCursor` remains the adapter-owned cursor carrier.

## Data Mapping

Provider implementations must map exchange REST responses into adapter-owned `BarterMarketEvent` and then `BarterIngestionEnvelope::from_backfill_event`.

The public boundary remains:

- `BarterMarketEvent`
- `BarterMarketPayload::{Candle, Trade, ...}`
- `BarterIngestionEnvelope`
- `DataQualityFlags` with `is_backfill = true`

## Testing Strategy

Default tests must not call the network.

Contract tests should prove:

1. Registry dispatches a request to the matching provider.
2. Registry rejects unknown exchanges.
3. Registry rejects unsupported kind/market/interval/limit using adapter errors.
4. Binance Spot OHLCV descriptor creates the expected Barter-rs-style REST request shape.
5. Fake provider pages preserve request, envelopes, cursor, and completeness metadata.
6. `HistoricalProviderRegistry` implements `HistoricalBackfillSource` for source compatibility.
7. Barter-rs dependency grep outside `crates/fdc-adapter/barter` remains empty.

Ignored smoke tests may later use `FDC_BARTER_HISTORICAL_SMOKE=1` to execute public REST requests.

## Implementation Order

1. Add capabilities and provider trait.
2. Add registry and source-trait implementation.
3. Add unsupported historical error variants.
4. Add Binance Spot OHLCV REST descriptor builder and tests.
5. Add offline fake-provider contracts.
6. Re-export new adapter-owned historical types.
7. Update development status.

## Compatibility

Existing tests and callers using `HistoricalBackfillSource` should continue to compile. New types extend the historical module without changing `HistoricalBackfillRequest` field names or semantics.

## Self-Review

- No Barter-rs types are exposed outside `fdc-barter` public API.
- The design uses Barter-rs network concepts but does not depend on unfinished upstream historical APIs.
- First implementation slice is offline-testable.
- Scope remains inside `fdc-barter` and does not include storage, API, SQL, or production runtime work.
