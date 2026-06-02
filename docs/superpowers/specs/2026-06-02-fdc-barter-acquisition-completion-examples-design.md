# fdc-barter Acquisition Completion and Examples Design

Date: 2026-06-02
Module: `crates/fdc-adapter/barter`
Status: approved design for implementation planning

## Purpose

Complete the `fdc-barter` internal data-acquisition loop before connecting storage or API layers. The module should be able to collect each currently supported live data type for a bounded number of envelopes, run historical Binance Spot backfills across multiple REST pages, report acquisition summaries, and provide examples for each supported acquisition type.

This design intentionally keeps the scope inside `fdc-barter`. It does not write to storage, does not add API endpoints, and does not introduce dependencies from downstream crates onto Barter-rs internals.

## Current Supported Data Sources

### Live

`fdc-barter` already supports live Barter-rs stream initialization and mapping for:

- Binance Spot
  - trades
  - order book L1
  - order book L2
- Binance Futures USD perpetual
  - trades
  - order book L1
  - order book L2
  - liquidations

Default live subscriptions cover BTC/USDT and ETH/USDT. Custom subscriptions are supported through `LiveMarketDataSubscription`.

### Historical

`fdc-barter` already supports single-page historical REST execution for:

- Binance Spot OHLCV via `/api/v3/klines`
- Binance Spot historical aggregate trades via `/api/v3/aggTrades`

Both paths use adapter-owned descriptors and execute network I/O through `barter-integration::RestClient` via `BarterIntegrationHistoricalRestExecutor`.

## Scope

In scope:

1. Add a focused acquisition layer inside `fdc-barter`.
2. Add bounded live collection with summary metadata.
3. Add multi-page historical backfill runner for existing Binance Spot OHLCV and historical trades page fetchers.
4. Add adapter-owned outcome structs for live and historical acquisition.
5. Add offline tests using fake live streams and fake historical page fetchers/executors.
6. Add examples for every currently supported acquisition data type.
7. Update the `fdc-barter` capability matrix so Binance Spot advertises implemented historical kinds.
8. Keep all default tests offline and network-free.

Out of scope:

- Storage writes.
- API endpoints.
- Query integration.
- Durable checkpoint persistence.
- Database schemas.
- Unbounded production daemons.
- New exchange support.
- Futures historical REST.
- Live candle streams.

## Architecture

Add a new module:

```text
crates/fdc-adapter/barter/src/ingestion/acquisition.rs
```

This module owns bounded acquisition orchestration while reusing existing lower-level pieces:

- live stream mapping and collection from `ingestion/live.rs`
- historical page request/response/provider/executor types from `ingestion/historical.rs`
- adapter event/envelope/checkpoint models from `model/*`

The acquisition layer should not expose Barter-rs stream types in its outcome structs. It may accept generic `Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>>` for testable live collection because that is already the current live boundary, but user-facing examples should call existing initializer helpers.

## Public API Shape

### Live acquisition

Add request and outcome structs:

```rust
pub struct LiveCollectionRequest {
    pub source_id: String,
    pub limit: usize,
}

pub struct LiveCollectionOutcome {
    pub source_id: String,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub records_received: usize,
    pub requested_limit: usize,
    pub complete: bool,
    pub skipped_reconnects: usize,
}
```

Add helper:

```rust
pub async fn collect_live_envelopes_with_summary<S>(
    request: LiveCollectionRequest,
    stream: S,
) -> Result<LiveCollectionOutcome>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin;
```

Behavior:

- Collect up to `request.limit` emitted envelopes.
- Skip reconnect events.
- Count skipped reconnects.
- Stop early if the stream ends.
- `complete = records_received == requested_limit`.
- Reject `limit == 0` with `InvalidHistoricalRequest` or a new acquisition-specific error if added.

### Historical acquisition

Add request and outcome structs:

```rust
pub struct HistoricalBackfillRunRequest {
    pub first_request: HistoricalBackfillRequest,
    pub max_pages: usize,
    pub max_records: Option<usize>,
}

pub struct HistoricalBackfillRunOutcome {
    pub pages: Vec<HistoricalBackfillPage>,
    pub records_received: usize,
    pub final_cursor: Option<HistoricalCursor>,
    pub complete: bool,
    pub stopped_reason: HistoricalBackfillStopReason,
}

pub enum HistoricalBackfillStopReason {
    SourceComplete,
    MaxPagesReached,
    MaxRecordsReached,
}
```

Add a generic fetcher trait:

```rust
#[async_trait]
pub trait HistoricalPageFetcher: Send + Sync {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage>;
}
```

Add runner:

```rust
pub async fn run_historical_backfill_pages(
    fetcher: &dyn HistoricalPageFetcher,
    request: HistoricalBackfillRunRequest,
) -> Result<HistoricalBackfillRunOutcome>;
```

Behavior:

- Validate `max_pages > 0`.
- Start with `first_request`.
- Fetch one page at a time.
- Append each page to the outcome.
- Stop when the page is `complete`.
- Stop when `max_pages` is reached.
- Stop when `max_records` is reached or exceeded.
- For the next request, copy the previous request and apply `page.next_cursor.next_start` to `request.start`.
- Preserve request fields such as source, exchange, market type, symbol, kind, interval, end, and limit.
- Set `final_cursor` to the latest page cursor if any.

### Binance Spot historical fetchers

Add small fetcher adapters that reuse existing executor helpers:

```rust
pub struct BinanceSpotOhlcvHistoricalPageFetcher<'a> {
    pub executor: &'a dyn HistoricalRestExecutor,
}

pub struct BinanceSpotTradesHistoricalPageFetcher<'a> {
    pub executor: &'a dyn HistoricalRestExecutor,
}
```

Each implements `HistoricalPageFetcher` by calling:

- `execute_binance_spot_ohlcv_rest()`
- `execute_binance_spot_historical_trades_rest()`

These types keep REST networking reuse through `barter-integration` and keep exchange-specific behavior inside `fdc-barter`.

## Capability Matrix Update

Update `supported_crypto_market_data_capabilities()` so `binance_spot` reports implemented historical kinds:

- `BarterMarketDataKind::Candle`
- `BarterMarketDataKind::Trade`

No other exchanges should claim historical support in this slice.

## Examples

Create examples under `crates/fdc-adapter/barter/examples/`.

All examples that can call real exchanges must be gated by environment variables. If the variable is missing, the example should print a short message and exit successfully.

Required examples:

1. `live_binance_spot_trades.rs`
   - Starts Binance Spot public trades for BTC/USDT.
   - Collects a small bounded count.
   - Prints event summaries.

2. `live_binance_spot_order_books.rs`
   - Starts Binance Spot L1 and L2 for BTC/USDT.
   - Collects a small bounded count.
   - Prints kind, symbol, sequence, and timestamp.

3. `live_binance_futures_usd_market_data.rs`
   - Starts Binance Futures USD perpetual trades, L1, L2, and liquidations for BTC/USDT.
   - Collects a small bounded count.
   - Prints event summaries.

4. `historical_binance_spot_ohlcv.rs`
   - Runs a bounded Binance Spot OHLCV backfill across one or more pages.
   - Uses `BarterIntegrationHistoricalRestExecutor`.
   - Prints page count, record count, final cursor, and sample candle summaries.

5. `historical_binance_spot_trades.rs`
   - Runs a bounded Binance Spot aggregate-trades backfill across one or more pages.
   - Uses `BarterIntegrationHistoricalRestExecutor`.
   - Prints page count, record count, final cursor, and sample trade summaries.

Environment variables:

- Live examples require `FDC_BARTER_LIVE_EXAMPLE=1`.
- Historical examples require `FDC_BARTER_HISTORICAL_EXAMPLE=1`.

Default commands should compile examples without running network calls:

```bash
CARGO_NET_OFFLINE=true cargo test -p fdc-barter --examples --no-run
```

## Error Handling

Use existing adapter error conventions where possible:

- Invalid bounded acquisition settings, such as zero limits or zero max pages, should return an adapter error with a clear message.
- Live stream item errors should continue to return `BarterAdapterError::LiveStreamItem` through existing mapping behavior.
- Historical page fetch failures should propagate unchanged.
- Missing `next_cursor` on an incomplete page should stop with `SourceComplete` only if the page reports `complete`; otherwise it should return a clear historical/acquisition error because the runner cannot advance safely.

## Testing Strategy

Default tests remain offline.

Add tests for:

1. Live acquisition summary collects up to a limit.
2. Live acquisition summary skips reconnect events and counts them.
3. Live acquisition rejects zero limit.
4. Historical runner stops on source completion.
5. Historical runner stops on max pages.
6. Historical runner stops on max records.
7. Historical runner advances `start` from `next_cursor.next_start`.
8. Historical runner rejects zero max pages.
9. Binance Spot OHLCV fetcher delegates to existing executor helper using a fake executor.
10. Binance Spot trades fetcher delegates to existing executor helper using a fake executor.
11. Capability matrix reports Binance Spot historical candle and trade support.
12. Examples compile offline with `--examples --no-run`.

## Acceptance Criteria

1. `fdc-barter` exposes a bounded live acquisition helper with summaries.
2. `fdc-barter` exposes a multi-page historical backfill runner.
3. Binance Spot OHLCV and historical trades can use the runner through fetcher adapters.
4. Default tests remain offline and pass.
5. Examples exist for every currently supported live/historical acquisition data type.
6. Network examples are environment-gated and exit successfully when not enabled.
7. Capability matrix accurately advertises Binance Spot historical candle/trade support.
8. No storage/API/database integration is added in this slice.
9. Barter-rs and exchange details remain isolated inside `fdc-barter`.

## Verification Commands

Default verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --examples --no-run
```

Optional live examples:

```bash
FDC_BARTER_LIVE_EXAMPLE=1 cargo run -p fdc-barter --example live_binance_spot_trades
FDC_BARTER_LIVE_EXAMPLE=1 cargo run -p fdc-barter --example live_binance_spot_order_books
FDC_BARTER_LIVE_EXAMPLE=1 cargo run -p fdc-barter --example live_binance_futures_usd_market_data
```

Optional historical examples:

```bash
FDC_BARTER_HISTORICAL_EXAMPLE=1 cargo run -p fdc-barter --example historical_binance_spot_ohlcv
FDC_BARTER_HISTORICAL_EXAMPLE=1 cargo run -p fdc-barter --example historical_binance_spot_trades
```
