# FDC Multi-source Data Adapter Architecture Design

Date: 2026-05-31
Branch: `mdb-mqdev`
Status: design proposal

## 1. Purpose

This document defines the target architecture for the Financial Data Center multi-source data adapter layer.

The immediate implementation target remains crypto realtime market-data collection through the existing Barter-rs integration. The architecture must also leave explicit, stable extension points for future A-share, US equity, futures, options, news, sentiment, on-chain, reference-data, and custom sources.

The primary design goal is to introduce a reusable adapter/runtime boundary without breaking the responsibilities already established across the current workspace crates.

## 2. Current Project Context

The repository already has useful crate boundaries that should be preserved.

### 2.1 `fdc-adapter/barter`

Current responsibility:

- Own the Barter-rs dependency boundary.
- Own Barter-specific request, event, envelope, checkpoint, and capability models.
- Convert Barter-rs market stream output into `BarterIngestionEnvelope`.
- Keep Barter-rs acquisition details out of ingestion, transform, storage, API, and server core.

Current important types and functions:

- `BarterMarketDataRequest`
- `BarterMarketEvent`
- `BarterMarketPayload`
- `BarterIngestionEnvelope`
- `BarterSourceCapabilities`
- `LiveTradeSubscription`
- `init_binance_spot_public_trades`
- `public_trade_result_to_data_kind`
- `map_live_trade_result`
- `collect_live_trade_envelopes`

Current limitation:

- The live acquisition boundary is crypto/Barter-specific and currently supports only the first Binance Spot public-trade slice.
- Continuous production runtime concerns are partially implemented in `fdc-server`, not represented as a reusable adapter runtime.

### 2.2 `fdc-ingestion`

Current responsibility:

- Own generic source-envelope and validation primitives.
- Stay independent of concrete adapters such as Barter.
- Provide generic source path building blocks, not real network acquisition.

Current important types:

- `SourceEnvelope<T>`
- `SourceMetadata`
- `SourceType`
- `SourceCheckpoint`
- `SourceQualityFlags`
- `SourceValidator`
- `SourceBatchProcessor<T>`
- `run_source_pipeline_once`

Boundary rule:

- `fdc-ingestion` must not depend on `fdc-barter`, future A-share adapters, future US equity adapters, news adapters, or on-chain adapters.

### 2.3 `fdc-transform`

Current responsibility:

- Own neutral DTO boundaries for downstream transformation.
- Current implemented domain is market data through `MarketDataDto` and related payload DTOs.
- Must not embed adapter-specific mapping logic.

Current important types:

- `MarketDataDto`
- `MarketDataKind`
- `MarketDataPayload`
- `TradeDto`
- `OrderBookL1Dto`
- `CandleDto`
- `TransformQualityFlags`
- `MarketDataTransformSink`

Current limitation:

- Non-market-data DTO domains such as news, sentiment, on-chain events, and reference data are not yet modeled.

### 2.4 `fdc-storage`

Current responsibility:

- Own storage write boundaries and queryable MVP store.
- Stay independent of adapter, ingestion, transform, orchestrator, API, and server crates.

Current important types:

- `StorageWriteRecord`
- `StorageWriteBatch`
- `StorageWriteSink`
- `RecordingStorageSink`
- `QueryableMarketDataStore`
- `MarketDataQuery`

Boundary rule:

- `fdc-storage` must not depend on `fdc-transform`, `fdc-ingestion`, `fdc-barter`, future adapters, or `fdc-server`.

### 2.5 `fdc-orchestrator`

Current responsibility:

- Own bounded cross-layer glue.
- Convert Barter adapter envelopes into generic source envelopes.
- Convert source envelopes into neutral market-data DTOs.
- Convert DTOs into storage write records.

Current important functions:

- `barter_envelope_to_source_envelope`
- `barter_event_to_market_data_dto`
- `market_data_dto_to_storage_record`
- `run_barter_envelopes_to_storage_once`

Boundary rule:

- Concrete cross-layer mapping belongs here or in future dedicated orchestration/integration crates, not in adapter, ingestion, transform, storage, or server internals.

### 2.6 `fdc-server`

Current responsibility:

- Own production application assembly and HTTP runtime.
- Hold runtime state, in-memory MVP market-data store, supervisor state, and production routes.
- Provide live market-data control routes.

Current production routes:

- `GET /health`
- `GET /ready`
- `POST /market-data/live/start`
- `GET /market-data/live/status`
- `POST /market-data/live/stop`
- `GET /market-data/trades`

Current limitation:

- The production live runner still knows too much about the current Barter/Binance implementation.
- The background live runner uses repeated bounded collection cycles rather than a durable source runtime/actor that owns continuous stream consumption.

## 3. External Reference Analysis

### 3.1 MarketBridge

MarketBridge demonstrates a useful application-level data collection pattern:

- `ExchangeSource` trait for source adapters.
- `SourceRuntime` that spawns one task per source.
- `SourceContext` that gives sources a channel, metrics, and backpressure behavior.
- Stop through `CancellationToken`.
- Restart loop with exponential backoff.
- `EventRouter` that receives source events and publishes to an event bus, aggregator, and metrics.
- `EventBus` that keeps domain snapshots and offers API reads.
- Configuration-driven source registry.

What mdb should borrow:

- Runtime-owned lifecycle and restart semantics.
- Source context with explicit backpressure.
- Registry-driven source startup.
- Router-style separation between acquisition and downstream consumers.
- Status and metrics as first-class runtime outputs.

What mdb should not copy directly:

- MarketBridge's concrete event model, because mdb already has `SourceEnvelope`, `MarketDataDto`, storage write boundaries, and orchestrator glue.
- MarketBridge's direct bus snapshot design as the only storage model, because mdb already has `QueryableMarketDataStore` and future tier-aware storage plans.

### 3.2 Barter-rs

Barter-rs already provides the crypto exchange connector layer:

- `Streams` and `StreamBuilder` for exchange market streams.
- `MultiStreamBuilder` for combining multiple subscription-kind streams.
- `DynamicStreams` for dynamic `ExchangeId + SubKind` subscription batches.
- Reconnect primitives:
  - `init_market_stream`
  - `init_reconnecting_stream`
  - `with_reconnect_backoff`
  - `with_termination_on_error`
  - `with_reconnection_events`
  - socket-level `init_reconnecting_socket`
- Standard event model:
  - `MarketEvent { time_exchange, time_received, exchange, instrument, kind }`
  - `DataKind::{Trade, OrderBookL1, OrderBook, Candle, Liquidation}`
- Subscription validation, deduplication, and exchange/instrument-kind support checks.
- Existing exchange support including Binance, Bybit, OKX, Kraken, Coinbase, Gateio, Bitfinex, and Bitmex.

What mdb should reuse:

- Barter-rs exchange WebSocket connectors.
- Barter-rs subscription and reconnection behavior.
- Barter-rs market-data event normalization as the upstream crypto input boundary.

What mdb should own:

- Application runtime lifecycle.
- Source registry and source status snapshots.
- Mapping from Barter-rs event semantics into mdb source envelopes, DTOs, and storage writes.
- Operator-facing API semantics.
- Cross-source adapter extension model.

## 4. Architectural Principles

1. Concrete adapter crates own vendor SDK boundaries.
2. Generic ingestion primitives never depend on concrete adapters.
3. Transform DTOs are neutral domain boundaries, not adapter-specific models.
4. Storage remains generic and storage-owned.
5. Orchestrator owns cross-layer glue and concrete mappings.
6. Server owns application assembly, route compatibility, and runtime process state, but should not embed vendor-specific stream logic.
7. Adapter runtime must support multiple source domains even if the first implementation only enables crypto market data.
8. First implementation must preserve the accepted realtime crypto MVP behavior.
9. Default tests remain offline; live smoke tests remain gated.

## 5. Proposed Module Structure

The preferred long-term shape is to introduce a generic adapter core boundary and keep the existing Barter adapter as the first provider.

```text
crates/
  fdc-adapter/
    core/                  # proposed generic adapter abstractions, if split into a crate
    barter/                # existing Barter-rs crypto adapter
    future providers...    # A-share, US equity, futures, news, on-chain, etc.
```

Because the current workspace already has `crates/fdc-adapter/barter`, there are two viable implementation layouts:

### Option 1: New workspace crate `fdc-adapter-core`

```text
crates/fdc-adapter/core
```

Pros:

- Clear generic boundary.
- Concrete adapters can depend on it.
- Server/orchestrator can depend on core abstractions without depending on Barter.

Cons:

- Adds another workspace crate.

### Option 2: Add generic modules inside `fdc-orchestrator` or `fdc-server`

Pros:

- Less initial crate churn.

Cons:

- Blurs responsibilities.
- Makes it harder for future adapter crates to share the same runtime model.
- Risks turning `fdc-server` into the generic adapter platform.

Recommendation: Option 1, a generic adapter core crate. If the implementation wants to minimize workspace churn, the same types can first be introduced as a small module, but the design target should remain a separate adapter core boundary.

## 6. Adapter Core Model

The adapter core should be vendor-neutral and domain-aware.

### 6.1 Source domains

```rust
pub enum AdapterDomain {
    MarketData,
    ReferenceData,
    News,
    Sentiment,
    OnChain,
    Custom(String),
}
```

### 6.2 Markets

```rust
pub enum MarketRegion {
    Crypto,
    AShare,
    USEquity,
    Futures,
    Options,
    FX,
    GlobalMacro,
    OnChain,
    Custom(String),
}
```

`MarketRegion` describes the business market or source family, not the transport protocol.

### 6.3 Modes

```rust
pub enum SourceMode {
    Live,
    Historical,
    Replay,
    Snapshot,
    Polling,
}
```

The first implementation only needs `Live` for crypto trades. `Polling` is reserved for REST-style sources such as news, sentiment, and some reference data.

### 6.4 Data kinds

The adapter core should not force all source kinds into `MarketDataKind`. A generic `SourceDataKind` should identify the broad business kind.

```rust
pub enum SourceDataKind {
    Trade,
    OrderBookL1,
    OrderBookL2,
    Candle,
    Liquidation,
    Quote,
    ReferenceInstrument,
    NewsArticle,
    SentimentScore,
    OnChainTransfer,
    OnChainSwap,
    Custom(String),
}
```

Market-data kinds can later map to `fdc-transform::MarketDataKind`. News, sentiment, and on-chain kinds should map to future domain DTOs, not `MarketDataDto`.

### 6.5 Subscription model

```rust
pub struct SourceSubscription {
    pub source_id: String,
    pub adapter_id: String,
    pub domain: AdapterDomain,
    pub market: MarketRegion,
    pub exchange: Option<String>,
    pub symbols: Vec<String>,
    pub kinds: Vec<SourceDataKind>,
    pub mode: SourceMode,
    pub attributes: BTreeMap<String, String>,
}
```

Examples:

```text
barter crypto live trades:
  source_id = crypto-barter-binance-spot-trades
  adapter_id = barter_crypto
  domain = MarketData
  market = Crypto
  exchange = binance_spot
  symbols = [BTCUSDT, ETHUSDT]
  kinds = [Trade]
  mode = Live

future A-share quote source:
  source_id = ashare-vendor-x-level1
  adapter_id = ashare_vendor_x
  domain = MarketData
  market = AShare
  exchange = sse/szse or none depending on vendor
  symbols = [600519.SH, 000001.SZ]
  kinds = [Quote, Trade]
  mode = Live or Polling

future news source:
  source_id = news-vendor-y-global
  adapter_id = news_vendor_y
  domain = News
  market = GlobalMacro
  exchange = none
  symbols = [] or topic tickers
  kinds = [NewsArticle]
  mode = Polling or Live

future on-chain source:
  source_id = onchain-ethereum-transfers
  adapter_id = ethereum_rpc
  domain = OnChain
  market = OnChain
  exchange = none
  symbols = [ETH, USDC] or contract addresses
  kinds = [OnChainTransfer, OnChainSwap]
  mode = Live
```

### 6.6 Capabilities

```rust
pub struct AdapterCapabilities {
    pub adapter_id: String,
    pub domains: Vec<AdapterDomain>,
    pub markets: Vec<MarketRegion>,
    pub modes: Vec<SourceMode>,
    pub data_kinds: Vec<SourceDataKind>,
    pub exchanges: Vec<String>,
    pub supports_dynamic_subscriptions: bool,
    pub supports_backfill: bool,
    pub supports_replay: bool,
    pub rate_limits: Vec<AdapterRateLimitRule>,
}
```

Capabilities must be queryable by server/API and testable without live network access.

## 7. Adapter Runtime Model

The adapter runtime should own lifecycle, not concrete adapters and not low-level ingestion primitives.

### 7.1 Commands

```rust
pub enum SourceCommand {
    Start(SourceSubscription),
    Stop { source_id: String, reason: String },
    Reconfigure(SourceSubscription),
    QueryStatus { source_id: String },
}
```

`Reconfigure` is reserved for later. The first implementation may reject it explicitly.

### 7.2 Events

```rust
pub enum SourceRuntimeEvent<T> {
    Data(SourceEnvelope<T>),
    Heartbeat(SourceHeartbeat),
    Reconnecting(SourceReconnectEvent),
    Error(SourceErrorEvent),
    Stopped(SourceStoppedEvent),
}
```

In the first Barter slice, the data payload may remain `BarterMarketEvent` after conversion from `BarterIngestionEnvelope` through orchestration. The generic runtime should not require all adapters to output the same payload type in the same channel until a domain DTO router exists.

A practical first implementation can use an adapter-owned stream that is consumed by an orchestrator-specific runner:

```text
Barter stream -> BarterIngestionEnvelope -> fdc-orchestrator -> StorageWriteRecord
```

The generic event model should be designed, but only the Barter path needs to be implemented first.

### 7.3 Status

```rust
pub enum SourceRuntimeState {
    Idle,
    Starting,
    Running,
    Reconnecting,
    Stopping,
    Stopped,
    Completed,
    Failed,
}
```

```rust
pub struct SourceRuntimeStatus {
    pub source_id: String,
    pub adapter_id: String,
    pub state: SourceRuntimeState,
    pub task_id: Option<String>,
    pub subscription: Option<SourceSubscription>,
    pub started_at_ns: Option<u64>,
    pub stopped_at_ns: Option<u64>,
    pub last_event_at_ns: Option<u64>,
    pub last_error: Option<String>,
    pub stop_reason: Option<String>,
    pub counters: SourceRuntimeCounters,
}
```

```rust
pub struct SourceRuntimeCounters {
    pub events_received: u64,
    pub events_forwarded: u64,
    pub events_dropped: u64,
    pub errors_observed: u64,
    pub reconnects_observed: u64,
    pub storage_records_written: u64,
}
```

The existing `MarketDataSupervisor` status fields map naturally into this structure and can be migrated incrementally.

### 7.4 Backpressure

The runtime should explicitly model backpressure, inspired by MarketBridge:

```rust
pub enum SourceBackpressureMode {
    Block,
    DropNewest,
}
```

First implementation may use blocking behavior to avoid silent loss. Drop behavior should be added only when status counters and tests make loss visible.

### 7.5 Cancellation

Every live source task must receive a cancellation token.

Stop semantics for the first implementation:

- `POST /market-data/live/stop` requests cancellation and returns quickly with `Stopping` or `Stopped`.
- The actor records `Stopped` when the stream loop exits.
- A later API can offer `stop_and_wait` semantics if needed.

## 8. Adapter Traits

The first trait set should be small.

```rust
pub trait DataSourceAdapter: Send + Sync {
    fn adapter_id(&self) -> &'static str;
    fn capabilities(&self) -> AdapterCapabilities;
}
```

```rust
#[async_trait]
pub trait LiveSourceAdapter: DataSourceAdapter {
    async fn run_live(
        &self,
        subscription: SourceSubscription,
        context: LiveSourceContext,
    ) -> fdc_core::Result<()>;
}
```

`LiveSourceContext` should contain:

- cancellation token
- status/progress reporter
- backpressure-aware event sink or orchestrator callback
- runtime logger/metrics handles when available

Historical and polling traits should be separate and added later:

```rust
#[async_trait]
pub trait HistoricalSourceAdapter: DataSourceAdapter {
    async fn fetch_historical_page(&self, request: HistoricalSourceRequest) -> fdc_core::Result<HistoricalSourcePage>;
}
```

```rust
#[async_trait]
pub trait PollingSourceAdapter: DataSourceAdapter {
    async fn poll_once(&self, request: PollingSourceRequest) -> fdc_core::Result<PollingSourceBatch>;
}
```

Do not make the first live trait handle historical, polling, replay, and snapshot modes all at once.

## 9. Barter Crypto Adapter as First Provider

The current `fdc-adapter/barter` should become the first implementation of the generic live adapter pattern.

### 9.1 First supported capability

```text
adapter_id: barter_crypto
market: Crypto
mode: Live
exchange: binance_spot
kind: Trade
symbols: BTCUSDT, ETHUSDT by default
```

### 9.2 Reused Barter-rs functionality

- `Streams::<PublicTrades>::builder()`
- `subscribe(...)`
- `init()`
- `select_all()`
- Barter-rs reconnection events
- Barter-rs `MarketEvent` timestamps and exchange identifiers

### 9.3 Mapping

The mapping chain should remain layered:

```text
Barter-rs MarketStreamResult
  -> fdc-barter map_live_trade_result
  -> BarterIngestionEnvelope
  -> fdc-orchestrator barter_envelope_to_source_envelope
  -> fdc-orchestrator barter_event_to_market_data_dto
  -> fdc-orchestrator market_data_dto_to_storage_record
  -> fdc-storage StorageWriteSink
```

This preserves all existing responsibilities.

### 9.4 Avoided coupling

The Barter adapter must not:

- Depend on `fdc-server`.
- Start HTTP routes.
- Write directly to storage.
- Depend on future A-share, US equity, news, or on-chain adapters.
- Own generic runtime status APIs.

The server must not:

- Construct Barter-rs native subscriptions directly after the runtime migration.
- Depend on Barter-rs types beyond the adapter/orchestrator boundary.

## 10. Server Integration Strategy

The production server should evolve from market-data-specific live control toward source runtime control while preserving existing routes.

### 10.1 Compatibility routes

Keep existing MVP routes:

```text
POST /market-data/live/start
GET  /market-data/live/status
POST /market-data/live/stop
GET  /market-data/trades
```

Internally, these should call the new source runtime using the default Barter crypto subscription.

### 10.2 Future generic routes

Add later, after the runtime boundary is stable:

```text
GET  /sources/capabilities
POST /sources/start
POST /sources/stop
GET  /sources/status
GET  /sources/status/:source_id
```

These generic routes should not be required for the first implementation slice.

### 10.3 Production state

`ProductionServerState` should eventually hold:

- `ServerRuntimeConfig`
- `QueryableMarketDataStore` for current MVP reads
- source runtime or source supervisor registry
- adapter registry

The existing `MarketDataSupervisor` can be either migrated into a generic source supervisor or wrapped by it during transition.

## 11. Data Domain Evolution

The first implementation only writes market-data records. Future source domains should not be forced into `MarketDataDto`.

Recommended future DTO expansion:

```text
fdc-transform
  market_data.rs      # current
  reference_data.rs   # instruments, corporate actions, exchange calendars
  news.rs             # article, provider, tickers, language, sentiment hints
  sentiment.rs        # score, model, provider, horizon
  onchain.rs          # transfer, swap, block, transaction, address activity
```

Future storage mappings should be added through orchestrator modules:

```text
fdc-orchestrator
  market_data.rs
  reference_data.rs
  news.rs
  sentiment.rs
  onchain.rs
```

This keeps transform neutral and storage generic.

## 12. Dependency Rules

The following dependency rules are required:

1. `fdc-adapter-core` may depend on `fdc-core` and serialization utilities.
2. Concrete adapters may depend on `fdc-adapter-core` and their vendor SDKs.
3. `fdc-barter` may depend on Barter-rs and `fdc-core`.
4. `fdc-ingestion` must not depend on any concrete adapter.
5. `fdc-transform` must not depend on any concrete adapter or ingestion.
6. `fdc-storage` must not depend on adapter, ingestion, transform, orchestrator, API, or server crates.
7. `fdc-orchestrator` may depend on concrete adapters, ingestion, transform, and storage to implement bounded glue.
8. `fdc-server` may depend on orchestrator and adapter runtime abstractions for application assembly, but should not embed vendor SDK logic.
9. `fdc-api` should remain API-facing and not own data acquisition logic.

## 13. First Implementation Slice Scope

The first slice after this design should be small and testable.

### In scope

- Introduce generic adapter/source runtime model types.
- Add adapter capability declarations for Barter crypto live trade support.
- Add a runtime/supervisor design that can start and stop a single Barter crypto source.
- Preserve the current production `/market-data/live/*` behavior.
- Replace repeated bounded background collection cycles with a continuous stream consumer actor for Barter live trades.
- Keep default tests offline.
- Keep real Binance live smoke gated.

### Out of scope

- Real A-share data collection.
- Real US equity data collection.
- Real futures data collection outside Barter-supported crypto futures.
- News, sentiment, and on-chain real integrations.
- Durable storage migration.
- SQL query integration.
- Runtime dynamic subscription mutation.
- Multi-tenant source permissions.
- Full metrics dashboard.
- Complete Barter-rs exchange/kind enablement.

## 14. Acceptance Criteria for Design Implementation

### AC-1: Adapter core boundary

Given the workspace builds offline, when generic adapter model tests run, then `SourceSubscription`, capabilities, runtime status, and source state types validate without depending on Barter-rs.

### AC-2: Barter capability declaration

Given the Barter adapter is available, when its capabilities are queried, then it reports crypto live trade support for the first supported Binance Spot slice.

### AC-3: Dependency boundary

Given dependency guard tests run, when scanning `fdc-ingestion`, `fdc-transform`, and `fdc-storage`, then they do not depend on concrete adapter crates.

### AC-4: Existing production route compatibility

Given `FDC_LIVE_ENABLED=1`, when `POST /market-data/live/start` is called, then the response still returns successful running state and later records are queryable through `GET /market-data/trades`.

### AC-5: Stop semantics

Given a Barter live source is running, when `POST /market-data/live/stop` is called, then the runtime records a stop request, the source loop receives cancellation, and status eventually reaches `Stopped` or a terminal failure state with a visible error.

### AC-6: Offline test default

Given normal test commands run with `CARGO_NET_OFFLINE=true`, when package tests execute, then no test requires live exchange network access.

### AC-7: Gated live smoke

Given live network testing is explicitly enabled, when the gated production live smoke runs, then at least one real Binance trade is written and returned through the market-data query route.

## 15. Recommended Follow-up Phases

### C1: Adapter core model and design guards

- Add generic model types.
- Add contract tests for capabilities, subscriptions, status, and dependency boundaries.
- Do not start network streams yet.

### C2: Barter crypto adapter provider boundary

- Wrap current Barter live trade subscription support behind the generic adapter capability model.
- Keep existing Barter-specific event/envelope models.
- Add tests for supported and unsupported subscription shapes.

### C3: Continuous source runtime actor

- Add a runtime actor that owns one live source task.
- Use cancellation token for stop.
- Track source status and counters.
- Consume Barter stream continuously rather than via repeated bounded cycles.

### C4: Server compatibility integration

- Route existing `/market-data/live/*` handlers through the new runtime.
- Keep response DTO compatibility.
- Preserve current readiness and query behavior.

### C5: Generic source API

- Add `/sources/capabilities` and `/sources/status` after the runtime is stable.
- Defer generic start/stop API until security and request shape are reviewed.

### C6: Next domain adapter designs

Separate design documents should be written for:

- A-share market data adapter.
- US equity market data adapter.
- Futures market data adapter.
- News/sentiment adapter.
- On-chain adapter.

Each should define provider choice, credentials, rate limits, data model, legal/compliance constraints, and storage/query shape.

## 16. Risks and Mitigations

### Risk: Over-abstracting before more adapters exist

Mitigation:

- Keep first traits small.
- Implement only Barter crypto live in the first slice.
- Reserve but do not implement historical/polling/replay traits until needed.

### Risk: Breaking accepted MVP routes

Mitigation:

- Keep route DTOs and URLs unchanged in the first migration.
- Add compatibility contract tests before replacing internals.

### Risk: Duplicating Barter-rs reconnect logic

Mitigation:

- Let Barter-rs own crypto exchange stream reconnect.
- mdb runtime only observes reconnect events and records status/counters.

### Risk: Forcing non-market data into market-data DTOs

Mitigation:

- Treat news, sentiment, reference data, and on-chain as separate future DTO domains.
- Keep adapter core domain-aware.

### Risk: Server becomes a vendor integration layer

Mitigation:

- Move vendor-specific code into concrete adapter crates.
- Keep server integration through runtime/registry/orchestrator boundaries.

## 17. Conclusion

The project should evolve toward a multi-source adapter architecture with a generic adapter core and concrete provider adapters. The existing Barter-rs integration should remain the first provider and should continue to own crypto exchange acquisition details.

The next implementation work should not attempt to implement every future data source. It should establish the adapter core boundary, wrap the existing Barter crypto live path as the first provider, and migrate `fdc-server` from repeated bounded live cycles to a continuous source runtime actor while preserving the current production market-data routes.
