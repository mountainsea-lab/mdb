# B4 Transform Sink Boundary Design

Date: 2026-05-25
Branch: `mdb-mqdev`

## Intent

Create the first bounded transform-facing handoff for market data after B1-B3 source ingestion work. The slice must support future multiple adapters without making `fdc-ingestion` depend on `fdc-barter` or any adapter crate.

## Architecture Decision

The neutral market-data model belongs above adapters and outside generic ingestion. For this repository, the first implementation should live in `fdc-transform` because it is the next consumer boundary and is currently a skeleton crate.

Rejected placements:

- Adapter crate: would duplicate neutral DTO definitions across future adapters and make each adapter own a cross-adapter contract.
- `fdc-ingestion`: would mix generic envelope/batch transport with domain-specific market-data semantics.
- `fdc-core`: plausible later if the DTO becomes a global persisted contract, but too broad for this slice.

Chosen placement:

- `fdc-transform` defines neutral market-data DTOs and transform sink traits.
- Adapter crates may depend on `fdc-transform` to map adapter-native payloads into neutral DTOs.
- `fdc-ingestion` remains generic and adapter-independent.

## Components

### `fdc-transform::market_data`

Defines:

- `MarketDataDto`: envelope-like neutral record for transform input.
- `MarketDataPayload`: enum for supported payloads.
- `TradeDto`: first implemented payload, matching B3 live trade needs.
- `OrderBookL1Dto`, `CandleDto`, `RawMarketDataDto`: schema placeholders for existing `fdc-barter` payload families, with no live implementation required in B4.
- `MarketDataKind`: neutral event kind.
- `TradeSide`: neutral buy/sell/unknown side.
- `TransformQualityFlags`: transform-facing quality summary.

DTO fields preserve source identity, exchange, symbol, timestamps, source sequence, ingestion sequence, source event id, and metadata required by downstream transforms.

### `fdc-transform::sink`

Defines:

- `MarketDataTransformSink`: async trait accepting validated `MarketDataDto` batches.
- `MarketDataTransformSinkResult`: accepted/rejected counters.
- `RecordingMarketDataSink`: in-memory test/demo sink.

This slice does not implement real storage, analytics, or DB I/O.

### `fdc-barter` bridge

Adds a mapper from `BarterMarketEvent` or `BarterIngestionEnvelope` into `MarketDataDto`.

For B4, live public trades are required. Unsupported payloads may map to typed placeholder DTO variants when information is already present, or return a clear unsupported error if mapping would be lossy.

## Data Flow

```text
Barter live stream
  -> BarterMarketEvent
  -> BarterIngestionEnvelope
  -> SourceEnvelope<BarterMarketEvent> via B2 bridge
  -> SourceBatchItem<BarterMarketEvent> via B1 pipeline
  -> fdc-barter DTO mapper
  -> MarketDataDto
  -> MarketDataTransformSink
```

`fdc-ingestion` is not aware of `MarketDataDto` and remains reusable for non-market-data sources.

## Error Handling

- DTO mapping must fail with an explicit adapter error when required fields are unavailable.
- Transform sinks must report accepted and rejected counts.
- In-memory sink should reject invalid empty batches only if the contract requires it; otherwise empty batches return zero counts.
- No panic-based error handling in public APIs.

## Testing Strategy

Contract tests must cover:

1. `fdc-transform` DTO construction for a trade.
2. `RecordingMarketDataSink` accepts a bounded batch and records DTOs.
3. `fdc-barter` maps a fixture live trade envelope into a neutral `MarketDataDto` without losing identity, symbol, price, quantity, side, and timestamps.
4. `fdc-ingestion` dependency guard still finds no `fdc-barter` or `fdc-transform` references unless a later explicit design changes that.

## Non-goals

- No database or storage writes.
- No `fdc-storage -> fdc-transform` dependency or storage API that directly accepts `MarketDataDto`.
- No transform analytics logic.
- No historical acquisition implementation.
- No checkpoint persistence.
- No lifecycle runner for infinite streams.

## Acceptance Criteria

- `fdc-transform` exposes neutral DTO and sink APIs.
- `fdc-barter` can map B3 trade envelopes into `MarketDataDto`.
- `fdc-ingestion` still does not depend on adapter crates or transform crate.
- Contract tests pass for `fdc-transform`, `fdc-barter`, and `fdc-ingestion` baseline.
