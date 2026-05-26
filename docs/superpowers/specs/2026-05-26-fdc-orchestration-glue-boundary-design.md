# B6 Orchestration Glue Boundary Design

Date: 2026-05-26
Branch: `mdb-mqdev`

## Intent

Define a dedicated orchestration boundary for cross-layer glue in Financial Data Center without weakening the core crate dependency rules established through B4/B5.

B6 should create a clear home for bounded mappings and finite-pipeline wiring across these independently owned boundaries:

```text
fdc-adapter/barter -> fdc-ingestion -> fdc-transform -> fdc-storage
```

The approved direction is a new `fdc-orchestrator` crate. It will depend on the lower-level boundary crates and own the concrete glue code that must not live inside adapter, ingestion, transform, or storage core crates.

## Context

Current completed boundaries:

- `fdc-barter` owns Barter-specific acquisition, adapter events, and `BarterIngestionEnvelope`.
- `fdc-ingestion` owns generic `SourceEnvelope<T>`, validation, source batch processing, and finite source pipeline helpers.
- `fdc-transform` owns neutral `MarketDataDto`, payload DTOs, and `MarketDataTransformSink`.
- `fdc-storage` owns generic `StorageWriteRecord`, `StorageWriteBatch`, tier-aware `StoragePlacementHint`, and `StorageWriteSink`.

Important existing rules:

- `fdc-barter` must not depend on `fdc-ingestion`, `fdc-transform`, or `fdc-storage`.
- `fdc-ingestion` must not depend on adapter crates or `fdc-transform`.
- `fdc-transform` must not depend on adapter crates or `fdc-ingestion`.
- `fdc-storage` must not depend on `fdc-transform`, `fdc-ingestion`, or adapter crates.
- Cross-layer mapping belongs outside the core crates.

## Design Decision

Create a new workspace crate:

```text
crates/fdc-orchestrator
```

This crate is the integration boundary for application-facing, bounded glue. It may depend on:

- `fdc-core`
- `fdc-barter`
- `fdc-ingestion`
- `fdc-transform`
- `fdc-storage`

No reverse dependency is allowed. Core crates must not depend on `fdc-orchestrator`.

## Why Not `fdc-server` First

`fdc-server` should remain the application startup and lifecycle layer. It will eventually initialize configs, tracing, metrics, storage, query, ingestion runners, API state, and graceful shutdown.

Putting mapping logic directly in `fdc-server` would mix reusable bounded transformations with process lifecycle concerns. `fdc-server` should consume orchestrator APIs, not own their mapping internals.

## Why Not `fdc-api` First

`fdc-api` should expose external request protocols and route requests to application services. It should not own adapter-specific mapping or storage layout decisions.

Putting glue inside `fdc-api` would make non-HTTP paths, such as live Barter ingestion, CLI backfills, or future schedulers, reuse API internals incorrectly.

## B6 Scope

B6 is a boundary slice, not the full production runtime.

In scope:

1. Add the `fdc-orchestrator` crate to the workspace.
2. Define focused modules for bounded glue responsibilities.
3. Map `BarterIngestionEnvelope` into `SourceEnvelope<BarterMarketEvent>` in orchestrator code.
4. Map `BarterMarketEvent` into `MarketDataDto` in orchestrator code.
5. Map `MarketDataDto` into `StorageWriteRecord` in orchestrator code.
6. Provide a finite test helper that runs an in-memory fixture through the bounded path and records storage writes through `RecordingStorageSink`.
7. Add dependency guard tests proving core crates remain decoupled from `fdc-orchestrator` and from forbidden upstream/downstream crates.
8. Document that production streaming, checkpoint persistence, retry policy, and real DB writes remain future slices.

Out of scope:

- No infinite live stream runner.
- No production `TierManager` or `ShardManager` routing.
- No real database writes.
- No checkpoint persistence.
- No dedupe, gap detection, replay, or exactly-once semantics.
- No API handler integration.
- No server lifecycle integration.
- No schema registry or dynamic layout system.

## Proposed Module Structure

```text
crates/fdc-orchestrator/src/
  lib.rs
  barter.rs          # Barter adapter envelope/event glue
  market_data.rs     # Market data DTO construction policy
  storage.rs         # MarketDataDto -> StorageWriteRecord mapping policy
  pipeline.rs        # finite bounded fixture pipeline helper
```

Responsibilities:

- `barter.rs`
  - Convert `BarterIngestionEnvelope` to `SourceEnvelope<BarterMarketEvent>`.
  - Preserve source metadata, event time, and quality flags where generic ingestion types allow it.
  - Avoid changing `fdc-barter` or `fdc-ingestion` ownership.

- `market_data.rs`
  - Convert `BarterMarketEvent` payloads to neutral `MarketDataDto` payloads.
  - Keep Barter-specific interpretation in orchestrator glue, not in `fdc-transform` core.
  - Return validation/mapping errors for unsupported or malformed payloads.

- `storage.rs`
  - Convert `MarketDataDto` to generic `StorageWriteRecord`.
  - Choose a deterministic namespace, collection, key, serialized value, metadata, and placement hint.
  - Use advisory placement hints only. Do not invoke real tier routing.

- `pipeline.rs`
  - Compose the bounded path for tests and examples:

    ```text
    Vec<BarterIngestionEnvelope>
      -> SourceEnvelope<BarterMarketEvent>
      -> SourceValidator + SourceBatchProcessor
      -> MarketDataDto
      -> StorageWriteBatch
      -> StorageWriteSink
    ```

  - Use finite inputs only.
  - Surface aggregate counts so tests can verify accepted, rejected, transformed, and stored records.

## Mapping Policies

### Adapter Envelope to Source Envelope

The mapping should preserve the adapter event as the source payload:

```text
BarterIngestionEnvelope.event -> SourceEnvelope<BarterMarketEvent>.payload
```

Source metadata should use generic labels:

- source type: market data or external source, based on existing `SourceType` variants.
- source name: stable string such as `barter` or `barter:<exchange>` if exchange data is available.
- event timestamp: preserve adapter event timestamp when available.
- quality flags: translate only if `fdc-ingestion` has generic equivalent fields. Otherwise keep the first B6 mapping minimal and document the loss explicitly in tests.

### Barter Event to MarketDataDto

The DTO mapping should support the currently modeled market data payloads:

- trade
- order book L1
- candle
- raw payload

Each DTO must contain enough neutral identity for later storage:

- source label
- instrument or symbol label when available
- event timestamp
- market data kind
- payload-specific fields
- transform quality flags when available

Unsupported variants should return an error rather than silently producing raw records, unless the adapter event already uses an explicit raw payload.

### MarketDataDto to StorageWriteRecord

The storage mapping should be deterministic and generic:

- namespace: `market_data`
- collection: based on DTO kind, for example `trades`, `order_book_l1`, `candles`, or `raw`
- key: stable UTF-8 bytes derived from source, instrument, timestamp, and kind
- value: JSON bytes for B6 readability and test stability
- metadata content type: `application/json`
- metadata schema: `market_data.<kind>`
- metadata source: orchestrator or DTO source label
- placement hint:
  - target tier: none by default, unless B6 tests choose a simple advisory hint
  - access pattern: `Hot` for trades/order book, `Warm` for candles, `Unspecified` for raw
  - durability: `Persistent` for normalized market data, `Unspecified` for raw
  - shard key: source + instrument bytes when available
  - ttl: none

B6 must not hard-code production storage layout beyond this deterministic test/demo policy.

## Error Handling

Use `fdc_core::Result` and existing `fdc_core::error::Error` helpers where available.

Expected B6 mapping errors:

- Missing required source metadata for a stable key.
- Missing required symbol/instrument information when the target DTO kind requires it.
- Unsupported adapter event payload.
- Serialization failure when converting DTO to storage value.
- Sink failure from `StorageWriteSink`.

Finite pipeline helper behavior:

- Invalid source envelopes should be counted and excluded by existing ingestion validation semantics.
- Mapping errors should be returned to the caller for B6 rather than hidden as partial successes.
- Storage sink validation failures should propagate.
- No retry policy in B6.

## Testing Strategy

Add contract tests under:

```text
crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs
```

Required tests:

1. `BarterIngestionEnvelope` maps to `SourceEnvelope<BarterMarketEvent>` without adding dependencies to core crates.
2. A Barter trade event maps to a neutral `MarketDataDto` trade payload.
3. A trade DTO maps to a deterministic `StorageWriteRecord` with namespace `market_data`, collection `trades`, JSON value, metadata, and advisory placement.
4. A finite Barter fixture can flow through the orchestrator helper into `RecordingStorageSink` and record one storage write.
5. Unsupported or malformed payloads return explicit errors.
6. Dependency guard: `fdc-barter`, `fdc-ingestion`, `fdc-transform`, and `fdc-storage` must not reference `fdc-orchestrator` in their manifests or source.
7. Dependency guard: existing forbidden core dependency directions remain blocked.

Verification commands for B6 implementation:

```bash
rtk cargo fmt --package fdc-orchestrator --check
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
rtk cargo test -p fdc-orchestrator
rtk cargo test -p fdc-barter -p fdc-ingestion -p fdc-transform -p fdc-storage
```

If local Barter-rs path dependencies are unavailable, document that explicitly and still run all non-Barter checks possible. Current resume validation confirmed the local Barter-rs checkout exists.

## Acceptance Criteria

- `fdc-orchestrator` is the only new crate responsible for B6 cross-layer glue.
- Core crates remain decoupled and do not depend on `fdc-orchestrator`.
- Adapter, ingestion, transform, and storage core crates keep their existing ownership boundaries.
- B6 supports a finite, deterministic Barter market-data fixture path into `RecordingStorageSink`.
- B6 does not introduce real storage writes, production streaming lifecycle, or checkpoint persistence.
- Contract tests prove the mapping path and dependency boundaries.
- Development status docs are updated after implementation with verification evidence and the next recommended slice.

## Future Work After B6

Recommended follow-up slices:

1. B7: server/application assembly uses `fdc-orchestrator` APIs from `fdc-server`.
2. B8: `/insert` or backfill API integration through orchestrator services.
3. B9: checkpoint persistence boundary.
4. B10: production finite/backfill runner with retry and observability.
5. Later: live infinite stream lifecycle, dedupe/gap detection, and production tier-aware storage runtime routing.
