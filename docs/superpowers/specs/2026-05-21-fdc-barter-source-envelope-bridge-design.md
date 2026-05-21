# B2 Design: fdc-barter to SourceEnvelope Bridge

Date: 2026-05-21
Branch: `mdb-mqdev`
Status: Proposed for implementation planning

## Context

B1a through B1c established the generic structured source ingestion path in `fdc-ingestion`:

- `SourceEnvelope<T>` for generic source handoff.
- `SourceValidator` for stateless validation.
- `SourceBatchProcessor<T>` for bounded batching.
- `run_source_pipeline_once` for finite fixture or demo pipelines.

`fdc-barter` already owns Barter-specific market-data boundaries:

- `BarterMarketEvent`
- `BarterIngestionEnvelope`
- `BarterCheckpoint`
- `DataQualityFlags`

B2 connects these layers without reversing the dependency boundary. `fdc-ingestion` must remain generic and must not depend on `fdc-barter`.

## Goal

Add a bounded bridge from `fdc-barter` ingestion envelopes into `fdc-ingestion` source envelopes so Barter market-data fixtures can flow through the B1 source pipeline.

The initial bridge should be deterministic, testable, and free of real network, storage, transform, and checkpoint-persistence I/O.

## Non-Goals

B2 will not implement:

- Real Barter WebSocket runtime integration.
- Historical REST pagination runtime.
- Storage sink or transform sink handoff.
- Checkpoint persistence.
- Cross-event dedupe or gap detection state machines.
- A new workspace integration crate.

## Chosen Approach

Place the bridge inside `fdc-barter` under the existing ingestion boundary.

Proposed files:

- `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`
- `crates/fdc-adapter/barter/tests/source_bridge_contract.rs`

Dependency direction:

```text
fdc-barter -> fdc-ingestion
fdc-ingestion -X-> fdc-barter
```

This is acceptable because `fdc-barter` is the Barter-specific adapter crate and `fdc-ingestion` is the generic source primitive crate.

## Alternatives Considered

### A. Bridge in `fdc-barter`  Recommended

Pros:

- Directly aligns with B2: Barter adapter emits generic ingestion envelopes.
- Keeps `fdc-ingestion` independent from Barter details.
- Minimal workspace churn.
- Easy to test with existing `fdc-barter` contract-test style.

Cons:

- `fdc-barter` gains a dependency on `fdc-ingestion`.
- Future non-Barter adapters may duplicate similar bridge code until a shared adapter pattern is extracted.

### B. New integration crate

Pros:

- Very explicit glue-layer boundary.
- Keeps `fdc-barter` free from `fdc-ingestion` dependency.

Cons:

- Premature for current scope.
- Adds workspace/API complexity before more adapters exist.
- Harder to justify until there are multiple integration flows.

### C. Test-only helper

Pros:

- Lowest production API commitment.
- Useful for experiments.

Cons:

- Does not produce a reusable B2 bridge.
- Delays the real integration boundary decision.

## Public API Shape

Use an explicit trait or function that converts owned `BarterIngestionEnvelope` values into `SourceEnvelope<BarterMarketEvent>`.

Recommended initial API:

```rust
pub trait IntoSourceEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent>;
}

impl IntoSourceEnvelope for BarterIngestionEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent>;
}
```

This consumes the Barter envelope and preserves the full `BarterMarketEvent` as the source payload. Keeping the original Barter event as payload avoids premature transform/schema normalization. Later phases can transform `BarterMarketEvent` into canonical market-data schemas.

## Field Mapping

### Source envelope identity

- `SourceEnvelope.envelope_id` = `BarterIngestionEnvelope.envelope_id`
- `SourceEnvelope.source_id` = `BarterIngestionEnvelope.source_id`
- `SourceEnvelope.source_type`:
  - `MarketData` for live Barter market data.
  - `Replay` for historical or replay/backfill-oriented events.

### Timing

- `SourceEnvelope.event_time` = `BarterMarketEvent.timestamp`
- `SourceEnvelope.received_at` = `BarterMarketEvent.received_at`
- `SourceEnvelope.emitted_at` = `BarterIngestionEnvelope.emitted_at`

### Payload and sequence

- `SourceEnvelope.payload` = `BarterIngestionEnvelope.event`
- `SourceEnvelope.sequence` = `BarterMarketEvent.sequence`

### Quality flags

Map equivalent fields directly:

- `is_replay`
- `is_backfill`
- `is_duplicate_candidate`
- `has_gap_before`
- `is_out_of_order`

If `BarterMarketEvent.mode == Historical`, the bridge should set `SourceQualityFlags.is_backfill = true` even when the Barter envelope quality flag is false. This preserves historical-mode semantics for downstream validators and tests.

### Metadata

Populate `SourceMetadata`:

- `adapter` = `BarterMarketEvent.source`
- `exchange` = `BarterMarketEvent.exchange`
- `symbol` = `BarterMarketEvent.symbol.to_string()`
- `kind` = `format!("{:?}", BarterMarketEvent.kind)`

Populate `attributes` with stable string fields:

- `mode`: `Live` or `Historical`
- `payload_kind`: `format!("{:?}", BarterMarketPayload::kind())`

The metadata should avoid serializing the whole payload. Payload remains in `SourceEnvelope.payload`.

### Checkpoint

Map `BarterCheckpoint` into `SourceCheckpoint` when present:

- `checkpoint_id`: stable string derived from source/exchange/symbol/kind/mode/last_event_time.
- `source_id`: `BarterCheckpoint.source_id`
- `partition.exchange`: `BarterCheckpoint.exchange`
- `partition.symbol`: `BarterCheckpoint.symbol`
- `partition.kind`: `format!("{:?}", BarterCheckpoint.kind)`
- `partition.shard`: `None`
- `position`:
  - `PageToken(token)` when historical cursor has `page_token`.
  - `Timestamp(next_start)` when cursor has `next_start` and no page token.
  - `Sequence(last_seen_exchange_id)` when cursor has `last_seen_exchange_id` and neither page token nor next start exists.
  - `Timestamp(last_event_time)` otherwise.
- `updated_at`: `BarterCheckpoint.updated_at`

The bridge does not persist checkpoints.

## Pipeline Demonstration Scope

B2 should prove the bridge works with the existing bounded pipeline helper. Contract tests should create small in-memory Barter fixtures, convert them into `SourceEnvelope<BarterMarketEvent>`, and feed them through `run_source_pipeline_once` with a recording sink.

This demonstrates the intended flow:

```text
BarterIngestionEnvelope
        ↓ bridge
SourceEnvelope<BarterMarketEvent>
        ↓ run_source_pipeline_once
SourceValidator + SourceBatchProcessor
        ↓
recording test sink
```

No real exchange, network, transform, or database component participates in B2.

## Error Handling

The conversion itself should be infallible for the currently modeled Barter envelope because all required source-envelope fields already exist.

Validation errors remain the responsibility of `SourceValidator` after conversion. Sink errors remain the responsibility of `SourceBatchProcessor` / `run_source_pipeline_once`.

## Tests

Add contract tests covering:

1. Live trade envelope maps identity, timing, payload, sequence, quality, and metadata.
2. Historical envelope maps to replay/backfill-oriented source semantics.
3. Barter checkpoint maps into `SourceCheckpoint` partition and position.
4. Multiple bridged Barter envelopes run through `run_source_pipeline_once` and reach a recording sink.
5. `fdc-ingestion` still has no `fdc-barter` dependency or source reference.

## Verification Commands

Initial B2 verification should include:

```bash
cargo fmt --package fdc-barter
cargo test -p fdc-barter --test source_bridge_contract
cargo test -p fdc-barter -p fdc-ingestion
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

## Completion Criteria

B2 is complete when:

- `fdc-barter` exposes the bridge from its ingestion boundary.
- `fdc-barter` can convert bounded Barter fixture envelopes into `SourceEnvelope<BarterMarketEvent>`.
- Contract tests prove field mapping, checkpoint mapping, and bounded pipeline flow.
- `fdc-ingestion` remains independent from `fdc-barter`.
- `docs/DEVELOPMENT_STATUS.md` is updated with B2 results and the next recommended slice.
