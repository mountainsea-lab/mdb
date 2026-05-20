# fdc-ingestion Phase B1c Source Pipeline Design

## Goal

Add a bounded source pipeline helper to `fdc-ingestion` that wires the existing B1a source envelope/validator and B1b source batch processor together for finite, testable ingestion flows.

## Context

B1a added generic `SourceEnvelope<T>`, source checkpoint/quality metadata, and `SourceValidator`.
B1b added `SourceBatchItem<T>`, `SourceBatchProcessor<T>`, `SourceBatchSink<T>`, batch results, and batch stats.
B1c should prove these pieces compose without adding real network, storage, transform, or Barter-specific coupling.

## Recommended Approach

Use a small bounded helper rather than a long-running stream runtime.

The new API will live in `crates/fdc-ingestion/src/source/pipeline.rs` and expose a function named `run_source_pipeline_once` plus a summary result type such as `SourcePipelineResult`.

The helper will accept:

- A finite collection of `SourceEnvelope<T>` values.
- A `SourceValidator`.
- A `SourceBatchProcessor<T>`.

The helper will:

1. Validate each envelope.
2. Wrap each envelope and validation result in `SourceBatchItem<T>`.
3. Add each item to `SourceBatchProcessor<T>`.
4. Collect any batch results emitted by size or timeout-triggered processing.
5. Flush the processor at the end.
6. Return an aggregate `SourcePipelineResult`.

## Public API Shape

The intended public API is:

```rust
pub struct SourcePipelineResult {
    pub input_count: usize,
    pub validation_success_count: usize,
    pub validation_failure_count: usize,
    pub batch_results: Vec<SourceBatchResult>,
}

impl SourcePipelineResult {
    pub fn processed_count(&self) -> usize;
    pub fn success_count(&self) -> usize;
    pub fn failure_count(&self) -> usize;
    pub fn batch_count(&self) -> usize;
}

pub async fn run_source_pipeline_once<T, I>(
    envelopes: I,
    validator: &SourceValidator,
    processor: &SourceBatchProcessor<T>,
) -> fdc_core::error::Result<SourcePipelineResult>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = SourceEnvelope<T>>;
```

The exact method names may be refined during implementation, but the API should remain bounded, generic, and independent of `fdc-barter`.

## Components

### `source::pipeline`

Responsibilities:

- Coordinate validation and batch processing for a finite set of source envelopes.
- Own the aggregate pipeline result type.
- Keep orchestration logic separate from `SourceValidator` and `SourceBatchProcessor` internals.

Non-responsibilities:

- No real network I/O.
- No REST pagination.
- No storage writes except through the already supplied `SourceBatchSink<T>`.
- No transform/schema conversion.
- No checkpoint persistence.
- No stateful dedupe or gap detection.
- No direct `fdc-barter` dependency.

### Existing `SourceValidator`

Responsibilities in B1c:

- Validate each envelope once.
- Maintain its existing stats.
- Remain stateless with respect to cross-event ordering.

### Existing `SourceBatchProcessor<T>`

Responsibilities in B1c:

- Buffer validated items.
- Filter invalid items before sink handoff.
- Emit `SourceBatchResult` when batch size/timeout is reached or flush is called.
- Maintain existing batch stats.

## Data Flow

```text
Vec<SourceEnvelope<T>>
        |
        v
run_source_pipeline_once
        |
        +--> SourceValidator::validate(&envelope)
        |
        +--> SourceBatchItem::new(envelope, validation_result)
        |
        +--> SourceBatchProcessor::add_item(item)
        |        |
        |        +--> Option<SourceBatchResult>
        |
        +--> SourceBatchProcessor::flush()
        |        |
        |        +--> Option<SourceBatchResult>
        |
        v
SourcePipelineResult
```

## Error Handling

- Validation failures are not returned as function errors. They should flow into `SourceBatchItem<T>` and be counted in `SourcePipelineResult`.
- Sink errors from `SourceBatchProcessor<T>` should propagate as `Err(...)` from `run_source_pipeline_once`.
- If processing an empty input, the function should return an empty successful `SourcePipelineResult` and should not call the sink.
- Partial sink writes remain represented by `SourceBatchResult` according to existing B1b semantics.

## Testing Strategy

Add `crates/fdc-ingestion/tests/source_pipeline_contract.rs` with dummy payloads and in-memory sinks.

Required contract coverage:

1. Empty input returns zero counts and no batch results.
2. Valid envelopes are validated, batched, flushed, and written to the sink.
3. Invalid envelopes are counted as validation failures and do not reach the sink.
4. Batch-size-triggered results and final flush results are both collected.
5. Sink errors propagate from `run_source_pipeline_once`.
6. `fdc-ingestion` remains independent from `fdc-barter`.

Verification commands:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract
cargo test -p fdc-ingestion
cargo test -p fdc-barter -p fdc-ingestion
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

## Scope Boundaries

In scope:

- `crates/fdc-ingestion/src/source/pipeline.rs`
- `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
- Re-exports from `source::mod` and crate root.
- Documentation/status updates after implementation.

Out of scope:

- Barter-specific adapter glue.
- Real stream runtime.
- Infinite loops.
- Background timers.
- REST/WebSocket clients.
- Transform or storage sinks.
- Checkpoint persistence.
- Stateful dedupe/gap detection.
- Warning cleanup in older modules.

## Future Follow-ups

After B1c, separate plans can address:

1. Barter-to-source adapter bridge outside `fdc-ingestion`.
2. Checkpoint persistence boundary.
3. Transform sink integration.
4. Storage sink integration.
5. Stream/runtime loop with cancellation and backpressure semantics.
