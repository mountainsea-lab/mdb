# Development Status

Last updated: 2026-05-20
Branch: `mdb-mqdev`
Remote: `origin/mdb-mqdev`
Latest checkpoint commit when this file was written: `aea0879 test: cover source pipeline batch results`

This file is the entry point for resuming development. Read it first, then open the referenced design and plan documents only as needed.

## Current Focus

Build the Financial Data Center market-data source ingestion path in small, testable slices:

1. `fdc-barter` owns the Barter-rs integration boundary and Barter-specific market-data models.
2. `fdc-ingestion` owns a generic structured source path that must not depend on `fdc-barter`.
3. Future glue should connect bounded source pipeline helpers and later transform/storage boundaries without introducing real network or storage I/O too early.

## Environment and Toolchain

The repository now pins Rust with `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.95"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

Reason:

- The machine default was nightly `1.97.0-nightly`.
- A build failure initially looked like a rustc ICE in `barter-data`.
- Root cause investigation showed `/Volumes/wdata` was full and rustc could not write `.rmeta`, `.bc`, and `.rlib` files.
- `cargo clean` released about 97.4GiB and tests passed under pinned stable Rust 1.95.

If the same failure appears again, check disk first:

```bash
df -h .
du -sh target target/debug target/debug/incremental 2>/dev/null || true
```

Safe cleanup command when build artifacts consume the disk:

```bash
cargo clean
```

## Completed Work

### fdc-barter Phase A

Implemented in `crates/fdc-adapter/barter`.

Completed capabilities:

- Split adapter module shell into focused modules:
  - `src/config.rs`
  - `src/error.rs`
  - `src/capability/*`
  - `src/ingestion/*`
  - `src/mapper/*`
  - `src/model/*`
- Added Barter payload and event models:
  - `BarterMarketEvent`
  - `BarterMarketPayload`
  - `TradePayload`
  - `OrderBookL1Payload`
  - `CandlePayload`
  - `RawPayload`
  - `DecimalQuantity`
- Added historical checkpoint and request models:
  - `BarterCheckpoint`
  - `HistoricalCursor`
  - `HistoricalPageRequest`
  - `BarterMarketDataRequest`
- Added ingestion envelope:
  - `BarterIngestionEnvelope`
  - `DataQualityFlags`
- Added source capability metadata:
  - `BarterSourceCapabilities`
  - `RateLimitRule`
- Added public exports from `crates/fdc-adapter/barter/src/lib.rs`.

Contract tests:

- `crates/fdc-adapter/barter/tests/adapter_contract.rs`
- `crates/fdc-adapter/barter/tests/checkpoint_contract.rs`
- `crates/fdc-adapter/barter/tests/envelope_contract.rs`
- `crates/fdc-adapter/barter/tests/model_contract.rs`

Important docs:

- `docs/architecture/fdc-barter-design-review.md`
- `docs/architecture/fdc-barter-phase-a-pseudocode-review.md`
- `docs/superpowers/plans/2026-05-18-fdc-barter-phase-a-module-refactor.md`

### fdc-ingestion Phase B1a: Source Envelope and Validator

Implemented in `crates/fdc-ingestion/src/source`.

Completed capabilities:

- Added generic structured source envelope path:
  - `SourceEnvelope<T>`
  - `SourceMetadata`
  - `SourceType`
- Added source checkpoint model:
  - `SourceCheckpoint`
  - `SourcePartition`
  - `SourcePosition`
- Added source quality flags:
  - `SourceQualityFlags`
- Added stateless source validator:
  - `SourceValidator`
  - `SourceValidationResult`
  - `SourceValidationError`
  - `SourceValidationWarning`
  - `SourceValidatorStats`
- Re-exported public source types from:
  - `crates/fdc-ingestion/src/source/mod.rs`
  - `crates/fdc-ingestion/src/lib.rs`
- Confirmed `fdc-ingestion` has no `fdc-barter` dependency.

Contract tests:

- `crates/fdc-ingestion/tests/source_envelope_contract.rs`
- `crates/fdc-ingestion/tests/source_validator_contract.rs`

Important docs:

- `docs/architecture/fdc-ingestion-phase-b1-source-path-pseudocode-review.md`
- `docs/superpowers/plans/2026-05-19-fdc-ingestion-phase-b1a-source-envelope-validator.md`

### fdc-ingestion Phase B1b: Source Batch Processor

Implemented in `crates/fdc-ingestion/src/source/batch.rs`.

Completed capabilities:

- Added `SourceBatchItem<T>`.
- Added `SourceBatchSink<T>` async trait.
- Added `SourceBatchResult`.
- Added `SourceBatchProcessor<T>`.
- Added `SourceBatchProcessorStats`.
- Implemented valid/invalid filtering before sink handoff.
- Implemented batch size-triggered processing and explicit flush.
- Implemented short-write and sink-error failure semantics.
- Implemented stats recording and reset behavior.
- Clarified timeout semantics in code documentation: source batch processing is size-triggered or explicit-flush only; time-based triggering belongs to a future runner.
- Confirmed `fdc-ingestion` remains independent from `fdc-barter`.

Contract tests:

- `crates/fdc-ingestion/tests/source_batch_contract.rs`

Important docs:

- `docs/architecture/fdc-ingestion-phase-b1b-source-batch-pseudocode-review.md`
- `docs/superpowers/plans/2026-05-19-fdc-ingestion-phase-b1b-source-batch.md`

### fdc-ingestion Phase B1c: Source Pipeline Helper

Implemented in `crates/fdc-ingestion/src/source/pipeline.rs`.

Completed capabilities:

- Added `SourcePipelineResult` aggregate counts.
- Added `run_source_pipeline_once` for finite source envelope collections.
- The helper validates each `SourceEnvelope<T>` with `SourceValidator`.
- The helper wraps validation results into `SourceBatchItem<T>`.
- The helper sends items through `SourceBatchProcessor<T>`.
- The helper collects size-triggered batch results and final flush results.
- Validation failures are counted and flow through existing batch invalid-item filtering.
- Sink errors propagate to the caller.
- `fdc-ingestion` remains independent from `fdc-barter`.

Contract tests:

- `crates/fdc-ingestion/tests/source_pipeline_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-20-fdc-ingestion-phase-b1c-source-pipeline-design.md`
- `docs/superpowers/plans/2026-05-20-fdc-ingestion-phase-b1c-source-pipeline.md`

## Current Verification Baseline

Last successful verification after B1c source pipeline implementation:

```bash
cargo fmt --package fdc-ingestion
cargo test -p fdc-ingestion --test source_pipeline_contract
cargo test -p fdc-ingestion
cargo test -p fdc-barter -p fdc-ingestion
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Result:

- Exit code: 0 for the full verification chain above.
- `source_pipeline_contract` passed with 5 tests.
- `fdc-ingestion` unit and source contract tests passed.
- `fdc-barter` contract tests passed alongside `fdc-ingestion`.
- Dependency guard found no `fdc-barter` / `fdc_barter` references in `fdc-ingestion`.
- Existing warnings remain and are intentionally not addressed yet.

Note: `cargo fmt --package fdc-ingestion` can format older `fdc-ingestion` files outside the B1c source pipeline scope. Those unrelated formatting changes were reverted for this slice.

### External Local Dependency Caveat

`fdc-barter` currently depends on local Barter-rs crates by absolute path:

```toml
barter-data = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-data" }
barter-instrument = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-instrument" }
```

The local Barter-rs checkout is required before running `fdc-barter` tests. It was present for the B1c final verification, and `cargo test -p fdc-barter -p fdc-ingestion` passed. If a future session sees a missing `barter-data/Cargo.toml` error, restore or clone the Barter-rs checkout at that path, or change the dependency strategy in a separate planned task.

## Known Warnings

Warnings currently exist in older modules such as:

- `crates/fdc-wasm`
- `crates/fdc-types`
- `crates/fdc-ingestion/src/parser.rs`
- `crates/fdc-ingestion/src/receiver.rs` test imports

They are not part of the current source path milestone and should not block continuation unless the next task explicitly targets cleanup.

## Important Boundaries

Do not violate these without a new design review:

- `fdc-ingestion` must not depend on `fdc-barter`.
- B1a/B1b/B1c are generic source-path building blocks, not real Barter stream integration.
- Do not add real WebSocket, REST, storage, or transform I/O inside B1 source-path primitives.
- Existing network byte ingestion path in `crates/fdc-ingestion/src/batch.rs`, `receiver.rs`, `parser.rs`, and `validator.rs` should remain unchanged unless a specific plan says otherwise.
- Checkpoint persistence is not implemented yet.
- Cross-event dedupe/gap detection state machine is not implemented yet.

## Next Recommended Development Slice

### Phase B2: fdc-barter to SourceEnvelope Bridge

Goal: connect the Barter-specific collection boundary to the generic B1 source ingestion primitives without making `fdc-ingestion` depend on `fdc-barter`.

Recommended scope:

- Add a Barter-to-`SourceEnvelope<T>` adapter bridge outside `fdc-ingestion`, likely in `fdc-barter` or a higher-level integration crate.
- Map `BarterIngestionEnvelope` / `BarterMarketEvent` metadata into generic `SourceEnvelope` fields.
- Preserve source identifiers, event times, received/emitted times, quality flags, symbols, exchange metadata, and checkpoint hints where available.
- Feed bounded fixtures through `run_source_pipeline_once` using dummy or Barter fixture payloads.
- Keep the bridge bounded and test-only/demo-friendly at first; do not add a real WebSocket, REST historical pagination runtime, storage sink, or transform sink in this slice.
- Preserve the dependency boundary: `fdc-ingestion` remains generic and independent from `fdc-barter`.

Likely starting references:

- `crates/fdc-adapter/barter/src/ingestion/*`
- `crates/fdc-adapter/barter/src/model/*`
- `crates/fdc-ingestion/src/source/envelope.rs`
- `crates/fdc-ingestion/src/source/pipeline.rs`
- `crates/fdc-ingestion/tests/source_pipeline_contract.rs`

Initial verification commands for B2 should include:

```bash
cargo test -p fdc-barter -p fdc-ingestion
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

## Later Work After B1c

These should be separate plans, not bundled into B1c or B2:

1. Checkpoint persistence boundary.
2. Transform sink boundary from source batch output into `fdc-transform`.
3. Storage sink boundary.
4. Stateful dedupe/gap detection.
5. Warning cleanup across existing crates.
6. Revisit `.gitignore` ignoring `Cargo.lock`. For application/workspace reproducibility, committing `Cargo.lock` is usually preferable, but this repository currently ignores it and already has an untracked/ignored lockfile history pattern.

## Resume Checklist

When starting the next session:

1. Run:

   ```bash
   git status --short
   git branch --show-current
   rustup show active-toolchain
   df -h .
   test -f /Volumes/wdata/mountainsea-lab/barter-rs/barter-data/Cargo.toml
   ```

2. Confirm branch is `mdb-mqdev` and toolchain is overridden by `rust-toolchain.toml` to Rust 1.95.
3. Confirm the local Barter-rs checkout exists if you need to run `fdc-barter` tests.
4. Read this file.
5. Read the B1 source path review and the B1c pipeline plan/status before designing B2.
6. Write a B2 implementation plan before editing code.
7. Use TDD: create failing contract tests before implementation.
8. Keep `fdc-ingestion` independent from `fdc-barter`.
9. Run the verification baseline before committing, or document why the local Barter-rs dependency is unavailable.
10. Update this file at the end of the session with completed work, verification evidence, and next recommended slice.
