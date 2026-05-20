# Development Status

Last updated: 2026-05-20
Branch: `mdb-mqdev`
Remote: `origin/mdb-mqdev`
Latest checkpoint commit when this file was written: `9436631 build: pin rust toolchain`

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

## Current Verification Baseline

Last successful verification after fixing disk/toolchain issue:

```bash
cargo test -p fdc-barter -p fdc-ingestion
```

Result:

- Exit code: 0
- `fdc-barter` contract tests passed.
- `fdc-ingestion` unit and source contract tests passed.
- Existing warnings remain and are intentionally not addressed yet.

Fast compile-only check that also passed:

```bash
cargo test -p fdc-barter -p fdc-ingestion --no-run
```

### External Local Dependency Caveat

`fdc-barter` currently depends on local Barter-rs crates by absolute path:

```toml
barter-data = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-data" }
barter-instrument = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-instrument" }
```

At the time this status document was updated, a fresh verification attempt failed because `/Volumes/wdata/mountainsea-lab/barter-rs` was not present on disk:

```text
failed to read `/Volumes/wdata/mountainsea-lab/barter-rs/barter-data/Cargo.toml`
No such file or directory (os error 2)
```

This is a local dependency availability issue, not a source-path code failure. Before running `fdc-barter` tests in a new session, restore or clone the Barter-rs checkout at that path, or change the dependency strategy in a separate planned task.

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
- B1a/B1b are generic source-path building blocks, not real Barter stream integration.
- Do not add real WebSocket, REST, storage, or transform I/O inside B1 source-path primitives.
- Existing network byte ingestion path in `crates/fdc-ingestion/src/batch.rs`, `receiver.rs`, `parser.rs`, and `validator.rs` should remain unchanged unless a specific plan says otherwise.
- Checkpoint persistence is not implemented yet.
- Cross-event dedupe/gap detection state machine is not implemented yet.

## Next Recommended Development Slice

### Phase B1c: Bounded Source Pipeline Helper and Demo Fixture

Source design reference:

- `docs/architecture/fdc-ingestion-phase-b1-source-path-pseudocode-review.md`, section `Phase B1c：pipeline glue 和 demo fixture`.

Recommended plan document to create next:

- `docs/superpowers/plans/2026-05-20-fdc-ingestion-phase-b1c-source-pipeline.md`

Suggested B1c scope:

- Add a bounded helper such as `run_source_pipeline_once` or similarly named API.
- The helper should accept a finite set/stream of already constructed envelopes or payloads.
- It should validate envelopes with `SourceValidator`.
- It should wrap validation results into `SourceBatchItem<T>`.
- It should hand items to `SourceBatchProcessor<T>`.
- It should flush remaining buffered items before returning.
- It should use dummy payloads in tests, not `fdc-barter` types.
- It should not be an infinite loop.
- It should not do real network, REST, transform, or storage I/O.
- It should not implement checkpoint persistence.

Likely files for B1c:

- Create: `crates/fdc-ingestion/src/source/pipeline.rs`
- Create: `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
- Modify: `crates/fdc-ingestion/src/source/mod.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`

Initial verification commands for B1c:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract
cargo test -p fdc-ingestion
cargo test -p fdc-barter -p fdc-ingestion
```

Dependency guard:

```bash
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

## Later Work After B1c

These should be separate plans, not bundled into B1c:

1. Barter-to-source adapter bridge outside `fdc-ingestion`, likely in `fdc-barter` or a higher-level integration crate.
2. Checkpoint persistence boundary.
3. Transform sink boundary from source batch output into `fdc-transform`.
4. Storage sink boundary.
5. Stateful dedupe/gap detection.
6. Warning cleanup across existing crates.
7. Revisit `.gitignore` ignoring `Cargo.lock`. For application/workspace reproducibility, committing `Cargo.lock` is usually preferable, but this repository currently ignores it and already has an untracked/ignored lockfile history pattern.

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
5. Read the B1 source path review sections for B1c.
6. Write a B1c implementation plan before editing code.
7. Use TDD: create failing contract tests before implementation.
8. Keep `fdc-ingestion` independent from `fdc-barter`.
9. Run the verification baseline before committing, or document why the local Barter-rs dependency is unavailable.
10. Update this file at the end of the session with completed work, verification evidence, and next recommended slice.
