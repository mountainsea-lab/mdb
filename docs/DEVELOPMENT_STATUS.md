# Development Status

Last updated: 2026-05-21
Branch: `mdb-mqdev`
Remote: `origin/mdb-mqdev`
Latest checkpoint commit when this file was written: `HEAD` (`docs: update live acquisition status`)

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

### fdc-barter Phase B2: SourceEnvelope Bridge

Implemented in `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`.

Completed capabilities:

- Added `IntoSourceEnvelope` for converting `BarterIngestionEnvelope` into `SourceEnvelope<BarterMarketEvent>`.
- Preserved Barter envelope identity, source id, sequence, timing, payload, quality flags, metadata, and checkpoint hints.
- Mapped live events to `SourceType::MarketData`.
- Mapped historical events to `SourceType::Replay` and ensured historical events carry backfill semantics.
- Mapped `BarterCheckpoint` into `SourceCheckpoint` without persistence.
- Demonstrated bounded Barter fixture flow through `run_source_pipeline_once` with a recording sink.
- Preserved the dependency boundary: `fdc-ingestion` still has no `fdc-barter` dependency.

Contract tests:

- `crates/fdc-adapter/barter/tests/source_bridge_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-21-fdc-barter-source-envelope-bridge-design.md`
- `docs/superpowers/plans/2026-05-21-fdc-barter-source-envelope-bridge.md`

### fdc-barter Phase B3: Live Exchange Acquisition

Implemented in `crates/fdc-adapter/barter/src/ingestion/live.rs`.

Completed capabilities:

- Added a thin live acquisition path for Binance Spot public trades using Barter-rs `Streams::<PublicTrades>`.
- Default first-slice subscriptions are BTC/USDT and ETH/USDT.
- Added public live acquisition API:
  - `LiveExchange`
  - `LiveTradeSubscription`
  - `default_binance_spot_trade_subscriptions`
  - `init_binance_spot_public_trades`
  - `public_trade_result_to_data_kind`
  - `map_live_trade_result`
  - `collect_live_trade_envelopes`
- Live Barter market events map into `BarterIngestionEnvelope` and remain compatible with the B2 `IntoSourceEnvelope` bridge.
- Reconnect behavior is delegated to Barter-rs. `fdc-barter` does not implement a custom reconnect or lifecycle framework.
- Historical data, persistence, transform sinks, and storage remain deferred.
- Optional real Binance Spot smoke validation is gated behind `#[ignore]` and `FDC_BARTER_LIVE_SMOKE=1`.

Contract tests:

- `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-21-fdc-barter-live-acquisition-design.md`
- `docs/superpowers/plans/2026-05-21-fdc-barter-live-acquisition.md`

Verification:

- `rtk cargo test -p fdc-barter --test live_acquisition_contract`
- `rtk cargo test -p fdc-barter -p fdc-ingestion`
- `FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-barter --test live_acquisition_contract ignored_live_smoke_can_collect_one_binance_spot_trade -- --ignored --nocapture`
- Dependency guard: no `fdc-barter` or `fdc_barter` references inside `crates/fdc-ingestion`.

### fdc-transform Phase B4: Market Data Transform Boundary

Implemented in `crates/fdc-transform` and `crates/fdc-adapter/barter/src/mapper/transform.rs`.

Completed capabilities:

- Added neutral `MarketDataDto` and market-data payload DTOs in `fdc-transform`.
- Added `MarketDataTransformSink` and in-memory `RecordingMarketDataSink` for bounded test/demo handoff.
- Added `fdc-barter` mapping from `BarterIngestionEnvelope` into neutral `MarketDataDto`.
- Demonstrated validated `SourceEnvelope<BarterMarketEvent>` batches can forward into the transform sink.
- Preserved dependency boundary: `fdc-ingestion` has no dependency on `fdc-barter` or `fdc-transform`.

Contract tests:

- `crates/fdc-transform/tests/market_data_boundary_contract.rs`
- `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-25-fdc-transform-market-data-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-fdc-transform-market-data-boundary.md`

Verification:

- `rtk cargo fmt --package fdc-transform --package fdc-barter --check`
- `rtk cargo test -p fdc-transform`
- `rtk cargo test -p fdc-barter --test transform_boundary_contract`
- `rtk cargo test -p fdc-barter --test live_acquisition_contract`
- `rtk cargo test -p fdc-barter -p fdc-ingestion`

## Current Verification Baseline

Last successful verification after B4 market data transform boundary:

```bash
rtk cargo fmt --package fdc-transform --package fdc-barter --check
rtk cargo test -p fdc-transform
rtk cargo test -p fdc-barter --test transform_boundary_contract
rtk cargo test -p fdc-barter --test live_acquisition_contract
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Optional live smoke validation was previously run with network access after B3:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-barter --test live_acquisition_contract ignored_live_smoke_can_collect_one_binance_spot_trade -- --ignored --nocapture
```

Result:

- `rtk cargo fmt --package fdc-transform --package fdc-barter --check` exit 0.
- `rtk cargo test -p fdc-transform` exit 0, 2 passed.
- `rtk cargo test -p fdc-barter --test transform_boundary_contract` exit 0, 3 passed.
- `rtk cargo test -p fdc-barter --test live_acquisition_contract` exit 0, 7 passed and 2 ignored.
- `rtk cargo test -p fdc-barter -p fdc-ingestion` exit 0, 60 passed and 2 ignored.
- Dependency guard exit 0, no `fdc-barter` / `fdc_barter` / `fdc-transform` / `fdc_transform` references in `fdc-ingestion` contract scan.
- Optional B3 live smoke test exit 0, 1 passed and collected a Binance Spot trade.
- Existing warnings remain in older crates and are intentionally not addressed yet.

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

### Phase B5: Storage Sink Boundary

Goal: connect neutral `MarketDataDto` batches to a bounded storage-facing handoff without introducing a production database runtime.

Recommended scope:

- Define a storage sink trait that accepts validated `MarketDataDto` batches.
- Add an in-memory or file-free recording sink contract first.
- Preserve dependency direction: storage-facing code should depend on neutral DTOs, not adapter crates.
- Do not add real DB writes, checkpoint persistence, or infinite stream lifecycle management in this slice.

## Later Work After B4

These should be separate plans, not bundled into the completed B4 transform boundary. B5 should address only the storage sink boundary in a bounded, testable slice before broader runtime work:

1. Checkpoint persistence boundary.
2. Storage sink boundary.
3. Stateful dedupe/gap detection.
4. Warning cleanup across existing crates.
5. Revisit `.gitignore` ignoring `Cargo.lock`. For application/workspace reproducibility, committing `Cargo.lock` is usually preferable, but this repository currently ignores it and already has an untracked/ignored lockfile history pattern.

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
5. Read the B4 transform boundary design, plan, and status before designing B5.
6. Write a B5 implementation plan before editing code.
7. Use TDD: create failing contract tests before implementation.
8. Keep `fdc-ingestion` independent from `fdc-barter`.
9. Run the verification baseline before committing, or document why the local Barter-rs dependency is unavailable.
10. Update this file at the end of the session with completed work, verification evidence, and next recommended slice.
