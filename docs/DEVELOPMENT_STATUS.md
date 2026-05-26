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

### fdc-barter Phase B2: SourceEnvelope Bridge (superseded)

This slice was implemented earlier in `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`, but it was later removed by the B4b architecture audit because adapter crates should not depend on `fdc-ingestion`.

Superseded capabilities:

- Converted `BarterIngestionEnvelope` into `SourceEnvelope<BarterMarketEvent>`.
- Demonstrated bounded Barter fixture flow through `run_source_pipeline_once`.

Current status:

- The adapter-local bridge and `crates/fdc-adapter/barter/tests/source_bridge_contract.rs` have been removed.
- Future adapter-to-ingestion bridge code should live in an orchestration/glue layer or a dedicated integration crate, not in `fdc-barter` or `fdc-transform` core.
- `fdc-barter` now only owns Barter-rs acquisition and adapter-owned event/envelope models.

Historical docs:

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
- Live Barter market events map into `BarterIngestionEnvelope`; downstream source-envelope bridging is deferred to orchestration/glue.
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

### fdc-transform Phase B4/B4b: Market Data DTO Boundary and Adapter Decoupling

Implemented in `crates/fdc-transform`, with adapter-owned envelopes/events produced by `crates/fdc-adapter/barter`.

Completed capabilities:

- Added neutral `MarketDataDto` and market-data payload DTOs in `fdc-transform`.
- Added `MarketDataTransformSink` and in-memory `RecordingMarketDataSink` for bounded test/demo handoff.
- Removed adapter-specific Barter bridge/mapper code from `fdc-transform` after architecture audit.
- Preserved dependency boundaries: `fdc-barter` has no dependency on `fdc-ingestion` or `fdc-transform`; `fdc-ingestion` has no dependency on `fdc-barter` or `fdc-transform`; `fdc-transform` has no dependency on `fdc-barter` or `fdc-ingestion`.
- Deferred adapter-to-ingestion and adapter-to-transform glue to a future orchestration/integration slice.

Contract tests:

- `crates/fdc-transform/tests/market_data_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-25-fdc-transform-market-data-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-fdc-transform-market-data-boundary.md`
- `docs/superpowers/specs/2026-05-25-b4b-ingestion-centered-transform-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-b4b-ingestion-centered-transform-boundary.md`

Verification:

- `rtk cargo fmt --package fdc-transform --package fdc-barter --check`
- `rtk cargo test -p fdc-transform`
- `rtk cargo test -p fdc-barter --test live_acquisition_contract`
- `rtk cargo test -p fdc-barter -p fdc-ingestion`

## Current Verification Baseline

Last successful verification after B7 server assembly boundary implementation on branch `mdb-mqdev` at local commit `28c247c` before status-doc commit:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-orchestrator -p fdc-storage
```

Previous B6 orchestration glue boundary baseline also passed:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-orchestrator --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter -p fdc-ingestion -p fdc-transform -p fdc-storage
```

Previous B5 tier-aware storage sink boundary baseline also passed:

```bash
rtk cargo fmt --package fdc-storage --check
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
rtk cargo test -p fdc-storage
! grep -RInE "fdc-transform|fdc_transform|fdc-ingestion|fdc_ingestion|fdc-barter|fdc_barter" crates/fdc-storage/Cargo.toml crates/fdc-storage/src
```

Previous B4b architecture audit baseline also passed:

```bash
rtk cargo fmt --package fdc-transform --package fdc-barter --check
rtk cargo test -p fdc-transform
rtk cargo test -p fdc-barter --test live_acquisition_contract
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Optional live smoke validation was previously run with network access after B3:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-barter --test live_acquisition_contract ignored_live_smoke_can_collect_one_binance_spot_trade -- --ignored --nocapture
```

Result:

- `rtk cargo fmt --package fdc-storage --check` exit 0.
- `rtk cargo test -p fdc-storage --test storage_sink_boundary_contract` exit 0, 8 passed.
- `rtk cargo test -p fdc-storage` exit 0, 45 passed.
- Storage dependency guard exit 0, no upstream pipeline or adapter crate references in `fdc-storage` manifest/src.
- Previous B4b checks passed before B5:
  - `rtk cargo fmt --package fdc-transform --package fdc-barter --check` exit 0.
  - `rtk cargo test -p fdc-transform` exit 0, 2 passed.
  - `rtk cargo test -p fdc-barter --test live_acquisition_contract` exit 0, 6 passed and 2 ignored.
  - `rtk cargo test -p fdc-barter -p fdc-ingestion` exit 0, 51 passed and 2 ignored.
  - Dependency guard exit 0, no downstream crate references in `fdc-barter`, no adapter/transform references in `fdc-ingestion`, no adapter/ingestion references in `fdc-transform`.
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
- `fdc-storage` must not depend on `fdc-transform`, `fdc-ingestion`, or adapter crates.
- `fdc-storage` write boundary must remain storage-owned and generic; no `MarketDataDto`, adapter event, or `SourceEnvelope` types inside storage core.
- B5 `StoragePlacementHint` is advisory only; do not add production `TierManager` / `ShardManager` / engine routing without a new design review.
- B5 `RecordingStorageSink` is file-free and database-free; do not hide real DB writes behind it.
- Concrete `MarketDataDto -> StorageWriteRecord` mapping belongs in `fdc-orchestrator`, not in `fdc-storage`.
- `fdc-orchestrator` currently implements Barter market-data glue only, but its crate boundary must remain extensible for future adapter data sources; do not bake Barter-only assumptions into crate-wide APIs.

## Completed Development Slice

### Phase B5: Tier-aware Storage Sink Boundary

Implemented in `crates/fdc-storage`.

Completed capabilities:

- Added storage-owned generic write boundary types:
  - `StorageWriteRecord`
  - `StorageWriteBatch`
  - `StorageWriteMetadata`
  - `StorageBatchMetadata`
- Added tier-aware placement hints aligned with existing L1/L2/L3/L4 storage architecture:
  - `StoragePlacementHint`
  - `StorageAccessPatternHint`
  - `StorageDurabilityHint`
  - optional shard routing key and TTL
- Added async `StorageWriteSink` trait.
- Added file-free, database-free `RecordingStorageSink` for contract tests and future examples.
- Preserved dependency boundaries: `fdc-storage` does not depend on `fdc-transform`, `fdc-ingestion`, or adapter crates.
- Deferred production `TierManager` / `ShardManager` / engine routing to a future storage runtime slice.
- Deferred `MarketDataDto -> StorageWriteRecord` mapping to a future orchestration/integration slice.

Contract tests:

- `crates/fdc-storage/tests/storage_sink_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-25-fdc-storage-sink-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-fdc-storage-sink-boundary.md`

Verification:

- `rtk cargo fmt --package fdc-storage --check`
- `rtk cargo test -p fdc-storage --test storage_sink_boundary_contract`
- `rtk cargo test -p fdc-storage`
- Dependency guard: no `fdc-transform`, `fdc_transform`, `fdc-ingestion`, `fdc_ingestion`, `fdc-barter`, or `fdc_barter` references in `crates/fdc-storage/Cargo.toml` or `crates/fdc-storage/src`.

### Phase B6: Orchestration Glue Boundary

Implemented in `crates/fdc-orchestrator`.

Completed capabilities:

- Added a dedicated integration/orchestration crate for bounded cross-layer glue.
- Registered `fdc-orchestrator` as a workspace member without adding reverse dependencies from core crates.
- Mapped `BarterIngestionEnvelope` to `SourceEnvelope<BarterMarketEvent>` without adding downstream dependencies to `fdc-barter`.
- Mapped Barter market events to neutral `MarketDataDto` values without adding adapter dependencies to `fdc-transform`.
- Mapped `MarketDataDto` to generic `StorageWriteRecord` values without adding transform dependencies to `fdc-storage`.
- Added a finite in-memory helper that validates source envelopes and writes storage records to any `StorageWriteSink`.
- Verified the bounded fixture path with `RecordingStorageSink` and dependency guard tests.
- Kept B6 market-data-only while preserving the `fdc-orchestrator` crate boundary for future non-Barter adapter data sources.

Contract tests:

- `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-26-fdc-orchestration-glue-boundary-design.md`
- `docs/superpowers/plans/2026-05-26-fdc-orchestration-glue-boundary.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-orchestrator --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter -p fdc-ingestion -p fdc-transform -p fdc-storage` exit 0, 98 passed and 2 ignored.

### Phase B7: Server Assembly Boundary

Implemented in `crates/fdc-server`.

Completed capabilities:

- Replaced the template `fdc-server` crate with a real application assembly boundary.
- Added `FdcServerConfig` and `ServerEnvironment` for server-level assembly configuration.
- Added `ServerComponents` with an injectable `Arc<dyn StorageWriteSink>` market-data storage sink.
- Added default database-free assembly using `RecordingStorageSink`.
- Added `FdcServerApp` and `ServerLifecycleState` for deterministic lifecycle transitions.
- Verified that `fdc-server` can consume `fdc-orchestrator` public types without moving orchestration glue into server code.
- Preserved dependency direction: lower-level crates and `fdc-api` do not reference `fdc-server`.

Contract tests:

- `crates/fdc-server/tests/server_assembly_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-26-fdc-server-assembly-boundary-design.md`
- `docs/superpowers/plans/2026-05-26-fdc-server-assembly-boundary.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract` exit 0, 7 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server` exit 0, 7 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-orchestrator -p fdc-storage` exit 0, 57 passed.

## Next Recommended Development Slice

### Phase B8: API State Boundary

Goal: define how `fdc-api` receives application state assembled by `fdc-server` without owning orchestrator mapping logic.

Recommended scope:

- Define API-facing state handles and readiness projection.
- Keep HTTP handlers simulated unless a separate B8 implementation plan explicitly wires one bounded endpoint.
- Do not start real network services or production storage writes yet.

## Later Work After B5

These should be separate plans, not bundled into the completed B5 storage sink boundary before broader runtime work:

1. Checkpoint persistence boundary.
2. Production tier-aware storage runtime routing.
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
5. Read the B7 server assembly boundary design, plan, and status before designing B8.
6. Start B8 with architecture review and design/spec before editing code.
7. Keep storage/transform/ingestion/adapter/server boundaries decoupled; concrete cross-layer mappings belong in `fdc-orchestrator` modules.
8. Use TDD for any implementation after the B8 design/spec is approved.
9. Run the relevant verification baseline before committing, or document why a local dependency is unavailable.
10. Update this file at the end of the session with completed work, verification evidence, pushed commit, and next recommended slice.
