# Development Status

Last updated: 2026-05-29
Branch: `mdb-mqdev`
Remote: `origin/mdb-mqdev`
Latest checkpoint commit when this file was written: `HEAD` (`feat: add production server binary`)

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

### Phase B8: API State Boundary

Implemented in `crates/fdc-api`.

Completed capabilities:

- Added `ApiAppState` as a shared API-facing handle around `FdcServerApp`.
- Added serializable readiness projection types:
  - `ApiReadinessStatus`
  - `ApiReadinessProjection`
- Added stable API labels for server environment and lifecycle state.
- Added `readiness_response_from_state` as a pure readiness response helper that does not start network services.
- Preserved dependency direction: `fdc-api` consumes `fdc-server`; lower-level/application assembly crates do not reference `fdc-api`.
- Kept orchestrator mapping logic, network startup, and production storage writes out of `fdc-api`.

Contract tests:

- `crates/fdc-api/tests/api_state_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-27-fdc-api-state-boundary-design.md`
- `docs/superpowers/plans/2026-05-27-fdc-api-state-boundary.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --package fdc-server --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test api_state_boundary_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 27 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server` exit 0, 34 passed.

### Phase B9: Queryable Market Data Storage Boundary

Implemented in `crates/fdc-storage`, with an integration contract in `crates/fdc-orchestrator`.

Completed capabilities:

- Added `QueryableMarketDataStore`, an in-memory `StorageWriteSink` for MVP market-data reads.
- Added `MarketDataQuery` with filters for namespace, collection, symbol, kind, and limit.
- Preserved insertion-order reads and atomic validation before writes mutate store state.
- Kept `fdc-storage` decoupled from `fdc-transform`, `fdc-ingestion`, `fdc-barter`, `fdc-orchestrator`, and `fdc-api`.
- Verified the existing Barter fixture orchestration path can write into `QueryableMarketDataStore` and query the stored BTCUSDT trade record back.
- Deferred SQL engine integration, persistence, live runner lifecycle, and API route exposure to later slices.

Contract tests:

- `crates/fdc-storage/tests/queryable_market_data_store_contract.rs`
- `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-27-fdc-queryable-market-data-storage-boundary-design.md`
- `docs/superpowers/plans/2026-05-27-fdc-queryable-market-data-storage-boundary.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-storage --package fdc-orchestrator --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage --test queryable_market_data_store_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract` exit 0, 1 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-storage -p fdc-orchestrator` exit 0, 56 passed.

### Phase B10: Bounded API Market Data Query Route

Implemented in `crates/fdc-api`.

Completed capabilities:

- Extended `ApiAppState` with an injectable shared `QueryableMarketDataStore`.
- Added API market-data DTOs for trade query responses.
- Added `query_market_data_trades` pure helper for storage-backed API responses.
- Added `build_market_data_router` with bounded in-memory route `GET /market-data/trades`.
- Verified in-memory Axum route querying seeded B9 store data without starting real listeners.
- Kept acquisition, orchestrator mapping, SQL query engine integration, and production persistence out of `fdc-api`.

Contract tests:

- `crates/fdc-api/tests/market_data_route_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-27-fdc-api-market-data-query-route-design.md`
- `docs/superpowers/plans/2026-05-27-fdc-api-market-data-query-route.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 31 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-storage` exit 0, 81 passed.

### Phase B11: Bounded Acquisition-to-API MVP Runner

Implemented in `crates/fdc-server`, with end-to-end API contract coverage in `crates/fdc-api`.

Completed capabilities:

- Added `BoundedMarketDataMvpRunner` and `run_barter_fixture_mvp_once` as a fixture-only assembly helper.
- Reused `fdc-orchestrator::run_barter_envelopes_to_storage_once` for Barter envelope mapping and storage writes.
- Populated a shared `QueryableMarketDataStore` and queried it through the B10 in-memory market-data API route.
- Kept production lifecycle, persistence, SQL integration, and ungated network tests out of scope.

Contract tests:

- `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-bounded-acquisition-api-mvp-runner-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-bounded-acquisition-api-mvp-runner.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-orchestrator -p fdc-storage` exit 0, 96 passed.

### Phase B12: Ignored Live Acquisition Smoke to MVP Store

Implemented as an ignored, network-gated contract test in `crates/fdc-api`.

Completed capabilities:

- Added an opt-in live smoke test that initializes Binance Spot public trade streams via `fdc-barter`.
- Collects one live trade envelope with a bounded timeout.
- Writes the live envelope through the B11 `run_barter_fixture_mvp_once` helper into a shared `QueryableMarketDataStore`.
- Queries the shared store through the B10 in-memory API route and asserts a returned trade record.
- Keeps default test runs offline because the live smoke is `#[ignore]` and gated by `FDC_BARTER_LIVE_SMOKE=1`.

Contract tests:

- `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-live-acquisition-api-smoke-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-live-acquisition-api-smoke.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract` exit 0, 2 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server -p fdc-barter` exit 0, 52 passed and 3 ignored.
- Optional live command: `FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture`.

### Phase B13: Bounded Application Runner Lifecycle

Implemented in `crates/fdc-server`.

Completed capabilities:

- Added `BoundedMarketDataRunnerHandle` for fixture-only finite market-data MVP runs.
- Added `BoundedRunnerState` with `Created`, `Running`, `Completed`, `Cancelled`, and `Failed` states.
- Added deterministic pre-start cancellation and invalid transition validation.
- Stored successful B11 MVP results and failure messages for later status projection work.
- Kept production background tasks, live stream supervision, persistence, SQL integration, and API status routes out of scope.

Contract tests:

- `crates/fdc-server/tests/bounded_runner_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-bounded-application-runner-lifecycle-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-bounded-application-runner-lifecycle.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server` exit 0, 11 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-storage` exit 0, 94 passed and 1 ignored.

### Phase B14: Runner Status API Projection

Implemented in `crates/fdc-api`.

Completed capabilities:

- Extended `ApiAppState` with an optional shared `BoundedMarketDataRunnerHandle`.
- Added serializable runner status projection types for configured state, lifecycle state, last result counts, and failure message.
- Added `runner_status_response_from_state` pure helper.
- Added bounded in-memory `GET /runner/status` route.
- Kept runner mutation, background runtime, live acquisition, persistence, and SQL integration out of scope.

Contract tests:

- `crates/fdc-api/tests/runner_status_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-runner-status-api-projection-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-runner-status-api-projection.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 38 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server` exit 0, 49 passed and 1 ignored.

### Phase B15: Bounded Runner Control API

Implemented in `crates/fdc-api`.

Completed capabilities:

- Extended `ApiAppState` with an optional mutable `Arc<tokio::sync::Mutex<BoundedMarketDataRunnerHandle>>` control handle for demo/test use.
- Added `RunnerStartFixtureRequest` and `RunnerFixtureTradeInput` fixture DTOs.
- Added pure async control helpers:
  - `start_fixture_runner_from_state`
  - `cancel_runner_from_state`
- Added bounded in-memory control routes:
  - `POST /runner/start-fixture`
  - `POST /runner/cancel`
- Start-fixture runs finite fixture trades through the B13 runner and writes to the shared B9 queryable store, making records readable through the B10 market-data API route.
- Control responses reuse the B14 runner status projection shape.
- Missing control handles and invalid fixture input return structured `ApiResponse` errors with status projection data.
- Kept live network acquisition, background daemon supervision, persistence, and SQL integration out of scope.

Contract tests:

- `crates/fdc-api/tests/runner_control_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-bounded-runner-control-api-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-bounded-runner-control-api.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract` failed before implementation with missing control API symbols.
- `rtk cargo fmt --package fdc-api` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 43 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server` exit 0, 54 passed and 1 ignored.

### Phase B16: Unified Demo API Router

Implemented in `crates/fdc-api/src/demo.rs`.

Completed capabilities:

- Added `build_demo_router(state)` for in-memory bounded MVP demos.
- Combined typed readiness, runner status, runner control, and market-data query routes into one shared-state Axum router.
- Verified `POST /runner/start-fixture` writes records that are immediately readable through `GET /market-data/trades` on the same demo router.
- Verified `GET /runner/status` observes the mutable B15 control handle when present, so start/status/query use the same runner state.
- Verified `POST /runner/cancel` works through the unified router for a fresh created runner.
- Preserved dependency boundaries: lower-level crates do not reference `fdc-api`.
- Kept real listener binding, live network acquisition, persistence, SQL integration, and production daemon supervision out of scope.

Contract tests:

- `crates/fdc-api/tests/demo_router_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-unified-demo-api-router-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-unified-demo-api-router.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract` failed before implementation with missing `build_demo_router`.
- Root-cause fix during implementation: runner status route now prefers the B15 mutable control handle when present; otherwise it falls back to the B14 read-only handle.
- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 47 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server` exit 0, 58 passed and 1 ignored.

### Phase B17: Local Demo Flow Helper

Implemented in `crates/fdc-api/src/demo_flow.rs`.

Completed capabilities:

- Added `default_demo_flow_request` with deterministic BTCUSDT fixture input.
- Added `run_demo_flow_once` for no-listener in-memory demo execution.
- Added typed demo DTOs:
  - `DemoFixtureTrade`
  - `DemoFlowRequest`
  - `DemoFlowSummary`
- Exercised the B16 unified router through Axum/Tower `oneshot` calls rather than direct helper shortcuts.
- Verified ready -> start-fixture -> status -> market-data query returns one typed summary.
- Verified multiple fixture trades can be filtered by query symbol.
- Preserved dependency boundaries: lower-level crates do not reference `fdc-api`.
- Kept real listener binding, CLI/binary startup, live network acquisition, persistence, SQL integration, and production daemon supervision out of scope.

Contract tests:

- `crates/fdc-api/tests/demo_flow_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-local-demo-flow-helper-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-local-demo-flow-helper.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract` failed before implementation with missing demo flow API symbols.
- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 52 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server` exit 0, 63 passed and 1 ignored.

### Phase B18: MVP Demo Documentation

Implemented in `docs/mvp/first-mvp-demo.md`, with a documentation contract in `crates/fdc-api/tests/demo_documentation_contract.rs`.

Completed capabilities:

- Added a human-facing first MVP demo guide.
- Documented the no-listener, in-memory, deterministic MVP scope.
- Documented the exact demo-flow verification command.
- Documented programmatic usage of `default_demo_flow_request` and `run_demo_flow_once`.
- Documented default fixture request shape and expected `DemoFlowSummary` highlights.
- Mapped B16 routes to B17 summary fields.
- Documented optional live smoke validation as outside the default MVP.
- Added a documentation contract test to keep the guide anchored to real APIs, routes, and scope boundaries.

Contract tests:

- `crates/fdc-api/tests/demo_documentation_contract.rs`

Important docs:

- `docs/mvp/first-mvp-demo.md`
- `docs/superpowers/specs/2026-05-29-fdc-mvp-demo-documentation-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-mvp-demo-documentation.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract` failed before implementation because `docs/mvp/first-mvp-demo.md` did not exist.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 54 passed and 1 ignored.

### Phase B19: MVP Acceptance Report

Implemented in `docs/mvp/first-mvp-acceptance-report.md`, with a documentation contract in `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`.

Completed capabilities:

- Declared the first internal MVP accepted.
- Froze the accepted MVP as a no-listener, in-memory market-data demo.
- Summarized included capabilities from Barter fixture models through API-readable market-data records.
- Listed exact verification commands and expected pass counts.
- Linked the first MVP demo guide.
- Froze non-goals: persistence, SQL integration, auth, production daemon supervision, default live network acquisition, and performance claims.
- Listed residual risks and recommended post-MVP tracks.
- Added a documentation contract test to keep the acceptance report anchored to real APIs, commands, and scope boundaries.

Contract tests:

- `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`

Important docs:

- `docs/mvp/first-mvp-acceptance-report.md`
- `docs/mvp/first-mvp-demo.md`
- `docs/superpowers/specs/2026-05-29-fdc-mvp-acceptance-report-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-mvp-acceptance-report.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract` failed before implementation because `docs/mvp/first-mvp-acceptance-report.md` did not exist.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract` exit 0, 5 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 56 passed and 1 ignored.

### Phase B20a: Post-MVP Stabilization Baseline

Implemented in `docs/mvp/post-mvp-stabilization-baseline.md`, with a documentation contract in `crates/fdc-api/tests/stabilization_baseline_contract.rs`.

Completed capabilities:

- Recorded the post-MVP stabilization baseline.
- Documented `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api` warning baseline: 0 errors, 28 warnings.
- Listed affected crates: `fdc-wasm`, `fdc-types`, `fdc-storage`, `fdc-query`, and `fdc-ingestion`.
- Documented current `Cargo.lock` and `.gitignore` policy without changing it.
- Re-stated dependency boundary expectations after MVP acceptance.
- Defined follow-up stabilization tracks: B20b warning cleanup by crate, B20c Cargo.lock policy decision, and B20d dependency boundary audit report.
- Added a documentation contract test to keep the stabilization baseline anchored.

Contract tests:

- `crates/fdc-api/tests/stabilization_baseline_contract.rs`

Important docs:

- `docs/mvp/post-mvp-stabilization-baseline.md`
- `docs/superpowers/specs/2026-05-29-fdc-post-mvp-stabilization-baseline-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-post-mvp-stabilization-baseline.md`

Verification:

- RED check: `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract` failed before implementation because `docs/mvp/post-mvp-stabilization-baseline.md` did not exist.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api` exit 0, 58 passed and 1 ignored.

### Realtime Market Data MVP Scope Reset

Implemented in `crates/fdc-server/src/realtime.rs`, with API-facing validation in `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`.

Completed capabilities:

- Paused warning cleanup and reset the MVP target to realtime market data acquisition -> storage -> query.
- Added `fdc_server::RealtimeMarketDataMvpConfig` for runtime-window and idle-timeout bounded stream processing.
- Added `fdc_server::RealtimeMarketDataMvpSummary` with observed envelope, pipeline, storage, queryable-record, and timestamp counts.
- Added `fdc_server::run_realtime_barter_envelope_stream`, which consumes a stream of `BarterIngestionEnvelope` values until the runtime/idle boundary and does not stop after exactly one record.
- Added offline realtime contract tests proving multiple live-style events are written and queryable.
- Added API-facing realtime contract coverage proving data written by the realtime runner is returned through `/market-data/trades`.
- Updated ignored live smoke semantics so real Binance Spot validation remains opt-in with `FDC_BARTER_LIVE_SMOKE=1` and asserts at least one real queryable record, not exactly one.
- Updated MVP demo and acceptance docs to define the first MVP as realtime acquisition -> storage -> query.
- Added a runnable demo API listener binary: `cargo run -p fdc-api --bin fdc_demo_api`.
- Added `fdc_api::initialized_demo_app_state_with_control_runner` so the listener and tests share the initialized app/store/runner-control setup.
- Added `POST /runner/start-live`, gated by `FDC_BARTER_LIVE_SMOKE=1`, to trigger real Binance Spot public trade acquisition and write results to the shared queryable store.
- Added live acquisition progress logs for stream start, envelope collection, and storage completion.

Contract tests:

- `crates/fdc-server/tests/realtime_mvp_contract.rs`
- `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`
- `crates/fdc-api/tests/demo_http_listener_contract.rs`
- `crates/fdc-api/tests/live_runner_route_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-realtime-market-data-mvp-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-realtime-market-data-mvp.md`
- `docs/mvp/first-mvp-demo.md`
- `docs/mvp/first-mvp-acceptance-report.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server` exit 0, 13 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract` exit 0, 3 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_http_listener_contract` exit 0, 1 passed.
- Actual local HTTP verification on `http://127.0.0.1:18080` with `target/debug/fdc_demo_api`:
  - `GET /ready` returned `status=success`, `data.status=ready`, `server_lifecycle_state=initialized`.
  - `POST /runner/start-fixture` with 3 trades returned `state=completed`, `storage_records_written=3`, `market_data_store_records=3`.
  - `GET /runner/status` returned the same completed runner result.
  - `GET /market-data/trades?symbol=BTCUSDT&limit=10` returned 2 BTCUSDT records with trade IDs `http-demo-btc-1` and `http-demo-btc-2`.
- Actual gated live HTTP verification on `http://127.0.0.1:18082` with `FDC_BARTER_LIVE_SMOKE=1 target/debug/fdc_demo_api`:
  - `POST /runner/start-live` with `timeout_secs=20`, `max_envelopes=20` returned `status=success`, `started=true`, `envelopes_received=20`, `storage_records_written=20`, `market_data_store_records=20`.
  - `GET /market-data/trades?limit=5` returned 5 real Binance Spot trade records across BTCUSDT/ETHUSDT.
  - Service logs included `fdc live runner: starting Binance Spot public trades`, `collected 20 live envelopes`, and `completed envelopes_received=20 storage_records_written=20 market_data_store_records=20`.

### Production Server Runtime Slice

Implemented in `crates/fdc-server/src/runtime`, `crates/fdc-server/src/health`, `crates/fdc-server/src/market_data`, and `crates/fdc-server/src/bin/fdc_server.rs`.

Completed capabilities:

- Added formal production startup entrypoint:

  ```bash
  cargo run -p fdc-server --bin fdc_server
  ```

- Added `ServerRuntimeConfig` and `ServerRuntimeEnvironment` with env/default parsing:
  - `FDC_SERVER_ADDR`, default `127.0.0.1:18080`.
  - `FDC_SERVER_ENV`, default `development`.
  - `FDC_LIVE_ENABLED`, default disabled.
  - `FDC_LIVE_DEFAULT_TIMEOUT_SECS`, default `30`.
  - `FDC_LIVE_DEFAULT_MAX_ENVELOPES`, default `100`.
- Added business-module layout under `fdc-server`:
  - `runtime/config.rs`, `runtime/app.rs`, `runtime/shutdown.rs`.
  - `health/model.rs`, `health/service.rs`, `health/router.rs`.
  - `market_data/model.rs`, `market_data/service.rs`, `market_data/router.rs`, `market_data/supervisor.rs`.
- Added production HTTP routes:
  - `GET /health`.
  - `GET /ready`.
  - `POST /market-data/live/start`.
  - `GET /market-data/live/status`.
  - `GET /market-data/trades`.
- Added disabled-by-default live acquisition gate for the production route with explicit `FDC_LIVE_ENABLED=1` message.
- Added shared in-memory market-data store in `ProductionServerState` and verified query after test ingestion.
- Added graceful shutdown helper for the production binary.

Contract tests:

- `crates/fdc-server/tests/runtime_config_contract.rs`
- `crates/fdc-server/tests/production_server_router_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-production-server-runtime-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-production-server-runtime.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract` exit 0, 3 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract` exit 0, 3 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server` exit 0, 0 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server` exit 0, 19 passed.

## Next Recommended Development Slice

### Phase B22: Production Live Supervisor Enabled Mode

Recommended scope:

- Move the already validated real Binance live route behavior from `fdc-api` demo route into `fdc-server::market_data::service`.
- Add `FDC_LIVE_ENABLED=1` production live start support behind `/market-data/live/start`.
- Update `MarketDataSupervisor` to track starting/running/completed/failed states and prevent concurrent starts.
- Add explicit stop route behavior for active/background live runs.
- Keep default tests offline; add ignored live HTTP smoke for `fdc-server --bin fdc_server`.

### Phase B20b: Warning Cleanup by Crate

Warning cleanup is intentionally paused until the realtime MVP path is accepted. When resumed, suggested scope remains:

- Start with one crate only, preferably `fdc-wasm` because it appears first in the current warning baseline.
- Remove unused imports, unused variables, and unnecessary mutability only when behavior is clearly unaffected.
- Avoid broad refactors and do not turn warnings into hard errors yet.
- Verify with that crate's tests plus the MVP API tests.

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

## Session Checkpoint: Production Live Supervisor Enabled Mode

Last updated: 2026-05-29 session checkpoint
Checkpoint commit before this note: `feat: enable production live market data start`

Current active work: production live supervisor enabled mode in `fdc-server`.

Design and plan committed:

- `docs/superpowers/specs/2026-05-29-fdc-production-live-supervisor-enabled-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-production-live-supervisor-enabled.md`

Completed in code and committed:

- `MarketDataSupervisor` state transitions:
  - `try_start()`
  - `complete(result)`
  - `fail(message)`
  - `status()`
- Supervisor now rejects concurrent starts while already starting/running/stopping.
- `fdc-server::market_data::service::start_live(...)` now supports enabled production live acquisition when `ServerRuntimeConfig.live_enabled == true`.
- Production service migrated the proven live path from the `fdc-api` demo route:
  - `init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())`
  - `streams.select_all().map(public_trade_result_to_data_kind)`
  - `collect_live_trade_envelopes(...)`
  - `run_realtime_barter_envelope_stream(...)`
- Because Barter stream types are not safe to hold across Axum's Send handler future, production service uses `tokio::task::spawn_blocking` plus a current-thread Tokio runtime for collection/write.
- `POST /market-data/live/start` now calls the production service when live is enabled instead of always returning the disabled gate.
- Added ignored enabled-config route test placeholder in `production_server_router_contract.rs`.

Latest verification before stopping:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server
```

Result:

```text
production_server_router_contract: 4 passed, 1 ignored
fdc_server bin: 0 passed
```

Immediate resume steps:

1. Add ignored/gated production live smoke test:
   - file: `crates/fdc-server/tests/production_live_smoke.rs`
   - test name: `ignored_production_live_start_writes_real_trades_and_query_reads_them`
2. Verify default behavior:

   ```bash
   CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_live_smoke
   ```

   Expected: pass with 1 ignored.

3. If public internet is available, run real production live smoke:

   ```bash
   FDC_LIVE_ENABLED=1 cargo test -p fdc-server --test production_live_smoke ignored_production_live_start_writes_real_trades_and_query_reads_them -- --ignored --nocapture
   ```

4. Optionally verify actual production HTTP server:

   ```bash
   FDC_LIVE_ENABLED=1 FDC_SERVER_ADDR=127.0.0.1:18083 cargo run -p fdc-server --bin fdc_server
   curl -X POST http://127.0.0.1:18083/market-data/live/start \
     -H 'content-type: application/json' \
     -d '{"timeout_secs":20,"max_envelopes":20}'
   curl 'http://127.0.0.1:18083/market-data/live/status'
   curl 'http://127.0.0.1:18083/market-data/trades?limit=5'
   ```

5. Update this file with real production live smoke evidence and commit:

   ```bash
   rtk git add docs/DEVELOPMENT_STATUS.md
   rtk git commit -m "docs: record production live supervisor enabled mode"
   ```

Next recommended development slice after production live smoke is verified: true background live runner plus `/market-data/live/stop`.
