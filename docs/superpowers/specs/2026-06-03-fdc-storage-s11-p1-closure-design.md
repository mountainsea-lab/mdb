# fdc-storage S11 P1 Closure Design

Date: 2026-06-03
Module: `crates/fdc-storage`
Status: approved for implementation

## Goal

S11 closes the remaining P1 `fdc-storage` production-readiness gaps before broad module integration. It keeps `fdc-storage` generic and module-local while replacing string-based compaction classification, adding structured tracing, freezing lifecycle duplicate-key TTL semantics, and documenting public API stability rules.

## Non-goals

- No `fdc-barter -> fdc-ingestion -> fdc-storage` full pipeline glue.
- No server route, runtime exporter, or API integration.
- No background maintenance scheduler.
- No durable audit sink implementation.
- No disk usage/path health checks.
- No query indexes, cursor pagination, physical shard routing, replication orchestration, or backup orchestration.
- No market-data-specific DTO/type logic inside `fdc-storage`.
- No changes to unrelated dirty `fdc-server health` files.

## Scope

S11 implements exactly four P1 closure items:

1. Engine-level compaction capability/error typing.
2. Structured tracing spans around write, query, lifecycle, and maintenance operations.
3. Test and documentation lock-in for duplicate-key TTL hard-delete semantics across tiers.
4. Public API stability and breaking-change policy documentation.

P2/P3 production deployment items remain documented follow-ups after S11.

## Architecture

### Engine-level compaction typing

Current S10 maintenance distinguishes unsupported compaction from failed compaction by inspecting error text from `StorageEngine::compact()`. S11 replaces that behavior with typed engine-level semantics.

Add focused types in `crates/fdc-storage/src/engine.rs`:

- `StorageEngineFeature`: an enum for optional engine features, initially including `Compaction`, `Snapshot`, `Restore`, and `SqlQuery`.
- `StorageEngineFeatureError`: a structured error payload containing `feature: StorageEngineFeature`, `engine_type: StorageEngineType`, and `message: String`.
- Helper constructors for unsupported feature errors that format stable `fdc_core::Error` messages.

Extend the `StorageEngine` trait with a non-breaking default method:

```rust
fn supports_feature(&self, feature: StorageEngineFeature) -> bool
```

The default implementation derives support from `capabilities()` for known feature flags. Engines may override it when a capability cannot express a feature precisely.

`StorageEngine::compact()` remains source-compatible as `async fn compact(&self) -> Result<()>`, but its default unsupported branch will return an error created through the typed helper. `TieredStorageStore` will no longer classify unsupported compaction by arbitrary message matching. Instead it will ask `engine.supports_feature(StorageEngineFeature::Compaction)` through `TierManager` before calling compaction. Unsupported engines produce `StorageCompactionOutcomeKind::Unsupported` without attempting compaction. Supported engines that return an error produce `StorageCompactionOutcomeKind::Failed`.

This avoids a broad public API break while eliminating S10's fragile string inspection from maintenance.

### TierManager compaction surface

Add a small typed method to `TierManager`:

```rust
pub async fn compact_tier_with_outcome(&self, tier: &StorageTier) -> Result<StorageCompactionOutcome>
```

Behavior:

- Missing/uninitialized tier returns a storage error, as today.
- Initialized engine without compaction support returns `StorageCompactionOutcome::unsupported(...)`.
- Initialized engine with support and successful `compact()` returns `StorageCompactionOutcome::compacted(...)`.
- Initialized engine with support and failed `compact()` returns `StorageCompactionOutcome::failed(...)`.

`compact_tier()` remains for compatibility and delegates to the existing success/error behavior where appropriate. `TieredStorageStore::run_maintenance_once_inner()` uses the new outcome method and computes existing report fields from outcomes.

### Structured tracing

Add `tracing` spans at module boundaries that will matter during integration:

- `StorageWriteSink for TieredStorageStore::write_batch`
- `QueryableStorage for TieredStorageStore::query_storage`
- `TieredStorageStore::query_storage_with_metrics`
- `TieredStorageStore::run_lifecycle_once`
- `TieredStorageStore::storage_health_snapshot`
- `TieredStorageStore::run_maintenance_once_with_options`
- `TieredStorageStore::run_maintenance_once_inner`

Spans should include storage-owned fields only:

- namespace
- collection when available
- batch size
- query limit
- tier scope
- scanned/returned counts where available
- lifecycle action counts
- compaction outcome counts
- maintenance duration

Do not log record payload bytes, business DTO fields, secrets, or high-cardinality arbitrary tag maps. Use `tracing::instrument(skip(...))` where method parameters may be large, and explicit `tracing::debug!`, `tracing::info!`, or `tracing::warn!` events for summary outcomes.

### Lifecycle duplicate-key TTL semantics

S11 keeps the current S10 behavior: if a decoded `StorageWriteRecord` is expired by TTL, `run_lifecycle_once()` deletes that storage key from all initialized tiers through `TierManager::delete(&key)`. This is intentional because the storage key is `namespace\0collection\0record_key`; multiple tier copies represent the same logical record, not independent versions.

Add a regression test that writes the same expired logical record into at least two tiers, runs lifecycle once, and proves the key is absent from all initialized tiers afterward. The report should count at least one `TtlExpiredDelete` action. The test must use memory-backed tiers or temp durable paths so it remains deterministic and fast.

Document this semantic in:

- `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- `crates/fdc-storage/docs/production-hardening-followups.md`

### Public API stability policy

Add `crates/fdc-storage/docs/public-api-stability.md`.

Policy:

- Public re-exports in `crates/fdc-storage/src/lib.rs` are the supported API surface for integration consumers.
- Changes that remove, rename, or change semantics of re-exported types, trait methods, enum variants, or constructors are breaking changes.
- New fields on serde structs should be additive and have defaults where practical.
- Existing public methods should prefer additive overloads/new methods instead of signature changes.
- Deprecated APIs should remain for at least one planned migration window unless a correctness or safety issue requires immediate removal.
- Tests in `tests/public_api_stability.rs` should be updated whenever the public API intentionally expands.

Update `storage-boundary-acceptance-report.md` to reference this document and mark S11 P1 closure status.

## Error handling

- Unsupported compaction is not treated as a maintenance failure.
- Supported compaction returning an error is treated as failed compaction and appears in `compaction_errors` and `compaction_outcomes`.
- Existing `StorageMaintenanceErrorKind::AlreadyRunning`, `Timeout`, and `AuditFailed` behavior remains unchanged.
- Tracing must add observability without changing returned errors.

## Testing strategy

Focused tests:

- Engine feature support defaults classify memory/redb/duckdb as compaction unsupported unless explicitly supported.
- RocksDB reports compaction support and successful compaction outcome.
- `TierManager::compact_tier_with_outcome()` returns unsupported without relying on error string content.
- `TieredStorageStore::run_maintenance_once()` report counts unsupported/compacted/failed outcomes correctly.
- Duplicate-key expired TTL lifecycle deletes all tier copies.
- Public API smoke test imports new compaction feature types and existing S10 types.

Full verification:

```bash
rtk cargo fmt -p fdc-storage
rtk cargo test -p fdc-storage
```

Clean generated storage data after tests before committing or merging:

```bash
rm -rf crates/fdc-storage/data
```

## Documentation updates

Update:

- `crates/fdc-storage/docs/production-hardening-followups.md`
- `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- `docs/superpowers/progress/2026-06-03-mdb-storage-s1-s10-progress.md` only if renamed or extended to S11 handoff; otherwise create a new S11 progress note after implementation.

Create:

- `crates/fdc-storage/docs/public-api-stability.md`

## Acceptance criteria

- Maintenance compaction classification no longer depends on string matching.
- Unsupported compaction is represented by typed engine capability checks.
- Structured tracing spans exist for write/query/lifecycle/maintenance boundaries.
- Duplicate-key TTL hard-delete across tiers is covered by a regression test and documented as intended behavior.
- Public API stability policy exists and is referenced by acceptance docs.
- `rtk cargo test -p fdc-storage` passes.
- Generated `crates/fdc-storage/data` files are removed after tests.
- Unrelated `fdc-server health` dirty files remain untouched.

## Remaining after S11

After S11, `fdc-storage` should be acceptable for broad module integration as a generic storage boundary. It is still not full production deployment ready until P2/P3 items are implemented, especially runtime scheduler/exporter wiring, durable audit persistence, disk health, index/pagination, shard routing, and backup orchestration.

## Self-review

- Completeness scan: no incomplete work markers.
- Scope check: focused on four S11 P1 closure items only.
- Consistency check: design preserves public API compatibility while adding typed compaction support.
- Ambiguity check: lifecycle duplicate-key TTL semantics are explicitly defined as delete all tier copies for one logical storage key.
