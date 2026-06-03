# fdc-storage S11 P1 Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining P1 `fdc-storage` production-readiness gaps by replacing string-based compaction classification, adding tracing spans, freezing duplicate-key TTL semantics, and documenting public API stability.

**Architecture:** Keep all changes inside `crates/fdc-storage` and its docs. Add typed engine feature support in `engine.rs`, expose typed compaction outcomes from `TierManager`, update `TieredStorageStore` maintenance to consume those outcomes, and add tracing at storage-owned boundaries without changing business semantics.

**Tech Stack:** Rust, async-trait, tokio, tracing, chrono, serde, existing `fdc_core::Error`/`Result`, existing storage engine abstractions.

---

## Files

- Modify: `crates/fdc-storage/src/engine.rs` — add `StorageEngineFeature`, `StorageEngineFeatureError`, `supports_feature()`, and typed unsupported helper tests.
- Modify: `crates/fdc-storage/src/tier.rs` — add `compact_tier_with_outcome()` and tests.
- Modify: `crates/fdc-storage/src/tiered_store.rs` — replace string-based compaction classification, add tracing spans/events, add duplicate-key TTL regression test.
- Modify: `crates/fdc-storage/src/engines/rocksdb.rs` — override or rely on feature support for compaction, add explicit test if needed.
- Modify: `crates/fdc-storage/src/lib.rs` — re-export new engine feature types.
- Modify: `crates/fdc-storage/tests/public_api_stability.rs` — import and exercise new public feature types.
- Create: `crates/fdc-storage/docs/public-api-stability.md` — document supported API surface and breaking-change policy.
- Modify: `crates/fdc-storage/docs/production-hardening-followups.md` — move S11 P1 items to done and leave P2/P3.
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md` — mark S11 P1 closure and link API policy.
- Create: `docs/superpowers/progress/2026-06-03-mdb-storage-s11-progress.md` — record S11 implementation result after verification.

## Task 1: Engine-level feature typing

**Files:**
- Modify: `crates/fdc-storage/src/engine.rs`
- Modify: `crates/fdc-storage/src/lib.rs`
- Modify: `crates/fdc-storage/tests/public_api_stability.rs`

- [ ] **Step 1: Add failing tests for feature support in `engine.rs`**

Add these tests inside `#[cfg(test)] mod tests` in `crates/fdc-storage/src/engine.rs`:

```rust
#[test]
fn engine_feature_support_maps_from_capabilities() {
    assert!(!StorageEngineFactory::get_capabilities(&StorageEngineType::Memory).supports_sql);
    assert!(StorageEngineFactory::get_capabilities(&StorageEngineType::DuckDB).supports_sql);

    struct DummyEngine {
        engine_type: StorageEngineType,
        capabilities: EngineCapabilities,
    }

    #[async_trait]
    impl StorageEngine for DummyEngine {
        fn engine_type(&self) -> StorageEngineType {
            self.engine_type.clone()
        }

        fn capabilities(&self) -> EngineCapabilities {
            self.capabilities.clone()
        }

        async fn initialize(&mut self) -> Result<()> { Ok(()) }
        async fn shutdown(&mut self) -> Result<()> { Ok(()) }
        async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> { Ok(None) }
        async fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> { Ok(()) }
        async fn delete(&self, _key: &[u8]) -> Result<()> { Ok(()) }
        async fn batch(&self, _operations: Vec<BatchOperation>) -> Result<()> { Ok(()) }
        async fn scan(
            &self,
            _start_key: Option<&[u8]>,
            _end_key: Option<&[u8]>,
            _limit: Option<usize>,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> { Ok(Vec::new()) }
        async fn stats(&self) -> Result<StorageStats> { Ok(StorageStats::default()) }
    }

    let unsupported = DummyEngine {
        engine_type: StorageEngineType::Memory,
        capabilities: EngineCapabilities::default(),
    };
    assert!(!unsupported.supports_feature(StorageEngineFeature::Compaction));
    assert!(!unsupported.supports_feature(StorageEngineFeature::SqlQuery));

    let supported = DummyEngine {
        engine_type: StorageEngineType::RocksDB,
        capabilities: EngineCapabilities {
            supports_compression: true,
            supports_sql: true,
            supports_backup: true,
            ..EngineCapabilities::default()
        },
    };
    assert!(supported.supports_feature(StorageEngineFeature::Compaction));
    assert!(supported.supports_feature(StorageEngineFeature::SqlQuery));
    assert!(supported.supports_feature(StorageEngineFeature::Snapshot));
    assert!(supported.supports_feature(StorageEngineFeature::Restore));
}

#[test]
fn unsupported_feature_error_formats_stable_message() {
    let error = StorageEngineFeatureError::unsupported(
        StorageEngineType::Memory,
        StorageEngineFeature::Compaction,
    )
    .into_error();

    let message = error.to_string();
    assert!(message.contains("unsupported storage engine feature"));
    assert!(message.contains("memory"));
    assert!(message.contains("compaction"));
}
```

- [ ] **Step 2: Run failing focused tests**

Run:

```bash
rtk cargo test -p fdc-storage engine_feature_support_maps_from_capabilities unsupported_feature_error_formats_stable_message
```

Expected: fail because `StorageEngineFeature` and `StorageEngineFeatureError` do not exist.

- [ ] **Step 3: Implement feature types and default support mapping**

In `crates/fdc-storage/src/engine.rs`, after `StorageEngineType` display impl, add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageEngineFeature {
    Compaction,
    Snapshot,
    Restore,
    SqlQuery,
}

impl std::fmt::Display for StorageEngineFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngineFeature::Compaction => write!(f, "compaction"),
            StorageEngineFeature::Snapshot => write!(f, "snapshot"),
            StorageEngineFeature::Restore => write!(f, "restore"),
            StorageEngineFeature::SqlQuery => write!(f, "sql_query"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEngineFeatureError {
    pub engine_type: StorageEngineType,
    pub feature: StorageEngineFeature,
    pub message: String,
}

impl StorageEngineFeatureError {
    pub fn unsupported(engine_type: StorageEngineType, feature: StorageEngineFeature) -> Self {
        Self {
            engine_type,
            feature,
            message: "feature is not supported by this storage engine".to_string(),
        }
    }

    pub fn into_error(self) -> Error {
        Error::unimplemented(format!(
            "unsupported storage engine feature: engine={}, feature={}, message={}",
            self.engine_type, self.feature, self.message
        ))
    }
}
```

In the `StorageEngine` trait, after `fn capabilities(&self) -> EngineCapabilities;`, add:

```rust
    fn supports_feature(&self, feature: StorageEngineFeature) -> bool {
        let capabilities = self.capabilities();
        match feature {
            StorageEngineFeature::Compaction => capabilities.supports_compression,
            StorageEngineFeature::Snapshot => capabilities.supports_backup,
            StorageEngineFeature::Restore => capabilities.supports_backup,
            StorageEngineFeature::SqlQuery => capabilities.supports_sql,
        }
    }
```

Replace the default `compact()`, `snapshot()`, `restore()`, and `query()` unsupported branches with typed helpers:

```rust
    async fn compact(&self) -> Result<()> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Compaction,
        )
        .into_error())
    }

    async fn snapshot(&self) -> Result<String> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Snapshot,
        )
        .into_error())
    }

    async fn restore(&self, _snapshot_id: &str) -> Result<()> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Restore,
        )
        .into_error())
    }

    async fn query(&self, _sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::SqlQuery,
        )
        .into_error())
    }
```

- [ ] **Step 4: Re-export public types**

In `crates/fdc-storage/src/lib.rs`, change the engine re-export line to include new types:

```rust
pub use engine::{
    EngineCapabilities, StorageEngine, StorageEngineFeature, StorageEngineFeatureError,
    StorageEngineType,
};
```

- [ ] **Step 5: Update public API smoke test**

In `crates/fdc-storage/tests/public_api_stability.rs`, add imports for the new types and a small assertion. If the file has a grouped `use fdc_storage::{...};`, include:

```rust
StorageEngineFeature, StorageEngineFeatureError, StorageEngineType,
```

Inside `public_storage_api_smoke_compiles_and_runs`, add:

```rust
let feature = StorageEngineFeature::Compaction;
let feature_error = StorageEngineFeatureError::unsupported(StorageEngineType::Memory, feature);
assert_eq!(feature_error.feature, StorageEngineFeature::Compaction);
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
rtk cargo test -p fdc-storage engine_feature_support_maps_from_capabilities unsupported_feature_error_formats_stable_message public_storage_api_smoke_compiles_and_runs
```

Expected: pass.

- [ ] **Step 7: Commit**

Run:

```bash
git add crates/fdc-storage/src/engine.rs crates/fdc-storage/src/lib.rs crates/fdc-storage/tests/public_api_stability.rs
git commit -m "feat(storage): add engine feature typing"
```

## Task 2: Typed TierManager compaction outcome

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Add failing TierManager tests**

Add these tests inside `#[cfg(test)] mod tests` in `crates/fdc-storage/src/tier.rs` near the existing compaction test:

```rust
#[tokio::test]
async fn tier_manager_compact_tier_with_outcome_reports_unsupported_without_error_string_matching() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.initialize().await.unwrap();

    let outcome = manager
        .compact_tier_with_outcome(&StorageTier::L1)
        .await
        .unwrap();

    assert_eq!(outcome.tier, StorageTier::L1);
    assert_eq!(outcome.kind, crate::StorageCompactionOutcomeKind::Unsupported);
    assert!(outcome.message.contains("compaction"));
}

#[tokio::test]
async fn tier_manager_compact_tier_with_outcome_reports_compacted_for_rocksdb() {
    let dir = tempfile::tempdir().unwrap();
    let config = TierConfig::new(StorageTier::L4).with_engine_config(
        "db_path".to_string(),
        dir.path().join("rocksdb").to_string_lossy().to_string(),
    );
    let mut manager = TierManager::new();
    manager.add_tier(config);
    manager.initialize().await.unwrap();

    let outcome = manager
        .compact_tier_with_outcome(&StorageTier::L4)
        .await
        .unwrap();

    assert_eq!(outcome.tier, StorageTier::L4);
    assert_eq!(outcome.kind, crate::StorageCompactionOutcomeKind::Compacted);
}
```

- [ ] **Step 2: Run failing focused tests**

Run:

```bash
rtk cargo test -p fdc-storage tier_manager_compact_tier_with_outcome
```

Expected: fail because `compact_tier_with_outcome` does not exist.

- [ ] **Step 3: Implement `compact_tier_with_outcome`**

In `crates/fdc-storage/src/tier.rs`, extend imports at the top:

```rust
use crate::{
    StorageAccessPatternHint, StorageCompactionOutcome, StorageCompactionOutcomeKind,
    StorageDurabilityHint, StoragePlacementHint, StorageTierScope,
};
```

Replace or extend the compaction section around `compact_tier` with:

```rust
    pub async fn compact_tier_with_outcome(
        &self,
        tier: &StorageTier,
    ) -> Result<StorageCompactionOutcome> {
        if let Some(engine) = self.engines.get(tier) {
            let engine_guard = engine.read().await;
            if !engine_guard.supports_feature(crate::StorageEngineFeature::Compaction) {
                return Ok(StorageCompactionOutcome::unsupported(
                    tier.clone(),
                    format!("{} compaction is not supported", engine_guard.engine_type()),
                ));
            }

            return match engine_guard.compact().await {
                Ok(()) => Ok(StorageCompactionOutcome::compacted(tier.clone())),
                Err(error) => Ok(StorageCompactionOutcome::failed(tier.clone(), error.to_string())),
            };
        }
        Err(Error::validation(format!(
            "storage tier {:?} is not initialized",
            tier
        )))
    }

    pub async fn compact_tier(&self, tier: &StorageTier) -> Result<()> {
        let outcome = self.compact_tier_with_outcome(tier).await?;
        match outcome.kind {
            StorageCompactionOutcomeKind::Compacted => Ok(()),
            StorageCompactionOutcomeKind::Unsupported | StorageCompactionOutcomeKind::Failed => {
                Err(Error::storage(outcome.message))
            }
        }
    }
```

- [ ] **Step 4: Update `TieredStorageStore` to consume typed outcome**

In `crates/fdc-storage/src/tiered_store.rs`, remove `is_unsupported_compaction_error` usage from `run_maintenance_once_inner()`. Replace the loop beginning with `for tier in self.tier_manager.initialized_tiers()` with:

```rust
        for tier in self.tier_manager.initialized_tiers() {
            let outcome = self.tier_manager.compact_tier_with_outcome(&tier).await?;
            match outcome.kind {
                crate::StorageCompactionOutcomeKind::Compacted => {
                    compacted_tiers.push(tier.clone());
                    compaction_outcomes.push(outcome);
                }
                crate::StorageCompactionOutcomeKind::Unsupported => {
                    compaction_unsupported += 1;
                    compaction_outcomes.push(outcome);
                }
                crate::StorageCompactionOutcomeKind::Failed => {
                    compaction_failed += 1;
                    compaction_errors.insert(tier.clone(), outcome.message.clone());
                    compaction_outcomes.push(outcome);
                }
            }
        }
```

Then delete the helper function:

```rust
fn is_unsupported_compaction_error(error: &Error) -> bool { ... }
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
rtk cargo test -p fdc-storage tier_manager_compact_tier_with_outcome maintenance_run_reports_compaction_outcomes
```

Expected: pass. If existing tests expected string matching behavior, update assertions to check `StorageCompactionOutcomeKind::Unsupported` instead of error text.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): classify compaction by engine capability"
```

## Task 3: Structured tracing spans

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Add tracing imports**

In `crates/fdc-storage/src/tiered_store.rs`, add:

```rust
use tracing::{debug, info, instrument, warn};
```

- [ ] **Step 2: Add instrumentation attributes**

Add these attributes to methods:

```rust
    #[instrument(skip(self, query), fields(
        namespace = %query.namespace,
        collection = query.collection.as_deref().unwrap_or(""),
        limit = ?query.limit,
        tier_scope = ?query.tier_scope
    ))]
    pub async fn query_storage_with_metrics(...)
```

```rust
    #[instrument(skip(self))]
    pub async fn run_lifecycle_once(&self) -> Result<TierLifecycleReport> {
```

```rust
    #[instrument(skip(self))]
    pub async fn storage_health_snapshot(&self) -> Result<StorageHealthSnapshot> {
```

```rust
    #[instrument(skip(self, options), fields(timeout_ms = ?options.timeout.map(|timeout| timeout.as_millis())))]
    pub async fn run_maintenance_once_with_options(...)
```

```rust
    #[instrument(skip(self))]
    async fn run_maintenance_once_inner(&self) -> Result<StorageMaintenanceReport> {
```

In the `StorageWriteSink for TieredStorageStore` impl, add to `write_batch`:

```rust
    #[instrument(skip(self, batch), fields(batch_size = batch.records.len()))]
```

In the `QueryableStorage for TieredStorageStore` impl, add to `query_storage`:

```rust
    #[instrument(skip(self, query), fields(namespace = %query.namespace, limit = ?query.limit))]
```

- [ ] **Step 3: Add summary events without payload bytes**

After query metrics are finalized in `query_storage_with_metrics`, add:

```rust
debug!(
    scanned_entries = metrics.scanned_entries,
    decoded_records = metrics.decoded_records,
    returned_records = metrics.returned_records,
    "storage query completed"
);
```

Before returning lifecycle report in `run_lifecycle_once`, add:

```rust
info!(
    scanned_entries = report.scanned_entries,
    ttl_deleted = report.ttl_deleted,
    retention_demoted = report.retention_demoted,
    retention_deleted = report.retention_deleted,
    retained = report.retained,
    decode_errors = report.decode_errors,
    "storage lifecycle pass completed"
);
```

Inside compaction outcome matching in maintenance, add `warn!` for failed outcomes:

```rust
warn!(tier = ?tier, error = %outcome.message, "storage compaction failed");
```

Before returning maintenance report, add:

```rust
info!(
    compaction_compacted = compacted_tiers.len(),
    compaction_unsupported,
    compaction_failed,
    "storage maintenance pass completed"
);
```

Do not log record values, raw keys beyond existing aggregate fields, metadata tags, or serialized payloads.

- [ ] **Step 4: Run compile-focused test**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store
```

Expected: pass. The test primarily verifies tracing syntax compiles and does not alter behavior.

- [ ] **Step 5: Commit**

Run:

```bash
git add crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): trace storage boundary operations"
```

## Task 4: Duplicate-key TTL hard-delete regression

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Modify: `crates/fdc-storage/docs/production-hardening-followups.md`

- [ ] **Step 1: Add failing regression test**

In `crates/fdc-storage/src/tiered_store.rs` test module, add:

```rust
#[tokio::test]
async fn lifecycle_ttl_delete_removes_duplicate_key_from_all_tiers() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L2).with_engine_config(
        "max_size".to_string(),
        "1048576".to_string(),
    ));
    manager.initialize().await.unwrap();
    let manager = Arc::new(manager);
    let store = TieredStorageStore::new(Arc::clone(&manager));

    let expired = StorageWriteRecord::new(
        "market_data",
        "trades",
        b"BTCUSDT/1".to_vec(),
        b"payload".to_vec(),
    )
    .with_timestamp(Utc::now() - chrono::Duration::hours(2))
    .with_ttl(chrono::Duration::minutes(1));
    let storage_key = TieredStorageStore::storage_key_for_record(&expired);
    let encoded = encode_record(&expired).unwrap();

    manager
        .put_to_specific_tier(&storage_key, &encoded, &StorageTier::L1)
        .await
        .unwrap();
    manager
        .put_to_specific_tier(&storage_key, &encoded, &StorageTier::L2)
        .await
        .unwrap();

    assert!(manager.get_from_tier(&storage_key, &StorageTier::L1).await.unwrap().is_some());
    assert!(manager.get_from_tier(&storage_key, &StorageTier::L2).await.unwrap().is_some());

    let report = store.run_lifecycle_once().await.unwrap();

    assert!(report.ttl_deleted >= 1);
    assert!(manager.get_from_tier(&storage_key, &StorageTier::L1).await.unwrap().is_none());
    assert!(manager.get_from_tier(&storage_key, &StorageTier::L2).await.unwrap().is_none());
}
```

If `get_from_tier` does not exist, add it in Task 4 Step 3 as a narrow testable helper on `TierManager`.

- [ ] **Step 2: Run failing focused test**

Run:

```bash
rtk cargo test -p fdc-storage lifecycle_ttl_delete_removes_duplicate_key_from_all_tiers
```

Expected: fail if `get_from_tier` is missing, or pass if current behavior is already testable. If it passes immediately, keep the regression test because S11's purpose is to lock the semantic.

- [ ] **Step 3: Add `get_from_tier` helper if needed**

If Step 2 failed because `get_from_tier` is missing, add this method to `TierManager` near `put_to_specific_tier`:

```rust
    pub async fn get_from_tier(&self, key: &[u8], tier: &StorageTier) -> Result<Option<Vec<u8>>> {
        if let Some(engine) = self.engines.get(tier) {
            let engine_guard = engine.read().await;
            return engine_guard.get(key).await;
        }
        Ok(None)
    }
```

Then rerun:

```bash
rtk cargo test -p fdc-storage lifecycle_ttl_delete_removes_duplicate_key_from_all_tiers
```

Expected: pass.

- [ ] **Step 4: Document TTL duplicate-key semantics**

In `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`, update `Known Limitations` or add an `S11 P1 Closure` section with:

```markdown
## S11 P1 Closure Notes

- Compaction unsupported classification is based on engine feature support instead of error-string matching.
- TTL hard-delete semantics are explicit: when one logical storage key expires, lifecycle deletes that key from all initialized tiers. Duplicate tier copies are treated as copies of the same logical record, not independent versions.
- Public API stability rules are documented in `public-api-stability.md`.
```

In `crates/fdc-storage/docs/production-hardening-followups.md`, change the S10 done heading to S11 and move the S11 P1 items out of Remaining P1:

```markdown
## Done through S11 P1 closure

- Maintenance re-entry protection for explicit `run_maintenance_once*` calls.
- Caller-provided maintenance timeout option, with zero timeout defined as immediate timeout.
- Optional maintenance audit sink trait and audit entry DTO.
- Compaction outcome classification into compacted, unsupported, and failed.
- Maintenance metrics snapshot DTO and Prometheus text rendering helper.
- Engine-level compaction support detection replaces error-string matching.
- Duplicate-key TTL hard-delete semantics are documented and regression-tested.
- Structured tracing spans exist around storage write/query/lifecycle/maintenance boundaries.
- Public API stability and breaking-change policy are documented.

## Remaining P2: Before production deployment
```

Ensure P2/P3 items remain listed.

- [ ] **Step 5: Run focused tests**

Run:

```bash
rtk cargo test -p fdc-storage lifecycle_ttl_delete_removes_duplicate_key_from_all_tiers
```

Expected: pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/fdc-storage/src/tiered_store.rs crates/fdc-storage/src/tier.rs crates/fdc-storage/docs/storage-boundary-acceptance-report.md crates/fdc-storage/docs/production-hardening-followups.md
git commit -m "test(storage): lock ttl duplicate key lifecycle semantics"
```

## Task 5: Public API stability documentation

**Files:**
- Create: `crates/fdc-storage/docs/public-api-stability.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`

- [ ] **Step 1: Create public API stability document**

Create `crates/fdc-storage/docs/public-api-stability.md` with:

```markdown
# fdc-storage Public API Stability Policy

Date: 2026-06-03
Scope: `crates/fdc-storage` integration-facing API

## Supported public surface

The supported integration-facing API is the set of public items re-exported from `crates/fdc-storage/src/lib.rs`, plus documented module paths used by those re-exports.

Consumers should prefer imports from `fdc_storage::{...}` instead of deep module paths. Deep module paths may remain public for Rust module organization, but re-exports define the compatibility contract.

## Breaking changes

A change is breaking when it removes, renames, or changes the meaning of any re-exported type, trait, enum variant, public field, constructor, or method that integration consumers can call.

Examples of breaking changes:

- Removing a re-export from `lib.rs`.
- Renaming `StorageWriteRecord`, `StorageQuery`, `TieredStorageStore`, or other re-exported types.
- Changing an existing trait method signature.
- Changing lifecycle semantics for TTL hard-delete across tiers.
- Changing serialized field names for persisted or externally exchanged DTOs.

## Additive changes

Additive changes are preferred for P1/P2 hardening:

- Add new methods instead of changing existing method signatures.
- Add new enum variants only when consumers can handle them safely.
- Add serde fields with defaults where practical.
- Keep compatibility methods delegating to newer explicit methods.

## Deprecation policy

Deprecated public APIs should remain available for at least one planned migration window. Immediate removal is reserved for correctness, data-loss, or safety issues and must be documented in the acceptance report.

## Tests

`crates/fdc-storage/tests/public_api_stability.rs` is the smoke test for the re-exported public API. Update it whenever the public API intentionally expands.
```

- [ ] **Step 2: Link the policy from acceptance report**

In `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`, add under `Production Follow-up Reference`:

```markdown
## Public API Stability

The integration-facing API stability policy is documented in `public-api-stability.md`. Public re-exports from `crates/fdc-storage/src/lib.rs` define the preferred consumer surface.
```

- [ ] **Step 3: Commit**

Run:

```bash
git add crates/fdc-storage/docs/public-api-stability.md crates/fdc-storage/docs/storage-boundary-acceptance-report.md
git commit -m "docs(storage): document public api stability policy"
```

## Task 6: Full verification and S11 progress handoff

**Files:**
- Create: `docs/superpowers/progress/2026-06-03-mdb-storage-s11-progress.md`

- [ ] **Step 1: Run formatting**

Run:

```bash
rtk cargo fmt -p fdc-storage
```

Expected: command succeeds.

- [ ] **Step 2: Run full storage tests**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: all tests pass.

- [ ] **Step 3: Remove generated test data**

Run:

```bash
rm -rf crates/fdc-storage/data
rtk git status --short
```

Expected: no `crates/fdc-storage/data` files remain.

- [ ] **Step 4: Create S11 progress handoff**

Create `docs/superpowers/progress/2026-06-03-mdb-storage-s11-progress.md` with:

```markdown
# MDB fdc-storage S11 Progress Handoff

Date: 2026-06-03
Branch: `mdb-mqdev`
Module: `crates/fdc-storage`

## Completed

S11 P1 closure is complete.

- Added engine-level feature typing for optional capabilities such as compaction, snapshot, restore, and SQL query.
- Replaced maintenance compaction unsupported classification based on error-string matching with engine feature support checks.
- Added typed `TierManager::compact_tier_with_outcome()` and kept compatibility for `compact_tier()`.
- Added structured tracing spans/events around storage write/query/lifecycle/maintenance boundaries without logging payload bytes or business DTO fields.
- Added regression coverage for duplicate-key TTL hard-delete across tiers.
- Documented public API stability and breaking-change policy.
- Updated production hardening follow-ups and acceptance report.

## Verification

```bash
rtk cargo fmt -p fdc-storage
rtk cargo test -p fdc-storage
```

Result: all `fdc-storage` tests pass.

## Remaining production work

`fdc-storage` is ready for broad module integration as a generic storage boundary after S11. It is still not full production deployment ready until P2/P3 items are implemented:

- runtime maintenance scheduler/exporter wiring;
- durable audit persistence;
- disk usage/path health and richer degraded health states;
- query indexes/cursor pagination;
- physical shard routing/rebalancing;
- backup/restore orchestration.

## Dirty files to avoid

Unrelated `fdc-server health` dirty files pre-existed and were not modified by S11.
```

- [ ] **Step 5: Commit final docs/format changes**

Run:

```bash
git add docs/superpowers/progress/2026-06-03-mdb-storage-s11-progress.md crates/fdc-storage
git status --short
```

If only intended S11 files are staged, run:

```bash
git commit -m "docs(storage): record s11 p1 closure handoff"
```

- [ ] **Step 6: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: only unrelated `fdc-server health` dirty files remain in the main worktree after merge, or a clean isolated worktree before merge.

## Self-review

- Spec coverage: Task 1 and Task 2 cover engine-level compaction typing; Task 3 covers tracing; Task 4 covers duplicate-key TTL semantics; Task 5 covers public API policy; Task 6 covers verification and progress handoff.
- Completeness scan: no incomplete work markers are left for implementers.
- Type consistency: `StorageEngineFeature`, `StorageEngineFeatureError`, `compact_tier_with_outcome`, `StorageCompactionOutcome`, and `StorageCompactionOutcomeKind` names match the approved S11 design.
