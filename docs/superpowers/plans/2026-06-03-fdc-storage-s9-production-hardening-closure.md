# fdc-storage S9 Production Hardening Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `fdc-storage` as a module-internal, pre-integration-ready storage boundary with acceptance docs, production follow-up docs, dependency guard, contract tests, and API stability smoke tests.

**Architecture:** Keep S9 intentionally lightweight: no new runtime subsystem. Add docs under `crates/fdc-storage/docs/`, add integration-style contract tests under `crates/fdc-storage/tests/`, and add a dependency guard test that reads `Cargo.toml` and rejects upper-layer crate dependencies.

**Tech Stack:** Rust tests, serde, `JsonStorageCodec`, `TypedStorageRecord`, `TieredStorageStore`, `StorageQuery`, `StorageTierScope`, docs in Markdown.

---

## Production Follow-up Notes Required

S9 must preserve a production follow-up reference in `crates/fdc-storage/docs/production-hardening-followups.md`. Keep P1/P2/P3 priorities explicit so future work can continue without rediscovering context.

## Task 1: Acceptance and Production Follow-up Docs

**Files:**
- Create: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Create: `crates/fdc-storage/docs/production-hardening-followups.md`

- [ ] **Step 1: Write acceptance report**

Create `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`:

```markdown
# fdc-storage Storage Boundary Acceptance Report

Date: 2026-06-03
Status: pre-integration-ready module baseline

## Scope

`fdc-storage` is a generic storage module. It stores bytes and storage-owned metadata. Business modules provide DTOs through codecs, namespace, collection, tags, and placement hints.

## Implemented Capability Matrix

| Area | Status | Notes |
| --- | --- | --- |
| Generic write boundary | Done | `StorageWriteRecord`, `StorageWriteBatch`, `StorageWriteSink` |
| Generic query boundary | Done | namespace, collection, tags, key filters, time filters, order, limit |
| Typed facade | Done | `StorageCodec`, JSON/bincode codecs, typed read helpers |
| Tier-aware store | Done | `TieredStorageStore` routes writes by placement and queries by tier scope |
| L1 engine | Done | Memory engine |
| L2 engine | Done | redb persistent KV |
| L3 engine | Done | DuckDB KV + SQL query support |
| L4 engine | Done | RocksDB persistent cold KV |
| Tier-scoped query | Done | all/only/hot/warm/cold scope with metrics |
| Lifecycle | Done | explicit TTL hard-delete, retention demotion, retention delete |
| Maintenance/health | Done | explicit maintenance pass, health snapshot, compaction error capture |
| Business dependency isolation | Guarded | `fdc-storage` must not depend on barter/ingestion/transform/api/server/orchestrator |

## Verification

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected result as of S9: all `fdc-storage` tests pass.

## Integration Readiness Statement

`fdc-storage` is ready for business modules to depend on its generic write/query/typed APIs. It is not yet wired into full ingestion or server runtime in this module-completion phase.

## Known Limitations

- Maintenance is explicit and caller-driven, not scheduled.
- Compaction semantics are engine-specific; unsupported compaction is captured as an error in maintenance reports.
- Health snapshot is module-local and not yet exported as API/Prometheus metrics.
- Index, physical shard routing, and backup orchestration are intentionally deferred.
- Lifecycle hard-delete deletes all tier copies for the same storage key when TTL is expired.

## Production Follow-up Reference

See `production-hardening-followups.md` for P1/P2/P3 follow-up items.
```

- [ ] **Step 2: Write production follow-up document**

Create `crates/fdc-storage/docs/production-hardening-followups.md`:

```markdown
# fdc-storage Production Hardening Follow-ups

Date: 2026-06-03
Scope: future production-grade improvements after S9 pre-integration closure

## P1: Before broad integration

- Stabilize public API naming and document breaking-change policy.
- Decide lifecycle hard-delete semantics for duplicate keys across tiers.
- Clarify compaction errors: distinguish expected unsupported no-op from operational failure.
- Add maintenance re-entry protection before any scheduler calls `run_maintenance_once()`.
- Add structured tracing spans around write/query/lifecycle/maintenance operations.

## P2: Before production deployment

- Add Prometheus/exporter adapter for `StorageHealthSnapshot` and `StorageMaintenanceReport`.
- Add maintenance scheduler with interval config, timeout, cancellation, and shutdown token.
- Persist maintenance reports to an audit sink or system namespace.
- Add disk usage/path health for durable engines.
- Add engine-specific compaction/vacuum/checkpoint policies.
- Add degraded health states for stale stats, recent write failures, high disk usage, and compaction failures.

## P3: Before high-scale operation

- Design and implement namespace/collection/tag/time indexes.
- Add physical shard routing and rebalancing.
- Add engine-specific backup/restore orchestration.
- Add cold archive formats such as Parquet/Arrow.
- Add quota management and backpressure.

## Deferred by design

S9 does not implement these items. It records them so future production hardening can continue from a clear checklist.
```

- [ ] **Step 3: Commit docs**

```bash
git add crates/fdc-storage/docs/storage-boundary-acceptance-report.md crates/fdc-storage/docs/production-hardening-followups.md
git commit -m "docs(storage): add pre-integration acceptance report"
```

## Task 2: Dependency Guard Test

**Files:**
- Create: `crates/fdc-storage/tests/dependency_guard.rs`

- [ ] **Step 1: Add guard test**

Create `crates/fdc-storage/tests/dependency_guard.rs`:

```rust
#[test]
fn fdc_storage_does_not_depend_on_upper_layer_crates() {
    let manifest = std::fs::read_to_string("Cargo.toml").expect("workspace Cargo.toml should exist");
    let storage_manifest_path = manifest
        .lines()
        .find(|line| line.contains("crates/fdc-storage"))
        .expect("workspace manifest should include fdc-storage");
    assert!(storage_manifest_path.contains("crates/fdc-storage"));

    let storage_manifest = std::fs::read_to_string("crates/fdc-storage/Cargo.toml")
        .expect("fdc-storage Cargo.toml should exist");
    for forbidden in [
        "fdc-barter",
        "fdc-ingestion",
        "fdc-transform",
        "fdc-api",
        "fdc-server",
        "fdc-orchestrator",
    ] {
        assert!(
            !storage_manifest.contains(forbidden),
            "fdc-storage must not depend on upper-layer crate {forbidden}"
        );
    }
}
```

- [ ] **Step 2: Verify**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: dependency guard passes.

- [ ] **Step 3: Commit**

```bash
git add crates/fdc-storage/tests/dependency_guard.rs
git commit -m "test(storage): guard storage dependency boundary"
```

## Task 3: Generic Business Contract Test

**Files:**
- Create: `crates/fdc-storage/tests/generic_business_contract.rs`

- [ ] **Step 1: Add contract test**

Create `crates/fdc-storage/tests/generic_business_contract.rs`:

```rust
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use fdc_storage::{
    JsonStorageCodec, QueryableStorage, StorageCodec, StoragePlacementHint, StorageQuery,
    StorageTier, StorageTierScope, StorageTypeDescriptor, StorageWriteBatch, StorageWriteMetadata,
    StorageWriteRecord, StorageWriteSink, TierConfig, TierManager, TieredStorageStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BusinessEvent {
    tenant: String,
    kind: String,
    sequence: u64,
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = fdc_storage::StorageEngineType::Memory;
    config
}

async fn memory_tiered_store() -> TieredStorageStore {
    let mut manager = TierManager::new();
    manager.add_tier(memory_tier_config(StorageTier::L1));
    manager.add_tier(memory_tier_config(StorageTier::L2));
    manager.initialize().await.unwrap();
    TieredStorageStore::new(Arc::new(manager))
}

#[tokio::test]
async fn generic_storage_boundary_supports_business_like_dto_without_business_dependency() {
    let store = memory_tiered_store().await;
    let codec = JsonStorageCodec::<BusinessEvent>::new();
    let event = BusinessEvent {
        tenant: "tenant-a".to_string(),
        kind: "created".to_string(),
        sequence: 42,
    };
    let descriptor = StorageTypeDescriptor::new("business.event", "1");
    let encoded = codec.encode(&event).unwrap();
    let mut metadata = StorageWriteMetadata::default();
    metadata.schema = Some(descriptor.schema.clone());
    metadata.schema_version = Some(descriptor.schema_version.clone());
    metadata.tags.insert("tenant".to_string(), event.tenant.clone());
    metadata.tags.insert("kind".to_string(), event.kind.clone());

    let record = StorageWriteRecord::new(
        "business_test",
        "events",
        b"tenant-a/000042".to_vec(),
        encoded,
    )
    .with_timestamp(Utc.timestamp_opt(42, 0).unwrap())
    .with_metadata(metadata)
    .with_placement(StoragePlacementHint::for_tier(StorageTier::L2));

    store
        .write_batch(StorageWriteBatch::new(vec![record]))
        .await
        .unwrap();

    let result = store
        .query_storage(
            &StorageQuery::new("business_test")
                .with_collection("events")
                .with_tag("tenant", "tenant-a")
                .with_tag("kind", "created")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].metadata.schema.as_deref(), Some("business.event"));
    let decoded = codec.decode(&result[0].value).unwrap();
    assert_eq!(decoded, event);
}
```

- [ ] **Step 2: Verify**

Run:

```bash
rtk cargo test -p fdc-storage --test generic_business_contract
```

Expected: contract test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/fdc-storage/tests/generic_business_contract.rs
git commit -m "test(storage): add generic business contract"
```

## Task 4: Public API Stability Smoke Test

**Files:**
- Create: `crates/fdc-storage/tests/public_api_stability.rs`

- [ ] **Step 1: Add public API smoke test**

Create `crates/fdc-storage/tests/public_api_stability.rs`:

```rust
use fdc_storage::{
    StorageMaintenanceReport, StoragePlacementHint, StorageQuery, StorageTier, StorageTierScope,
    TierLifecycleReport, TieredStorageStore,
};

#[tokio::test]
async fn public_storage_api_smoke_compiles_and_runs() {
    let store = TieredStorageStore::memory_only().await.unwrap();
    let query = StorageQuery::new("api_smoke")
        .with_collection("records")
        .with_tier_scope(StorageTierScope::Only(StorageTier::L1));
    query.validate().unwrap();

    let placement = StoragePlacementHint::for_tier(StorageTier::L1);
    assert_eq!(placement.target_tier, Some(StorageTier::L1));

    let lifecycle = TierLifecycleReport::default();
    assert_eq!(lifecycle.scanned_entries, 0);

    let maintenance: StorageMaintenanceReport = store.run_maintenance_once().await.unwrap();
    assert!(maintenance.finished_at >= maintenance.started_at);
    assert!(maintenance.health.tiers.contains_key(&StorageTier::L1));
}
```

- [ ] **Step 2: Verify**

Run:

```bash
rtk cargo test -p fdc-storage --test public_api_stability
```

Expected: public API smoke test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/fdc-storage/tests/public_api_stability.rs
git commit -m "test(storage): add public api stability smoke"
```

## Task 5: Full Verification and Merge

- [ ] **Step 1: Format and full test**

```bash
rtk cargo fmt -p fdc-storage && rtk cargo test -p fdc-storage
```

Expected: all tests pass.

- [ ] **Step 2: Remove generated data and commit fmt residue if needed**

```bash
rm -rf crates/fdc-storage/data
rtk git status --short
```

If code changed:

```bash
git add crates/fdc-storage/tests/dependency_guard.rs crates/fdc-storage/tests/generic_business_contract.rs crates/fdc-storage/tests/public_api_stability.rs
git commit -m "style(storage): format closure tests"
```

- [ ] **Step 3: Merge and verify on `mdb-mqdev`**

```bash
git checkout mdb-mqdev
git merge --ff-only <s9-branch-name>
rtk cargo test -p fdc-storage
rm -rf crates/fdc-storage/data
git worktree remove <s9-worktree-path>
git worktree prune
git branch -d <s9-branch-name>
```

Expected: tests pass on main and only the unrelated `fdc-server health` dirty files remain.

## Self-Review

- Covers acceptance docs, production follow-up docs, dependency guard, generic business contract, public API smoke, verification, merge.
- No placeholders.
- Does not implement pipeline glue, index, backup, shard, or server routes.
