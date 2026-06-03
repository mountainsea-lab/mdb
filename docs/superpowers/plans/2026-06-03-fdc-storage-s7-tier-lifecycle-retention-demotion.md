# fdc-storage S7 Tier Lifecycle Retention and Demotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit, generic lifecycle maintenance pass for TTL hard-delete, retention demotion, retention expiration, and lifecycle reporting in `fdc-storage`.

**Architecture:** Introduce focused lifecycle report types in a new `lifecycle.rs` module, add tier-specific write/delete and colder-tier helpers to `TierManager`, and implement `TieredStorageStore::run_lifecycle_once()` as an explicit maintenance pass over initialized tiers. The pass decodes storage-owned records, applies TTL before retention, demotes to colder tiers when possible, deletes when no colder tier exists, and returns verifiable counts.

**Tech Stack:** Rust, tokio async tests, serde, chrono, bincode, existing `StorageEngine` trait and memory/redb/duckdb/rocksdb engines.

---

## File Structure

- Create: `crates/fdc-storage/src/lifecycle.rs`
  - Owns `TierLifecycleAction`, `TierLifecycleTierReport`, `TierLifecycleReport`.
- Modify: `crates/fdc-storage/src/lib.rs`
  - Add `pub mod lifecycle`.
  - Re-export lifecycle types.
- Modify: `crates/fdc-storage/src/tier.rs`
  - Add `put_to_specific_tier`, `delete_from_tier`, `next_colder_available_tier`.
  - Add tests for targeted delete and colder-tier resolution.
- Modify: `crates/fdc-storage/src/tiered_store.rs`
  - Add `run_lifecycle_once`.
  - Add helper to update lifecycle reports.
  - Add tests for TTL hard-delete, retention demotion, retention expiration, and report counts.

## Task 1: Lifecycle Report Types

**Files:**
- Create: `crates/fdc-storage/src/lifecycle.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Create lifecycle module with report types and unit test**

Create `crates/fdc-storage/src/lifecycle.rs` with:

```rust
//! Tier lifecycle report types.
//!
//! Lifecycle logic is executed by `TieredStorageStore`, while this module owns
//! storage-generic result types that callers can inspect or expose as metrics.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::StorageTier;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TierLifecycleAction {
    TtlExpiredDelete,
    RetentionDemote,
    RetentionExpiredDelete,
    Retain,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TierLifecycleTierReport {
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
}

impl TierLifecycleTierReport {
    pub fn record_action(&mut self, action: TierLifecycleAction) {
        match action {
            TierLifecycleAction::TtlExpiredDelete => self.ttl_deleted += 1,
            TierLifecycleAction::RetentionDemote => self.retention_demoted += 1,
            TierLifecycleAction::RetentionExpiredDelete => self.retention_deleted += 1,
            TierLifecycleAction::Retain => self.retained += 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TierLifecycleReport {
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub tier_reports: BTreeMap<StorageTier, TierLifecycleTierReport>,
}

impl TierLifecycleReport {
    pub fn tier_report_mut(&mut self, tier: StorageTier) -> &mut TierLifecycleTierReport {
        self.tier_reports.entry(tier).or_default()
    }

    pub fn record_scanned(&mut self, tier: StorageTier) {
        self.scanned_entries += 1;
        self.tier_report_mut(tier).scanned_entries += 1;
    }

    pub fn record_decode_error(&mut self, tier: StorageTier) {
        self.decode_errors += 1;
        self.tier_report_mut(tier).decode_errors += 1;
    }

    pub fn record_action(&mut self, tier: StorageTier, action: TierLifecycleAction) {
        match action {
            TierLifecycleAction::TtlExpiredDelete => self.ttl_deleted += 1,
            TierLifecycleAction::RetentionDemote => self.retention_demoted += 1,
            TierLifecycleAction::RetentionExpiredDelete => self.retention_deleted += 1,
            TierLifecycleAction::Retain => self.retained += 1,
        }
        self.tier_report_mut(tier).record_action(action);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_report_records_global_and_per_tier_counts() {
        let mut report = TierLifecycleReport::default();
        report.record_scanned(StorageTier::L1);
        report.record_action(StorageTier::L1, TierLifecycleAction::RetentionDemote);
        report.record_decode_error(StorageTier::L2);

        assert_eq!(report.scanned_entries, 1);
        assert_eq!(report.retention_demoted, 1);
        assert_eq!(report.decode_errors, 1);
        assert_eq!(report.tier_reports[&StorageTier::L1].scanned_entries, 1);
        assert_eq!(report.tier_reports[&StorageTier::L1].retention_demoted, 1);
        assert_eq!(report.tier_reports[&StorageTier::L2].decode_errors, 1);
    }
}
```

- [ ] **Step 2: Re-export lifecycle module and types**

In `crates/fdc-storage/src/lib.rs`, add near other modules:

```rust
pub mod lifecycle; // tier lifecycle reports and maintenance result types
```

Add near re-exports:

```rust
pub use lifecycle::{TierLifecycleAction, TierLifecycleReport, TierLifecycleTierReport};
```

- [ ] **Step 3: Run lifecycle type tests**

Run:

```bash
rtk cargo test -p fdc-storage lifecycle::tests
```

Expected: lifecycle module test passes.

- [ ] **Step 4: Commit lifecycle report types**

```bash
git add crates/fdc-storage/src/lifecycle.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add tier lifecycle report types"
```

## Task 2: TierManager Tier-Specific Operations

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`

- [ ] **Step 1: Add tests for colder-tier resolution and targeted delete**

Add these tests to `crates/fdc-storage/src/tier.rs` test module:

```rust
#[tokio::test]
async fn tier_manager_finds_next_colder_initialized_tier() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L3));
    manager.add_tier(TierConfig::new(StorageTier::L4));
    manager.initialize().await.unwrap();

    assert_eq!(
        manager.next_colder_available_tier(&StorageTier::L1),
        Some(StorageTier::L3)
    );
    assert_eq!(
        manager.next_colder_available_tier(&StorageTier::L3),
        Some(StorageTier::L4)
    );
    assert_eq!(manager.next_colder_available_tier(&StorageTier::L4), None);
}

#[tokio::test]
async fn tier_manager_deletes_only_from_requested_tier() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L2));
    manager.initialize().await.unwrap();

    manager
        .put_to_specific_tier(b"same", b"l1", &StorageTier::L1)
        .await
        .unwrap();
    manager
        .put_to_specific_tier(b"same", b"l2", &StorageTier::L2)
        .await
        .unwrap();

    manager.delete_from_tier(b"same", &StorageTier::L1).await.unwrap();

    let l1 = manager
        .scan_prefix_in_tiers(b"same", &[StorageTier::L1], None)
        .await
        .unwrap();
    let l2 = manager
        .scan_prefix_in_tiers(b"same", &[StorageTier::L2], None)
        .await
        .unwrap();

    assert!(l1.is_empty());
    assert_eq!(l2.len(), 1);
    assert_eq!(l2[0].2, b"l2".to_vec());
}
```

- [ ] **Step 2: Run tests and verify they fail before implementation**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests::tier_manager_finds_next_colder_initialized_tier tier::tests::tier_manager_deletes_only_from_requested_tier
```

Expected: compile failure for missing `next_colder_available_tier`, `put_to_specific_tier`, and `delete_from_tier`.

- [ ] **Step 3: Implement TierManager helpers**

In `impl TierManager` in `crates/fdc-storage/src/tier.rs`, change private `put_to_tier` to call a new public wrapper. Replace:

```rust
async fn put_to_tier(&self, key: &[u8], value: &[u8], target_tier: &StorageTier) -> Result<()> {
```

with:

```rust
pub async fn put_to_specific_tier(
    &self,
    key: &[u8],
    value: &[u8],
    target_tier: &StorageTier,
) -> Result<()> {
```

Then update existing calls from `put_to_tier` to `put_to_specific_tier` in `put` and `put_with_placement`:

```rust
self.put_to_specific_tier(key, value, &target_tier).await
```

Add methods after `delete`:

```rust
pub async fn delete_from_tier(&self, key: &[u8], tier: &StorageTier) -> Result<()> {
    if let Some(engine) = self.engines.get(tier) {
        let engine_guard = engine.read().await;
        engine_guard.delete(key).await?;
    }
    Ok(())
}

pub fn next_colder_available_tier(&self, current_tier: &StorageTier) -> Option<StorageTier> {
    let mut tiers: Vec<_> = self
        .engines
        .keys()
        .filter(|tier| tier.priority() > current_tier.priority())
        .cloned()
        .collect();
    tiers.sort_by_key(|tier| tier.priority());
    tiers.into_iter().next()
}
```

- [ ] **Step 4: Run TierManager tests**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests
```

Expected: all tier tests pass.

- [ ] **Step 5: Commit TierManager helpers**

```bash
git add crates/fdc-storage/src/tier.rs
git commit -m "feat(storage): add tier-specific lifecycle operations"
```

## Task 3: TieredStorageStore Lifecycle Pass

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Add memory-tier test helper**

If not already present, add this helper to the `tiered_store.rs` test module:

```rust
async fn lifecycle_store_with_tiers(configs: Vec<TierConfig>) -> TieredStorageStore {
    let mut manager = TierManager::new();
    for config in configs {
        manager.add_tier(config);
    }
    manager.initialize().await.unwrap();
    TieredStorageStore::new(Arc::new(manager))
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = StorageEngineType::Memory;
    config
}
```

- [ ] **Step 2: Add failing lifecycle tests**

Add these tests to `tiered_store.rs` test module:

```rust
#[tokio::test]
async fn lifecycle_hard_deletes_ttl_expired_record_from_all_tiers() {
    let store = lifecycle_store_with_tiers(vec![
        memory_tier_config(StorageTier::L1),
        memory_tier_config(StorageTier::L2),
    ])
    .await;

    let record = tagged_record(b"ttl", "BTCUSDT")
        .with_timestamp(Utc::now() - Duration::seconds(10))
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L1).with_ttl(Duration::seconds(1)));
    let key = TieredStorageStore::storage_key_for_record(&record);
    let value = encode_record(&record).unwrap();

    store
        .tier_manager()
        .put_to_specific_tier(&key, &value, &StorageTier::L1)
        .await
        .unwrap();
    store
        .tier_manager()
        .put_to_specific_tier(&key, &value, &StorageTier::L2)
        .await
        .unwrap();

    let report = store.run_lifecycle_once().await.unwrap();

    assert_eq!(report.scanned_entries, 1);
    assert_eq!(report.ttl_deleted, 1);
    assert!(store.tier_manager().get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn lifecycle_demotes_retention_expired_record_to_next_colder_tier() {
    let mut l1 = memory_tier_config(StorageTier::L1);
    l1.retention_duration = Some(Duration::seconds(1));
    let l2 = memory_tier_config(StorageTier::L2);
    let store = lifecycle_store_with_tiers(vec![l1, l2]).await;

    let record = tagged_record(b"demote", "BTCUSDT")
        .with_timestamp(Utc::now() - Duration::seconds(10))
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
    store.write_batch(StorageWriteBatch::new(vec![record])).await.unwrap();

    let report = store.run_lifecycle_once().await.unwrap();
    let query = StorageQuery::new("market_data")
        .with_tier_scope(StorageTierScope::Only(StorageTier::L2));
    let result = store.query_storage_with_metrics(&query).await.unwrap();

    assert_eq!(report.retention_demoted, 1);
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].key, b"demote".to_vec());
}

#[tokio::test]
async fn lifecycle_deletes_retention_expired_record_when_no_colder_tier_exists() {
    let mut l4 = memory_tier_config(StorageTier::L4);
    l4.retention_duration = Some(Duration::seconds(1));
    let store = lifecycle_store_with_tiers(vec![l4]).await;

    let record = tagged_record(b"delete", "BTCUSDT")
        .with_timestamp(Utc::now() - Duration::seconds(10))
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L4));
    store.write_batch(StorageWriteBatch::new(vec![record])).await.unwrap();

    let report = store.run_lifecycle_once().await.unwrap();
    let result = store
        .query_storage_with_metrics(&StorageQuery::new("market_data"))
        .await
        .unwrap();

    assert_eq!(report.retention_deleted, 1);
    assert!(result.records.is_empty());
}

#[tokio::test]
async fn lifecycle_report_counts_retained_and_per_tier_actions() {
    let mut l1 = memory_tier_config(StorageTier::L1);
    l1.retention_duration = Some(Duration::days(1));
    let store = lifecycle_store_with_tiers(vec![l1]).await;

    let record = tagged_record(b"keep", "BTCUSDT")
        .with_timestamp(Utc::now())
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
    store.write_batch(StorageWriteBatch::new(vec![record])).await.unwrap();

    let report = store.run_lifecycle_once().await.unwrap();

    assert_eq!(report.scanned_entries, 1);
    assert_eq!(report.retained, 1);
    assert_eq!(report.tier_reports[&StorageTier::L1].scanned_entries, 1);
    assert_eq!(report.tier_reports[&StorageTier::L1].retained, 1);
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests::lifecycle_
```

Expected: compile failure for missing `run_lifecycle_once`.

- [ ] **Step 4: Implement lifecycle pass imports**

In `tiered_store.rs`, add lifecycle imports in the `use crate::{ ... }` block:

```rust
TierLifecycleAction, TierLifecycleReport,
```

- [ ] **Step 5: Implement `run_lifecycle_once`**

Add inside `impl TieredStorageStore`:

```rust
pub async fn run_lifecycle_once(&self) -> Result<TierLifecycleReport> {
    let now = Utc::now();
    let tiers = self.tier_manager.tiers_for_scope(&crate::StorageTierScope::All);
    let mut report = TierLifecycleReport::default();

    for tier in tiers {
        let entries = self
            .tier_manager
            .scan_prefix_in_tiers(&[], &[tier.clone()], None)
            .await?;

        for (_, key, value) in entries {
            report.record_scanned(tier.clone());

            let record = match decode_record(&value) {
                Ok(record) => record,
                Err(_) => {
                    report.record_decode_error(tier.clone());
                    continue;
                }
            };

            if record_is_expired(&record, now) {
                self.tier_manager.delete(&key).await?;
                report.record_action(tier.clone(), TierLifecycleAction::TtlExpiredDelete);
                continue;
            }

            let retention_expired = self
                .tier_manager
                .tier_config(&tier)
                .and_then(|config| config.retention_duration)
                .map(|retention| record.timestamp + retention < now)
                .unwrap_or(false);

            if retention_expired {
                if let Some(target_tier) = self.tier_manager.next_colder_available_tier(&tier) {
                    self.tier_manager
                        .put_to_specific_tier(&key, &value, &target_tier)
                        .await?;
                    self.tier_manager.delete_from_tier(&key, &tier).await?;
                    report.record_action(tier.clone(), TierLifecycleAction::RetentionDemote);
                } else {
                    self.tier_manager.delete_from_tier(&key, &tier).await?;
                    report.record_action(tier.clone(), TierLifecycleAction::RetentionExpiredDelete);
                }
            } else {
                report.record_action(tier.clone(), TierLifecycleAction::Retain);
            }
        }
    }

    Ok(report)
}
```

This requires `TierManager::tier_config`; add it in Task 2 area or now in `tier.rs`:

```rust
pub fn tier_config(&self, tier: &StorageTier) -> Option<&TierConfig> {
    self.tiers.get(tier)
}
```

- [ ] **Step 6: Run lifecycle tests**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests::lifecycle_
```

Expected: all lifecycle tests pass.

- [ ] **Step 7: Run all tiered store tests**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests
```

Expected: all tiered store tests pass.

- [ ] **Step 8: Commit lifecycle pass**

```bash
git add crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): add explicit tier lifecycle pass"
```

## Task 4: Full Verification and Merge

**Files:**
- Verify all modified files.

- [ ] **Step 1: Format**

Run:

```bash
rtk cargo fmt -p fdc-storage
```

Expected: command succeeds.

- [ ] **Step 2: Run full fdc-storage tests**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: all tests pass.

- [ ] **Step 3: Remove generated test data if present**

Run:

```bash
rm -rf crates/fdc-storage/data
rtk git status --short
```

Expected: no generated `crates/fdc-storage/data` remains.

- [ ] **Step 4: Commit formatting-only residue if needed**

If `cargo fmt` changed files after previous commits:

```bash
git add crates/fdc-storage/src/lifecycle.rs crates/fdc-storage/src/lib.rs crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "style(storage): format tier lifecycle"
```

Expected: commit created only if formatting changes exist.

- [ ] **Step 5: Merge back to `mdb-mqdev`**

From the main working tree:

```bash
git checkout mdb-mqdev
git merge --ff-only <s7-branch-name>
```

Expected: fast-forward merge succeeds.

- [ ] **Step 6: Verify on main branch**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: all tests pass on `mdb-mqdev`.

- [ ] **Step 7: Clean generated test data on main if present**

Run:

```bash
rm -rf crates/fdc-storage/data
rtk git status --short --branch
```

Expected: only the existing unrelated `fdc-server health` dirty files remain.

- [ ] **Step 8: Clean S7 worktree and branch**

Run:

```bash
git worktree remove <s7-worktree-path>
git worktree prune
git branch -d <s7-branch-name>
```

Expected: worktree and branch are removed.

## Self-Review

- Spec coverage: lifecycle report types, TTL hard-delete, retention demotion, retention expiration, per-tier report counts, explicit run API, no scheduler, no pipeline glue.
- Placeholder scan: no TBD/TODO/fill-in placeholders. All commands and code snippets are concrete.
- Type consistency: `TierLifecycleAction`, `TierLifecycleReport`, `TierLifecycleTierReport`, `run_lifecycle_once`, `put_to_specific_tier`, `delete_from_tier`, `next_colder_available_tier`, and `tier_config` are named consistently.
