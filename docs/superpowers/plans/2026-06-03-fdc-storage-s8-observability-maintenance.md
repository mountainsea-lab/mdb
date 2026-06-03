# fdc-storage S8 Observability and Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add storage-owned health snapshot and explicit maintenance report APIs that consolidate lifecycle, tier stats, and compaction attempts.

**Architecture:** Add DTOs in a new `maintenance.rs` module, add observability/compaction helpers to `TierManager`, and implement `TieredStorageStore::storage_health_snapshot()` plus `run_maintenance_once()`. Maintenance executes lifecycle first, attempts compaction per initialized tier without failing the whole pass, then returns a health snapshot and report.

**Tech Stack:** Rust, tokio tests, serde, chrono, existing `StorageEngine`, `StorageStats`, `TierLifecycleReport`.

---

## Production Follow-up Notes Required

Keep `docs/superpowers/specs/2026-06-03-fdc-storage-s8-observability-maintenance-design.md` as the production follow-up reference. It records future work for metrics/exporter, scheduler, compaction semantics, health semantics, and audit persistence. Do not remove those sections.

## Task 1: Maintenance DTOs

**Files:**
- Create: `crates/fdc-storage/src/maintenance.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Add DTOs and tests**

Create `crates/fdc-storage/src/maintenance.rs`:

```rust
//! Storage health snapshot and maintenance report types.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{StorageStats, StorageTier, TierLifecycleReport};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTierHealthStatus {
    Healthy,
    MissingEngine,
    StatsUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTierHealth {
    pub tier: StorageTier,
    pub enabled: bool,
    pub initialized: bool,
    pub status: StorageTierHealthStatus,
    pub stats: Option<StorageStats>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageHealthSnapshot {
    pub captured_at: DateTime<Utc>,
    pub tiers: BTreeMap<StorageTier, StorageTierHealth>,
    pub access_patterns: usize,
    pub migration_queue_len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMaintenanceReport {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub lifecycle: TierLifecycleReport,
    pub health: StorageHealthSnapshot,
    pub compacted_tiers: Vec<StorageTier>,
    pub compaction_errors: BTreeMap<StorageTier, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_health_snapshot_can_record_tier_health() {
        let mut tiers = BTreeMap::new();
        tiers.insert(
            StorageTier::L1,
            StorageTierHealth {
                tier: StorageTier::L1,
                enabled: true,
                initialized: true,
                status: StorageTierHealthStatus::Healthy,
                stats: Some(StorageStats::default()),
                error: None,
            },
        );
        let snapshot = StorageHealthSnapshot {
            captured_at: Utc::now(),
            tiers,
            access_patterns: 1,
            migration_queue_len: 2,
        };

        assert_eq!(snapshot.tiers[&StorageTier::L1].status, StorageTierHealthStatus::Healthy);
        assert_eq!(snapshot.access_patterns, 1);
        assert_eq!(snapshot.migration_queue_len, 2);
    }
}
```

- [ ] **Step 2: Export module and types**

In `lib.rs` add:

```rust
pub mod maintenance; // storage health snapshots and maintenance reports
```

and re-export:

```rust
pub use maintenance::{
    StorageHealthSnapshot, StorageMaintenanceReport, StorageTierHealth, StorageTierHealthStatus,
};
```

- [ ] **Step 3: Verify**

Run:

```bash
rtk cargo test -p fdc-storage maintenance::tests
```

Expected: tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-storage/src/maintenance.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add maintenance report types"
```

## Task 2: TierManager Observability Helpers

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`

- [ ] **Step 1: Add tests**

Add to `tier.rs` tests:

```rust
#[tokio::test]
async fn tier_manager_lists_configured_and_initialized_tiers() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    let mut disabled = TierConfig::new(StorageTier::L2);
    disabled.enabled = false;
    manager.add_tier(disabled);
    manager.initialize().await.unwrap();

    assert_eq!(manager.configured_tiers(), vec![StorageTier::L1, StorageTier::L2]);
    assert_eq!(manager.initialized_tiers(), vec![StorageTier::L1]);
}

#[tokio::test]
async fn tier_manager_compact_tier_reports_engine_result() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.initialize().await.unwrap();

    let error = manager.compact_tier(&StorageTier::L1).await.unwrap_err();
    assert!(error.to_string().contains("Compaction not supported"));
}
```

- [ ] **Step 2: Implement helpers**

Add in `impl TierManager`:

```rust
pub fn configured_tiers(&self) -> Vec<StorageTier> {
    let mut tiers: Vec<_> = self.tiers.keys().cloned().collect();
    tiers.sort_by_key(|tier| tier.priority());
    tiers
}

pub fn initialized_tiers(&self) -> Vec<StorageTier> {
    let mut tiers: Vec<_> = self.engines.keys().cloned().collect();
    tiers.sort_by_key(|tier| tier.priority());
    tiers
}

pub async fn compact_tier(&self, tier: &StorageTier) -> Result<()> {
    if let Some(engine) = self.engines.get(tier) {
        let engine_guard = engine.read().await;
        return engine_guard.compact().await;
    }
    Err(Error::validation(format!("storage tier {:?} is not initialized", tier)))
}
```

- [ ] **Step 3: Verify**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests
```

Expected: all tier tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-storage/src/tier.rs
git commit -m "feat(storage): add tier observability helpers"
```

## Task 3: Store Health Snapshot and Maintenance Pass

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Add tests**

Add to `tiered_store.rs` tests:

```rust
#[tokio::test]
async fn storage_health_snapshot_reports_initialized_and_disabled_tiers() {
    let l1 = memory_tier_config(StorageTier::L1);
    let mut l2 = memory_tier_config(StorageTier::L2);
    l2.enabled = false;
    let store = lifecycle_store_with_tiers(vec![l1, l2]).await;

    let snapshot = store.storage_health_snapshot().await.unwrap();

    assert_eq!(snapshot.tiers[&StorageTier::L1].status, StorageTierHealthStatus::Healthy);
    assert!(snapshot.tiers[&StorageTier::L1].initialized);
    assert!(!snapshot.tiers[&StorageTier::L2].enabled);
    assert!(!snapshot.tiers[&StorageTier::L2].initialized);
}

#[tokio::test]
async fn maintenance_pass_runs_lifecycle_and_records_compaction_errors() {
    let mut l1 = memory_tier_config(StorageTier::L1);
    l1.retention_duration = Some(Duration::seconds(1));
    let l2 = memory_tier_config(StorageTier::L2);
    let store = lifecycle_store_with_tiers(vec![l1, l2]).await;
    let record = tagged_record(b"maintenance-demote", "BTCUSDT")
        .with_timestamp(Utc::now() - Duration::seconds(10))
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
    store.write_batch(StorageWriteBatch::new(vec![record])).await.unwrap();

    let report = store.run_maintenance_once().await.unwrap();

    assert_eq!(report.lifecycle.retention_demoted, 1);
    assert!(report.health.tiers.contains_key(&StorageTier::L1));
    assert!(report.compaction_errors.contains_key(&StorageTier::L1));
    assert!(report.compaction_errors.contains_key(&StorageTier::L2));
}
```

- [ ] **Step 2: Implement imports**

Import these types in `tiered_store.rs`:

```rust
StorageHealthSnapshot, StorageMaintenanceReport, StorageTierHealth, StorageTierHealthStatus,
```

Add `BTreeMap` to the std collections import.

- [ ] **Step 3: Implement methods**

Add to `impl TieredStorageStore`:

```rust
pub async fn storage_health_snapshot(&self) -> Result<StorageHealthSnapshot> {
    let stats = self.tier_manager.get_tier_stats().await?;
    let initialized: std::collections::BTreeSet<_> = self
        .tier_manager
        .initialized_tiers()
        .into_iter()
        .collect();
    let mut tiers = BTreeMap::new();

    for tier in self.tier_manager.configured_tiers() {
        let enabled = self
            .tier_manager
            .tier_config(&tier)
            .map(|config| config.enabled)
            .unwrap_or(false);
        let initialized_tier = initialized.contains(&tier);
        let tier_stats = stats.get(&tier).cloned();
        let status = if enabled && !initialized_tier {
            StorageTierHealthStatus::MissingEngine
        } else if initialized_tier && tier_stats.is_none() {
            StorageTierHealthStatus::StatsUnavailable
        } else {
            StorageTierHealthStatus::Healthy
        };
        tiers.insert(
            tier.clone(),
            StorageTierHealth {
                tier,
                enabled,
                initialized: initialized_tier,
                status,
                stats: tier_stats,
                error: None,
            },
        );
    }

    Ok(StorageHealthSnapshot {
        captured_at: Utc::now(),
        tiers,
        access_patterns: self.tier_manager.get_access_patterns_count().await,
        migration_queue_len: self.tier_manager.get_migration_queue_length().await,
    })
}

pub async fn run_maintenance_once(&self) -> Result<StorageMaintenanceReport> {
    let started_at = Utc::now();
    let lifecycle = self.run_lifecycle_once().await?;
    let mut compacted_tiers = Vec::new();
    let mut compaction_errors = BTreeMap::new();

    for tier in self.tier_manager.initialized_tiers() {
        match self.tier_manager.compact_tier(&tier).await {
            Ok(()) => compacted_tiers.push(tier),
            Err(error) => {
                compaction_errors.insert(tier, error.to_string());
            }
        }
    }

    let health = self.storage_health_snapshot().await?;
    Ok(StorageMaintenanceReport {
        started_at,
        finished_at: Utc::now(),
        lifecycle,
        health,
        compacted_tiers,
        compaction_errors,
    })
}
```

- [ ] **Step 4: Verify**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests
```

Expected: all tiered store tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): add storage health and maintenance pass"
```

## Task 4: Full Verification and Merge

- [ ] **Step 1: Format and full test**

```bash
rtk cargo fmt -p fdc-storage && rtk cargo test -p fdc-storage
```

Expected: all tests pass.

- [ ] **Step 2: Remove generated test data and commit fmt residue if needed**

```bash
rm -rf crates/fdc-storage/data
rtk git status --short
```

If code files changed:

```bash
git add crates/fdc-storage/src/maintenance.rs crates/fdc-storage/src/lib.rs crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "style(storage): format maintenance observability"
```

- [ ] **Step 3: Merge and verify on `mdb-mqdev`**

```bash
git checkout mdb-mqdev
git merge --ff-only <s8-branch-name>
rtk cargo test -p fdc-storage
rm -rf crates/fdc-storage/data
git worktree remove <s8-worktree-path>
git worktree prune
git branch -d <s8-branch-name>
```

Expected: full tests pass on main, generated data removed, worktree cleaned.

## Self-Review

- Covers health snapshot, maintenance report, tier helpers, compaction errors, lifecycle integration, production follow-up notes.
- No placeholders.
- Type names match the S8 spec.
