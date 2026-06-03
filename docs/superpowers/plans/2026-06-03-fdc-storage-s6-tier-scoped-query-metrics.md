# fdc-storage S6 Tier-Scoped Query and Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add generic tier-scoped querying and query metrics to `fdc-storage` without breaking the existing `QueryableStorage` API.

**Architecture:** Extend the storage-owned `StorageQuery` model with a `StorageTierScope` enum and introduce `StorageQueryResult`/`StorageQueryMetrics`. Add scoped prefix scan support to `TierManager`, then have `TieredStorageStore` execute scoped scans, dedupe hotter-tier results first, apply existing generic filters/order/limit, and expose metrics through a new inherent method while keeping the existing trait method compatible.

**Tech Stack:** Rust, async-trait, tokio tests, serde, chrono, fdc-core errors, existing `StorageEngine` implementations.

---

## File Structure

- Modify: `crates/fdc-storage/src/query.rs`
  - Add `StorageTierScope`, `StorageQueryMetrics`, `StorageQueryResult`.
  - Add `StorageQuery::tier_scope` field and builder.
  - Add unit tests for defaults, builder, metrics constructor semantics.
- Modify: `crates/fdc-storage/src/tier.rs`
  - Import `StorageTierScope`.
  - Add `tiers_for_scope` and `scan_prefix_in_tiers`.
  - Keep `scan_prefix` backward-compatible by delegating to scoped scan.
  - Add unit tests for scope resolution with enabled initialized tiers.
- Modify: `crates/fdc-storage/src/tiered_store.rs`
  - Add `query_storage_with_metrics` inherent method.
  - Update trait `query_storage` to call the new method and return records.
  - Add integration-style tests using memory tiers for scope behavior and metrics.
- Modify: `crates/fdc-storage/src/lib.rs`
  - Re-export `StorageTierScope`, `StorageQueryMetrics`, `StorageQueryResult`.

## Task 1: Query Model Types

**Files:**
- Modify: `crates/fdc-storage/src/query.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Write failing tests for tier scope defaults and builder**

Add to `crates/fdc-storage/src/query.rs` test module:

```rust
#[test]
fn storage_tier_scope_defaults_to_all() {
    let query = StorageQuery::new("market_data");
    assert_eq!(query.tier_scope, StorageTierScope::All);
}

#[test]
fn storage_query_builder_sets_tier_scope() {
    let query = StorageQuery::new("market_data")
        .with_tier_scope(StorageTierScope::Only(StorageTier::L3));
    assert_eq!(query.tier_scope, StorageTierScope::Only(StorageTier::L3));
}

#[test]
fn storage_query_metrics_tracks_counts_and_tier_hits() {
    let mut metrics = StorageQueryMetrics::default();
    metrics.scanned_entries = 3;
    metrics.decoded_records = 2;
    metrics.returned_records = 1;
    metrics.tier_hits.insert(StorageTier::L2, 3);

    assert_eq!(metrics.scanned_entries, 3);
    assert_eq!(metrics.decoded_records, 2);
    assert_eq!(metrics.returned_records, 1);
    assert_eq!(metrics.tier_hits.get(&StorageTier::L2), Some(&3));
}
```

Also add imports inside the test module:

```rust
use crate::StorageTier;
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-storage query::tests::storage_tier_scope_defaults_to_all query::tests::storage_query_builder_sets_tier_scope query::tests::storage_query_metrics_tracks_counts_and_tier_hits
```

Expected: compile failure mentioning missing `StorageTierScope`, `StorageQueryMetrics`, or `tier_scope`.

- [ ] **Step 3: Implement query types**

In `crates/fdc-storage/src/query.rs`, add import:

```rust
use crate::StorageTier;
```

Add above `StorageQuery`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTierScope {
    All,
    Only(StorageTier),
    Hot,
    Warm,
    Cold,
}

impl Default for StorageTierScope {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StorageQueryMetrics {
    pub scanned_entries: usize,
    pub decoded_records: usize,
    pub returned_records: usize,
    pub tier_hits: BTreeMap<StorageTier, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQueryResult {
    pub records: Vec<StorageWriteRecord>,
    pub metrics: StorageQueryMetrics,
}
```

Add field to `StorageQuery`:

```rust
pub tier_scope: StorageTierScope,
```

Set default in `StorageQuery::new`:

```rust
tier_scope: StorageTierScope::default(),
```

Add builder:

```rust
pub fn with_tier_scope(mut self, tier_scope: StorageTierScope) -> Self {
    self.tier_scope = tier_scope;
    self
}
```

- [ ] **Step 4: Re-export new query types**

In `crates/fdc-storage/src/lib.rs`, replace:

```rust
pub use query::{QueryableStorage, StorageQuery, StorageQueryOrder};
```

with:

```rust
pub use query::{
    QueryableStorage, StorageQuery, StorageQueryMetrics, StorageQueryOrder, StorageQueryResult,
    StorageTierScope,
};
```

- [ ] **Step 5: Run focused tests and verify pass**

Run:

```bash
rtk cargo test -p fdc-storage query::tests::storage_tier_scope_defaults_to_all query::tests::storage_query_builder_sets_tier_scope query::tests::storage_query_metrics_tracks_counts_and_tier_hits
```

Expected: all three tests pass.

- [ ] **Step 6: Commit query model types**

```bash
git add crates/fdc-storage/src/query.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add tier scoped query model"
```

## Task 2: TierManager Scoped Scan

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`

- [ ] **Step 1: Write failing tests for scope resolution and scoped scan**

Add to `crates/fdc-storage/src/tier.rs` test module:

```rust
#[tokio::test]
async fn tier_manager_resolves_scope_to_available_initialized_tiers() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L3));
    manager.initialize().await.unwrap();

    assert_eq!(manager.tiers_for_scope(&StorageTierScope::All), vec![StorageTier::L1, StorageTier::L3]);
    assert_eq!(manager.tiers_for_scope(&StorageTierScope::Hot), vec![StorageTier::L1]);
    assert_eq!(manager.tiers_for_scope(&StorageTierScope::Warm), vec![StorageTier::L3]);
    assert_eq!(manager.tiers_for_scope(&StorageTierScope::Cold), Vec::<StorageTier>::new());
    assert_eq!(
        manager.tiers_for_scope(&StorageTierScope::Only(StorageTier::L3)),
        vec![StorageTier::L3]
    );
}

#[tokio::test]
async fn tier_manager_scans_prefix_only_in_requested_tiers() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L2));
    manager.initialize().await.unwrap();

    manager
        .put_with_placement(b"scope/a", b"l1", &StoragePlacementHint::for_tier(StorageTier::L1))
        .await
        .unwrap();
    manager
        .put_with_placement(b"scope/b", b"l2", &StoragePlacementHint::for_tier(StorageTier::L2))
        .await
        .unwrap();

    let l2_results = manager
        .scan_prefix_in_tiers(b"scope/", &[StorageTier::L2], None)
        .await
        .unwrap();

    assert_eq!(l2_results.len(), 1);
    assert_eq!(l2_results[0].0, StorageTier::L2);
    assert_eq!(l2_results[0].1, b"scope/b".to_vec());
    assert_eq!(l2_results[0].2, b"l2".to_vec());
}
```

Ensure test module imports include:

```rust
use crate::{StoragePlacementHint, StorageTierScope};
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests::tier_manager_resolves_scope_to_available_initialized_tiers tier::tests::tier_manager_scans_prefix_only_in_requested_tiers
```

Expected: compile failure for missing `tiers_for_scope` and `scan_prefix_in_tiers`.

- [ ] **Step 3: Implement scope resolution and scoped scan**

In `crates/fdc-storage/src/tier.rs`, import `StorageTierScope` from crate if needed:

```rust
use crate::StorageTierScope;
```

Add methods inside `impl TierManager`:

```rust
pub fn tiers_for_scope(&self, scope: &StorageTierScope) -> Vec<StorageTier> {
    let mut tiers = match scope {
        StorageTierScope::All => self.engines.keys().cloned().collect(),
        StorageTierScope::Only(tier) => {
            if self.engines.contains_key(tier) {
                vec![tier.clone()]
            } else {
                Vec::new()
            }
        }
        StorageTierScope::Hot => [StorageTier::L1, StorageTier::L2]
            .into_iter()
            .filter(|tier| self.engines.contains_key(tier))
            .collect(),
        StorageTierScope::Warm => {
            if self.engines.contains_key(&StorageTier::L3) {
                vec![StorageTier::L3]
            } else {
                Vec::new()
            }
        }
        StorageTierScope::Cold => {
            if self.engines.contains_key(&StorageTier::L4) {
                vec![StorageTier::L4]
            } else {
                Vec::new()
            }
        }
    };
    tiers.sort_by_key(|tier| tier.priority());
    tiers
}

pub async fn scan_prefix_in_tiers(
    &self,
    prefix: &[u8],
    tiers: &[StorageTier],
    limit: Option<usize>,
) -> Result<Vec<(StorageTier, Vec<u8>, Vec<u8>)>> {
    let mut results = Vec::new();
    let mut ordered_tiers = tiers.to_vec();
    ordered_tiers.sort_by_key(|tier| tier.priority());

    for tier in ordered_tiers {
        let remaining = limit.map(|limit| limit.saturating_sub(results.len()));
        if matches!(remaining, Some(0)) {
            break;
        }

        let Some(engine) = self.engines.get(&tier) else {
            continue;
        };

        let engine_guard = engine.read().await;
        let mut tier_results = engine_guard.scan(Some(prefix), None, remaining).await?;
        tier_results.retain(|(key, _)| key.starts_with(prefix));
        results.extend(
            tier_results
                .into_iter()
                .map(|(key, value)| (tier.clone(), key, value)),
        );
    }

    if let Some(limit) = limit {
        results.truncate(limit);
    }

    Ok(results)
}
```

Replace existing `scan_prefix` body with:

```rust
pub async fn scan_prefix(
    &self,
    prefix: &[u8],
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let tiers = self.tiers_for_scope(&StorageTierScope::All);
    Ok(self
        .scan_prefix_in_tiers(prefix, &tiers, limit)
        .await?
        .into_iter()
        .map(|(_, key, value)| (key, value))
        .collect())
}
```

- [ ] **Step 4: Run focused tests and verify pass**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests::tier_manager_resolves_scope_to_available_initialized_tiers tier::tests::tier_manager_scans_prefix_only_in_requested_tiers
```

Expected: both tests pass.

- [ ] **Step 5: Run existing tier tests**

Run:

```bash
rtk cargo test -p fdc-storage tier::tests
```

Expected: all tier tests pass.

- [ ] **Step 6: Commit TierManager scoped scan**

```bash
git add crates/fdc-storage/src/tier.rs
git commit -m "feat(storage): add scoped tier prefix scan"
```

## Task 3: TieredStorageStore Query Metrics

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Write failing tests for store tier scope and metrics**

Add helper to `crates/fdc-storage/src/tiered_store.rs` test module:

```rust
async fn memory_store_with_l1_to_l4() -> TieredStorageStore {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L2));
    manager.add_tier(TierConfig::new(StorageTier::L3));
    manager.add_tier(TierConfig::new(StorageTier::L4));
    manager.initialize().await.unwrap();
    TieredStorageStore::new(Arc::new(manager))
}
```

Add tests:

```rust
#[tokio::test]
async fn tiered_store_query_can_scope_to_only_one_tier() {
    let store = memory_store_with_l1_to_l4().await;
    let l1 = tagged_record(b"same", "BTCUSDT")
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
    let l3 = tagged_record(b"l3", "BTCUSDT")
        .with_placement(StoragePlacementHint::for_tier(StorageTier::L3));

    store.write_batch(StorageWriteBatch::new(vec![l1, l3])).await.unwrap();

    let result = store
        .query_storage_with_metrics(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
        )
        .await
        .unwrap();

    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].key, b"l3".to_vec());
    assert_eq!(result.metrics.tier_hits.get(&StorageTier::L3), Some(&1));
    assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L1));
}

#[tokio::test]
async fn tiered_store_query_hot_scope_reads_l1_l2_only() {
    let store = memory_store_with_l1_to_l4().await;
    store
        .write_batch(StorageWriteBatch::new(vec![
            tagged_record(b"l1", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L1)),
            tagged_record(b"l2", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L2)),
            tagged_record(b"l3", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L3)),
            tagged_record(b"l4", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L4)),
        ]))
        .await
        .unwrap();

    let result = store
        .query_storage_with_metrics(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Hot)
                .with_order(StorageQueryOrder::KeyAsc),
        )
        .await
        .unwrap();

    let keys: Vec<_> = result.records.iter().map(|record| record.key.as_slice()).collect();
    assert_eq!(keys, vec![b"l1".as_slice(), b"l2".as_slice()]);
    assert_eq!(result.metrics.tier_hits.get(&StorageTier::L1), Some(&1));
    assert_eq!(result.metrics.tier_hits.get(&StorageTier::L2), Some(&1));
    assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L3));
    assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L4));
}

#[tokio::test]
async fn tiered_store_query_warm_and_cold_scopes_read_expected_tiers() {
    let store = memory_store_with_l1_to_l4().await;
    store
        .write_batch(StorageWriteBatch::new(vec![
            tagged_record(b"l3", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L3)),
            tagged_record(b"l4", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L4)),
        ]))
        .await
        .unwrap();

    let warm = store
        .query_storage_with_metrics(&StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Warm))
        .await
        .unwrap();
    let cold = store
        .query_storage_with_metrics(&StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Cold))
        .await
        .unwrap();

    assert_eq!(warm.records.len(), 1);
    assert_eq!(warm.records[0].key, b"l3".to_vec());
    assert_eq!(cold.records.len(), 1);
    assert_eq!(cold.records[0].key, b"l4".to_vec());
}

#[tokio::test]
async fn tiered_store_query_metrics_report_scanned_decoded_returned_and_tier_hits() {
    let store = memory_store_with_l1_to_l4().await;
    store
        .write_batch(StorageWriteBatch::new(vec![
            tagged_record(b"btc", "BTCUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L1)),
            tagged_record(b"eth", "ETHUSDT").with_placement(StoragePlacementHint::for_tier(StorageTier::L2)),
        ]))
        .await
        .unwrap();

    let result = store
        .query_storage_with_metrics(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
        .await
        .unwrap();

    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].key, b"btc".to_vec());
    assert_eq!(result.metrics.scanned_entries, 2);
    assert_eq!(result.metrics.decoded_records, 2);
    assert_eq!(result.metrics.returned_records, 1);
    assert_eq!(result.metrics.tier_hits.get(&StorageTier::L1), Some(&1));
    assert_eq!(result.metrics.tier_hits.get(&StorageTier::L2), Some(&1));
}
```

Add import in test module:

```rust
use crate::StorageTierScope;
```

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_query_can_scope_to_only_one_tier tiered_store::tests::tiered_store_query_hot_scope_reads_l1_l2_only tiered_store::tests::tiered_store_query_warm_and_cold_scopes_read_expected_tiers tiered_store::tests::tiered_store_query_metrics_report_scanned_decoded_returned_and_tier_hits
```

Expected: compile failure for missing `query_storage_with_metrics` or `StorageTierScope` import/export.

- [ ] **Step 3: Implement `query_storage_with_metrics`**

At top of `crates/fdc-storage/src/tiered_store.rs`, change imports to include:

```rust
StorageQueryMetrics, StorageQueryResult,
```

Add inherent method inside `impl TieredStorageStore`:

```rust
pub async fn query_storage_with_metrics(
    &self,
    query: &StorageQuery,
) -> Result<StorageQueryResult> {
    query.validate()?;

    let prefix = query.collection.as_ref().map_or_else(
        || storage_namespace_prefix(&query.namespace),
        |collection| storage_collection_prefix(&query.namespace, collection),
    );

    let tiers = self.tier_manager.tiers_for_scope(&query.tier_scope);
    let now = Utc::now();
    let mut seen = BTreeSet::new();
    let mut records = Vec::new();
    let mut metrics = StorageQueryMetrics::default();

    for (tier, key, value) in self
        .tier_manager
        .scan_prefix_in_tiers(&prefix, &tiers, None)
        .await?
    {
        metrics.scanned_entries += 1;
        *metrics.tier_hits.entry(tier).or_insert(0) += 1;

        if !seen.insert(key) {
            continue;
        }

        let record = decode_record(&value)?;
        metrics.decoded_records += 1;
        if record_is_expired(&record, now) {
            continue;
        }
        if record_matches_storage_query(&record, query) {
            records.push(record);
        }
    }

    let records = apply_query_order_and_limit(records, query);
    metrics.returned_records = records.len();

    Ok(StorageQueryResult { records, metrics })
}
```

Replace trait implementation body with:

```rust
async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>> {
    Ok(self.query_storage_with_metrics(query).await?.records)
}
```

- [ ] **Step 4: Run focused tests and verify pass**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_query_can_scope_to_only_one_tier tiered_store::tests::tiered_store_query_hot_scope_reads_l1_l2_only tiered_store::tests::tiered_store_query_warm_and_cold_scopes_read_expected_tiers tiered_store::tests::tiered_store_query_metrics_report_scanned_decoded_returned_and_tier_hits
```

Expected: all four tests pass.

- [ ] **Step 5: Run all tiered store tests**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store::tests
```

Expected: all tiered store tests pass.

- [ ] **Step 6: Commit TieredStorageStore metrics**

```bash
git add crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): add tiered query metrics"
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

- [ ] **Step 3: Check no unrelated files were touched**

Run:

```bash
rtk git status --short
```

Expected: only S6 docs/code changes are staged/committed in the worktree, and no `crates/fdc-server/src/health` or `crates/fdc-server/src/bin/health` files changed by this work.

- [ ] **Step 4: Commit any formatting-only residue if needed**

If `cargo fmt` changed files after previous commits:

```bash
git add crates/fdc-storage/src/query.rs crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs crates/fdc-storage/src/lib.rs
git commit -m "style(storage): format tier scoped query"
```

Expected: commit created only if there were formatting changes.

- [ ] **Step 5: Merge back to `mdb-mqdev`**

From the main working tree:

```bash
git checkout mdb-mqdev
git merge --ff-only <s6-branch-name>
```

Expected: fast-forward merge succeeds.

- [ ] **Step 6: Verify on main branch**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: all tests pass on `mdb-mqdev`.

- [ ] **Step 7: Clean S6 worktree and branch**

Run:

```bash
git worktree remove <s6-worktree-path>
git branch -d <s6-branch-name>
```

Expected: worktree and branch are removed.

## Self-Review

- Spec coverage: covers tier scope enum, query field/builder, metrics types, scoped TierManager scan, TieredStorageStore metrics API, compatibility, tests, and merge verification.
- Placeholder scan: no TBD/TODO/fill-in placeholders. Commands and code snippets are concrete.
- Type consistency: `StorageTierScope`, `StorageQueryMetrics`, `StorageQueryResult`, `query_storage_with_metrics`, `tiers_for_scope`, and `scan_prefix_in_tiers` are named consistently across tasks.
