# Runtime Tiering Policy Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread the runtime-selected storage policy profile into the tiered storage write path so `generic_realtime` can affect actual tier placement.

**Architecture:** Add a `StorageTieringPolicy` field to `TierManager` with compatibility as the default. Add policy-aware constructors for `TierManager`, `TieredStorageStore`, and `QueryableMarketDataStore`, then map server runtime profile config to the correct storage policy in `fdc-server`.

**Tech Stack:** Rust, Tokio, `fdc-storage` tiered store and policy APIs, `fdc-server` runtime config, TDD with `rtk cargo test`.

---

## Reference design

- `docs/superpowers/specs/2026-06-07-runtime-tiering-policy-injection-design.md`
- `crates/fdc-storage/src/tier.rs`
- `crates/fdc-storage/src/tiered_store.rs`
- `crates/fdc-storage/src/queryable.rs`
- `crates/fdc-server/src/runtime/storage.rs`

## Task 1: Add failing storage policy injection tests

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`
- Modify: `crates/fdc-storage/src/tiered_store.rs`
- Modify: `crates/fdc-storage/src/queryable.rs`

- [ ] **Step 1: Add failing `TierManager` policy inspection test**

Add to `crates/fdc-storage/src/tier.rs` tests:

```rust
#[test]
fn tier_manager_can_be_created_with_injected_policy() {
    let manager = TierManager::with_policy(StorageTieringPolicy::generic_realtime());
    assert_eq!(
        manager.policy().profile(),
        crate::StorageTieringPolicyProfile::GenericRealtime
    );

    let default_manager = TierManager::new();
    assert_eq!(
        default_manager.policy().profile(),
        crate::StorageTieringPolicyProfile::Compatibility
    );
}
```

- [ ] **Step 2: Add failing `TieredStorageStore` routing test**

Add to `crates/fdc-storage/src/tiered_store.rs` tests:

```rust
#[tokio::test]
async fn tiered_store_memory_only_with_policy_routes_generic_realtime_tags() {
    let store = memory_store_with_l1_to_l4_and_policy(StorageTieringPolicy::generic_realtime()).await;

    let live = tagged_record(b"live", "BTCUSDT").with_metadata_tag("mode", "live");
    let backfill = tagged_record(b"backfill", "BTCUSDT").with_metadata_tag("mode", "backfill");
    store
        .write_batch(StorageWriteBatch::new(vec![live.clone(), backfill.clone()]))
        .await
        .unwrap();

    let live_key = TieredStorageStore::storage_key_for_record(&live);
    let backfill_key = TieredStorageStore::storage_key_for_record(&backfill);
    assert!(store
        .tier_manager()
        .get_from_tier(&live_key, &StorageTier::L2)
        .await
        .unwrap()
        .is_some());
    assert!(store
        .tier_manager()
        .get_from_tier(&backfill_key, &StorageTier::L3)
        .await
        .unwrap()
        .is_some());
}
```

Also add helper near existing test helpers:

```rust
async fn memory_store_with_l1_to_l4_and_policy(policy: StorageTieringPolicy) -> TieredStorageStore {
    let mut manager = TierManager::with_policy(policy);
    for tier in [StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4] {
        manager.add_tier(memory_tier_config(tier));
    }
    manager.initialize().await.unwrap();
    TieredStorageStore::new(Arc::new(manager))
}
```

- [ ] **Step 3: Add failing `QueryableMarketDataStore` constructor test**

Add to `crates/fdc-storage/src/queryable.rs` tests:

```rust
#[tokio::test]
async fn queryable_market_data_store_can_use_policy_configured_tiered_backend() {
    let store = QueryableMarketDataStore::memory_tiered_with_policy(
        StorageTieringPolicy::generic_realtime(),
    )
    .await
    .unwrap();

    store
        .write_batch(StorageWriteBatch::new(vec![record(
            "market_data",
            "trades",
            b"policy",
            "BTCUSDT",
            "trade",
        )]))
        .await
        .unwrap();

    assert_eq!(store.record_count(), 1);
}
```

- [ ] **Step 4: Run failing tests**

```bash
rtk cargo test -p fdc-storage tier_manager_can_be_created_with_injected_policy
rtk cargo test -p fdc-storage tiered_store_memory_only_with_policy_routes_generic_realtime_tags
rtk cargo test -p fdc-storage queryable_market_data_store_can_use_policy_configured_tiered_backend
```

Expected: FAIL because constructors/accessors do not exist.

---

## Task 2: Implement storage policy injection

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`
- Modify: `crates/fdc-storage/src/tiered_store.rs`
- Modify: `crates/fdc-storage/src/queryable.rs`

- [ ] **Step 1: Add policy field and constructors to `TierManager`**

In `TierManager`, add:

```rust
policy: StorageTieringPolicy,
```

Update `new()` to call `with_policy(StorageTieringPolicy::compatibility())`.

Add:

```rust
pub fn with_policy(policy: StorageTieringPolicy) -> Self { ... }
pub fn policy(&self) -> &StorageTieringPolicy { &self.policy }
```

Change `determine_tier_for_placement()` to call `self.policy.decide_initial_placement(...)` instead of `StorageTieringPolicy::compatibility()`.

- [ ] **Step 2: Add policy-aware store constructors**

In `TieredStorageStore`:

```rust
pub async fn memory_only_with_policy(policy: StorageTieringPolicy) -> Result<Self> {
    let mut manager = TierManager::with_policy(policy);
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.initialize().await?;
    Ok(Self::new(Arc::new(manager)))
}
```

Keep `memory_only()` as wrapper with compatibility.

In `QueryableMarketDataStore`:

```rust
pub async fn memory_tiered_with_policy(policy: StorageTieringPolicy) -> Result<Self> {
    let store = TieredStorageStore::memory_only_with_policy(policy).await?;
    Ok(Self::from_tiered_store(Arc::new(store)))
}
```

Keep `memory_tiered()` as wrapper with compatibility.

- [ ] **Step 3: Run tests and commit**

```bash
rtk cargo fmt --package fdc-storage
rtk cargo test -p fdc-storage tier_manager_can_be_created_with_injected_policy
rtk cargo test -p fdc-storage tiered_store_memory_only_with_policy_routes_generic_realtime_tags
rtk cargo test -p fdc-storage queryable_market_data_store_can_use_policy_configured_tiered_backend
rtk cargo test -p fdc-storage --test dependency_guard
```

Commit:

```bash
git add crates/fdc-storage/src/tier.rs crates/fdc-storage/src/tiered_store.rs crates/fdc-storage/src/queryable.rs
git commit -m "feat(storage): inject tiering policy into tiered writes"
```

---

## Task 3: Map server runtime profile to storage policy

**Files:**
- Modify: `crates/fdc-server/src/runtime/storage.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing server runtime builder test**

Append to `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn runtime_builder_accepts_tiered_generic_realtime_policy() {
    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Tiered,
        policy_profile: MarketDataStoragePolicyProfileConfig::GenericRealtime,
    })
    .await
    .unwrap();

    store
        .write_batch(StorageWriteBatch::new(vec![runtime_storage_record("generic-runtime")]))
        .await
        .unwrap();

    assert_eq!(store.record_count(), 1);
}
```

- [ ] **Step 2: Implement profile mapping**

In `crates/fdc-server/src/runtime/storage.rs`, import:

```rust
use fdc_storage::{QueryableMarketDataStore, StorageTieringPolicy};
use crate::MarketDataStoragePolicyProfileConfig;
```

Add helper:

```rust
fn storage_policy_for_profile(profile: MarketDataStoragePolicyProfileConfig) -> StorageTieringPolicy {
    match profile {
        MarketDataStoragePolicyProfileConfig::Compatibility => StorageTieringPolicy::compatibility(),
        MarketDataStoragePolicyProfileConfig::GenericRealtime => StorageTieringPolicy::generic_realtime(),
    }
}
```

Use `QueryableMarketDataStore::memory_tiered_with_policy(storage_policy_for_profile(config.policy_profile)).await` for tiered backend.

- [ ] **Step 3: Run tests and commit**

```bash
rtk cargo fmt --package fdc-server
rtk cargo test -p fdc-server --test production_server_router_contract runtime_builder_accepts_tiered_generic_realtime_policy
rtk cargo test -p fdc-server --test runtime_config_contract
```

Commit:

```bash
git add crates/fdc-server/src/runtime/storage.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): map runtime tiering profile to storage policy"
```

---

## Task 4: Docs and final verification

**Files:**
- Modify: `crates/fdc-storage/docs/public-api-stability.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update docs**

Record that runtime-selected tiering profiles now reach the tiered write path, while `fdc-storage` remains generic.

- [ ] **Step 2: Final verification**

```bash
rtk cargo fmt --package fdc-storage --check
rtk cargo fmt --package fdc-server --check
git diff --check
rtk cargo test -p fdc-storage tier_manager_can_be_created_with_injected_policy
rtk cargo test -p fdc-storage tiered_store_memory_only_with_policy_routes_generic_realtime_tags
rtk cargo test -p fdc-storage queryable_market_data_store_can_use_policy_configured_tiered_backend
rtk cargo test -p fdc-storage --test tiering_policy_contract
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server --test production_server_router_contract
rtk git status --short
```

- [ ] **Step 3: Commit docs**

```bash
git add crates/fdc-storage/docs/public-api-stability.md crates/fdc-storage/docs/storage-boundary-acceptance-report.md docs/DEVELOPMENT_STATUS.md
git commit -m "docs(storage): record runtime tiering policy injection"
```

---

## Self-review

- Spec coverage: storage injection, tiered constructors, queryable facade constructor, server mapping, docs, and verification are covered.
- Placeholder scan: no TBD/TODO/fill-in steps remain.
- Boundary check: no business dependency is added to `fdc-storage`.
- Compatibility check: existing constructors remain default compatibility wrappers.
