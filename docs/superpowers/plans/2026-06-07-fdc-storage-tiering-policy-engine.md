# fdc-storage Intelligent Tiering Policy Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic, explainable storage-owned tiering policy engine and wire the existing tier placement path through a compatibility policy without changing current S12 behavior.

**Architecture:** Introduce `crates/fdc-storage/src/policy.rs` as a generic policy module. The first profile, `Compatibility`, reproduces current `TierManager::determine_tier_for_placement()` behavior using explicit reasons. Then `TierManager` delegates tier decisions to the policy while preserving existing public write/query contracts.

**Tech Stack:** Rust, Tokio, chrono, serde, existing `fdc-storage` tier/write/query APIs, TDD with `rtk cargo test -p fdc-storage`.

---

## Reference design

Read first:

- `crates/fdc-storage/docs/storage-tiering-policy-engine-design.md`
- `crates/fdc-storage/docs/public-api-stability.md`
- `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`

## File structure

- Create `crates/fdc-storage/src/policy.rs`
  - Owns `StorageTieringPolicy`, `StorageTieringPolicyProfile`, `StorageTieringContext`, `StorageTieringDecision`, `StorageRetentionClass`, `StorageTieringReason`, and `AccessPatternSnapshot`.
  - Contains deterministic policy unit tests.
- Modify `crates/fdc-storage/src/lib.rs`
  - Add `pub mod policy;`.
  - Re-export the public policy types.
- Modify `crates/fdc-storage/src/tier.rs`
  - Add `AccessPattern::snapshot()`.
  - Replace duplicated placement decision logic with `StorageTieringPolicy::compatibility()`.
  - Keep `put_with_placement()` signature unchanged.
- Create `crates/fdc-storage/tests/tiering_policy_contract.rs`
  - Contract tests that prove public exports, generic policy decisions, and integrated write routing behavior.
- Modify `crates/fdc-storage/docs/public-api-stability.md`
  - Record the new P13 policy API as additive and experimental-stable for callers.
- Modify `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
  - Add a short P13 note once tests pass.

---

## Task 1: Add deterministic policy types and failing unit tests

**Files:**
- Create: `crates/fdc-storage/src/policy.rs`
- Test: `crates/fdc-storage/src/policy.rs`

- [ ] **Step 1: Create `policy.rs` with failing tests only**

Create `crates/fdc-storage/src/policy.rs` with this content:

```rust
//! Storage-owned tiering policy decisions.
//!
//! The policy layer converts generic storage facts and advisory hints into an
//! explainable initial tier decision. Business modules may provide hints, but
//! storage owns the final decision.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Duration;

    use crate::{
        AccessPatternSnapshot, StorageAccessPatternHint, StorageDurabilityHint,
        StoragePlacementHint, StorageRetentionClass, StorageTier, StorageTieringContext,
        StorageTieringPolicy, StorageTieringReason,
    };

    fn context_with_hint<'a>(
        hint: &'a StoragePlacementHint,
        available_tiers: &'a [StorageTier],
        metadata_tags: &'a BTreeMap<String, String>,
    ) -> StorageTieringContext<'a> {
        StorageTieringContext {
            namespace: "market_data",
            collection: "trades",
            key_len: 8,
            value_len: 128,
            timestamp_age_seconds: 0,
            metadata_tags,
            placement_hint: hint,
            available_tiers,
            prior_access: None,
        }
    }

    #[test]
    fn compatibility_policy_prefers_explicit_target_tier_hint() {
        let hint = StoragePlacementHint::for_tier(StorageTier::L4);
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.initial_tier, StorageTier::L4);
        assert_eq!(decision.retention_class, StorageRetentionClass::ArchiveCold);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::ExplicitTargetTierHint));
    }

    #[test]
    fn compatibility_policy_maps_access_pattern_hints_to_tiers() {
        let cases = [
            (StorageAccessPatternHint::UltraHot, StorageTier::L1),
            (StorageAccessPatternHint::Hot, StorageTier::L2),
            (StorageAccessPatternHint::Warm, StorageTier::L3),
            (StorageAccessPatternHint::Cold, StorageTier::L4),
        ];
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        for (access_pattern, expected_tier) in cases {
            let hint = StoragePlacementHint::default().with_access_pattern(access_pattern);
            let decision = StorageTieringPolicy::compatibility()
                .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

            assert_eq!(decision.initial_tier, expected_tier);
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::AccessPatternHint));
        }
    }

    #[test]
    fn compatibility_policy_maps_durability_hints_to_tiers() {
        let cases = [
            (StorageDurabilityHint::Ephemeral, StorageTier::L1),
            (StorageDurabilityHint::Cached, StorageTier::L2),
            (StorageDurabilityHint::Persistent, StorageTier::L3),
            (StorageDurabilityHint::Archival, StorageTier::L4),
        ];
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        for (durability, expected_tier) in cases {
            let hint = StoragePlacementHint::default().with_durability(durability);
            let decision = StorageTieringPolicy::compatibility()
                .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

            assert_eq!(decision.initial_tier, expected_tier);
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::DurabilityHint));
        }
    }

    #[test]
    fn compatibility_policy_uses_prior_access_when_hints_are_unspecified() {
        let hint = StoragePlacementHint::default();
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();
        let context = StorageTieringContext {
            namespace: "market_data",
            collection: "trades",
            key_len: 8,
            value_len: 128,
            timestamp_age_seconds: 0,
            metadata_tags: &tags,
            placement_hint: &hint,
            available_tiers: &tiers,
            prior_access: Some(AccessPatternSnapshot {
                access_count: 42,
                access_frequency: 12.0,
                data_size: 128,
                heat_score: 12.0,
                recommended_tier: StorageTier::L1,
            }),
        };

        let decision = StorageTieringPolicy::compatibility().decide_initial_placement(&context);

        assert_eq!(decision.initial_tier, StorageTier::L1);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::ExistingAccessPattern));
    }

    #[test]
    fn compatibility_policy_defaults_to_nearest_available_l2() {
        let hint = StoragePlacementHint::default();
        let tiers = vec![StorageTier::L1, StorageTier::L3];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.initial_tier, StorageTier::L1);
        assert_eq!(decision.ttl, None);
        assert_eq!(decision.retention_class, StorageRetentionClass::RealtimeHot);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::DefaultProfile));
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::NearestAvailableTierFallback));
    }

    #[test]
    fn compatibility_policy_preserves_hint_ttl_in_decision() {
        let hint = StoragePlacementHint::default().with_ttl(Duration::seconds(30));
        let tiers = vec![StorageTier::L1, StorageTier::L2];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.ttl, Some(Duration::seconds(30)));
    }
}
```

- [ ] **Step 2: Run the failing policy tests**

Run:

```bash
rtk cargo test -p fdc-storage policy::tests
```

Expected: FAIL because `policy` is not exported and the referenced policy types do not exist.

- [ ] **Step 3: Implement the policy types and compatibility decision logic**

Replace the content of `crates/fdc-storage/src/policy.rs` with this full implementation, keeping the tests from Step 1 at the bottom:

```rust
//! Storage-owned tiering policy decisions.
//!
//! The policy layer converts generic storage facts and advisory hints into an
//! explainable initial tier decision. Business modules may provide hints, but
//! storage owns the final decision.

use std::collections::BTreeMap;

use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::{
    StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageTier,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTieringPolicyProfile {
    Compatibility,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessPatternSnapshot {
    pub access_count: u64,
    pub access_frequency: f64,
    pub data_size: usize,
    pub heat_score: f64,
    pub recommended_tier: StorageTier,
}

#[derive(Debug, Clone)]
pub struct StorageTieringContext<'a> {
    pub namespace: &'a str,
    pub collection: &'a str,
    pub key_len: usize,
    pub value_len: usize,
    pub timestamp_age_seconds: i64,
    pub metadata_tags: &'a BTreeMap<String, String>,
    pub placement_hint: &'a StoragePlacementHint,
    pub available_tiers: &'a [StorageTier],
    pub prior_access: Option<AccessPatternSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTieringDecision {
    pub initial_tier: StorageTier,
    pub ttl: Option<Duration>,
    pub retention_class: StorageRetentionClass,
    pub reasons: Vec<StorageTieringReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageRetentionClass {
    Ephemeral,
    RealtimeHot,
    RecentDurable,
    AnalyticalWarm,
    ArchiveCold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTieringReason {
    ExplicitTargetTierHint,
    AccessPatternHint,
    DurabilityHint,
    ExistingAccessPattern,
    NearestAvailableTierFallback,
    DefaultProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTieringPolicy {
    profile: StorageTieringPolicyProfile,
}

impl StorageTieringPolicy {
    pub fn compatibility() -> Self {
        Self {
            profile: StorageTieringPolicyProfile::Compatibility,
        }
    }

    pub fn profile(&self) -> StorageTieringPolicyProfile {
        self.profile
    }

    pub fn decide_initial_placement(
        &self,
        context: &StorageTieringContext<'_>,
    ) -> StorageTieringDecision {
        match self.profile {
            StorageTieringPolicyProfile::Compatibility => self.decide_compatibility(context),
        }
    }

    fn decide_compatibility(&self, context: &StorageTieringContext<'_>) -> StorageTieringDecision {
        let mut reasons = Vec::new();
        let desired_tier = if let Some(target_tier) = &context.placement_hint.target_tier {
            reasons.push(StorageTieringReason::ExplicitTargetTierHint);
            target_tier.clone()
        } else if let Some(tier) = access_pattern_tier(&context.placement_hint.access_pattern) {
            reasons.push(StorageTieringReason::AccessPatternHint);
            tier
        } else if let Some(tier) = durability_tier(&context.placement_hint.durability) {
            reasons.push(StorageTieringReason::DurabilityHint);
            tier
        } else if let Some(access) = &context.prior_access {
            reasons.push(StorageTieringReason::ExistingAccessPattern);
            access.recommended_tier.clone()
        } else {
            reasons.push(StorageTieringReason::DefaultProfile);
            StorageTier::L2
        };

        let initial_tier = nearest_available_tier(&desired_tier, context.available_tiers)
            .unwrap_or_else(|| desired_tier.clone());
        if initial_tier != desired_tier {
            reasons.push(StorageTieringReason::NearestAvailableTierFallback);
        }

        StorageTieringDecision {
            retention_class: retention_class_for_tier(&initial_tier),
            initial_tier,
            ttl: context.placement_hint.ttl,
            reasons,
        }
    }
}

impl Default for StorageTieringPolicy {
    fn default() -> Self {
        Self::compatibility()
    }
}

fn access_pattern_tier(access_pattern: &StorageAccessPatternHint) -> Option<StorageTier> {
    match access_pattern {
        StorageAccessPatternHint::UltraHot => Some(StorageTier::L1),
        StorageAccessPatternHint::Hot => Some(StorageTier::L2),
        StorageAccessPatternHint::Warm => Some(StorageTier::L3),
        StorageAccessPatternHint::Cold => Some(StorageTier::L4),
        StorageAccessPatternHint::Unspecified => None,
    }
}

fn durability_tier(durability: &StorageDurabilityHint) -> Option<StorageTier> {
    match durability {
        StorageDurabilityHint::Ephemeral => Some(StorageTier::L1),
        StorageDurabilityHint::Cached => Some(StorageTier::L2),
        StorageDurabilityHint::Persistent => Some(StorageTier::L3),
        StorageDurabilityHint::Archival => Some(StorageTier::L4),
        StorageDurabilityHint::Unspecified => None,
    }
}

fn nearest_available_tier(desired: &StorageTier, available_tiers: &[StorageTier]) -> Option<StorageTier> {
    if available_tiers.iter().any(|tier| tier == desired) {
        return Some(desired.clone());
    }

    available_tiers
        .iter()
        .cloned()
        .min_by_key(|tier| (tier.priority() as i16 - desired.priority() as i16).abs())
}

fn retention_class_for_tier(tier: &StorageTier) -> StorageRetentionClass {
    match tier {
        StorageTier::L1 => StorageRetentionClass::RealtimeHot,
        StorageTier::L2 => StorageRetentionClass::RecentDurable,
        StorageTier::L3 => StorageRetentionClass::AnalyticalWarm,
        StorageTier::L4 => StorageRetentionClass::ArchiveCold,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Duration;

    use crate::{
        AccessPatternSnapshot, StorageAccessPatternHint, StorageDurabilityHint,
        StoragePlacementHint, StorageRetentionClass, StorageTier, StorageTieringContext,
        StorageTieringPolicy, StorageTieringReason,
    };

    fn context_with_hint<'a>(
        hint: &'a StoragePlacementHint,
        available_tiers: &'a [StorageTier],
        metadata_tags: &'a BTreeMap<String, String>,
    ) -> StorageTieringContext<'a> {
        StorageTieringContext {
            namespace: "market_data",
            collection: "trades",
            key_len: 8,
            value_len: 128,
            timestamp_age_seconds: 0,
            metadata_tags,
            placement_hint: hint,
            available_tiers,
            prior_access: None,
        }
    }

    #[test]
    fn compatibility_policy_prefers_explicit_target_tier_hint() {
        let hint = StoragePlacementHint::for_tier(StorageTier::L4);
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.initial_tier, StorageTier::L4);
        assert_eq!(decision.retention_class, StorageRetentionClass::ArchiveCold);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::ExplicitTargetTierHint));
    }

    #[test]
    fn compatibility_policy_maps_access_pattern_hints_to_tiers() {
        let cases = [
            (StorageAccessPatternHint::UltraHot, StorageTier::L1),
            (StorageAccessPatternHint::Hot, StorageTier::L2),
            (StorageAccessPatternHint::Warm, StorageTier::L3),
            (StorageAccessPatternHint::Cold, StorageTier::L4),
        ];
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        for (access_pattern, expected_tier) in cases {
            let hint = StoragePlacementHint::default().with_access_pattern(access_pattern);
            let decision = StorageTieringPolicy::compatibility()
                .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

            assert_eq!(decision.initial_tier, expected_tier);
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::AccessPatternHint));
        }
    }

    #[test]
    fn compatibility_policy_maps_durability_hints_to_tiers() {
        let cases = [
            (StorageDurabilityHint::Ephemeral, StorageTier::L1),
            (StorageDurabilityHint::Cached, StorageTier::L2),
            (StorageDurabilityHint::Persistent, StorageTier::L3),
            (StorageDurabilityHint::Archival, StorageTier::L4),
        ];
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();

        for (durability, expected_tier) in cases {
            let hint = StoragePlacementHint::default().with_durability(durability);
            let decision = StorageTieringPolicy::compatibility()
                .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

            assert_eq!(decision.initial_tier, expected_tier);
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::DurabilityHint));
        }
    }

    #[test]
    fn compatibility_policy_uses_prior_access_when_hints_are_unspecified() {
        let hint = StoragePlacementHint::default();
        let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
        let tags = BTreeMap::new();
        let context = StorageTieringContext {
            namespace: "market_data",
            collection: "trades",
            key_len: 8,
            value_len: 128,
            timestamp_age_seconds: 0,
            metadata_tags: &tags,
            placement_hint: &hint,
            available_tiers: &tiers,
            prior_access: Some(AccessPatternSnapshot {
                access_count: 42,
                access_frequency: 12.0,
                data_size: 128,
                heat_score: 12.0,
                recommended_tier: StorageTier::L1,
            }),
        };

        let decision = StorageTieringPolicy::compatibility().decide_initial_placement(&context);

        assert_eq!(decision.initial_tier, StorageTier::L1);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::ExistingAccessPattern));
    }

    #[test]
    fn compatibility_policy_defaults_to_nearest_available_l2() {
        let hint = StoragePlacementHint::default();
        let tiers = vec![StorageTier::L1, StorageTier::L3];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.initial_tier, StorageTier::L1);
        assert_eq!(decision.ttl, None);
        assert_eq!(decision.retention_class, StorageRetentionClass::RealtimeHot);
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::DefaultProfile));
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::NearestAvailableTierFallback));
    }

    #[test]
    fn compatibility_policy_preserves_hint_ttl_in_decision() {
        let hint = StoragePlacementHint::default().with_ttl(Duration::seconds(30));
        let tiers = vec![StorageTier::L1, StorageTier::L2];
        let tags = BTreeMap::new();

        let decision = StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context_with_hint(&hint, &tiers, &tags));

        assert_eq!(decision.ttl, Some(Duration::seconds(30)));
    }
}
```

- [ ] **Step 4: Export the policy module**

Modify `crates/fdc-storage/src/lib.rs`:

Add this module declaration after `pub mod metrics;`:

```rust
pub mod policy; // storage-owned tiering policy decisions
```

Add this public export after `pub use metrics::StorageMetrics;`:

```rust
pub use policy::{
    AccessPatternSnapshot, StorageRetentionClass, StorageTieringContext, StorageTieringDecision,
    StorageTieringPolicy, StorageTieringPolicyProfile, StorageTieringReason,
};
```

- [ ] **Step 5: Run policy unit tests**

Run:

```bash
rtk cargo test -p fdc-storage policy::tests
```

Expected: PASS. If compile fails because of import ordering or line width, run `rtk cargo fmt --package fdc-storage` and rerun the test.

- [ ] **Step 6: Commit Task 1**

```bash
git add crates/fdc-storage/src/policy.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add tiering policy decisions"
```

---

## Task 2: Add public contract tests for policy API

**Files:**
- Create: `crates/fdc-storage/tests/tiering_policy_contract.rs`

- [ ] **Step 1: Write contract tests against public exports**

Create `crates/fdc-storage/tests/tiering_policy_contract.rs`:

```rust
use std::collections::BTreeMap;

use fdc_storage::{
    StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageRetentionClass,
    StorageTier, StorageTieringContext, StorageTieringPolicy, StorageTieringReason,
};

fn context<'a>(
    hint: &'a StoragePlacementHint,
    tags: &'a BTreeMap<String, String>,
    available_tiers: &'a [StorageTier],
) -> StorageTieringContext<'a> {
    StorageTieringContext {
        namespace: "generic_namespace",
        collection: "generic_collection",
        key_len: 16,
        value_len: 256,
        timestamp_age_seconds: 0,
        metadata_tags: tags,
        placement_hint: hint,
        available_tiers,
        prior_access: None,
    }
}

#[test]
fn public_policy_api_explains_explicit_operator_override() {
    let tags = BTreeMap::new();
    let hint = StoragePlacementHint::for_tier(StorageTier::L4);
    let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];

    let decision = StorageTieringPolicy::compatibility().decide_initial_placement(&context(
        &hint,
        &tags,
        &tiers,
    ));

    assert_eq!(decision.initial_tier, StorageTier::L4);
    assert_eq!(decision.retention_class, StorageRetentionClass::ArchiveCold);
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::ExplicitTargetTierHint));
}

#[test]
fn public_policy_api_accepts_generic_tags_without_business_dependencies() {
    let mut tags = BTreeMap::new();
    tags.insert("kind".to_string(), "trade".to_string());
    tags.insert("mode".to_string(), "live".to_string());
    let hint = StoragePlacementHint::default().with_access_pattern(StorageAccessPatternHint::Hot);
    let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];

    let decision = StorageTieringPolicy::compatibility().decide_initial_placement(&context(
        &hint,
        &tags,
        &tiers,
    ));

    assert_eq!(decision.initial_tier, StorageTier::L2);
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::AccessPatternHint));
}

#[test]
fn public_policy_api_maps_durability_without_explicit_tier_choice() {
    let tags = BTreeMap::new();
    let hint = StoragePlacementHint::default().with_durability(StorageDurabilityHint::Persistent);
    let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];

    let decision = StorageTieringPolicy::compatibility().decide_initial_placement(&context(
        &hint,
        &tags,
        &tiers,
    ));

    assert_eq!(decision.initial_tier, StorageTier::L3);
    assert_eq!(decision.retention_class, StorageRetentionClass::AnalyticalWarm);
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::DurabilityHint));
}
```

- [ ] **Step 2: Run the new contract test**

Run:

```bash
rtk cargo test -p fdc-storage --test tiering_policy_contract
```

Expected: PASS.

- [ ] **Step 3: Run dependency guard**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: PASS, proving the new policy module did not introduce business-module dependencies.

- [ ] **Step 4: Commit Task 2**

```bash
git add crates/fdc-storage/tests/tiering_policy_contract.rs
git commit -m "test(storage): lock tiering policy public contract"
```

---

## Task 3: Integrate compatibility policy into TierManager routing

**Files:**
- Modify: `crates/fdc-storage/src/tier.rs`
- Test: `crates/fdc-storage/src/tier.rs`

- [ ] **Step 1: Add a failing integration test in `tier.rs`**

Append this test inside the existing `#[cfg(test)] mod tests` in `crates/fdc-storage/src/tier.rs`:

```rust
#[tokio::test]
async fn tier_manager_routes_placement_through_compatibility_policy() {
    let mut manager = TierManager::new();
    manager.add_tier(TierConfig::new(StorageTier::L1));
    manager.add_tier(TierConfig::new(StorageTier::L3));
    manager.initialize().await.expect("tiers should initialize");

    manager
        .put_with_placement(
            b"policy-routed",
            b"value",
            &StoragePlacementHint::default().with_durability(StorageDurabilityHint::Persistent),
        )
        .await
        .expect("policy-routed write should succeed");

    assert_eq!(
        manager
            .get_from_tier(b"policy-routed", &StorageTier::L3)
            .await
            .expect("L3 read should succeed"),
        Some(b"value".to_vec())
    );
    assert_eq!(
        manager
            .get_from_tier(b"policy-routed", &StorageTier::L1)
            .await
            .expect("L1 read should succeed"),
        None
    );
}
```

- [ ] **Step 2: Run the targeted test before refactor**

Run:

```bash
rtk cargo test -p fdc-storage tier_manager_routes_placement_through_compatibility_policy
```

Expected: PASS before refactor because current hand-written routing already maps persistent durability to L3. This is a characterization test, not a red test.

- [ ] **Step 3: Add `AccessPattern::snapshot()`**

In `crates/fdc-storage/src/tier.rs`, add `AccessPatternSnapshot` to the imports at the top:

```rust
use crate::{
    AccessPatternSnapshot, StorageAccessPatternHint, StorageCompactionOutcome,
    StorageCompactionOutcomeKind, StorageDurabilityHint, StorageEngineFeature,
    StoragePlacementHint, StorageTierScope, StorageTieringContext, StorageTieringPolicy,
};
```

Inside `impl AccessPattern`, after `recommended_tier()`, add:

```rust
    pub fn snapshot(&self) -> AccessPatternSnapshot {
        AccessPatternSnapshot {
            access_count: self.access_count,
            access_frequency: self.access_frequency,
            data_size: self.data_size,
            heat_score: self.heat_score,
            recommended_tier: self.recommended_tier(),
        }
    }
```

- [ ] **Step 4: Replace `determine_tier_for_placement()` internals with policy delegation**

Replace the body of `determine_tier_for_placement()` in `crates/fdc-storage/src/tier.rs` with:

```rust
    async fn determine_tier_for_placement(
        &self,
        key: &[u8],
        data_size: usize,
        placement: &StoragePlacementHint,
    ) -> StorageTier {
        let available_tiers = self.initialized_tiers();
        let prior_access = self
            .access_patterns
            .read()
            .await
            .get(key)
            .map(AccessPattern::snapshot);
        let metadata_tags = std::collections::BTreeMap::new();
        let context = StorageTieringContext {
            namespace: "",
            collection: "",
            key_len: key.len(),
            value_len: data_size,
            timestamp_age_seconds: 0,
            metadata_tags: &metadata_tags,
            placement_hint: placement,
            available_tiers: &available_tiers,
            prior_access,
        };

        StorageTieringPolicy::compatibility()
            .decide_initial_placement(&context)
            .initial_tier
    }
```

Keep `determine_initial_tier()` and `nearest_available_tier()` for now, because existing tests or future code may still exercise them. Remove only imports that become unused after running format/check.

- [ ] **Step 5: Run targeted tier tests**

Run:

```bash
rtk cargo test -p fdc-storage tier_manager_routes_placement_through_compatibility_policy
rtk cargo test -p fdc-storage tier::tests
```

Expected: both PASS.

- [ ] **Step 6: Run queryable market-data regression tests**

Run:

```bash
rtk cargo test -p fdc-storage --test queryable_market_data_store_contract
```

Expected: PASS, proving S12 market-data facade behavior is preserved.

- [ ] **Step 7: Commit Task 3**

```bash
git add crates/fdc-storage/src/tier.rs
git commit -m "feat(storage): route placements through tiering policy"
```

---

## Task 4: Document P13 API and validation evidence

**Files:**
- Modify: `crates/fdc-storage/docs/public-api-stability.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`

- [ ] **Step 1: Update public API stability docs**

Append this section to `crates/fdc-storage/docs/public-api-stability.md`:

```markdown
## P13 Tiering Policy API Additions

The P13 intelligent tiering policy API is additive. It introduces public policy decision types without removing or changing existing write/query APIs:

- `StorageTieringPolicy`
- `StorageTieringPolicyProfile`
- `StorageTieringContext`
- `StorageTieringDecision`
- `StorageRetentionClass`
- `StorageTieringReason`
- `AccessPatternSnapshot`

Compatibility policy behavior intentionally mirrors the pre-P13 placement routing rules. Business modules should treat `StoragePlacementHint` as advisory and should not depend on hand-selecting physical tiers for ordinary writes. Explicit `target_tier` remains supported for tests, migrations, and operator override scenarios.
```

- [ ] **Step 2: Update acceptance report**

In `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`, add this row to the implemented capability matrix after `Tier-aware store`:

```markdown
| Tiering policy engine | Done | Compatibility policy explains initial tier decisions and preserves existing placement behavior |
```

Add this section after `## S12 Market-data Integration Notes`:

```markdown
## P13 Tiering Policy Notes

- `fdc-storage` now has a storage-owned tiering policy API for deterministic, explainable initial tier decisions.
- The first profile is `Compatibility`, which preserves existing placement hint routing semantics.
- Business modules can continue to provide `StoragePlacementHint`, but the storage module owns the final tier decision.
- Market-data-specific adaptive routing remains future work and must use generic metadata/tags rather than depending on business DTO crates.
```

- [ ] **Step 3: Run docs diff check**

Run:

```bash
git diff --check
```

Expected: no output and exit 0.

- [ ] **Step 4: Commit Task 4**

```bash
git add crates/fdc-storage/docs/public-api-stability.md crates/fdc-storage/docs/storage-boundary-acceptance-report.md
git commit -m "docs(storage): record tiering policy api"
```

---

## Task 5: Full verification and handoff

**Files:**
- No source edits unless verification exposes an issue.

- [ ] **Step 1: Format check**

Run:

```bash
rtk cargo fmt --package fdc-storage --check
```

Expected: PASS. If it fails, run `rtk cargo fmt --package fdc-storage`, inspect the diff, and commit formatting with the affected task commit if not already committed.

- [ ] **Step 2: Run full fdc-storage tests**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: PASS.

- [ ] **Step 3: Confirm dependency isolation**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: PASS.

- [ ] **Step 4: Inspect final git state**

Run:

```bash
git status --short --branch
git log --oneline -n 6
```

Expected: working tree clean, branch ahead count increased by the task commits.

- [ ] **Step 5: Handoff summary**

Report:

- Policy types added.
- Compatibility profile behavior verified.
- TierManager placement routing delegated to policy.
- S12 market-data queryable store tests still pass.
- Full `fdc-storage` test suite result.
- Next recommended implementation slice: P13c market-data tag-aware profile, unless the user wants runtime backend/profile config first.
