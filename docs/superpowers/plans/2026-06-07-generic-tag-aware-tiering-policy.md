# Generic Tag-Aware Tiering Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a generic tag-aware storage tiering profile while preserving `fdc-storage` as a business-agnostic storage layer.

**Architecture:** Extend `StorageTieringPolicyProfile` with `GenericRealtime` and implement deterministic rules in `crates/fdc-storage/src/policy.rs` using only `StorageTieringContext` generic fields and metadata tags. Extend `fdc-server` runtime profile parsing to accept `generic_realtime` without introducing any storage dependency on server or market-data crates.

**Tech Stack:** Rust, serde, chrono, existing `fdc-storage` policy/tier APIs, `fdc-server` runtime config tests, TDD with `rtk cargo test`.

---

## Reference design

- `docs/superpowers/specs/2026-06-07-generic-tag-aware-tiering-policy-design.md`
- `crates/fdc-storage/src/policy.rs`
- `crates/fdc-storage/tests/tiering_policy_contract.rs`
- `crates/fdc-server/src/runtime/config.rs`
- `crates/fdc-server/tests/runtime_config_contract.rs`

## File structure

- Modify `crates/fdc-storage/src/policy.rs`
  - Add `StorageTieringPolicyProfile::GenericRealtime`.
  - Add `StorageTieringPolicy::generic_realtime()`.
  - Add generic reasons: `LiveRecentWrite`, `BackfillOrReplay`, `AggregateRecord`, `LargePayload`, `OldTimestamp`.
  - Implement tag-aware decisions using only `StorageTieringContext`.
- Modify `crates/fdc-storage/tests/tiering_policy_contract.rs`
  - Add public contract tests for the new profile and boundary-safe generic behavior.
- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add `MarketDataStoragePolicyProfileConfig::GenericRealtime`.
  - Parse `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime`.
- Modify `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add config parsing coverage for `generic_realtime`.
- Modify docs after verification:
  - `crates/fdc-storage/docs/public-api-stability.md`
  - `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
  - `docs/DEVELOPMENT_STATUS.md`

---

## Task 1: Add failing generic tag-aware storage policy tests

**Files:**
- Modify: `crates/fdc-storage/src/policy.rs`
- Modify: `crates/fdc-storage/tests/tiering_policy_contract.rs`

- [ ] **Step 1: Add failing unit tests in `policy.rs`**

Append tests inside the existing `#[cfg(test)] mod tests` in `crates/fdc-storage/src/policy.rs`:

```rust
    fn context_with_tags<'a>(
        metadata_tags: &'a BTreeMap<String, String>,
        timestamp_age_seconds: i64,
        value_len: usize,
    ) -> StorageTieringContext<'a> {
        static HINT: StoragePlacementHint = StoragePlacementHint {
            target_tier: None,
            access_pattern: StorageAccessPatternHint::Unspecified,
            durability: StorageDurabilityHint::Unspecified,
            shard_key: None,
            ttl: None,
        };
        static TIERS: [StorageTier; 4] = [
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ];
        StorageTieringContext {
            namespace: "generic",
            collection: "records",
            key_len: 8,
            value_len,
            timestamp_age_seconds,
            metadata_tags,
            placement_hint: &HINT,
            available_tiers: &TIERS,
            prior_access: None,
        }
    }

    #[test]
    fn generic_realtime_profile_routes_live_recent_records_to_hot_storage() {
        let mut tags = BTreeMap::new();
        tags.insert("mode".to_string(), "live".to_string());

        let decision = StorageTieringPolicy::generic_realtime()
            .decide_initial_placement(&context_with_tags(&tags, 30, 256));

        assert_eq!(decision.initial_tier, StorageTier::L2);
        assert_eq!(decision.retention_class, StorageRetentionClass::RealtimeHot);
        assert!(decision.reasons.contains(&StorageTieringReason::LiveRecentWrite));
    }

    #[test]
    fn generic_realtime_profile_routes_backfill_and_replay_to_analytical_storage() {
        for tags in [
            BTreeMap::from([("mode".to_string(), "backfill".to_string())]),
            BTreeMap::from([("quality.is_replay".to_string(), "true".to_string())]),
        ] {
            let decision = StorageTieringPolicy::generic_realtime()
                .decide_initial_placement(&context_with_tags(&tags, 60, 256));

            assert_eq!(decision.initial_tier, StorageTier::L3);
            assert_eq!(decision.retention_class, StorageRetentionClass::AnalyticalWarm);
            assert!(decision.reasons.contains(&StorageTieringReason::BackfillOrReplay));
        }
    }

    #[test]
    fn generic_realtime_profile_routes_aggregate_tags_to_analytical_storage() {
        for tags in [
            BTreeMap::from([("data.kind".to_string(), "aggregate".to_string())]),
            BTreeMap::from([("record.kind".to_string(), "candle".to_string())]),
        ] {
            let decision = StorageTieringPolicy::generic_realtime()
                .decide_initial_placement(&context_with_tags(&tags, 60, 256));

            assert_eq!(decision.initial_tier, StorageTier::L3);
            assert!(decision.reasons.contains(&StorageTieringReason::AggregateRecord));
        }
    }

    #[test]
    fn generic_realtime_profile_avoids_hot_tiers_for_large_or_old_records() {
        let tags = BTreeMap::from([("mode".to_string(), "live".to_string())]);

        let large = StorageTieringPolicy::generic_realtime()
            .decide_initial_placement(&context_with_tags(&tags, 30, 2 * 1024 * 1024));
        assert_eq!(large.initial_tier, StorageTier::L3);
        assert!(large.reasons.contains(&StorageTieringReason::LargePayload));

        let old = StorageTieringPolicy::generic_realtime()
            .decide_initial_placement(&context_with_tags(&tags, 91 * 24 * 60 * 60, 256));
        assert_eq!(old.initial_tier, StorageTier::L4);
        assert!(old.reasons.contains(&StorageTieringReason::OldTimestamp));
    }

    #[test]
    fn generic_realtime_profile_falls_back_to_compatibility_for_unknown_tags() {
        let tags = BTreeMap::from([("domain".to_string(), "caller-defined".to_string())]);

        let decision = StorageTieringPolicy::generic_realtime()
            .decide_initial_placement(&context_with_tags(&tags, 60, 256));

        assert_eq!(decision.initial_tier, StorageTier::L2);
        assert!(decision.reasons.contains(&StorageTieringReason::DefaultProfile));
    }
```

- [ ] **Step 2: Add public contract test**

Append to `crates/fdc-storage/tests/tiering_policy_contract.rs`:

```rust
#[test]
fn generic_realtime_policy_profile_is_public_and_generic() {
    let tags = std::collections::BTreeMap::from([("mode".to_string(), "live".to_string())]);
    let hint = StoragePlacementHint::default();
    let tiers = vec![StorageTier::L1, StorageTier::L2, StorageTier::L3, StorageTier::L4];
    let context = StorageTieringContext {
        namespace: "caller_defined_namespace",
        collection: "caller_defined_collection",
        key_len: 4,
        value_len: 128,
        timestamp_age_seconds: 1,
        metadata_tags: &tags,
        placement_hint: &hint,
        available_tiers: &tiers,
        prior_access: None,
    };

    let policy = StorageTieringPolicy::generic_realtime();
    assert_eq!(policy.profile(), StorageTieringPolicyProfile::GenericRealtime);

    let decision = policy.decide_initial_placement(&context);
    assert_eq!(decision.initial_tier, StorageTier::L2);
    assert!(decision.reasons.contains(&StorageTieringReason::LiveRecentWrite));
}
```

- [ ] **Step 3: Run failing tests**

```bash
rtk cargo test -p fdc-storage policy::tests::generic_realtime_profile
rtk cargo test -p fdc-storage --test tiering_policy_contract generic_realtime_policy_profile_is_public_and_generic
```

Expected: FAIL because the profile, constructor, and reasons do not exist.

---

## Task 2: Implement `GenericRealtime` policy

**Files:**
- Modify: `crates/fdc-storage/src/policy.rs`

- [ ] **Step 1: Add enum variants and constructor**

Add `GenericRealtime` to `StorageTieringPolicyProfile` and these reasons to `StorageTieringReason`:

```rust
LiveRecentWrite,
BackfillOrReplay,
AggregateRecord,
LargePayload,
OldTimestamp,
```

Add constructor:

```rust
pub fn generic_realtime() -> Self {
    Self {
        profile: StorageTieringPolicyProfile::GenericRealtime,
    }
}
```

Update `decide_initial_placement` match:

```rust
StorageTieringPolicyProfile::GenericRealtime => self.decide_generic_realtime(context),
```

- [ ] **Step 2: Implement generic helper functions and decision logic**

Add these constants/functions near existing helpers:

```rust
const LARGE_PAYLOAD_BYTES: usize = 1024 * 1024;
const OLD_RECORD_SECONDS: i64 = 7 * 24 * 60 * 60;
const ARCHIVE_RECORD_SECONDS: i64 = 90 * 24 * 60 * 60;
const LIVE_RECENT_SECONDS: i64 = 60 * 60;

fn tag_value<'a>(context: &'a StorageTieringContext<'_>, key: &str) -> Option<&'a str> {
    context.metadata_tags.get(key).map(String::as_str)
}

fn has_tag_value(context: &StorageTieringContext<'_>, key: &str, value: &str) -> bool {
    tag_value(context, key).is_some_and(|actual| actual == value)
}

fn is_backfill_or_replay(context: &StorageTieringContext<'_>) -> bool {
    has_tag_value(context, "mode", "backfill")
        || has_tag_value(context, "quality.is_replay", "true")
}

fn is_aggregate_record(context: &StorageTieringContext<'_>) -> bool {
    matches!(tag_value(context, "data.kind"), Some("aggregate" | "candle"))
        || matches!(tag_value(context, "record.kind"), Some("aggregate" | "candle"))
}
```

Add method inside `impl StorageTieringPolicy`:

```rust
fn decide_generic_realtime(&self, context: &StorageTieringContext<'_>) -> StorageTieringDecision {
    if context.placement_hint.target_tier.is_some() {
        return self.decide_compatibility(context);
    }

    let mut reasons = Vec::new();
    let (desired_tier, retention_class) = if context.timestamp_age_seconds >= ARCHIVE_RECORD_SECONDS {
        reasons.push(StorageTieringReason::OldTimestamp);
        (StorageTier::L4, StorageRetentionClass::ArchiveCold)
    } else if context.timestamp_age_seconds >= OLD_RECORD_SECONDS {
        reasons.push(StorageTieringReason::OldTimestamp);
        (StorageTier::L3, StorageRetentionClass::AnalyticalWarm)
    } else if context.value_len >= LARGE_PAYLOAD_BYTES {
        reasons.push(StorageTieringReason::LargePayload);
        (StorageTier::L3, StorageRetentionClass::AnalyticalWarm)
    } else if is_backfill_or_replay(context) {
        reasons.push(StorageTieringReason::BackfillOrReplay);
        (StorageTier::L3, StorageRetentionClass::AnalyticalWarm)
    } else if is_aggregate_record(context) {
        reasons.push(StorageTieringReason::AggregateRecord);
        (StorageTier::L3, StorageRetentionClass::AnalyticalWarm)
    } else if has_tag_value(context, "mode", "live")
        && context.timestamp_age_seconds <= LIVE_RECENT_SECONDS
    {
        reasons.push(StorageTieringReason::LiveRecentWrite);
        let desired = if matches!(context.placement_hint.durability, StorageDurabilityHint::Ephemeral) {
            StorageTier::L1
        } else {
            StorageTier::L2
        };
        (desired, StorageRetentionClass::RealtimeHot)
    } else {
        return self.decide_compatibility(context);
    };

    let initial_tier = nearest_available_tier(&desired_tier, context.available_tiers)
        .unwrap_or_else(|| desired_tier.clone());
    if initial_tier != desired_tier {
        reasons.push(StorageTieringReason::NearestAvailableTierFallback);
    }

    StorageTieringDecision {
        initial_tier,
        ttl: context.placement_hint.ttl,
        retention_class,
        reasons,
    }
}
```

- [ ] **Step 3: Run storage tests and commit**

```bash
rtk cargo fmt --package fdc-storage
rtk cargo test -p fdc-storage policy::tests::generic_realtime_profile
rtk cargo test -p fdc-storage --test tiering_policy_contract generic_realtime_policy_profile_is_public_and_generic
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: PASS.

Commit:

```bash
git add crates/fdc-storage/src/policy.rs crates/fdc-storage/tests/tiering_policy_contract.rs
git commit -m "feat(storage): add generic tag-aware tiering policy"
```

---

## Task 3: Accept `generic_realtime` in server runtime config

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`

- [ ] **Step 1: Add failing server config test**

Append to `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
#[test]
fn runtime_config_accepts_generic_realtime_storage_policy_profile() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE",
        "generic_realtime",
    )])
    .expect("generic realtime profile should parse");

    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::GenericRealtime
    );
}
```

Update the existing invalid profile test to use an unsupported value such as `typed_market_data`.

- [ ] **Step 2: Run failing server config test**

```bash
rtk cargo test -p fdc-server --test runtime_config_contract runtime_config_accepts_generic_realtime_storage_policy_profile
```

Expected: FAIL because `GenericRealtime` is missing.

- [ ] **Step 3: Implement config parsing**

Add enum variant:

```rust
GenericRealtime,
```

Update parsing arm:

```rust
"generic_realtime" => MarketDataStoragePolicyProfileConfig::GenericRealtime,
```

Update error message to:

```rust
"FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility or generic_realtime, got {other}"
```

- [ ] **Step 4: Run tests and commit**

```bash
rtk cargo fmt --package fdc-server
rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: PASS.

Commit:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): accept generic realtime storage policy profile"
```

---

## Task 4: Docs and final verification

**Files:**
- Modify: `crates/fdc-storage/docs/public-api-stability.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update docs**

Document that `GenericRealtime` is a generic tag-aware profile, not a market-data typed profile, and that callers provide tags.

- [ ] **Step 2: Run final verification**

```bash
rtk cargo fmt --package fdc-storage --check
rtk cargo fmt --package fdc-server --check
git diff --check
rtk cargo test -p fdc-storage policy::tests::generic_realtime_profile
rtk cargo test -p fdc-storage --test tiering_policy_contract
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo test -p fdc-server --test runtime_config_contract
rtk git status --short
```

Expected: PASS. If storage/server tests generate `data/` directories, remove untracked test artifacts and rerun `rtk git status --short`.

- [ ] **Step 3: Commit docs**

```bash
git add crates/fdc-storage/docs/public-api-stability.md crates/fdc-storage/docs/storage-boundary-acceptance-report.md docs/DEVELOPMENT_STATUS.md
git commit -m "docs(storage): record generic tag-aware tiering profile"
```

---

## Self-review

- Spec coverage: profile, generic tag rules, server parsing, docs, and dependency guard are covered.
- Placeholder scan: no TBD/TODO/fill-in instructions remain.
- Type consistency: `GenericRealtime` and `generic_realtime` names are consistent.
- Boundary check: no task adds business crate dependencies to `fdc-storage`.
