use std::collections::BTreeMap;

use fdc_storage::{
    StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageRetentionClass,
    StorageTier, StorageTieringContext, StorageTieringPolicy, StorageTieringPolicyProfile,
    StorageTieringReason,
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
    let tiers = vec![
        StorageTier::L1,
        StorageTier::L2,
        StorageTier::L3,
        StorageTier::L4,
    ];

    let decision = StorageTieringPolicy::compatibility()
        .decide_initial_placement(&context(&hint, &tags, &tiers));

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
    let tiers = vec![
        StorageTier::L1,
        StorageTier::L2,
        StorageTier::L3,
        StorageTier::L4,
    ];

    let decision = StorageTieringPolicy::compatibility()
        .decide_initial_placement(&context(&hint, &tags, &tiers));

    assert_eq!(decision.initial_tier, StorageTier::L2);
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::AccessPatternHint));
}

#[test]
fn public_policy_api_maps_durability_without_explicit_tier_choice() {
    let tags = BTreeMap::new();
    let hint = StoragePlacementHint::default().with_durability(StorageDurabilityHint::Persistent);
    let tiers = vec![
        StorageTier::L1,
        StorageTier::L2,
        StorageTier::L3,
        StorageTier::L4,
    ];

    let decision = StorageTieringPolicy::compatibility()
        .decide_initial_placement(&context(&hint, &tags, &tiers));

    assert_eq!(decision.initial_tier, StorageTier::L3);
    assert_eq!(
        decision.retention_class,
        StorageRetentionClass::AnalyticalWarm
    );
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::DurabilityHint));
}

#[test]
fn generic_realtime_policy_profile_is_public_and_generic() {
    let tags = BTreeMap::from([("mode".to_string(), "live".to_string())]);
    let hint = StoragePlacementHint::default();
    let tiers = vec![
        StorageTier::L1,
        StorageTier::L2,
        StorageTier::L3,
        StorageTier::L4,
    ];
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
    assert_eq!(
        policy.profile(),
        StorageTieringPolicyProfile::GenericRealtime
    );

    let decision = policy.decide_initial_placement(&context);
    assert_eq!(decision.initial_tier, StorageTier::L2);
    assert!(decision
        .reasons
        .contains(&StorageTieringReason::LiveRecentWrite));
}
