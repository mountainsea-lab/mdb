//! Storage-owned tiering policy decisions.
//!
//! The policy layer converts generic storage facts and advisory hints into an
//! explainable initial tier decision. Business modules may provide hints, but
//! storage owns the final decision.

use std::collections::BTreeMap;

use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::{StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageTier};

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

fn nearest_available_tier(
    desired: &StorageTier,
    available_tiers: &[StorageTier],
) -> Option<StorageTier> {
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
        let tiers = vec![
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ];
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
        let tiers = vec![
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ];
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
        let tiers = vec![
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ];
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
        let tiers = vec![
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ];
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
