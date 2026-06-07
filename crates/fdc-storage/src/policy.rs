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
    GenericRealtime,
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
    LiveRecentWrite,
    BackfillOrReplay,
    AggregateRecord,
    LargePayload,
    OldTimestamp,
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

    pub fn generic_realtime() -> Self {
        Self {
            profile: StorageTieringPolicyProfile::GenericRealtime,
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
            StorageTieringPolicyProfile::GenericRealtime => self.decide_generic_realtime(context),
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

    fn decide_generic_realtime(
        &self,
        context: &StorageTieringContext<'_>,
    ) -> StorageTieringDecision {
        if context.placement_hint.target_tier.is_some() {
            return self.decide_compatibility(context);
        }

        let mut reasons = Vec::new();
        let (desired_tier, retention_class) =
            if context.timestamp_age_seconds >= ARCHIVE_RECORD_SECONDS {
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
                let desired = if matches!(
                    context.placement_hint.durability,
                    StorageDurabilityHint::Ephemeral
                ) {
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
    matches!(
        tag_value(context, "data.kind"),
        Some("aggregate" | "candle")
    ) || matches!(
        tag_value(context, "record.kind"),
        Some("aggregate" | "candle")
    )
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
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::LiveRecentWrite));
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
            assert_eq!(
                decision.retention_class,
                StorageRetentionClass::AnalyticalWarm
            );
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::BackfillOrReplay));
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
            assert!(decision
                .reasons
                .contains(&StorageTieringReason::AggregateRecord));
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
        assert!(decision
            .reasons
            .contains(&StorageTieringReason::DefaultProfile));
    }
}
