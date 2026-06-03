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
