//! Storage health snapshot and maintenance report types.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{engine::StorageStats, StorageTier, TierLifecycleReport};

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

        assert_eq!(
            snapshot.tiers[&StorageTier::L1].status,
            StorageTierHealthStatus::Healthy
        );
        assert_eq!(snapshot.access_patterns, 1);
        assert_eq!(snapshot.migration_queue_len, 2);
    }
}
