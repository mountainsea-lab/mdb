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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageCompactionOutcomeKind {
    Compacted,
    Unsupported,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCompactionOutcome {
    pub tier: StorageTier,
    pub kind: StorageCompactionOutcomeKind,
    pub message: Option<String>,
}

impl StorageCompactionOutcome {
    pub fn compacted(tier: StorageTier) -> Self {
        Self {
            tier,
            kind: StorageCompactionOutcomeKind::Compacted,
            message: None,
        }
    }

    pub fn unsupported(tier: StorageTier, message: impl Into<String>) -> Self {
        Self {
            tier,
            kind: StorageCompactionOutcomeKind::Unsupported,
            message: Some(message.into()),
        }
    }

    pub fn failed(tier: StorageTier, message: impl Into<String>) -> Self {
        Self {
            tier,
            kind: StorageCompactionOutcomeKind::Failed,
            message: Some(message.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMaintenanceReport {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub lifecycle: TierLifecycleReport,
    pub health: StorageHealthSnapshot,
    pub compacted_tiers: Vec<StorageTier>,
    pub compaction_errors: BTreeMap<StorageTier, String>,
    pub compaction_outcomes: Vec<StorageCompactionOutcome>,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
}

impl StorageMaintenanceReport {
    pub fn duration_seconds(&self) -> f64 {
        self.finished_at
            .signed_duration_since(self.started_at)
            .num_milliseconds() as f64
            / 1000.0
    }

    pub fn healthy_tier_count(&self) -> usize {
        self.health
            .tiers
            .values()
            .filter(|tier| matches!(tier.status, StorageTierHealthStatus::Healthy))
            .count()
    }

    pub fn degraded_tier_count(&self) -> usize {
        self.health
            .tiers
            .len()
            .saturating_sub(self.healthy_tier_count())
    }

    pub fn metrics_snapshot(&self) -> StorageMaintenanceMetricsSnapshot {
        StorageMaintenanceMetricsSnapshot::from_report(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageMaintenanceMetricsSnapshot {
    pub duration_seconds: f64,
    pub lifecycle_scanned_entries: usize,
    pub lifecycle_ttl_deleted: usize,
    pub lifecycle_retention_demoted: usize,
    pub lifecycle_retention_deleted: usize,
    pub lifecycle_retained: usize,
    pub lifecycle_decode_errors: usize,
    pub health_tiers_total: usize,
    pub health_tiers_healthy: usize,
    pub health_tiers_degraded: usize,
    pub compaction_compacted: usize,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
}

impl StorageMaintenanceMetricsSnapshot {
    pub fn from_report(report: &StorageMaintenanceReport) -> Self {
        Self {
            duration_seconds: report.duration_seconds(),
            lifecycle_scanned_entries: report.lifecycle.scanned_entries,
            lifecycle_ttl_deleted: report.lifecycle.ttl_deleted,
            lifecycle_retention_demoted: report.lifecycle.retention_demoted,
            lifecycle_retention_deleted: report.lifecycle.retention_deleted,
            lifecycle_retained: report.lifecycle.retained,
            lifecycle_decode_errors: report.lifecycle.decode_errors,
            health_tiers_total: report.health.tiers.len(),
            health_tiers_healthy: report.healthy_tier_count(),
            health_tiers_degraded: report.degraded_tier_count(),
            compaction_compacted: report.compacted_tiers.len(),
            compaction_unsupported: report.compaction_unsupported,
            compaction_failed: report.compaction_failed,
        }
    }

    pub fn to_prometheus_text(&self) -> String {
        format!(
            "# HELP fdc_storage_maintenance_duration_seconds Last maintenance duration in seconds\n\
             # TYPE fdc_storage_maintenance_duration_seconds gauge\n\
             fdc_storage_maintenance_duration_seconds {}\n\
             # HELP fdc_storage_maintenance_lifecycle_entries_total Last maintenance lifecycle counts\n\
             # TYPE fdc_storage_maintenance_lifecycle_entries_total gauge\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"scanned\"}} {}\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"ttl_deleted\"}} {}\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"retention_demoted\"}} {}\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"retention_deleted\"}} {}\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"retained\"}} {}\n\
             fdc_storage_maintenance_lifecycle_entries_total{{action=\"decode_errors\"}} {}\n\
             # HELP fdc_storage_maintenance_health_tiers Last maintenance tier health counts\n\
             # TYPE fdc_storage_maintenance_health_tiers gauge\n\
             fdc_storage_maintenance_health_tiers{{status=\"total\"}} {}\n\
             fdc_storage_maintenance_health_tiers{{status=\"healthy\"}} {}\n\
             fdc_storage_maintenance_health_tiers{{status=\"degraded\"}} {}\n\
             # HELP fdc_storage_maintenance_compaction_tiers Last maintenance compaction outcome counts\n\
             # TYPE fdc_storage_maintenance_compaction_tiers gauge\n\
             fdc_storage_maintenance_compaction_tiers{{outcome=\"compacted\"}} {}\n\
             fdc_storage_maintenance_compaction_tiers{{outcome=\"unsupported\"}} {}\n\
             fdc_storage_maintenance_compaction_tiers{{outcome=\"failed\"}} {}\n",
            self.duration_seconds,
            self.lifecycle_scanned_entries,
            self.lifecycle_ttl_deleted,
            self.lifecycle_retention_demoted,
            self.lifecycle_retention_deleted,
            self.lifecycle_retained,
            self.lifecycle_decode_errors,
            self.health_tiers_total,
            self.health_tiers_healthy,
            self.health_tiers_degraded,
            self.compaction_compacted,
            self.compaction_unsupported,
            self.compaction_failed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health_snapshot() -> StorageHealthSnapshot {
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
        StorageHealthSnapshot {
            captured_at: Utc::now(),
            tiers,
            access_patterns: 1,
            migration_queue_len: 2,
        }
    }

    #[test]
    fn storage_health_snapshot_can_record_tier_health() {
        let snapshot = health_snapshot();
        assert_eq!(
            snapshot.tiers[&StorageTier::L1].status,
            StorageTierHealthStatus::Healthy
        );
        assert_eq!(snapshot.access_patterns, 1);
        assert_eq!(snapshot.migration_queue_len, 2);
    }

    #[test]
    fn maintenance_metrics_snapshot_renders_prometheus_text() {
        let started_at = Utc::now();
        let report = StorageMaintenanceReport {
            started_at,
            finished_at: started_at + chrono::Duration::milliseconds(250),
            lifecycle: TierLifecycleReport {
                scanned_entries: 3,
                ttl_deleted: 1,
                retention_demoted: 1,
                retention_deleted: 0,
                retained: 1,
                decode_errors: 0,
                tier_reports: BTreeMap::new(),
            },
            health: health_snapshot(),
            compacted_tiers: vec![StorageTier::L1],
            compaction_errors: BTreeMap::new(),
            compaction_outcomes: vec![StorageCompactionOutcome::compacted(StorageTier::L1)],
            compaction_unsupported: 0,
            compaction_failed: 0,
        };

        let metrics = StorageMaintenanceMetricsSnapshot::from_report(&report);
        assert_eq!(metrics.lifecycle_scanned_entries, 3);
        assert_eq!(metrics.health_tiers_healthy, 1);
        assert_eq!(metrics.compaction_compacted, 1);
        let prometheus = metrics.to_prometheus_text();
        assert!(prometheus.contains("fdc_storage_maintenance_duration_seconds 0.25"));
        assert!(prometheus.contains("outcome=\"compacted\""));
    }
}
