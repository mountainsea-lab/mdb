use std::collections::VecDeque;

use async_trait::async_trait;
use fdc_core::Result;
use fdc_storage::{StorageMaintenanceAuditEntry, StorageMaintenanceAuditSink};
use tokio::sync::Mutex;

pub const MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY: usize = 32;

#[derive(Debug)]
pub struct MarketDataStorageMaintenanceAuditLog {
    capacity: usize,
    entries: Mutex<VecDeque<StorageMaintenanceAuditEntry>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketDataStorageMaintenanceAuditSnapshot {
    pub total_entries: usize,
    pub entries: Vec<StorageMaintenanceAuditEntry>,
}

impl Default for MarketDataStorageMaintenanceAuditLog {
    fn default() -> Self {
        Self::new(MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY)
    }
}

impl MarketDataStorageMaintenanceAuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    pub async fn recent(&self, limit: usize) -> MarketDataStorageMaintenanceAuditSnapshot {
        let entries = self.entries.lock().await;
        let limit = limit.min(self.capacity);
        let recent = entries.iter().rev().take(limit).cloned().collect();
        MarketDataStorageMaintenanceAuditSnapshot {
            total_entries: entries.len(),
            entries: recent,
        }
    }

    pub async fn clear(&self) -> usize {
        let mut entries = self.entries.lock().await;
        let cleared = entries.len();
        entries.clear();
        cleared
    }
}

#[async_trait]
impl StorageMaintenanceAuditSink for MarketDataStorageMaintenanceAuditLog {
    async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()> {
        let mut entries = self.entries.lock().await;
        entries.push_back(entry);
        while entries.len() > self.capacity {
            entries.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn audit_entry(scanned_entries: usize) -> StorageMaintenanceAuditEntry {
        let started_at = Utc.timestamp_opt(scanned_entries as i64, 0).unwrap();
        StorageMaintenanceAuditEntry {
            recorded_at: started_at,
            started_at,
            finished_at: started_at,
            duration_ms: scanned_entries as i64,
            scanned_entries,
            ttl_deleted: 0,
            retention_demoted: 0,
            retention_deleted: 0,
            retained: 0,
            decode_errors: 0,
            compacted_tiers: 0,
            compaction_unsupported: 0,
            compaction_failed: 0,
            healthy_tiers: 4,
            degraded_tiers: 0,
        }
    }

    #[tokio::test]
    async fn audit_log_returns_newest_entries_first() {
        let log = MarketDataStorageMaintenanceAuditLog::new(32);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].scanned_entries, 2);
        assert_eq!(snapshot.entries[1].scanned_entries, 1);
    }

    #[tokio::test]
    async fn audit_log_drops_oldest_entries_at_capacity() {
        let log = MarketDataStorageMaintenanceAuditLog::new(2);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();
        log.record_maintenance(audit_entry(3)).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].scanned_entries, 3);
        assert_eq!(snapshot.entries[1].scanned_entries, 2);
    }

    #[tokio::test]
    async fn audit_log_recent_zero_returns_no_entries() {
        let log = MarketDataStorageMaintenanceAuditLog::new(2);
        log.record_maintenance(audit_entry(1)).await.unwrap();

        let snapshot = log.recent(0).await;

        assert_eq!(snapshot.total_entries, 1);
        assert!(snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn audit_log_clear_removes_entries_and_returns_count() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();

        let cleared = log.clear().await;
        let snapshot = log.recent(10).await;

        assert_eq!(cleared, 2);
        assert_eq!(snapshot.total_entries, 0);
        assert!(snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn audit_log_clear_empty_log_returns_zero() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);

        let cleared = log.clear().await;
        let snapshot = log.recent(10).await;

        assert_eq!(cleared, 0);
        assert_eq!(snapshot.total_entries, 0);
        assert!(snapshot.entries.is_empty());
    }
}
