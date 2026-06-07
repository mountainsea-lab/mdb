use std::collections::VecDeque;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::Result;
use fdc_storage::{StorageMaintenanceAuditEntry, StorageMaintenanceAuditSink};
use tokio::sync::Mutex;

pub const MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY: usize = 32;

#[derive(Debug)]
pub struct MarketDataStorageMaintenanceAuditLog {
    capacity: usize,
    state: Mutex<MarketDataStorageMaintenanceAuditState>,
}

#[derive(Debug, Default)]
struct MarketDataStorageMaintenanceAuditState {
    entries: VecDeque<StorageMaintenanceAuditEntry>,
    total_recorded_entries: u64,
    reset_count: u64,
    total_cleared_entries: u64,
    last_recorded_at: Option<DateTime<Utc>>,
    last_reset_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketDataStorageMaintenanceAuditSnapshot {
    pub capacity: usize,
    pub total_entries: usize,
    pub total_recorded_entries: u64,
    pub reset_count: u64,
    pub total_cleared_entries: u64,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub last_reset_at: Option<DateTime<Utc>>,
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
            state: Mutex::new(MarketDataStorageMaintenanceAuditState::default()),
        }
    }

    pub async fn recent(&self, limit: usize) -> MarketDataStorageMaintenanceAuditSnapshot {
        let state = self.state.lock().await;
        let limit = limit.min(self.capacity);
        let recent = state.entries.iter().rev().take(limit).cloned().collect();
        MarketDataStorageMaintenanceAuditSnapshot {
            capacity: self.capacity,
            total_entries: state.entries.len(),
            total_recorded_entries: state.total_recorded_entries,
            reset_count: state.reset_count,
            total_cleared_entries: state.total_cleared_entries,
            last_recorded_at: state.last_recorded_at,
            last_reset_at: state.last_reset_at,
            entries: recent,
        }
    }

    pub async fn clear(&self) -> usize {
        let mut state = self.state.lock().await;
        let cleared = state.entries.len();
        state.entries.clear();
        state.reset_count += 1;
        state.total_cleared_entries += cleared as u64;
        state.last_reset_at = Some(Utc::now());
        cleared
    }
}

#[async_trait]
impl StorageMaintenanceAuditSink for MarketDataStorageMaintenanceAuditLog {
    async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()> {
        let mut state = self.state.lock().await;
        state.last_recorded_at = Some(entry.recorded_at);
        state.total_recorded_entries += 1;
        state.entries.push_back(entry);
        while state.entries.len() > self.capacity {
            state.entries.pop_front();
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
    async fn audit_log_snapshot_includes_default_observability_metadata() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.capacity, 3);
        assert_eq!(snapshot.total_entries, 0);
        assert_eq!(snapshot.total_recorded_entries, 0);
        assert_eq!(snapshot.reset_count, 0);
        assert_eq!(snapshot.total_cleared_entries, 0);
        assert!(snapshot.last_recorded_at.is_none());
        assert!(snapshot.last_reset_at.is_none());
        assert!(snapshot.entries.is_empty());
    }

    #[tokio::test]
    async fn audit_log_recording_updates_process_lifetime_metadata() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);
        let first = audit_entry(1);
        let second = audit_entry(2);
        let expected_last_recorded_at = second.recorded_at;

        log.record_maintenance(first).await.unwrap();
        log.record_maintenance(second).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.capacity, 3);
        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.total_recorded_entries, 2);
        assert_eq!(snapshot.reset_count, 0);
        assert_eq!(snapshot.total_cleared_entries, 0);
        assert_eq!(snapshot.last_recorded_at, Some(expected_last_recorded_at));
        assert!(snapshot.last_reset_at.is_none());
    }

    #[tokio::test]
    async fn audit_log_eviction_preserves_total_recorded_entries() {
        let log = MarketDataStorageMaintenanceAuditLog::new(2);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();
        log.record_maintenance(audit_entry(3)).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.total_recorded_entries, 3);
        assert_eq!(snapshot.entries[0].scanned_entries, 3);
        assert_eq!(snapshot.entries[1].scanned_entries, 2);
    }

    #[tokio::test]
    async fn audit_log_clear_updates_reset_metadata() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();

        let cleared = log.clear().await;
        let snapshot = log.recent(10).await;

        assert_eq!(cleared, 2);
        assert_eq!(snapshot.total_entries, 0);
        assert_eq!(snapshot.total_recorded_entries, 2);
        assert_eq!(snapshot.reset_count, 1);
        assert_eq!(snapshot.total_cleared_entries, 2);
        assert!(snapshot.last_recorded_at.is_some());
        assert!(snapshot.last_reset_at.is_some());
    }

    #[tokio::test]
    async fn audit_log_clear_empty_log_updates_reset_count_without_cleared_entries() {
        let log = MarketDataStorageMaintenanceAuditLog::new(3);

        let cleared = log.clear().await;
        let snapshot = log.recent(10).await;

        assert_eq!(cleared, 0);
        assert_eq!(snapshot.total_entries, 0);
        assert_eq!(snapshot.total_recorded_entries, 0);
        assert_eq!(snapshot.reset_count, 1);
        assert_eq!(snapshot.total_cleared_entries, 0);
        assert!(snapshot.last_recorded_at.is_none());
        assert!(snapshot.last_reset_at.is_some());
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
