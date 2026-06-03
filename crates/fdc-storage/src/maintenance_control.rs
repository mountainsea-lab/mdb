//! Explicit maintenance control types.
//!
//! These types keep production-hardening hooks generic and module-local. Callers
//! can provide timeout and audit behavior without `fdc-storage` depending on any
//! server, scheduler, or business module.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use serde::{Deserialize, Serialize};

use crate::StorageMaintenanceReport;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageMaintenanceErrorKind {
    AlreadyRunning,
    Timeout,
    AuditFailed,
    Internal,
}

impl StorageMaintenanceErrorKind {
    pub fn storage_error(&self, message: impl Into<String>) -> Error {
        Error::storage(format!(
            "storage maintenance {:?}: {}",
            self,
            message.into()
        ))
    }
}

#[derive(Clone, Default)]
pub struct StorageMaintenanceOptions {
    pub timeout: Option<Duration>,
    pub audit_sink: Option<Arc<dyn StorageMaintenanceAuditSink>>,
}

impl StorageMaintenanceOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_audit_sink(mut self, audit_sink: Arc<dyn StorageMaintenanceAuditSink>) -> Self {
        self.audit_sink = Some(audit_sink);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageMaintenanceAuditEntry {
    pub recorded_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_ms: i64,
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub compacted_tiers: usize,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
    pub healthy_tiers: usize,
    pub degraded_tiers: usize,
}

impl StorageMaintenanceAuditEntry {
    pub fn from_report(report: &StorageMaintenanceReport) -> Self {
        Self {
            recorded_at: Utc::now(),
            started_at: report.started_at,
            finished_at: report.finished_at,
            duration_ms: report
                .finished_at
                .signed_duration_since(report.started_at)
                .num_milliseconds(),
            scanned_entries: report.lifecycle.scanned_entries,
            ttl_deleted: report.lifecycle.ttl_deleted,
            retention_demoted: report.lifecycle.retention_demoted,
            retention_deleted: report.lifecycle.retention_deleted,
            retained: report.lifecycle.retained,
            decode_errors: report.lifecycle.decode_errors,
            compacted_tiers: report.compacted_tiers.len(),
            compaction_unsupported: report.compaction_unsupported,
            compaction_failed: report.compaction_failed,
            healthy_tiers: report.healthy_tier_count(),
            degraded_tiers: report.degraded_tier_count(),
        }
    }
}

#[async_trait]
pub trait StorageMaintenanceAuditSink: Send + Sync {
    async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_options_default_has_no_timeout_or_audit_sink() {
        let options = StorageMaintenanceOptions::default();
        assert!(options.timeout.is_none());
        assert!(options.audit_sink.is_none());
    }

    #[test]
    fn maintenance_error_kind_formats_stable_storage_error() {
        let error = StorageMaintenanceErrorKind::AlreadyRunning.storage_error("busy");
        assert!(error.to_string().contains("AlreadyRunning"));
        assert!(error.to_string().contains("busy"));
    }
}
