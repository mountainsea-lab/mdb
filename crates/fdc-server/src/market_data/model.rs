use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataLiveState {
    Idle,
    Starting,
    Running,
    Completed,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveMarketDataRequest {
    pub timeout_secs: Option<u64>,
    pub max_envelopes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveMarketDataStatusResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub started_at_ns: Option<u64>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
    pub subscriptions: Vec<String>,
    pub last_record_at_ns: Option<u64>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
    pub last_result: Option<StartLiveMarketDataResponse>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageStatusResponse {
    pub backend: String,
    pub policy_profile: String,
    pub tiered: bool,
    pub durable_tiers_configured: usize,
    pub maintenance_enabled: bool,
    pub maintenance_audit_reset_enabled: bool,
    pub maintenance_audit_capacity: usize,
    pub tiers: Vec<MarketDataStorageTierStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageTierStatus {
    pub tier: String,
    pub engine: String,
    pub durable_path_configured: bool,
    pub path_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageHealthResponse {
    pub backend: String,
    pub tiered: bool,
    pub status: String,
    pub tiers: Vec<MarketDataStorageTierHealth>,
    pub access_patterns: usize,
    pub migration_queue_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageTierHealth {
    pub tier: String,
    pub enabled: bool,
    pub initialized: bool,
    pub status: String,
    pub key_count: Option<u64>,
    pub total_size: Option<u64>,
    pub durable_path_configured: bool,
    pub path_hint: Option<String>,
    pub path_exists: Option<bool>,
    pub path_parent_exists: Option<bool>,
    pub path_parent_writable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceRunRequest {
    pub confirm: String,
    pub timeout_ms: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceRunResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub duration_ms: Option<i64>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResponse {
    pub capacity: usize,
    pub total_entries: usize,
    pub returned_entries: usize,
    pub total_recorded_entries: u64,
    pub reset_count: u64,
    pub total_cleared_entries: u64,
    pub last_recorded_at: Option<String>,
    pub last_reset_at: Option<String>,
    pub entries: Vec<MarketDataStorageMaintenanceAuditEntryResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub cleared_entries: usize,
    pub remaining_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditEntryResponse {
    pub recorded_at: String,
    pub started_at: String,
    pub finished_at: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerStatusResponse {
    pub enabled: bool,
    pub running: bool,
    pub backend: String,
    pub tiered: bool,
    pub interval_seconds: u64,
    pub timeout_ms: u64,
    pub jitter_seconds: u64,
    pub max_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub total_runs: u64,
    pub successful_runs: u64,
    pub failed_runs: u64,
    pub skipped_runs: u64,
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub last_status: Option<String>,
    pub last_error: Option<String>,
    pub next_run_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResetRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResetResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResumeRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceSchedulerResumeResponse {
    pub accepted: bool,
    pub status: String,
    pub reason: Option<String>,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub task_started: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataTradesResponse {
    pub returned_records: usize,
    pub records: Vec<MarketDataTradeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataTradeRecord {
    pub key: String,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub source: Option<String>,
    pub payload: Value,
}
