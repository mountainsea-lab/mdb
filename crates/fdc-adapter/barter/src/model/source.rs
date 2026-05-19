use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use super::{BarterCheckpoint, BarterMarketDataMode};

/// Observable lifecycle state for a Barter-backed source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterSourceStatus {
    Created,
    Starting,
    Running,
    Backfilling,
    Reconnecting,
    RateLimited,
    Stopping,
    Stopped,
    Failed,
}

/// Runtime state snapshot for a Barter-backed source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterSourceState {
    pub source_id: String,
    pub mode: BarterMarketDataMode,
    pub status: BarterSourceStatus,
    pub started_at: Option<TimestampNs>,
    pub last_event_at: Option<TimestampNs>,
    pub last_error: Option<String>,
    pub checkpoint: Option<BarterCheckpoint>,
}

impl BarterSourceState {
    pub fn new(source_id: impl Into<String>, mode: BarterMarketDataMode) -> Self {
        Self {
            source_id: source_id.into(),
            mode,
            status: BarterSourceStatus::Created,
            started_at: None,
            last_event_at: None,
            last_error: None,
            checkpoint: None,
        }
    }
}
