use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use super::{BarterMarketDataKind, BarterMarketDataMode};

/// Cursor for resuming historical pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalCursor {
    pub exchange: String,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub next_start: Option<TimestampNs>,
    pub page_token: Option<String>,
    pub last_seen_exchange_id: Option<String>,
}

/// Checkpoint emitted by a Barter-backed source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterCheckpoint {
    pub source_id: String,
    pub exchange: String,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub mode: BarterMarketDataMode,
    pub last_event_time: TimestampNs,
    pub cursor: Option<HistoricalCursor>,
    pub updated_at: TimestampNs,
}
