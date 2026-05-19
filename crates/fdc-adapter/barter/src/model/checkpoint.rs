use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use super::{BarterMarketDataKind, BarterMarketDataMode, HistoricalPageRequest};

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

impl HistoricalCursor {
    pub fn next_start(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        kind: BarterMarketDataKind,
        next_start: TimestampNs,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            symbol: symbol.into(),
            kind,
            next_start: Some(next_start),
            page_token: None,
            last_seen_exchange_id: None,
        }
    }
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

impl BarterCheckpoint {
    pub fn from_historical_page(
        request: &HistoricalPageRequest,
        last_event_time: TimestampNs,
        cursor: HistoricalCursor,
    ) -> Self {
        Self {
            source_id: request.source_id.clone(),
            exchange: request.exchange.clone(),
            symbol: request.symbol.clone(),
            kind: request.kind,
            mode: BarterMarketDataMode::Historical,
            last_event_time,
            cursor: Some(cursor),
            updated_at: TimestampNs::now(),
        }
    }
}
