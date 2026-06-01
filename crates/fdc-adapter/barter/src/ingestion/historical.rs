use async_trait::async_trait;
use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    model::{BarterMarketDataKind, BarterMarketType, HistoricalCursor},
};

/// Adapter-owned request for one historical backfill page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalBackfillRequest {
    pub source_id: String,
    pub exchange: String,
    pub market_type: BarterMarketType,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub interval: Option<String>,
    pub start: TimestampNs,
    pub end: TimestampNs,
    pub limit: Option<usize>,
    pub cursor: Option<HistoricalCursor>,
}

/// One page of historical backfill envelopes and pagination metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBackfillPage {
    pub request: HistoricalBackfillRequest,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

/// Compact result projection for historical page processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPageOutcome {
    pub records_received: usize,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

impl From<&HistoricalBackfillPage> for HistoricalPageOutcome {
    fn from(page: &HistoricalBackfillPage) -> Self {
        Self {
            records_received: page.envelopes.len(),
            next_cursor: page.next_cursor.clone(),
            complete: page.complete,
        }
    }
}

/// Offline-testable boundary implemented by concrete historical providers.
#[async_trait]
pub trait HistoricalBackfillSource: Send + Sync {
    async fn fetch_page(&self, request: HistoricalBackfillRequest) -> Result<HistoricalBackfillPage>;
}

/// Validate a historical page request before provider I/O.
pub fn validate_historical_backfill_request(request: &HistoricalBackfillRequest) -> Result<()> {
    if request.source_id.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "source_id is empty".to_string(),
        ));
    }
    if request.exchange.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "exchange is empty".to_string(),
        ));
    }
    if request.symbol.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "symbol is empty".to_string(),
        ));
    }
    if request.start.as_nanos() >= request.end.as_nanos() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "start must be before end".to_string(),
        ));
    }
    if request.kind == BarterMarketDataKind::Candle
        && request.interval.as_deref().unwrap_or("").trim().is_empty()
    {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "candle interval is required".to_string(),
        ));
    }

    Ok(())
}

/// Build a stable dedupe key for historical public trade records.
pub fn historical_trade_dedupe_key(event: &crate::model::BarterMarketEvent) -> Option<String> {
    let crate::model::BarterMarketPayload::Trade(trade) = &event.payload else {
        return None;
    };

    Some(match &trade.trade_id {
        Some(trade_id) => format!("{}:{}:{}", event.exchange, event.symbol.as_str(), trade_id),
        None => format!(
            "{}:{}:{}:{}:{}",
            event.exchange,
            event.symbol.as_str(),
            event.timestamp.as_nanos(),
            trade.price.to_f64(),
            trade.quantity
        ),
    })
}
