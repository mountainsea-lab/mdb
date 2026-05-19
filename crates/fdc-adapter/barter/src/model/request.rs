use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use super::{BarterMarketDataKind, BarterMarketDataMode, HistoricalCursor};

/// Market data request accepted by Barter-backed sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterMarketDataRequest {
    pub source_id: String,
    pub exchange: String,
    pub symbols: Vec<String>,
    pub kinds: Vec<BarterMarketDataKind>,
    pub mode: BarterMarketDataMode,
}

impl BarterMarketDataRequest {
    pub fn live(
        exchange: impl Into<String>,
        symbols: Vec<&str>,
        kinds: Vec<BarterMarketDataKind>,
    ) -> Self {
        let exchange = exchange.into();
        Self {
            source_id: format!("barter-{exchange}-live"),
            exchange,
            symbols: symbols.into_iter().map(str::to_string).collect(),
            kinds,
            mode: BarterMarketDataMode::Live,
        }
    }
}

/// One historical pagination unit for a single exchange, symbol, and data kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPageRequest {
    pub source_id: String,
    pub exchange: String,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub start: TimestampNs,
    pub end: Option<TimestampNs>,
    pub limit: Option<usize>,
    pub cursor: Option<HistoricalCursor>,
}

impl HistoricalPageRequest {
    pub fn new(
        source_id: impl Into<String>,
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        kind: BarterMarketDataKind,
        start: TimestampNs,
        end: Option<TimestampNs>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            exchange: exchange.into(),
            symbol: symbol.into(),
            kind,
            start,
            end,
            limit,
            cursor: None,
        }
    }
}
