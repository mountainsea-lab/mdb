use fdc_core::types::{Price, Symbol, TimestampNs, Volume};
use serde::{Deserialize, Serialize};

use super::checkpoint::BarterCheckpoint;

/// mdb-normalized market data kind produced by `fdc-barter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarterMarketDataKind {
    Trade,
    OrderBookL1,
    OrderBook,
    Candle,
    Liquidation,
}

/// Data retrieval mode requested from Barter-backed sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterMarketDataMode {
    /// Real-time exchange market data.
    Live,
    /// Historical exchange market data or replay.
    Historical,
}

/// mdb-normalized market event produced from Barter market data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BarterMarketEvent {
    /// Adapter source name for lineage and downstream routing.
    pub source: String,
    /// Barter exchange identifier in snake_case form.
    pub exchange: String,
    /// mdb normalized symbol, for example BTCUSDT.
    pub symbol: Symbol,
    /// Event kind.
    pub kind: BarterMarketDataKind,
    /// Exchange timestamp in nanoseconds.
    pub timestamp: TimestampNs,
    /// Optional trade or quote price.
    pub price: Option<Price>,
    /// Optional trade or quote volume.
    pub volume: Option<Volume>,
    /// Optional source checkpoint for historical or replay events.
    pub checkpoint: Option<BarterCheckpoint>,
}
