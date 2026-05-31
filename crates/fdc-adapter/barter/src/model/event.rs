use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
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

/// Market/instrument class associated with a Barter market event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarterMarketType {
    Spot,
    Future,
    Perpetual,
    Option,
}

/// Decimal quantity used for crypto amounts that cannot be represented as integer volume.
pub type DecimalQuantity = Decimal;

/// Trade aggressor side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Public trade payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradePayload {
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub side: Option<TradeSide>,
}

/// Top-of-book payload boundary for later Barter L1 mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookL1Payload {
    pub bid_price: Option<Price>,
    pub bid_quantity: Option<DecimalQuantity>,
    pub ask_price: Option<Price>,
    pub ask_quantity: Option<DecimalQuantity>,
}

/// Candle payload boundary for later historical and live candle mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandlePayload {
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: DecimalQuantity,
}

/// Raw payload boundary for event kinds not fully modeled in Phase A.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawPayload {
    pub description: String,
}

/// Business payload carried by a Barter market event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BarterMarketPayload {
    Trade(TradePayload),
    OrderBookL1(OrderBookL1Payload),
    OrderBookDelta(RawPayload),
    Candle(CandlePayload),
    Liquidation(RawPayload),
    Raw(RawPayload),
}

impl BarterMarketPayload {
    pub fn kind(&self) -> BarterMarketDataKind {
        match self {
            Self::Trade(_) => BarterMarketDataKind::Trade,
            Self::OrderBookL1(_) => BarterMarketDataKind::OrderBookL1,
            Self::OrderBookDelta(_) => BarterMarketDataKind::OrderBook,
            Self::Candle(_) => BarterMarketDataKind::Candle,
            Self::Liquidation(_) => BarterMarketDataKind::Liquidation,
            Self::Raw(_) => BarterMarketDataKind::Trade,
        }
    }
}

/// mdb-normalized market event produced from Barter market data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BarterMarketEvent {
    /// Adapter source name for lineage and downstream routing.
    pub source: String,
    /// Requested data mode.
    pub mode: BarterMarketDataMode,
    /// Barter exchange identifier in snake_case form.
    pub exchange: String,
    /// mdb normalized symbol, for example BTCUSDT.
    pub symbol: Symbol,
    /// Event kind.
    pub kind: BarterMarketDataKind,
    /// Exchange timestamp in nanoseconds.
    pub timestamp: TimestampNs,
    /// Adapter receive timestamp in nanoseconds.
    pub received_at: TimestampNs,
    /// Business payload.
    pub payload: BarterMarketPayload,
    /// Optional source or exchange sequence.
    pub sequence: Option<String>,
    /// Optional source checkpoint for historical or replay events.
    pub checkpoint: Option<BarterCheckpoint>,
}
