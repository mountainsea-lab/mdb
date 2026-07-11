use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::checkpoint::BarterCheckpoint;

/// mdb-normalized market data kind produced by `fdc-barter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum BarterMarketDataKind {
    Trade,
    OrderBookL1,
    OrderBook,
    Candle,
    Liquidation,
    FundingRate,
    OpenInterest,
    MarkPrice,
    IndexPrice,
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

/// Order-book event shape emitted by Barter-rs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderBookUpdateKind {
    Snapshot,
    Update,
}

/// One price level in a normalized order book payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevelPayload {
    pub price: Price,
    pub quantity: DecimalQuantity,
}

/// Structured L2 order book payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookPayload {
    pub update_kind: OrderBookUpdateKind,
    pub bids: Vec<OrderBookLevelPayload>,
    pub asks: Vec<OrderBookLevelPayload>,
    pub sequence: Option<String>,
}

/// Structured liquidation payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiquidationPayload {
    pub side: TradeSide,
    pub price: Price,
    pub quantity: DecimalQuantity,
    pub liquidation_time: TimestampNs,
}

/// Candle payload boundary for historical and live candle mapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandlePayload {
    pub interval: Option<String>,
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: DecimalQuantity,
    pub trade_count: Option<u64>,
    pub quote_volume: Option<DecimalQuantity>,
}

/// Funding-rate payload for perpetual and futures markets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRatePayload {
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub mark_price: Option<Price>,
}

/// Open-interest payload for derivatives markets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenInterestPayload {
    pub open_interest: Decimal,
    pub timestamp: TimestampNs,
}

/// Mark-price payload for derivatives markets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPricePayload {
    pub mark_price: Price,
    pub index_price: Option<Price>,
    pub estimated_settle_price: Option<Price>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_time: Option<TimestampNs>,
}

/// Index-price payload for derivatives markets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexPricePayload {
    pub index_price: Price,
    pub timestamp: TimestampNs,
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
    OrderBook(OrderBookPayload),
    Candle(CandlePayload),
    Liquidation(LiquidationPayload),
    FundingRate(FundingRatePayload),
    OpenInterest(OpenInterestPayload),
    MarkPrice(MarkPricePayload),
    IndexPrice(IndexPricePayload),
    Raw(RawPayload),
}

impl BarterMarketPayload {
    pub fn kind(&self) -> BarterMarketDataKind {
        match self {
            Self::Trade(_) => BarterMarketDataKind::Trade,
            Self::OrderBookL1(_) => BarterMarketDataKind::OrderBookL1,
            Self::OrderBook(_) => BarterMarketDataKind::OrderBook,
            Self::Candle(_) => BarterMarketDataKind::Candle,
            Self::Liquidation(_) => BarterMarketDataKind::Liquidation,
            Self::FundingRate(_) => BarterMarketDataKind::FundingRate,
            Self::OpenInterest(_) => BarterMarketDataKind::OpenInterest,
            Self::MarkPrice(_) => BarterMarketDataKind::MarkPrice,
            Self::IndexPrice(_) => BarterMarketDataKind::IndexPrice,
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
    pub market_type: BarterMarketType,
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

#[cfg(test)]
mod derivatives_payload_tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn derivatives_payloads_report_expected_kinds() {
        let funding = BarterMarketPayload::FundingRate(FundingRatePayload {
            funding_rate: Decimal::new(125, 6),
            funding_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            mark_price: None,
        });
        assert_eq!(funding.kind(), BarterMarketDataKind::FundingRate);

        let open_interest = BarterMarketPayload::OpenInterest(OpenInterestPayload {
            open_interest: Decimal::new(12345, 2),
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        });
        assert_eq!(open_interest.kind(), BarterMarketDataKind::OpenInterest);
    }
}
