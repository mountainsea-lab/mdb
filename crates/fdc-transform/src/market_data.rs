use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketDataKind {
    Trade,
    OrderBookL1,
    OrderBook,
    Candle,
    Liquidation,
    FundingRate,
    OpenInterest,
    MarkPrice,
    IndexPrice,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeDto {
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: Decimal,
    pub side: TradeSide,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookL1Dto {
    pub bid_price: Option<Price>,
    pub bid_quantity: Option<Decimal>,
    pub ask_price: Option<Price>,
    pub ask_quantity: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandleDto {
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawMarketDataDto {
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRateDto {
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub mark_price: Option<Price>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenInterestDto {
    pub open_interest: Decimal,
    pub timestamp: TimestampNs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPriceDto {
    pub mark_price: Price,
    pub index_price: Option<Price>,
    pub estimated_settle_price: Option<Price>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_time: Option<TimestampNs>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexPriceDto {
    pub index_price: Price,
    pub timestamp: TimestampNs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketDataPayload {
    Trade(TradeDto),
    OrderBookL1(OrderBookL1Dto),
    OrderBookDelta(RawMarketDataDto),
    Candle(CandleDto),
    Liquidation(RawMarketDataDto),
    FundingRate(FundingRateDto),
    OpenInterest(OpenInterestDto),
    MarkPrice(MarkPriceDto),
    IndexPrice(IndexPriceDto),
    Raw(RawMarketDataDto),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TransformQualityFlags {
    pub is_replay: bool,
    pub is_backfill: bool,
    pub is_duplicate_candidate: bool,
    pub has_gap_before: bool,
    pub is_out_of_order: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataDto {
    pub event_id: String,
    pub source_id: String,
    pub adapter: String,
    pub exchange: String,
    pub symbol: Symbol,
    pub kind: MarketDataKind,
    pub event_time: TimestampNs,
    pub received_at: TimestampNs,
    pub emitted_at: TimestampNs,
    pub source_sequence: Option<String>,
    pub ingestion_sequence: Option<String>,
    pub quality: TransformQualityFlags,
    pub payload: MarketDataPayload,
}
