//! Barter-rs adapter for Financial Data Center.
//!
//! This crate is the integration boundary between Barter's exchange market data
//! streams and mdb's internal data pipeline. It intentionally keeps storage,
//! query, and trading execution concerns out of this adapter.

use barter_data::event::{DataKind, MarketEvent};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use fdc_core::types::{Price, Symbol, TimestampNs, Volume};
use serde::{Deserialize, Serialize};

/// Result type used by the Barter adapter.
pub type Result<T> = std::result::Result<T, BarterAdapterError>;

/// Errors produced while adapting Barter data into mdb data.
#[derive(Debug, thiserror::Error)]
pub enum BarterAdapterError {
    /// The Barter event kind is not supported by the current adapter stage.
    #[error("unsupported Barter market data kind: {0}")]
    UnsupportedKind(&'static str),

    /// A Barter numeric value cannot be represented by the target mdb type.
    #[error("invalid numeric value for field {field}: {value}")]
    InvalidNumericValue { field: &'static str, value: f64 },

    /// A timestamp cannot be represented as nanoseconds.
    #[error("timestamp cannot be represented as nanoseconds")]
    InvalidTimestamp,
}

/// Data retrieval mode requested from Barter-backed sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterDataMode {
    /// Real-time exchange market data.
    Live,
    /// Historical exchange market data or replay.
    Historical,
}

/// First-stage adapter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterAdapterConfig {
    /// Requested data mode.
    pub mode: BarterDataMode,
    /// Safety switch: first-stage skeleton must not start real network streams by default.
    pub start_network_streams: bool,
    /// Upper bound used when grouping Barter subscriptions into one connection.
    pub max_subscriptions_per_connection: usize,
}

impl Default for BarterAdapterConfig {
    fn default() -> Self {
        Self {
            mode: BarterDataMode::Live,
            start_network_streams: false,
            max_subscriptions_per_connection: 100,
        }
    }
}

/// mdb-normalized market data kind produced by `fdc-barter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BarterMarketDataKind {
    Trade,
    OrderBookL1,
    OrderBook,
    Candle,
    Liquidation,
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
}

impl TryFrom<MarketEvent<MarketDataInstrument, DataKind>> for BarterMarketEvent {
    type Error = BarterAdapterError;

    fn try_from(event: MarketEvent<MarketDataInstrument, DataKind>) -> Result<Self> {
        let timestamp = event
            .time_exchange
            .timestamp_nanos_opt()
            .map(TimestampNs::from_nanos)
            .ok_or(BarterAdapterError::InvalidTimestamp)?;
        let exchange = event.exchange.as_str().to_string();
        let symbol = symbol_from_market_instrument(&event.instrument);

        match event.kind {
            DataKind::Trade(trade) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::Trade,
                timestamp,
                price: Some(price_from_f64("price", trade.price)?),
                volume: Some(volume_from_f64("amount", trade.amount)?),
            }),
            DataKind::OrderBookL1(_) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::OrderBookL1,
                timestamp,
                price: None,
                volume: None,
            }),
            DataKind::OrderBook(_) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::OrderBook,
                timestamp,
                price: None,
                volume: None,
            }),
            DataKind::Candle(_) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::Candle,
                timestamp,
                price: None,
                volume: None,
            }),
            DataKind::Liquidation(_) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::Liquidation,
                timestamp,
                price: None,
                volume: None,
            }),
        }
    }
}

fn symbol_from_market_instrument(instrument: &MarketDataInstrument) -> Symbol {
    Symbol::new(format!("{}{}", instrument.base, instrument.quote))
}

fn price_from_f64(field: &'static str, value: f64) -> Result<Price> {
    Price::from_f64(value).ok_or(BarterAdapterError::InvalidNumericValue { field, value })
}

fn volume_from_f64(field: &'static str, value: f64) -> Result<Volume> {
    if !value.is_finite() || value < 0.0 {
        return Err(BarterAdapterError::InvalidNumericValue { field, value });
    }

    Ok(Volume::new(value as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_rejects_negative_values() {
        assert!(volume_from_f64("amount", -1.0).is_err());
    }
}
