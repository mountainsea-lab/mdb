use barter_data::event::{DataKind, MarketEvent};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use fdc_core::types::{Price, TimestampNs, Volume};

use crate::{
    error::{BarterAdapterError, Result},
    mapper::{exchange::to_exchange_code, instrument::to_symbol},
    model::{BarterMarketDataKind, BarterMarketEvent},
};

pub fn map_market_event(
    event: MarketEvent<MarketDataInstrument, DataKind>,
) -> Result<BarterMarketEvent> {
    BarterMarketEvent::try_from(event)
}

impl TryFrom<MarketEvent<MarketDataInstrument, DataKind>> for BarterMarketEvent {
    type Error = BarterAdapterError;

    fn try_from(event: MarketEvent<MarketDataInstrument, DataKind>) -> Result<Self> {
        let timestamp = event
            .time_exchange
            .timestamp_nanos_opt()
            .map(TimestampNs::from_nanos)
            .ok_or(BarterAdapterError::InvalidTimestamp)?;
        let exchange = to_exchange_code(event.exchange);
        let symbol = to_symbol(&event.instrument);

        match event.kind {
            DataKind::Trade(trade) => Ok(Self {
                source: "barter-rs".to_string(),
                exchange,
                symbol,
                kind: BarterMarketDataKind::Trade,
                timestamp,
                price: Some(price_from_f64("price", trade.price)?),
                volume: Some(volume_from_f64("amount", trade.amount)?),
                checkpoint: None,
            }),
            DataKind::OrderBookL1(_) => Ok(empty_event(
                exchange,
                symbol,
                BarterMarketDataKind::OrderBookL1,
                timestamp,
            )),
            DataKind::OrderBook(_) => Ok(empty_event(
                exchange,
                symbol,
                BarterMarketDataKind::OrderBook,
                timestamp,
            )),
            DataKind::Candle(_) => Ok(empty_event(
                exchange,
                symbol,
                BarterMarketDataKind::Candle,
                timestamp,
            )),
            DataKind::Liquidation(_) => Ok(empty_event(
                exchange,
                symbol,
                BarterMarketDataKind::Liquidation,
                timestamp,
            )),
        }
    }
}

fn empty_event(
    exchange: String,
    symbol: fdc_core::types::Symbol,
    kind: BarterMarketDataKind,
    timestamp: TimestampNs,
) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter-rs".to_string(),
        exchange,
        symbol,
        kind,
        timestamp,
        price: None,
        volume: None,
        checkpoint: None,
    }
}

fn price_from_f64(field: &'static str, value: f64) -> Result<Price> {
    Price::from_f64(value).ok_or(BarterAdapterError::InvalidNumericValue { field, value })
}

pub(crate) fn volume_from_f64(field: &'static str, value: f64) -> Result<Volume> {
    if !value.is_finite() || value < 0.0 {
        return Err(BarterAdapterError::InvalidNumericValue { field, value });
    }

    Ok(Volume::new(value as u64))
}
