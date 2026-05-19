use barter_data::event::{DataKind, MarketEvent};
use barter_instrument::{instrument::market_data::MarketDataInstrument, Side};
use fdc_core::types::{Price, TimestampNs};
use rust_decimal::Decimal;

use crate::{
    error::{BarterAdapterError, Result},
    mapper::{exchange::to_exchange_code, instrument::to_symbol},
    model::{
        BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload, RawPayload, TradePayload,
        TradeSide,
    },
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
        let received_at = event
            .time_received
            .timestamp_nanos_opt()
            .map(TimestampNs::from_nanos)
            .ok_or(BarterAdapterError::InvalidTimestamp)?;
        let exchange = to_exchange_code(event.exchange);
        let symbol = to_symbol(&event.instrument);
        let payload = match event.kind {
            DataKind::Trade(trade) => BarterMarketPayload::Trade(TradePayload {
                trade_id: Some(trade.id),
                price: price_from_f64("price", trade.price)?,
                quantity: decimal_from_f64("amount", trade.amount)?,
                side: Some(match trade.side {
                    Side::Buy => TradeSide::Buy,
                    Side::Sell => TradeSide::Sell,
                }),
            }),
            DataKind::OrderBookL1(_) => BarterMarketPayload::Raw(RawPayload {
                description: "order_book_l1".to_string(),
            }),
            DataKind::OrderBook(_) => BarterMarketPayload::OrderBookDelta(RawPayload {
                description: "order_book".to_string(),
            }),
            DataKind::Candle(_) => BarterMarketPayload::Raw(RawPayload {
                description: "candle".to_string(),
            }),
            DataKind::Liquidation(_) => BarterMarketPayload::Liquidation(RawPayload {
                description: "liquidation".to_string(),
            }),
        };
        let kind = payload.kind();

        Ok(Self {
            source: "barter-rs".to_string(),
            mode: BarterMarketDataMode::Live,
            exchange,
            symbol,
            kind,
            timestamp,
            received_at,
            payload,
            sequence: None,
            checkpoint: None,
        })
    }
}

fn price_from_f64(field: &'static str, value: f64) -> Result<Price> {
    Price::from_f64(value).ok_or(BarterAdapterError::InvalidNumericValue { field, value })
}

fn decimal_from_f64(field: &'static str, value: f64) -> Result<Decimal> {
    if !value.is_finite() || value < 0.0 {
        return Err(BarterAdapterError::InvalidNumericValue { field, value });
    }

    Decimal::try_from(value).map_err(|_| BarterAdapterError::InvalidNumericValue { field, value })
}
