use barter_data::{
    books::{Level, OrderBook},
    event::{DataKind, MarketEvent},
    subscription::book::OrderBookEvent,
};
use barter_instrument::{
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use fdc_core::types::{Price, TimestampNs};
use rust_decimal::Decimal;

use crate::{
    error::{BarterAdapterError, Result},
    mapper::{exchange::to_exchange_code, instrument::to_symbol},
    model::{
        BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload, BarterMarketType,
        CandlePayload, LiquidationPayload, OrderBookL1Payload, OrderBookLevelPayload,
        OrderBookPayload, OrderBookUpdateKind, TradePayload, TradeSide,
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
        let market_type = market_type_from_instrument(&event.instrument.kind);
        let mut sequence = None;
        let payload = match event.kind {
            DataKind::Trade(trade) => BarterMarketPayload::Trade(TradePayload {
                trade_id: Some(trade.id),
                price: price_from_f64("price", trade.price)?,
                quantity: decimal_from_f64("amount", trade.amount)?,
                side: Some(side_from_barter(trade.side)),
            }),
            DataKind::OrderBookL1(book) => BarterMarketPayload::OrderBookL1(OrderBookL1Payload {
                bid_price: book.best_bid.map(|level| Price::new(level.price)),
                bid_quantity: book.best_bid.map(|level| level.amount),
                ask_price: book.best_ask.map(|level| Price::new(level.price)),
                ask_quantity: book.best_ask.map(|level| level.amount),
            }),
            DataKind::OrderBook(book) => {
                let (payload, mapped_sequence) = map_order_book(book)?;
                sequence = mapped_sequence;
                BarterMarketPayload::OrderBook(payload)
            }
            DataKind::Candle(candle) => BarterMarketPayload::Candle(CandlePayload {
                interval: None,
                open_time: timestamp,
                close_time: candle
                    .close_time
                    .timestamp_nanos_opt()
                    .map(TimestampNs::from_nanos)
                    .ok_or(BarterAdapterError::InvalidTimestamp)?,
                open: price_from_f64("candle.open", candle.open)?,
                high: price_from_f64("candle.high", candle.high)?,
                low: price_from_f64("candle.low", candle.low)?,
                close: price_from_f64("candle.close", candle.close)?,
                volume: decimal_from_f64("candle.volume", candle.volume)?,
                trade_count: Some(candle.trade_count),
                quote_volume: None,
            }),
            DataKind::Liquidation(liquidation) => {
                BarterMarketPayload::Liquidation(LiquidationPayload {
                    side: side_from_barter(liquidation.side),
                    price: price_from_f64("liquidation.price", liquidation.price)?,
                    quantity: decimal_from_f64("liquidation.quantity", liquidation.quantity)?,
                    liquidation_time: liquidation
                        .time
                        .timestamp_nanos_opt()
                        .map(TimestampNs::from_nanos)
                        .ok_or(BarterAdapterError::InvalidTimestamp)?,
                })
            }
        };
        let kind = payload.kind();

        Ok(Self {
            source: "barter-rs".to_string(),
            mode: BarterMarketDataMode::Live,
            exchange,
            symbol,
            market_type,
            kind,
            timestamp,
            received_at,
            payload,
            sequence,
            checkpoint: None,
        })
    }
}

fn market_type_from_instrument(kind: &MarketDataInstrumentKind) -> BarterMarketType {
    match kind {
        MarketDataInstrumentKind::Spot => BarterMarketType::Spot,
        MarketDataInstrumentKind::Future(_) => BarterMarketType::Future,
        MarketDataInstrumentKind::Perpetual => BarterMarketType::Perpetual,
        MarketDataInstrumentKind::Option(_) => BarterMarketType::Option,
    }
}

fn side_from_barter(side: Side) -> TradeSide {
    match side {
        Side::Buy => TradeSide::Buy,
        Side::Sell => TradeSide::Sell,
    }
}

fn map_order_book(event: OrderBookEvent) -> Result<(OrderBookPayload, Option<String>)> {
    let (update_kind, book) = match event {
        OrderBookEvent::Snapshot(book) => (OrderBookUpdateKind::Snapshot, book),
        OrderBookEvent::Update(book) => (OrderBookUpdateKind::Update, book),
    };
    let sequence = Some(book.sequence().to_string());
    let (bids, asks) = order_book_levels(&book)?;

    Ok((
        OrderBookPayload {
            update_kind,
            bids,
            asks,
            sequence: sequence.clone(),
        },
        sequence,
    ))
}

fn order_book_levels(
    book: &OrderBook,
) -> Result<(Vec<OrderBookLevelPayload>, Vec<OrderBookLevelPayload>)> {
    let bids = book
        .bids()
        .levels()
        .iter()
        .copied()
        .map(level_to_payload)
        .collect::<Result<Vec<_>>>()?;
    let asks = book
        .asks()
        .levels()
        .iter()
        .copied()
        .map(level_to_payload)
        .collect::<Result<Vec<_>>>()?;

    Ok((bids, asks))
}

fn level_to_payload(level: Level) -> Result<OrderBookLevelPayload> {
    if level.price < Decimal::ZERO {
        return Err(BarterAdapterError::InvalidNumericValue {
            field: "order_book.price",
            value: level.price.to_string().parse::<f64>().unwrap_or(f64::NAN),
        });
    }
    if level.amount < Decimal::ZERO {
        return Err(BarterAdapterError::InvalidNumericValue {
            field: "order_book.amount",
            value: level.amount.to_string().parse::<f64>().unwrap_or(f64::NAN),
        });
    }

    Ok(OrderBookLevelPayload {
        price: Price::new(level.price),
        quantity: level.amount,
    })
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
