use fdc_transform::{
    CandleDto, MarketDataDto, MarketDataKind, MarketDataPayload, OrderBookL1Dto, RawMarketDataDto,
    TradeDto, TradeSide as TransformTradeSide, TransformQualityFlags,
};

use crate::{
    BarterAdapterError, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketPayload,
    TradeSide,
};

pub trait IntoMarketDataDto {
    fn into_market_data_dto(self) -> Result<MarketDataDto, BarterAdapterError>;
}

impl IntoMarketDataDto for BarterIngestionEnvelope {
    fn into_market_data_dto(self) -> Result<MarketDataDto, BarterAdapterError> {
        let kind = map_kind(self.event.kind);
        let payload = map_payload(self.event.payload)?;
        Ok(MarketDataDto {
            event_id: self.envelope_id.clone(),
            source_id: self.source_id,
            adapter: self.event.source,
            exchange: self.event.exchange,
            symbol: self.event.symbol,
            kind,
            event_time: self.event.timestamp,
            received_at: self.event.received_at,
            emitted_at: self.emitted_at,
            source_sequence: self.event.sequence,
            ingestion_sequence: Some(self.envelope_id),
            quality: TransformQualityFlags {
                is_replay: self.quality.is_replay,
                is_backfill: self.quality.is_backfill,
                is_duplicate_candidate: self.quality.is_duplicate_candidate,
                has_gap_before: self.quality.has_gap_before,
                is_out_of_order: self.quality.is_out_of_order,
            },
            payload,
        })
    }
}

fn map_kind(kind: BarterMarketDataKind) -> MarketDataKind {
    match kind {
        BarterMarketDataKind::Trade => MarketDataKind::Trade,
        BarterMarketDataKind::OrderBookL1 => MarketDataKind::OrderBookL1,
        BarterMarketDataKind::OrderBook => MarketDataKind::OrderBook,
        BarterMarketDataKind::Candle => MarketDataKind::Candle,
        BarterMarketDataKind::Liquidation => MarketDataKind::Liquidation,
    }
}

fn map_trade_side(side: Option<TradeSide>) -> TransformTradeSide {
    match side {
        Some(TradeSide::Buy) => TransformTradeSide::Buy,
        Some(TradeSide::Sell) => TransformTradeSide::Sell,
        None => TransformTradeSide::Unknown,
    }
}

fn map_payload(payload: BarterMarketPayload) -> Result<MarketDataPayload, BarterAdapterError> {
    Ok(match payload {
        BarterMarketPayload::Trade(trade) => MarketDataPayload::Trade(TradeDto {
            trade_id: trade.trade_id,
            price: trade.price,
            quantity: trade.quantity,
            side: map_trade_side(trade.side),
        }),
        BarterMarketPayload::OrderBookL1(book) => MarketDataPayload::OrderBookL1(OrderBookL1Dto {
            bid_price: book.bid_price,
            bid_quantity: book.bid_quantity,
            ask_price: book.ask_price,
            ask_quantity: book.ask_quantity,
        }),
        BarterMarketPayload::OrderBookDelta(raw) => {
            MarketDataPayload::OrderBookDelta(RawMarketDataDto {
                description: raw.description,
            })
        }
        BarterMarketPayload::Candle(candle) => MarketDataPayload::Candle(CandleDto {
            open_time: candle.open_time,
            close_time: candle.close_time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        }),
        BarterMarketPayload::Liquidation(raw) => MarketDataPayload::Liquidation(RawMarketDataDto {
            description: raw.description,
        }),
        BarterMarketPayload::Raw(raw) => MarketDataPayload::Raw(RawMarketDataDto {
            description: raw.description,
        }),
    })
}
