use fdc_barter::{
    BarterMarketDataKind, BarterMarketEvent, BarterMarketPayload, TradeSide as BarterTradeSide,
};
use fdc_core::{Result, error::Error};
use fdc_ingestion::SourceEnvelope;
use fdc_transform::{
    CandleDto, FundingRateDto, IndexPriceDto, MarkPriceDto, MarketDataDto, MarketDataKind,
    MarketDataPayload, OpenInterestDto, OrderBookL1Dto, RawMarketDataDto, TradeDto, TradeSide,
    TransformQualityFlags,
};

use crate::barter::barter_kind_label;

pub fn barter_event_to_market_data_dto(
    source: &SourceEnvelope<BarterMarketEvent>,
) -> Result<MarketDataDto> {
    let event = &source.payload;
    let payload = barter_payload_to_market_data_payload(&event.payload);
    let kind = barter_kind_to_market_data_kind(event.kind, &event.payload)?;

    Ok(MarketDataDto {
        event_id: source.envelope_id.clone(),
        source_id: source.source_id.clone(),
        adapter: source
            .metadata
            .adapter
            .clone()
            .unwrap_or_else(|| "barter".to_string()),
        exchange: event.exchange.clone(),
        symbol: event.symbol.clone(),
        kind,
        event_time: event.timestamp,
        received_at: event.received_at,
        emitted_at: source.emitted_at,
        source_sequence: event.sequence.clone(),
        ingestion_sequence: source.sequence.clone(),
        quality: TransformQualityFlags {
            is_replay: source.quality.is_replay,
            is_backfill: source.quality.is_backfill,
            is_duplicate_candidate: source.quality.is_duplicate_candidate,
            has_gap_before: source.quality.has_gap_before,
            is_out_of_order: source.quality.is_out_of_order,
        },
        payload,
    })
}

fn barter_kind_to_market_data_kind(
    kind: BarterMarketDataKind,
    payload: &BarterMarketPayload,
) -> Result<MarketDataKind> {
    match (kind, payload) {
        (BarterMarketDataKind::Trade, BarterMarketPayload::Trade(_)) => Ok(MarketDataKind::Trade),
        (BarterMarketDataKind::OrderBookL1, BarterMarketPayload::OrderBookL1(_)) => {
            Ok(MarketDataKind::OrderBookL1)
        }
        (BarterMarketDataKind::OrderBook, BarterMarketPayload::OrderBook(_)) => {
            Ok(MarketDataKind::OrderBook)
        }
        (BarterMarketDataKind::Candle, BarterMarketPayload::Candle(_)) => {
            Ok(MarketDataKind::Candle)
        }
        (BarterMarketDataKind::Liquidation, BarterMarketPayload::Liquidation(_)) => {
            Ok(MarketDataKind::Liquidation)
        }
        (BarterMarketDataKind::FundingRate, BarterMarketPayload::FundingRate(_)) => {
            Ok(MarketDataKind::FundingRate)
        }
        (BarterMarketDataKind::OpenInterest, BarterMarketPayload::OpenInterest(_)) => {
            Ok(MarketDataKind::OpenInterest)
        }
        (BarterMarketDataKind::MarkPrice, BarterMarketPayload::MarkPrice(_)) => {
            Ok(MarketDataKind::MarkPrice)
        }
        (BarterMarketDataKind::IndexPrice, BarterMarketPayload::IndexPrice(_)) => {
            Ok(MarketDataKind::IndexPrice)
        }
        (_, BarterMarketPayload::Raw(_)) => Ok(MarketDataKind::Raw),
        (kind, _) => Err(Error::validation(format!(
            "barter event kind {} does not match payload",
            barter_kind_label(kind)
        ))),
    }
}

fn barter_payload_to_market_data_payload(payload: &BarterMarketPayload) -> MarketDataPayload {
    match payload {
        BarterMarketPayload::Trade(trade) => MarketDataPayload::Trade(TradeDto {
            trade_id: trade.trade_id.clone(),
            price: trade.price,
            quantity: trade.quantity,
            side: match trade.side {
                Some(BarterTradeSide::Buy) => TradeSide::Buy,
                Some(BarterTradeSide::Sell) => TradeSide::Sell,
                None => TradeSide::Unknown,
            },
        }),
        BarterMarketPayload::OrderBookL1(book) => MarketDataPayload::OrderBookL1(OrderBookL1Dto {
            bid_price: book.bid_price,
            bid_quantity: book.bid_quantity,
            ask_price: book.ask_price,
            ask_quantity: book.ask_quantity,
        }),
        BarterMarketPayload::OrderBook(book) => {
            MarketDataPayload::OrderBookDelta(RawMarketDataDto {
                description: format!(
                    "order_book {:?} bids={} asks={} sequence={}",
                    book.update_kind,
                    book.bids.len(),
                    book.asks.len(),
                    book.sequence.as_deref().unwrap_or("none")
                ),
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
        BarterMarketPayload::Liquidation(liquidation) => {
            MarketDataPayload::Liquidation(RawMarketDataDto {
                description: format!(
                    "liquidation side={:?} price={} quantity={} liquidation_time={}",
                    liquidation.side,
                    liquidation.price,
                    liquidation.quantity,
                    liquidation.liquidation_time.as_nanos()
                ),
            })
        }
        BarterMarketPayload::FundingRate(funding) => {
            MarketDataPayload::FundingRate(FundingRateDto {
                funding_rate: funding.funding_rate,
                funding_time: funding.funding_time,
                mark_price: funding.mark_price,
            })
        }
        BarterMarketPayload::OpenInterest(open_interest) => {
            MarketDataPayload::OpenInterest(OpenInterestDto {
                open_interest: open_interest.open_interest,
                timestamp: open_interest.timestamp,
            })
        }
        BarterMarketPayload::MarkPrice(mark_price) => MarketDataPayload::MarkPrice(MarkPriceDto {
            mark_price: mark_price.mark_price,
            index_price: mark_price.index_price,
            estimated_settle_price: mark_price.estimated_settle_price,
            funding_rate: mark_price.funding_rate,
            next_funding_time: mark_price.next_funding_time,
        }),
        BarterMarketPayload::IndexPrice(index_price) => {
            MarketDataPayload::IndexPrice(IndexPriceDto {
                index_price: index_price.index_price,
                timestamp: index_price.timestamp,
            })
        }
        BarterMarketPayload::Raw(raw) => MarketDataPayload::Raw(RawMarketDataDto {
            description: raw.description.clone(),
        }),
    }
}
