use barter_data::{
    books::{Level, OrderBook},
    event::{DataKind, MarketEvent},
    subscription::{
        book::{OrderBookEvent, OrderBookL1},
        liquidation::Liquidation,
    },
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType, OrderBookUpdateKind, TradeSide,
};

fn market_event(
    kind: DataKind,
    instrument_kind: MarketDataInstrumentKind,
) -> MarketEvent<MarketDataInstrument, DataKind> {
    MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceFuturesUsd,
        instrument: MarketDataInstrument::new("btc", "usdt", instrument_kind),
        kind,
    }
}

#[test]
fn l1_event_maps_to_structured_payload() {
    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBookL1(OrderBookL1 {
            last_update_time: Utc.timestamp_nanos(1_700_000_000_000_000_500),
            best_bid: Some(Level::new(65000, 2)),
            best_ask: Some(Level::new(65001, 3)),
        }),
        MarketDataInstrumentKind::Perpetual,
    ))
    .expect("l1 should map");

    assert_eq!(event.market_type, BarterMarketType::Perpetual);
    assert_eq!(event.kind, BarterMarketDataKind::OrderBookL1);
    match event.payload {
        BarterMarketPayload::OrderBookL1(l1) => {
            assert_eq!(l1.bid_price.unwrap().to_f64(), 65000.0);
            assert_eq!(l1.bid_quantity.unwrap().to_string(), "2");
            assert_eq!(l1.ask_price.unwrap().to_f64(), 65001.0);
            assert_eq!(l1.ask_quantity.unwrap().to_string(), "3");
        }
        other => panic!("expected l1 payload, got {other:?}"),
    }
}

#[test]
fn l2_snapshot_maps_to_structured_payload() {
    let book = OrderBook::new(
        42,
        Some(Utc.timestamp_nanos(1_700_000_000_000_000_500)),
        vec![Level::new(65000, 2), Level::new(64999, 4)],
        vec![Level::new(65001, 3)],
    );

    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBook(OrderBookEvent::Snapshot(book)),
        MarketDataInstrumentKind::Spot,
    ))
    .expect("l2 snapshot should map");

    assert_eq!(event.market_type, BarterMarketType::Spot);
    assert_eq!(event.sequence.as_deref(), Some("42"));
    match event.payload {
        BarterMarketPayload::OrderBook(book) => {
            assert_eq!(book.update_kind, OrderBookUpdateKind::Snapshot);
            assert_eq!(book.sequence.as_deref(), Some("42"));
            assert_eq!(book.bids.len(), 2);
            assert_eq!(book.asks.len(), 1);
            assert_eq!(book.bids[0].price.to_f64(), 65000.0);
            assert_eq!(book.asks[0].quantity.to_string(), "3");
        }
        other => panic!("expected order book payload, got {other:?}"),
    }
}

#[test]
fn l2_update_maps_to_structured_payload() {
    let update = OrderBook::new(
        43,
        None,
        vec![Level::new(65002, 1)],
        vec![Level::new(65003, 5)],
    );

    let event = fdc_barter::map_market_event(market_event(
        DataKind::OrderBook(OrderBookEvent::Update(update)),
        MarketDataInstrumentKind::Spot,
    ))
    .expect("l2 update should map");

    match event.payload {
        BarterMarketPayload::OrderBook(book) => {
            assert_eq!(book.update_kind, OrderBookUpdateKind::Update);
            assert_eq!(book.sequence.as_deref(), Some("43"));
        }
        other => panic!("expected order book payload, got {other:?}"),
    }
}

#[test]
fn liquidation_event_maps_to_structured_payload() {
    let liquidation_time = Utc.timestamp_nanos(1_700_000_000_000_000_777);
    let event = fdc_barter::map_market_event(market_event(
        DataKind::Liquidation(Liquidation {
            side: Side::Sell,
            price: 64000.5,
            quantity: 2.25,
            time: liquidation_time,
        }),
        MarketDataInstrumentKind::Perpetual,
    ))
    .expect("liquidation should map");

    assert_eq!(event.kind, BarterMarketDataKind::Liquidation);
    assert_eq!(event.market_type, BarterMarketType::Perpetual);
    match event.payload {
        BarterMarketPayload::Liquidation(liquidation) => {
            assert_eq!(liquidation.side, TradeSide::Sell);
            assert_eq!(liquidation.price.to_f64(), 64000.5);
            assert_eq!(liquidation.quantity.to_string(), "2.25");
            assert_eq!(
                liquidation.liquidation_time.as_nanos(),
                1_700_000_000_000_000_777
            );
        }
        other => panic!("expected liquidation payload, got {other:?}"),
    }
}
