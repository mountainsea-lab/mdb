use barter_data::{
    error::DataError,
    event::{DataKind, MarketEvent},
    streams::{consumer::MarketStreamResult, reconnect},
    subscription::trade::PublicTrade,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    collect_live_envelopes_with_summary, BarterMarketDataKind, LiveCollectionRequest, TradeSide,
};
use futures::stream;

const SOURCE_ID: &str = "barter-binance-spot-live-trades";

fn barter_trade_event(
    base: &str,
    quote: &str,
    trade_id: &str,
) -> MarketStreamResult<MarketDataInstrument, DataKind> {
    reconnect::Event::Item(Ok(MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new(base, quote, MarketDataInstrumentKind::Spot),
        kind: DataKind::Trade(PublicTrade {
            id: trade_id.to_string(),
            price: 65_000.25,
            amount: 0.5,
            side: Side::Buy,
        }),
    }))
}

#[tokio::test]
async fn live_collection_rejects_zero_limit() {
    let input = stream::iter(vec![barter_trade_event("btc", "usdt", "trade-1")]);

    let error = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: SOURCE_ID.to_string(),
            limit: 0,
        },
        input,
    )
    .await
    .expect_err("zero limit should be rejected");

    assert!(error.to_string().contains("limit"));
}

#[tokio::test]
async fn live_collection_rejects_empty_source_id() {
    let input = stream::iter(vec![barter_trade_event("btc", "usdt", "trade-1")]);

    let error = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: String::new(),
            limit: 1,
        },
        input,
    )
    .await
    .expect_err("empty source_id should be rejected");

    assert!(error.to_string().contains("source_id"));
}

#[tokio::test]
async fn live_collection_returns_summary_when_limit_reached() {
    let input = stream::iter(vec![
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("eth", "usdt", "trade-2"),
        barter_trade_event("btc", "usdt", "trade-3"),
    ]);

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: SOURCE_ID.to_string(),
            limit: 2,
        },
        input,
    )
    .await
    .expect("bounded collection should succeed");

    assert_eq!(outcome.source_id, SOURCE_ID);
    assert_eq!(outcome.requested_limit, 2);
    assert_eq!(outcome.records_received, 2);
    assert!(outcome.complete);
    assert_eq!(outcome.skipped_reconnects, 1);
    assert_eq!(outcome.envelopes.len(), 2);
    assert_eq!(outcome.envelopes[0].event.symbol.to_string(), "BTCUSDT");
    assert_eq!(outcome.envelopes[1].event.symbol.to_string(), "ETHUSDT");
    assert_eq!(outcome.envelopes[0].event.kind, BarterMarketDataKind::Trade);
    match &outcome.envelopes[0].event.payload {
        fdc_barter::BarterMarketPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.side, Some(TradeSide::Buy));
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
}

#[tokio::test]
async fn live_collection_stops_when_stream_ends_before_limit() {
    let input = stream::iter(vec![
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
    ]);

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: SOURCE_ID.to_string(),
            limit: 3,
        },
        input,
    )
    .await
    .expect("collection should succeed when stream ends early");

    assert_eq!(outcome.source_id, SOURCE_ID);
    assert_eq!(outcome.requested_limit, 3);
    assert_eq!(outcome.records_received, 1);
    assert!(!outcome.complete);
    assert_eq!(outcome.skipped_reconnects, 1);
    assert_eq!(outcome.envelopes.len(), 1);
    assert_eq!(outcome.envelopes[0].event.symbol.to_string(), "BTCUSDT");
}

#[tokio::test]
async fn live_collection_counts_reconnects() {
    let input = stream::iter(vec![
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("eth", "usdt", "trade-2"),
    ]);

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: SOURCE_ID.to_string(),
            limit: 2,
        },
        input,
    )
    .await
    .expect("collection should succeed");

    assert_eq!(outcome.records_received, 2);
    assert!(outcome.complete);
    assert_eq!(outcome.skipped_reconnects, 3);
}

#[tokio::test]
async fn live_collection_returns_stream_item_errors() {
    let input = stream::iter(vec![reconnect::Event::Item(Err(
        DataError::SubscriptionsEmpty,
    ))]);

    let error = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: SOURCE_ID.to_string(),
            limit: 1,
        },
        input,
    )
    .await
    .expect_err("stream item error should be returned");

    assert!(error.to_string().contains("live stream item error"));
}
