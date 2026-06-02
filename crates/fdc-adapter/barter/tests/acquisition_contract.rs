use async_trait::async_trait;
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
    collect_live_envelopes_with_summary, run_historical_backfill_pages, BarterAdapterError,
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, HistoricalBackfillPage, HistoricalBackfillRequest,
    HistoricalBackfillRunRequest, HistoricalBackfillStopReason, HistoricalCursor,
    HistoricalPageFetcher, LiveCollectionRequest, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use futures::stream;
use rust_decimal::Decimal;

const SOURCE_ID: &str = "barter-binance-spot-live-trades";

fn fake_historical_envelope() -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: SOURCE_ID.to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000_i64),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000_i64),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("backfill-trade-1".to_string()),
            price: Price::from_f64(65_000.25).expect("finite price"),
            quantity: Decimal::new(5, 1),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("1".to_string()),
        checkpoint: None,
    };

    BarterIngestionEnvelope::from_backfill_event(SOURCE_ID, event)
}

fn historical_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: SOURCE_ID.to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000_i64),
        end: TimestampNs::from_nanos(1_700_000_100_000_000_000_i64),
        limit: Some(1000),
        cursor: None,
    }
}

fn page(
    request: &HistoricalBackfillRequest,
    record_count: usize,
    next_start: Option<i64>,
    complete: bool,
) -> HistoricalBackfillPage {
    HistoricalBackfillPage {
        request: request.clone(),
        envelopes: (0..record_count)
            .map(|_| fake_historical_envelope())
            .collect(),
        next_cursor: next_start.map(|next_start| HistoricalCursor {
            exchange: request.exchange.clone(),
            symbol: request.symbol.clone(),
            kind: request.kind,
            next_start: Some(TimestampNs::from_nanos(next_start)),
            page_token: None,
            last_seen_exchange_id: None,
        }),
        complete,
    }
}

#[derive(Debug)]
struct ScriptedFetcher {
    pages: std::sync::Mutex<Vec<HistoricalBackfillPage>>,
    requests: std::sync::Mutex<Vec<HistoricalBackfillRequest>>,
}

impl ScriptedFetcher {
    fn new(pages: Vec<HistoricalBackfillPage>) -> Self {
        Self {
            pages: std::sync::Mutex::new(pages),
            requests: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn requests(&self) -> Vec<HistoricalBackfillRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

#[async_trait]
impl HistoricalPageFetcher for ScriptedFetcher {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        self.requests.lock().expect("requests lock").push(request);
        let mut pages = self.pages.lock().expect("pages lock");
        if pages.is_empty() {
            panic!("scripted fetcher ran out of pages")
        }
        Ok(pages.remove(0))
    }
}

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
            timeout: None,
        },
        input,
    )
    .await
    .expect_err("zero limit should be rejected");

    match error {
        BarterAdapterError::InvalidLiveCollectionRequest(message) => {
            assert_eq!(message, "limit must be greater than zero");
        }
        other => panic!("expected InvalidLiveCollectionRequest, got {other:?}"),
    }
}

#[tokio::test]
async fn live_collection_rejects_empty_source_id() {
    let input = stream::iter(vec![barter_trade_event("btc", "usdt", "trade-1")]);

    let error = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: String::new(),
            limit: 1,
            timeout: None,
        },
        input,
    )
    .await
    .expect_err("empty source_id should be rejected");

    match error {
        BarterAdapterError::InvalidLiveCollectionRequest(message) => {
            assert_eq!(message, "source_id must not be empty");
        }
        other => panic!("expected InvalidLiveCollectionRequest, got {other:?}"),
    }
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
            timeout: None,
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
            timeout: None,
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
            timeout: None,
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
            timeout: None,
        },
        input,
    )
    .await
    .expect_err("stream item error should be returned");

    assert!(error.to_string().contains("live stream item error"));
}

#[tokio::test]
async fn historical_runner_rejects_zero_max_pages() {
    let fetcher = ScriptedFetcher::new(Vec::new());

    let error = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: historical_request(),
            max_pages: 0,
            max_records: Some(10),
        },
    )
    .await
    .expect_err("zero max_pages must be rejected");

    match error {
        BarterAdapterError::InvalidHistoricalRequest(message) => {
            assert_eq!(
                message,
                "historical backfill max_pages must be greater than zero"
            );
        }
        other => panic!("expected InvalidHistoricalRequest, got {other:?}"),
    }
}

#[tokio::test]
async fn historical_runner_stops_on_source_complete() {
    let first_request = historical_request();
    let first_page = page(&first_request, 1, Some(1_700_000_010_000_000_000), false);
    let second_request = HistoricalBackfillRequest {
        start: TimestampNs::from_nanos(1_700_000_010_000_000_000_i64),
        cursor: first_page.next_cursor.clone(),
        ..first_request.clone()
    };
    let second_page = page(&second_request, 2, Some(1_700_000_020_000_000_000), true);
    let fetcher = ScriptedFetcher::new(vec![first_page.clone(), second_page.clone()]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request: first_request.clone(),
            max_pages: 5,
            max_records: None,
        },
    )
    .await
    .expect("scripted fetcher should succeed");

    assert_eq!(outcome.pages, vec![first_page.clone(), second_page.clone()]);
    assert_eq!(outcome.records_received, 3);
    assert_eq!(outcome.final_cursor, second_page.next_cursor.clone());
    assert!(outcome.complete);
    assert_eq!(
        outcome.stopped_reason,
        HistoricalBackfillStopReason::SourceComplete
    );

    let requests = fetcher.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], first_request);
    assert_eq!(requests[1].start, second_request.start);
    assert_eq!(requests[1].cursor, second_request.cursor);
}

#[tokio::test]
async fn historical_runner_stops_on_max_pages() {
    let first_request = historical_request();
    let first_page = page(&first_request, 1, Some(1_700_000_010_000_000_000), false);
    let second_request = HistoricalBackfillRequest {
        start: TimestampNs::from_nanos(1_700_000_010_000_000_000_i64),
        cursor: first_page.next_cursor.clone(),
        ..first_request.clone()
    };
    let second_page = page(&second_request, 1, Some(1_700_000_020_000_000_000), false);
    let fetcher = ScriptedFetcher::new(vec![first_page.clone(), second_page.clone()]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request,
            max_pages: 2,
            max_records: None,
        },
    )
    .await
    .expect("scripted fetcher should succeed");

    assert_eq!(outcome.pages.len(), 2);
    assert_eq!(outcome.records_received, 2);
    assert_eq!(outcome.final_cursor, second_page.next_cursor.clone());
    assert!(!outcome.complete);
    assert_eq!(
        outcome.stopped_reason,
        HistoricalBackfillStopReason::MaxPagesReached
    );
}

#[tokio::test]
async fn historical_runner_stops_on_max_records() {
    let first_request = historical_request();
    let first_page = page(&first_request, 2, Some(1_700_000_010_000_000_000), false);
    let second_request = HistoricalBackfillRequest {
        start: TimestampNs::from_nanos(1_700_000_010_000_000_000_i64),
        cursor: first_page.next_cursor.clone(),
        ..first_request.clone()
    };
    let second_page = page(&second_request, 2, Some(1_700_000_020_000_000_000), false);
    let fetcher = ScriptedFetcher::new(vec![first_page.clone(), second_page.clone()]);

    let outcome = run_historical_backfill_pages(
        &fetcher,
        HistoricalBackfillRunRequest {
            first_request,
            max_pages: 5,
            max_records: Some(3),
        },
    )
    .await
    .expect("scripted fetcher should succeed");

    assert_eq!(outcome.pages.len(), 2);
    assert_eq!(outcome.records_received, 4);
    assert_eq!(outcome.final_cursor, second_page.next_cursor.clone());
    assert!(!outcome.complete);
    assert_eq!(
        outcome.stopped_reason,
        HistoricalBackfillStopReason::MaxRecordsReached
    );
}
