use async_trait::async_trait;
use fdc_barter::{
    binance_spot_historical_trades_provider_from_response,
    binance_spot_historical_trades_rest_request_descriptor,
    execute_binance_spot_historical_trades_rest, historical_trade_dedupe_key, BarterMarketDataKind,
    BarterMarketPayload, BarterMarketType, HistoricalBackfillRequest, HistoricalExchangeProvider,
    HistoricalRestExecutor, HistoricalRestRequestDescriptor, TradeSide,
};
use fdc_core::types::TimestampNs;

fn trade_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-trades-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

fn sample_agg_trades() -> &'static str {
    r#"[
        {"a":26129,"p":"100.10","q":"0.25000000","f":27781,"l":27781,"T":1700000000000,"m":true,"M":true},
        {"a":26130,"p":"101.20","q":"1.50000000","f":27782,"l":27783,"T":1700000001000,"m":false,"M":true}
    ]"#
}

#[test]
fn binance_spot_historical_trades_descriptor_matches_agg_trades_shape() {
    let descriptor = binance_spot_historical_trades_rest_request_descriptor(&trade_request())
        .expect("valid trade request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_spot");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/api/v3/aggTrades");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("startTime".to_string(), "1700000000000".to_string()),
            ("endTime".to_string(), "1700000060000".to_string()),
            ("limit".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn binance_spot_historical_trades_descriptor_rejects_unsupported_shape() {
    let mut candle = trade_request();
    candle.kind = BarterMarketDataKind::Candle;
    assert!(binance_spot_historical_trades_rest_request_descriptor(&candle).is_err());

    let mut futures = trade_request();
    futures.market_type = BarterMarketType::Perpetual;
    assert!(binance_spot_historical_trades_rest_request_descriptor(&futures).is_err());

    let mut high_limit = trade_request();
    high_limit.limit = Some(1001);
    let error = binance_spot_historical_trades_rest_request_descriptor(&high_limit)
        .expect_err("Binance aggregate trades max limit is 1000");
    assert!(error.to_string().contains("limit"));
}

#[tokio::test]
async fn binance_spot_historical_trades_provider_maps_agg_trades_to_trade_envelopes() {
    let provider = binance_spot_historical_trades_provider_from_response(sample_agg_trades())
        .expect("sample aggregate trades should parse");

    let page = provider
        .fetch_page(trade_request())
        .await
        .expect("provider should return parsed trades");

    assert_eq!(page.envelopes.len(), 2);
    assert!(!page.complete);
    assert_eq!(
        page.next_cursor
            .as_ref()
            .unwrap()
            .next_start
            .unwrap()
            .as_nanos(),
        1_700_000_001_001_000_000
    );

    let first = &page.envelopes[0];
    assert!(first.quality.is_backfill);
    assert_eq!(first.event.exchange, "binance_spot");
    assert_eq!(first.event.symbol.as_str(), "BTCUSDT");
    assert_eq!(first.event.timestamp.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(first.event.sequence.as_deref(), Some("26129"));

    let BarterMarketPayload::Trade(trade) = &first.event.payload else {
        panic!("expected trade payload");
    };
    assert_eq!(trade.trade_id.as_deref(), Some("26129"));
    assert_eq!(trade.price.to_f64(), 100.10);
    assert_eq!(trade.quantity.to_string(), "0.25000000");
    assert_eq!(trade.side, Some(TradeSide::Sell));
    assert_eq!(
        historical_trade_dedupe_key(&first.event).unwrap(),
        "binance_spot:BTCUSDT:26129"
    );

    let BarterMarketPayload::Trade(second) = &page.envelopes[1].event.payload else {
        panic!("expected trade payload");
    };
    assert_eq!(second.side, Some(TradeSide::Buy));
}

#[tokio::test]
async fn binance_spot_historical_trades_provider_marks_complete_when_short_page() {
    let provider = binance_spot_historical_trades_provider_from_response(sample_agg_trades())
        .expect("sample aggregate trades should parse");

    let mut request = trade_request();
    request.limit = Some(3);
    let page = provider
        .fetch_page(request)
        .await
        .expect("provider should return page");

    assert!(page.complete);
    assert!(page.next_cursor.is_none());
}

struct FakeTradesExecutor;

#[async_trait]
impl HistoricalRestExecutor for FakeTradesExecutor {
    async fn execute(
        &self,
        descriptor: &HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        assert_eq!(descriptor.exchange, "binance_spot");
        assert_eq!(descriptor.method, "GET");
        assert_eq!(descriptor.path, "/api/v3/aggTrades");
        assert!(descriptor
            .query
            .contains(&("symbol".to_string(), "BTCUSDT".to_string())));
        Ok(sample_agg_trades().to_string())
    }
}

#[tokio::test]
async fn fake_executor_fetches_binance_spot_historical_trades_without_network() {
    let page = execute_binance_spot_historical_trades_rest(&FakeTradesExecutor, trade_request())
        .await
        .expect("fake executor response should parse into historical trades");

    assert_eq!(page.envelopes.len(), 2);
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Trade);
}

#[tokio::test]
#[ignore = "requires FDC_BARTER_HISTORICAL_SMOKE=1 and public Binance REST access"]
async fn ignored_live_smoke_fetches_binance_spot_historical_trades() {
    if std::env::var("FDC_BARTER_HISTORICAL_SMOKE").as_deref() != Ok("1") {
        eprintln!("set FDC_BARTER_HISTORICAL_SMOKE=1 to run real historical trades smoke");
        return;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - 10 * 60_000;
    let end_ms = now_ms - 9 * 60_000;

    let request = HistoricalBackfillRequest {
        source_id: "barter-binance-spot-trades-history-smoke".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        limit: Some(10),
        cursor: None,
    };

    let executor = fdc_barter::BarterIntegrationHistoricalRestExecutor::binance_spot();
    let page = execute_binance_spot_historical_trades_rest(&executor, request)
        .await
        .expect("real Binance Spot aggregate trades smoke should fetch and parse one page");

    assert!(!page.envelopes.is_empty());
    assert!(page.envelopes[0].quality.is_backfill);
    assert_eq!(page.envelopes[0].event.exchange, "binance_spot");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Trade);
}
