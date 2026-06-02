use async_trait::async_trait;
use fdc_barter::{
    execute_binance_spot_ohlcv_rest, BarterMarketDataKind, BarterMarketPayload, BarterMarketType,
    HistoricalBackfillRequest, HistoricalRestExecutor, HistoricalRestRequestDescriptor,
};
use fdc_core::types::TimestampNs;

fn ohlcv_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(1),
        cursor: None,
    }
}

struct FakeExecutor;

#[async_trait]
impl HistoricalRestExecutor for FakeExecutor {
    async fn execute(
        &self,
        descriptor: &HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        assert_eq!(descriptor.exchange, "binance_spot");
        assert_eq!(descriptor.method, "GET");
        assert_eq!(descriptor.path, "/api/v3/klines");
        assert!(descriptor
            .query
            .contains(&("symbol".to_string(), "BTCUSDT".to_string())));
        assert!(descriptor
            .query
            .contains(&("interval".to_string(), "1m".to_string())));

        Ok(r#"[[1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]]"#.to_string())
    }
}

#[tokio::test]
async fn executor_fetches_binance_spot_ohlcv_page_without_network_in_default_tests() {
    let page = execute_binance_spot_ohlcv_rest(&FakeExecutor, ohlcv_request())
        .await
        .expect("fake executor response should parse into a historical page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(!page.complete);
    assert!(page.envelopes[0].quality.is_backfill);

    let BarterMarketPayload::Candle(candle) = &page.envelopes[0].event.payload else {
        panic!("expected candle payload");
    };

    assert_eq!(candle.interval.as_deref(), Some("1m"));
    assert_eq!(candle.open.to_f64(), 100.10);
    assert_eq!(candle.trade_count, Some(42));
    assert_eq!(candle.quote_volume.unwrap().to_string(), "12999.99000000");
}

#[tokio::test]
#[ignore = "requires FDC_BARTER_HISTORICAL_SMOKE=1 and public Binance REST access"]
async fn ignored_live_smoke_fetches_one_binance_spot_ohlcv_candle() {
    if std::env::var("FDC_BARTER_HISTORICAL_SMOKE").as_deref() != Ok("1") {
        eprintln!("set FDC_BARTER_HISTORICAL_SMOKE=1 to run real historical REST smoke");
        return;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - 10 * 60_000;
    let end_ms = now_ms - 9 * 60_000;

    let request = HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history-smoke".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        limit: Some(1),
        cursor: None,
    };

    let executor = fdc_barter::BarterIntegrationHistoricalRestExecutor::binance_spot();
    let page = execute_binance_spot_ohlcv_rest(&executor, request)
        .await
        .expect("real Binance Spot kline smoke should fetch and parse one page");

    assert!(!page.envelopes.is_empty());
    assert!(page.envelopes[0].quality.is_backfill);
    assert_eq!(page.envelopes[0].event.exchange, "binance_spot");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Candle);
}
