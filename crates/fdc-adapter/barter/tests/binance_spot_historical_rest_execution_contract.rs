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
