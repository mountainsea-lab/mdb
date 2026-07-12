use async_trait::async_trait;
use fdc_barter::{
    execute_binance_futures_usd_funding_rate_rest, execute_binance_futures_usd_mark_price_rest,
    execute_binance_futures_usd_ohlcv_rest, execute_binance_futures_usd_open_interest_rest,
    BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind, BarterMarketPayload,
    BarterMarketType, HistoricalBackfillRequest, HistoricalRestExecutor,
    HistoricalRestRequestDescriptor,
};
use fdc_core::types::TimestampNs;

fn request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-futures-usd-history".to_string(),
        exchange: "binance_futures_usd".to_string(),
        market_type: BarterMarketType::Perpetual,
        symbol: "BTCUSDT".to_string(),
        kind,
        interval: (kind == BarterMarketDataKind::Candle).then(|| "1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_120_000_000_000),
        limit: Some(1),
        cursor: None,
    }
}

struct FakeFuturesExecutor;

#[async_trait]
impl HistoricalRestExecutor for FakeFuturesExecutor {
    async fn execute(
        &self,
        descriptor: &HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        assert_eq!(descriptor.exchange, "binance_futures_usd");
        assert_eq!(descriptor.method, "GET");
        assert!(descriptor
            .query
            .contains(&("symbol".to_string(), "BTCUSDT".to_string())));

        let body = match descriptor.path.as_str() {
            "/fapi/v1/fundingRate" => {
                assert!(descriptor
                    .query
                    .contains(&("limit".to_string(), "1".to_string())));
                r#"[{"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"0.00010000","markPrice":"35000.10"}]"#
            }
            "/fapi/v1/openInterest" => r#"{"symbol":"BTCUSDT","openInterest":"12345.678"}"#,
            "/fapi/v1/premiumIndex" => {
                r#"{
                "symbol":"BTCUSDT",
                "markPrice":"35000.10",
                "indexPrice":"34990.00",
                "estimatedSettlePrice":"34995.00",
                "lastFundingRate":"0.0001",
                "nextFundingTime":1700003600000,
                "time":1700000000000
            }"#
            }
            "/fapi/v1/klines" => {
                assert!(descriptor
                    .query
                    .contains(&("interval".to_string(), "1m".to_string())));
                r#"[[1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]]"#
            }
            unsupported => panic!("unexpected descriptor path {unsupported}"),
        };

        Ok(body.to_string())
    }
}

#[tokio::test]
async fn executor_fetches_binance_futures_funding_rate_page_without_network() {
    let page = execute_binance_futures_usd_funding_rate_rest(
        &FakeFuturesExecutor,
        request(BarterMarketDataKind::FundingRate),
    )
    .await
    .expect("fake funding response should parse");

    assert_eq!(page.envelopes.len(), 1);
    assert!(!page.complete);
    assert_eq!(page.envelopes[0].event.exchange, "binance_futures_usd");
    assert_eq!(
        page.envelopes[0].event.kind,
        BarterMarketDataKind::FundingRate
    );
    let BarterMarketPayload::FundingRate(payload) = &page.envelopes[0].event.payload else {
        panic!("expected funding payload");
    };
    assert_eq!(payload.funding_rate.to_string(), "0.00010000");
}

#[tokio::test]
async fn executor_fetches_binance_futures_open_interest_page_without_network() {
    let page = execute_binance_futures_usd_open_interest_rest(
        &FakeFuturesExecutor,
        request(BarterMarketDataKind::OpenInterest),
    )
    .await
    .expect("fake open interest response should parse");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::OpenInterest(payload) = &page.envelopes[0].event.payload else {
        panic!("expected open interest payload");
    };
    assert_eq!(payload.open_interest.to_string(), "12345.678");
}

#[tokio::test]
async fn executor_fetches_binance_futures_mark_price_page_without_network() {
    let page = execute_binance_futures_usd_mark_price_rest(
        &FakeFuturesExecutor,
        request(BarterMarketDataKind::MarkPrice),
    )
    .await
    .expect("fake mark price response should parse");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::MarkPrice(payload) = &page.envelopes[0].event.payload else {
        panic!("expected mark price payload");
    };
    assert_eq!(payload.mark_price.to_f64(), 35000.10);
}

#[tokio::test]
async fn executor_fetches_binance_futures_ohlcv_page_without_network() {
    let page = execute_binance_futures_usd_ohlcv_rest(
        &FakeFuturesExecutor,
        request(BarterMarketDataKind::Candle),
    )
    .await
    .expect("fake futures kline response should parse");

    assert_eq!(page.envelopes.len(), 1);
    assert!(!page.complete);
    let BarterMarketPayload::Candle(payload) = &page.envelopes[0].event.payload else {
        panic!("expected candle payload");
    };
    assert_eq!(payload.interval.as_deref(), Some("1m"));
    assert_eq!(payload.open.to_f64(), 100.10);
}

#[tokio::test]
async fn binance_futures_ohlcv_page_fetcher_delegates_to_rest_executor() {
    use fdc_barter::{BinanceFuturesUsdOhlcvHistoricalPageFetcher, HistoricalPageFetcher};

    let fetcher = BinanceFuturesUsdOhlcvHistoricalPageFetcher::new(&FakeFuturesExecutor);
    let page = fetcher
        .fetch_page(request(BarterMarketDataKind::Candle))
        .await
        .expect("futures candle page should fetch");

    assert_eq!(page.envelopes.len(), 1);
    assert_eq!(page.envelopes[0].event.exchange, "binance_futures_usd");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Candle);
}

#[tokio::test]
#[ignore = "requires MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1 and public Binance Futures REST access"]
async fn ignored_live_smoke_fetches_one_binance_futures_funding_rate_record() {
    if std::env::var("MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES").as_deref() != Ok("1") {
        eprintln!(
            "set MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1 to run real Binance Futures REST smoke"
        );
        return;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - 12 * 60 * 60_000;
    let end_ms = now_ms;

    let request = HistoricalBackfillRequest {
        source_id: "barter-binance-futures-usd-history-smoke".to_string(),
        exchange: "binance_futures_usd".to_string(),
        market_type: BarterMarketType::Perpetual,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::FundingRate,
        interval: None,
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        limit: Some(1),
        cursor: None,
    };

    let executor = BarterIntegrationHistoricalRestExecutor::binance_futures_usd();
    let page = execute_binance_futures_usd_funding_rate_rest(&executor, request)
        .await
        .expect("real Binance Futures funding smoke should fetch and parse one page");

    assert!(!page.envelopes.is_empty());
    assert!(page.envelopes[0].quality.is_backfill);
    assert_eq!(page.envelopes[0].event.exchange, "binance_futures_usd");
    assert_eq!(
        page.envelopes[0].event.kind,
        BarterMarketDataKind::FundingRate
    );
}
