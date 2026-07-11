use fdc_barter::{
    binance_futures_usd_funding_rate_provider_from_response,
    binance_futures_usd_mark_price_provider_from_response,
    binance_futures_usd_ohlcv_provider_from_response,
    binance_futures_usd_open_interest_provider_from_response, BarterMarketDataKind,
    BarterMarketPayload, BarterMarketType, HistoricalBackfillRequest, HistoricalExchangeProvider,
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
        limit: Some(2),
        cursor: None,
    }
}

#[tokio::test]
async fn funding_provider_maps_response_to_envelopes() {
    let response = r#"[
        {"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"0.00010000","markPrice":"35000.10"}
    ]"#;
    let provider = binance_futures_usd_funding_rate_provider_from_response(response)
        .expect("funding fixture should parse");
    let page = provider
        .fetch_page(request(BarterMarketDataKind::FundingRate))
        .await
        .expect("provider should return funding page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let first = &page.envelopes[0];
    assert!(first.quality.is_backfill);
    assert_eq!(first.event.exchange, "binance_futures_usd");
    assert_eq!(first.event.market_type, BarterMarketType::Perpetual);
    assert_eq!(first.event.symbol.as_str(), "BTCUSDT");
    assert_eq!(first.event.timestamp.as_nanos(), 1_700_000_000_000_000_000);

    let BarterMarketPayload::FundingRate(payload) = &first.event.payload else {
        panic!("expected funding payload");
    };
    assert_eq!(payload.funding_rate.to_string(), "0.00010000");
    assert_eq!(payload.funding_time.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(payload.mark_price.as_ref().unwrap().to_f64(), 35000.10);
}

#[tokio::test]
async fn open_interest_provider_maps_response_to_envelope() {
    let response = r#"{"symbol":"BTCUSDT","openInterest":"12345.678"}"#;
    let provider = binance_futures_usd_open_interest_provider_from_response(response)
        .expect("open interest fixture should parse");
    let page = provider
        .fetch_page(request(BarterMarketDataKind::OpenInterest))
        .await
        .expect("provider should return open interest page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::OpenInterest(payload) = &page.envelopes[0].event.payload else {
        panic!("expected open interest payload");
    };
    assert_eq!(payload.open_interest.to_string(), "12345.678");
    assert_eq!(payload.timestamp, page.envelopes[0].event.received_at);
}

#[tokio::test]
async fn mark_price_provider_maps_premium_index_to_envelope() {
    let response = r#"{
        "symbol":"BTCUSDT",
        "markPrice":"35000.10",
        "indexPrice":"34990.00",
        "estimatedSettlePrice":"34995.00",
        "lastFundingRate":"0.0001",
        "nextFundingTime":1700003600000,
        "time":1700000000000
    }"#;
    let provider = binance_futures_usd_mark_price_provider_from_response(response)
        .expect("premium index fixture should parse");
    let page = provider
        .fetch_page(request(BarterMarketDataKind::MarkPrice))
        .await
        .expect("provider should return mark price page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::MarkPrice(payload) = &page.envelopes[0].event.payload else {
        panic!("expected mark price payload");
    };
    assert_eq!(payload.mark_price.to_f64(), 35000.10);
    assert_eq!(payload.index_price.as_ref().unwrap().to_f64(), 34990.00);
    assert_eq!(
        payload.estimated_settle_price.as_ref().unwrap().to_f64(),
        34995.00
    );
    assert_eq!(payload.funding_rate.unwrap().to_string(), "0.0001");
    assert_eq!(
        payload.next_funding_time.unwrap().as_nanos(),
        1_700_003_600_000_000_000
    );
}

#[tokio::test]
async fn futures_ohlcv_provider_maps_klines_to_candle_envelopes() {
    let response = r#"[
        [1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]
    ]"#;
    let provider = binance_futures_usd_ohlcv_provider_from_response(response)
        .expect("futures kline fixture should parse");
    let page = provider
        .fetch_page(request(BarterMarketDataKind::Candle))
        .await
        .expect("provider should return candle page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::Candle(payload) = &page.envelopes[0].event.payload else {
        panic!("expected candle payload");
    };
    assert_eq!(payload.interval.as_deref(), Some("1m"));
    assert_eq!(payload.open.to_f64(), 100.10);
    assert_eq!(payload.high.to_f64(), 110.20);
    assert_eq!(payload.low.to_f64(), 90.30);
    assert_eq!(payload.close.to_f64(), 105.40);
    assert_eq!(payload.volume.to_string(), "123.45000000");
    assert_eq!(payload.trade_count, Some(42));
}

#[test]
fn futures_provider_rejects_invalid_numeric_payloads() {
    let invalid = r#"[
        {"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"not-a-number","markPrice":"35000.10"}
    ]"#;
    let error = binance_futures_usd_funding_rate_provider_from_response(invalid)
        .expect_err("invalid numeric payload should be rejected");
    assert!(error.to_string().contains("invalid numeric value"));
}
