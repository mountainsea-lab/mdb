use fdc_barter::{
    binance_futures_usd_funding_rate_provider_from_response,
    binance_futures_usd_mark_price_provider_from_response,
    binance_futures_usd_ohlcv_provider_from_response,
    binance_futures_usd_open_interest_provider_from_response,
    execute_binance_futures_usd_funding_rate_rest, execute_binance_futures_usd_mark_price_rest,
    execute_binance_futures_usd_ohlcv_rest, execute_binance_futures_usd_open_interest_rest,
    BarterIntegrationHistoricalRestExecutor, BarterMarketDataKind, BarterMarketType,
    HistoricalBackfillPage, HistoricalBackfillRequest, HistoricalExchangeProvider,
};
use fdc_core::types::TimestampNs;

// Manual run commands for troubleshooting:
//
// Default real Binance Futures REST run, prints visible records:
//   cargo run --example historical_binance_futures_usd_derivatives
//
// Offline fixture run, no network access:
//   MDB_BARTER_EXAMPLE_MODE=fixture cargo run --example historical_binance_futures_usd_derivatives
#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    let use_fixture = std::env::var("MDB_BARTER_EXAMPLE_MODE").as_deref() == Ok("fixture");
    println!(
        "example=historical_binance_futures_usd_derivatives mode={} symbol=BTCUSDT",
        if use_fixture {
            "fixture"
        } else {
            "real_network"
        }
    );

    let pages = if use_fixture {
        fetch_fixture_pages().await?
    } else {
        fetch_real_pages().await?
    };

    for (endpoint, page) in pages {
        print_page(endpoint, &page);
    }

    println!("example=historical_binance_futures_usd_derivatives complete=true");
    Ok(())
}

async fn fetch_fixture_pages() -> fdc_barter::Result<Vec<(&'static str, HistoricalBackfillPage)>> {
    let funding = binance_futures_usd_funding_rate_provider_from_response(
        r#"[{"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"0.00010000","markPrice":"35000.10"}]"#,
    )?
    .fetch_page(request(BarterMarketDataKind::FundingRate))
    .await?;

    let open_interest = binance_futures_usd_open_interest_provider_from_response(
        r#"{"symbol":"BTCUSDT","openInterest":"12345.678"}"#,
    )?
    .fetch_page(request(BarterMarketDataKind::OpenInterest))
    .await?;

    let mark_price = binance_futures_usd_mark_price_provider_from_response(
        r#"{
            "symbol":"BTCUSDT",
            "markPrice":"35000.10",
            "indexPrice":"34990.00",
            "estimatedSettlePrice":"34995.00",
            "lastFundingRate":"0.0001",
            "nextFundingTime":1700003600000,
            "time":1700000000000
        }"#,
    )?
    .fetch_page(request(BarterMarketDataKind::MarkPrice))
    .await?;

    let ohlcv = binance_futures_usd_ohlcv_provider_from_response(
        r#"[[1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]]"#,
    )?
    .fetch_page(request(BarterMarketDataKind::Candle))
    .await?;

    Ok(vec![
        ("/fapi/v1/fundingRate", funding),
        ("/fapi/v1/openInterest", open_interest),
        ("/fapi/v1/premiumIndex", mark_price),
        ("/fapi/v1/klines", ohlcv),
    ])
}

async fn fetch_real_pages() -> fdc_barter::Result<Vec<(&'static str, HistoricalBackfillPage)>> {
    let executor = BarterIntegrationHistoricalRestExecutor::binance_futures_usd();
    let funding = execute_binance_futures_usd_funding_rate_rest(
        &executor,
        live_request(BarterMarketDataKind::FundingRate),
    )
    .await?;
    let open_interest = execute_binance_futures_usd_open_interest_rest(
        &executor,
        live_request(BarterMarketDataKind::OpenInterest),
    )
    .await?;
    let mark_price = execute_binance_futures_usd_mark_price_rest(
        &executor,
        live_request(BarterMarketDataKind::MarkPrice),
    )
    .await?;
    let ohlcv = execute_binance_futures_usd_ohlcv_rest(
        &executor,
        live_request(BarterMarketDataKind::Candle),
    )
    .await?;

    Ok(vec![
        ("/fapi/v1/fundingRate", funding),
        ("/fapi/v1/openInterest", open_interest),
        ("/fapi/v1/premiumIndex", mark_price),
        ("/fapi/v1/klines", ohlcv),
    ])
}

fn request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "example-binance-futures-usd-derivatives".to_string(),
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

fn live_request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = match kind {
        BarterMarketDataKind::FundingRate => now_ms - 12 * 60 * 60_000,
        BarterMarketDataKind::Candle => now_ms - 10 * 60_000,
        _ => now_ms - 60_000,
    };
    let end_ms = match kind {
        BarterMarketDataKind::Candle => now_ms - 9 * 60_000,
        _ => now_ms,
    };

    HistoricalBackfillRequest {
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        ..request(kind)
    }
}

fn print_page(endpoint: &str, page: &HistoricalBackfillPage) {
    let first = page.envelopes.first();
    println!(
        "endpoint={} exchange={} market_type={:?} symbol={} kind={:?} records={} first_event_time={} first_payload={:?} complete={}",
        endpoint,
        page.request.exchange,
        page.request.market_type,
        page.request.symbol,
        page.request.kind,
        page.envelopes.len(),
        first
            .map(|envelope| envelope.event.timestamp.as_nanos().to_string())
            .unwrap_or_else(|| "none".to_string()),
        first.map(|envelope| &envelope.event.payload),
        page.complete
    );
}
