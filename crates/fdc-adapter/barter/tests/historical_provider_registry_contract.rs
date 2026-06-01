use async_trait::async_trait;
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketType, HistoricalBackfillPage,
    HistoricalBackfillRequest, HistoricalBackfillSource, HistoricalCursor,
    HistoricalExchangeProvider, HistoricalProviderCapabilities, HistoricalProviderRegistry,
};
use fdc_core::types::TimestampNs;

fn request(exchange: &str, kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: format!("barter-{exchange}-history"),
        exchange: exchange.to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(500),
        cursor: None,
    }
}

struct FakeProvider {
    capabilities: HistoricalProviderCapabilities,
}

impl FakeProvider {
    fn binance_spot() -> Self {
        Self {
            capabilities: HistoricalProviderCapabilities {
                exchange: "binance_spot".to_string(),
                market_types: vec![BarterMarketType::Spot],
                kinds: vec![BarterMarketDataKind::Candle],
                intervals: vec!["1m".to_string(), "5m".to_string()],
                max_limit: Some(1000),
            },
        }
    }
}

#[async_trait]
impl HistoricalExchangeProvider for FakeProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> fdc_barter::Result<HistoricalBackfillPage> {
        Ok(HistoricalBackfillPage {
            next_cursor: Some(HistoricalCursor::next_start(
                request.exchange.clone(),
                request.symbol.clone(),
                request.kind,
                request.end,
            )),
            request,
            envelopes: Vec::<BarterIngestionEnvelope>::new(),
            complete: true,
        })
    }
}

#[tokio::test]
async fn registry_dispatches_supported_request_to_matching_exchange_provider() {
    let registry = HistoricalProviderRegistry::new().with_provider(FakeProvider::binance_spot());

    let page = registry
        .fetch_page(request("binance_spot", BarterMarketDataKind::Candle))
        .await
        .expect("registry should dispatch to matching provider");

    assert_eq!(page.request.exchange, "binance_spot");
    assert!(page.complete);
    assert_eq!(page.next_cursor.as_ref().unwrap().symbol, "BTCUSDT");
}

#[tokio::test]
async fn registry_implements_historical_backfill_source_for_compatibility() {
    let source: Box<dyn HistoricalBackfillSource> =
        Box::new(HistoricalProviderRegistry::new().with_provider(FakeProvider::binance_spot()));

    let page = source
        .fetch_page(request("binance_spot", BarterMarketDataKind::Candle))
        .await
        .expect("registry should remain usable through source trait");

    assert_eq!(page.request.source_id, "barter-binance_spot-history");
}

#[tokio::test]
async fn registry_rejects_unknown_exchange_before_provider_io() {
    let registry = HistoricalProviderRegistry::new().with_provider(FakeProvider::binance_spot());

    let error = registry
        .fetch_page(request("okx", BarterMarketDataKind::Candle))
        .await
        .expect_err("unknown exchange should be rejected");

    assert!(error
        .to_string()
        .contains("unsupported historical exchange"));
}

#[tokio::test]
async fn registry_rejects_unsupported_kind_interval_and_limit() {
    let registry = HistoricalProviderRegistry::new().with_provider(FakeProvider::binance_spot());

    let unsupported_kind = registry
        .fetch_page(request("binance_spot", BarterMarketDataKind::Trade))
        .await
        .expect_err("trade is not in fake provider capabilities");
    assert!(unsupported_kind
        .to_string()
        .contains("unsupported historical subscription"));

    let mut unsupported_interval = request("binance_spot", BarterMarketDataKind::Candle);
    unsupported_interval.interval = Some("2m".to_string());
    let interval_error = registry
        .fetch_page(unsupported_interval)
        .await
        .expect_err("unsupported interval should be rejected");
    assert!(interval_error.to_string().contains("interval"));

    let mut limit_too_high = request("binance_spot", BarterMarketDataKind::Candle);
    limit_too_high.limit = Some(1001);
    let limit_error = registry
        .fetch_page(limit_too_high)
        .await
        .expect_err("limit above provider max should be rejected");
    assert!(limit_error.to_string().contains("limit"));
}
