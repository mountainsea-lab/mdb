use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use barter_integration::{
    error::SocketError,
    protocol::http::{
        public::PublicNoHeaders,
        rest::{client::RestClient, RestRequest},
        HttpParser,
    },
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    model::{
        BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
        BarterMarketType, CandlePayload, FundingRatePayload, HistoricalCursor, MarkPricePayload,
        OpenInterestPayload, TradePayload, TradeSide,
    },
};

/// Adapter-owned request for one historical backfill page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalBackfillRequest {
    pub source_id: String,
    pub exchange: String,
    pub market_type: BarterMarketType,
    pub symbol: String,
    pub kind: BarterMarketDataKind,
    pub interval: Option<String>,
    pub start: TimestampNs,
    pub end: TimestampNs,
    pub limit: Option<usize>,
    pub cursor: Option<HistoricalCursor>,
}

/// One page of historical backfill envelopes and pagination metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBackfillPage {
    pub request: HistoricalBackfillRequest,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

/// Compact result projection for historical page processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalPageOutcome {
    pub records_received: usize,
    pub next_cursor: Option<HistoricalCursor>,
    pub complete: bool,
}

impl From<&HistoricalBackfillPage> for HistoricalPageOutcome {
    fn from(page: &HistoricalBackfillPage) -> Self {
        Self {
            records_received: page.envelopes.len(),
            next_cursor: page.next_cursor.clone(),
            complete: page.complete,
        }
    }
}

/// Capability metadata exposed by one historical exchange provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalProviderCapabilities {
    pub exchange: String,
    pub market_types: Vec<BarterMarketType>,
    pub kinds: Vec<BarterMarketDataKind>,
    pub intervals: Vec<String>,
    pub max_limit: Option<usize>,
}

impl HistoricalProviderCapabilities {
    fn normalized_exchange(&self) -> String {
        normalize_exchange(&self.exchange)
    }

    fn validate_request(&self, request: &HistoricalBackfillRequest) -> Result<()> {
        if !self.market_types.contains(&request.market_type) {
            return Err(BarterAdapterError::UnsupportedHistoricalSubscription(
                format!(
                    "exchange {} does not support market type {:?}",
                    self.exchange, request.market_type
                ),
            ));
        }

        if !self.kinds.contains(&request.kind) {
            return Err(BarterAdapterError::UnsupportedHistoricalSubscription(
                format!(
                    "exchange {} does not support kind {:?}",
                    self.exchange, request.kind
                ),
            ));
        }

        if request.kind == BarterMarketDataKind::Candle {
            let interval = request.interval.as_deref().unwrap_or_default();
            if !self.intervals.is_empty() && !self.intervals.iter().any(|item| item == interval) {
                return Err(BarterAdapterError::UnsupportedHistoricalSubscription(
                    format!(
                        "exchange {} does not support interval {}",
                        self.exchange, interval
                    ),
                ));
            }
        }

        if let (Some(limit), Some(max_limit)) = (request.limit, self.max_limit) {
            if limit > max_limit {
                return Err(BarterAdapterError::UnsupportedHistoricalSubscription(
                    format!(
                        "limit {limit} exceeds max_limit {max_limit} for exchange {}",
                        self.exchange
                    ),
                ));
            }
        }

        Ok(())
    }
}

/// Barter-integration-inspired descriptor for a public historical REST request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalRestRequestDescriptor {
    pub exchange: String,
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub timeout_ms: u64,
}

/// Adapter-owned execution boundary for public historical REST descriptors.
#[async_trait]
pub trait HistoricalRestExecutor: Send + Sync {
    async fn execute(&self, descriptor: &HistoricalRestRequestDescriptor) -> Result<String>;
}

#[derive(Debug, Clone, Serialize)]
struct BinanceSpotKlinesQuery {
    symbol: String,
    interval: String,
    #[serde(rename = "startTime")]
    start_time: String,
    #[serde(rename = "endTime")]
    end_time: String,
    limit: String,
}

#[derive(Debug, Clone)]
struct BinanceSpotKlinesRestRequest {
    path: String,
    query: BinanceSpotKlinesQuery,
}

impl BinanceSpotKlinesRestRequest {
    fn from_descriptor(descriptor: &HistoricalRestRequestDescriptor) -> Result<Self> {
        if descriptor.exchange != "binance_spot" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST exchange {}",
                descriptor.exchange
            )));
        }
        if descriptor.method != "GET" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST method {}",
                descriptor.method
            )));
        }
        if descriptor.path != "/api/v3/klines" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported Binance Spot historical path {}",
                descriptor.path
            )));
        }

        let query_value = |key: &str| -> Result<String> {
            descriptor
                .query
                .iter()
                .find_map(|(candidate, value)| (candidate == key).then(|| value.clone()))
                .ok_or_else(|| {
                    BarterAdapterError::HistoricalRest(format!("missing query param {key}"))
                })
        };

        Ok(Self {
            path: descriptor.path.clone(),
            query: BinanceSpotKlinesQuery {
                symbol: query_value("symbol")?,
                interval: query_value("interval")?,
                start_time: query_value("startTime")?,
                end_time: query_value("endTime")?,
                limit: query_value("limit")?,
            },
        })
    }
}

impl RestRequest for BinanceSpotKlinesRestRequest {
    type Response = serde_json::Value;
    type QueryParams = BinanceSpotKlinesQuery;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Owned(self.path.clone())
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.query)
    }

    fn timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Debug, Clone, Serialize)]
struct BinanceSpotAggTradesQuery {
    symbol: String,
    #[serde(rename = "startTime")]
    start_time: String,
    #[serde(rename = "endTime")]
    end_time: String,
    limit: String,
}

#[derive(Debug, Clone)]
struct BinanceSpotAggTradesRestRequest {
    path: String,
    query: BinanceSpotAggTradesQuery,
}

impl BinanceSpotAggTradesRestRequest {
    fn from_descriptor(descriptor: &HistoricalRestRequestDescriptor) -> Result<Self> {
        if descriptor.exchange != "binance_spot" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST exchange {}",
                descriptor.exchange
            )));
        }
        if descriptor.method != "GET" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST method {}",
                descriptor.method
            )));
        }
        if descriptor.path != "/api/v3/aggTrades" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported Binance Spot historical trades path {}",
                descriptor.path
            )));
        }

        let query_value = |key: &str| -> Result<String> {
            descriptor
                .query
                .iter()
                .find_map(|(candidate, value)| (candidate == key).then(|| value.clone()))
                .ok_or_else(|| {
                    BarterAdapterError::HistoricalRest(format!("missing query param {key}"))
                })
        };

        Ok(Self {
            path: descriptor.path.clone(),
            query: BinanceSpotAggTradesQuery {
                symbol: query_value("symbol")?,
                start_time: query_value("startTime")?,
                end_time: query_value("endTime")?,
                limit: query_value("limit")?,
            },
        })
    }
}

impl RestRequest for BinanceSpotAggTradesRestRequest {
    type Response = serde_json::Value;
    type QueryParams = BinanceSpotAggTradesQuery;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Owned(self.path.clone())
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.query)
    }

    fn timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceSpotApiError {
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Debug, Clone)]
struct BinanceSpotHistoricalHttpParser;

#[derive(Debug, thiserror::Error)]
enum BinanceSpotHistoricalRestError {
    #[error(transparent)]
    Socket(#[from] SocketError),
    #[error("Binance Spot historical REST API error status={status}: code={code:?} msg={msg:?}")]
    Api {
        status: reqwest::StatusCode,
        code: Option<i64>,
        msg: Option<String>,
    },
}

impl HttpParser for BinanceSpotHistoricalHttpParser {
    type ApiError = BinanceSpotApiError;
    type OutputError = BinanceSpotHistoricalRestError;

    fn parse_api_error(
        &self,
        status: reqwest::StatusCode,
        error: Self::ApiError,
    ) -> Self::OutputError {
        BinanceSpotHistoricalRestError::Api {
            status,
            code: error.code,
            msg: error.msg,
        }
    }
}

impl From<BinanceSpotHistoricalRestError> for BarterAdapterError {
    fn from(error: BinanceSpotHistoricalRestError) -> Self {
        BarterAdapterError::HistoricalRest(error.to_string())
    }
}

/// Historical REST executor backed by barter-integration's RestClient.
#[derive(Debug)]
pub struct BarterIntegrationHistoricalRestExecutor {
    client: RestClient<PublicNoHeaders, BinanceSpotHistoricalHttpParser>,
}

impl BarterIntegrationHistoricalRestExecutor {
    pub fn binance_spot() -> Self {
        Self {
            client: RestClient::new(
                "https://api.binance.com",
                PublicNoHeaders,
                BinanceSpotHistoricalHttpParser,
            ),
        }
    }
}

#[async_trait]
impl HistoricalRestExecutor for BarterIntegrationHistoricalRestExecutor {
    async fn execute(&self, descriptor: &HistoricalRestRequestDescriptor) -> Result<String> {
        let payload = match descriptor.path.as_str() {
            "/api/v3/klines" => {
                let request = BinanceSpotKlinesRestRequest::from_descriptor(descriptor)?;
                let (payload, _metric) = self
                    .client
                    .execute(request)
                    .await
                    .map_err(BarterAdapterError::from)?;
                payload
            }
            "/api/v3/aggTrades" => {
                let request = BinanceSpotAggTradesRestRequest::from_descriptor(descriptor)?;
                let (payload, _metric) = self
                    .client
                    .execute(request)
                    .await
                    .map_err(BarterAdapterError::from)?;
                payload
            }
            unsupported => {
                return Err(BarterAdapterError::HistoricalRest(format!(
                    "unsupported Binance Spot historical REST path {unsupported}"
                )));
            }
        };

        serde_json::to_string(&payload)
            .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))
    }
}

/// Offline-testable boundary implemented by concrete historical providers.
#[async_trait]
pub trait HistoricalBackfillSource: Send + Sync {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage>;
}

/// Multi-exchange historical provider boundary.
#[async_trait]
pub trait HistoricalExchangeProvider: Send + Sync {
    fn capabilities(&self) -> &HistoricalProviderCapabilities;

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage>;
}

/// Registry that routes historical backfill requests to exchange-specific providers.
#[derive(Clone, Default)]
pub struct HistoricalProviderRegistry {
    providers: HashMap<String, Arc<dyn HistoricalExchangeProvider>>,
}

impl std::fmt::Debug for HistoricalProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoricalProviderRegistry")
            .field("exchanges", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl HistoricalProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_provider(mut self, provider: impl HistoricalExchangeProvider + 'static) -> Self {
        self.register_provider(provider);
        self
    }

    pub fn register_provider(&mut self, provider: impl HistoricalExchangeProvider + 'static) {
        let exchange = provider.capabilities().normalized_exchange();
        self.providers.insert(exchange, Arc::new(provider));
    }

    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }

    pub fn capabilities(&self) -> Vec<HistoricalProviderCapabilities> {
        self.providers
            .values()
            .map(|provider| provider.capabilities().clone())
            .collect()
    }
}

#[async_trait]
impl HistoricalBackfillSource for HistoricalProviderRegistry {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;

        let exchange = normalize_exchange(&request.exchange);
        let provider = self.providers.get(&exchange).ok_or_else(|| {
            BarterAdapterError::UnsupportedHistoricalExchange(request.exchange.clone())
        })?;

        provider.capabilities().validate_request(&request)?;
        provider.fetch_page(request).await
    }
}

/// Validate a historical page request before provider I/O.
pub fn validate_historical_backfill_request(request: &HistoricalBackfillRequest) -> Result<()> {
    if request.source_id.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "source_id is empty".to_string(),
        ));
    }
    if request.exchange.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "exchange is empty".to_string(),
        ));
    }
    if request.symbol.trim().is_empty() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "symbol is empty".to_string(),
        ));
    }
    if request.start.as_nanos() >= request.end.as_nanos() {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "start must be before end".to_string(),
        ));
    }
    if request.kind == BarterMarketDataKind::Candle
        && request.interval.as_deref().unwrap_or("").trim().is_empty()
    {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "candle interval is required".to_string(),
        ));
    }

    Ok(())
}

/// Build the public Binance Spot klines REST descriptor for one OHLCV page.
pub fn binance_spot_ohlcv_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    validate_historical_backfill_request(request)?;

    let capabilities = binance_spot_ohlcv_capabilities();
    capabilities.validate_request(request)?;

    if normalize_exchange(&request.exchange) != capabilities.normalized_exchange() {
        return Err(BarterAdapterError::UnsupportedHistoricalExchange(
            request.exchange.clone(),
        ));
    }

    let interval = request.interval.clone().ok_or_else(|| {
        BarterAdapterError::InvalidHistoricalRequest("candle interval is required".to_string())
    })?;

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/api/v3/klines".to_string(),
        query: vec![
            ("symbol".to_string(), request.symbol.to_ascii_uppercase()),
            ("interval".to_string(), interval),
            (
                "startTime".to_string(),
                nanos_to_millis(request.start).to_string(),
            ),
            (
                "endTime".to_string(),
                nanos_to_millis(request.end).to_string(),
            ),
            (
                "limit".to_string(),
                request.limit.unwrap_or(500).to_string(),
            ),
        ],
        timeout_ms: 5_000,
    })
}

pub fn binance_spot_ohlcv_capabilities() -> HistoricalProviderCapabilities {
    HistoricalProviderCapabilities {
        exchange: "binance_spot".to_string(),
        market_types: vec![BarterMarketType::Spot],
        kinds: vec![BarterMarketDataKind::Candle],
        intervals: vec![
            "1s".to_string(),
            "1m".to_string(),
            "3m".to_string(),
            "5m".to_string(),
            "15m".to_string(),
            "30m".to_string(),
            "1h".to_string(),
            "2h".to_string(),
            "4h".to_string(),
            "6h".to_string(),
            "8h".to_string(),
            "12h".to_string(),
            "1d".to_string(),
            "3d".to_string(),
            "1w".to_string(),
            "1M".to_string(),
        ],
        max_limit: Some(1000),
    }
}

pub fn binance_spot_historical_trades_capabilities() -> HistoricalProviderCapabilities {
    HistoricalProviderCapabilities {
        exchange: "binance_spot".to_string(),
        market_types: vec![BarterMarketType::Spot],
        kinds: vec![BarterMarketDataKind::Trade],
        intervals: Vec::new(),
        max_limit: Some(1000),
    }
}

pub fn binance_spot_historical_trades_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    validate_historical_backfill_request(request)?;

    let capabilities = binance_spot_historical_trades_capabilities();
    capabilities.validate_request(request)?;

    if normalize_exchange(&request.exchange) != capabilities.normalized_exchange() {
        return Err(BarterAdapterError::UnsupportedHistoricalExchange(
            request.exchange.clone(),
        ));
    }

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/api/v3/aggTrades".to_string(),
        query: vec![
            ("symbol".to_string(), request.symbol.to_ascii_uppercase()),
            (
                "startTime".to_string(),
                nanos_to_millis(request.start).to_string(),
            ),
            (
                "endTime".to_string(),
                nanos_to_millis(request.end).to_string(),
            ),
            (
                "limit".to_string(),
                request.limit.unwrap_or(500).to_string(),
            ),
        ],
        timeout_ms: 5_000,
    })
}

fn binance_futures_usd_capabilities_for(
    kind: BarterMarketDataKind,
    intervals: Vec<String>,
    max_limit: Option<usize>,
) -> HistoricalProviderCapabilities {
    HistoricalProviderCapabilities {
        exchange: "binance_futures_usd".to_string(),
        market_types: vec![BarterMarketType::Perpetual],
        kinds: vec![kind],
        intervals,
        max_limit,
    }
}

pub fn binance_futures_usd_funding_rate_capabilities() -> HistoricalProviderCapabilities {
    binance_futures_usd_capabilities_for(BarterMarketDataKind::FundingRate, Vec::new(), Some(1000))
}

pub fn binance_futures_usd_open_interest_capabilities() -> HistoricalProviderCapabilities {
    binance_futures_usd_capabilities_for(BarterMarketDataKind::OpenInterest, Vec::new(), None)
}

pub fn binance_futures_usd_mark_price_capabilities() -> HistoricalProviderCapabilities {
    binance_futures_usd_capabilities_for(BarterMarketDataKind::MarkPrice, Vec::new(), None)
}

pub fn binance_futures_usd_ohlcv_capabilities() -> HistoricalProviderCapabilities {
    binance_futures_usd_capabilities_for(
        BarterMarketDataKind::Candle,
        vec![
            "1m".to_string(),
            "3m".to_string(),
            "5m".to_string(),
            "15m".to_string(),
            "30m".to_string(),
            "1h".to_string(),
            "2h".to_string(),
            "4h".to_string(),
            "6h".to_string(),
            "8h".to_string(),
            "12h".to_string(),
            "1d".to_string(),
            "3d".to_string(),
            "1w".to_string(),
            "1M".to_string(),
        ],
        Some(1500),
    )
}

fn validate_binance_futures_request_against(
    request: &HistoricalBackfillRequest,
    capabilities: &HistoricalProviderCapabilities,
) -> Result<()> {
    validate_historical_backfill_request(request)?;
    capabilities.validate_request(request)?;

    if normalize_exchange(&request.exchange) != capabilities.normalized_exchange() {
        return Err(BarterAdapterError::UnsupportedHistoricalExchange(
            request.exchange.clone(),
        ));
    }

    Ok(())
}

pub fn binance_futures_usd_funding_rate_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    let capabilities = binance_futures_usd_funding_rate_capabilities();
    validate_binance_futures_request_against(request, &capabilities)?;

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/fapi/v1/fundingRate".to_string(),
        query: vec![
            ("symbol".to_string(), request.symbol.to_ascii_uppercase()),
            (
                "startTime".to_string(),
                nanos_to_millis(request.start).to_string(),
            ),
            (
                "endTime".to_string(),
                nanos_to_millis(request.end).to_string(),
            ),
            (
                "limit".to_string(),
                request.limit.unwrap_or(1000).to_string(),
            ),
        ],
        timeout_ms: 5_000,
    })
}

pub fn binance_futures_usd_open_interest_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    let capabilities = binance_futures_usd_open_interest_capabilities();
    validate_binance_futures_request_against(request, &capabilities)?;

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/fapi/v1/openInterest".to_string(),
        query: vec![("symbol".to_string(), request.symbol.to_ascii_uppercase())],
        timeout_ms: 5_000,
    })
}

pub fn binance_futures_usd_mark_price_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    let capabilities = binance_futures_usd_mark_price_capabilities();
    validate_binance_futures_request_against(request, &capabilities)?;

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/fapi/v1/premiumIndex".to_string(),
        query: vec![("symbol".to_string(), request.symbol.to_ascii_uppercase())],
        timeout_ms: 5_000,
    })
}

pub fn binance_futures_usd_ohlcv_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    let capabilities = binance_futures_usd_ohlcv_capabilities();
    validate_binance_futures_request_against(request, &capabilities)?;

    let interval = request.interval.clone().ok_or_else(|| {
        BarterAdapterError::InvalidHistoricalRequest("candle interval is required".to_string())
    })?;

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/fapi/v1/klines".to_string(),
        query: vec![
            ("symbol".to_string(), request.symbol.to_ascii_uppercase()),
            ("interval".to_string(), interval),
            (
                "startTime".to_string(),
                nanos_to_millis(request.start).to_string(),
            ),
            (
                "endTime".to_string(),
                nanos_to_millis(request.end).to_string(),
            ),
            (
                "limit".to_string(),
                request.limit.unwrap_or(500).to_string(),
            ),
        ],
        timeout_ms: 5_000,
    })
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceFuturesFundingRateRow {
    symbol: String,
    #[serde(rename = "fundingTime")]
    funding_time_ms: i64,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "markPrice")]
    mark_price: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesFundingRateProvider {
    capabilities: HistoricalProviderCapabilities,
    rows: Vec<BinanceFuturesFundingRateRow>,
}

impl BinanceFuturesFundingRateProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let rows: Vec<BinanceFuturesFundingRateRow> = serde_json::from_str(response_body)
            .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))?;
        for row in &rows {
            parse_decimal_str(&row.funding_rate, "fundingRate")?;
            if let Some(mark_price) = &row.mark_price {
                parse_decimal_str(mark_price, "markPrice")?;
            }
        }
        Ok(Self {
            capabilities: binance_futures_usd_funding_rate_capabilities(),
            rows,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceFuturesFundingRateProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;
        let mut envelopes = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let event_time = TimestampNs::from_nanos(millis_to_nanos(row.funding_time_ms));
            let funding_rate = parse_decimal_str(&row.funding_rate, "fundingRate")?;
            let mark_price = row
                .mark_price
                .as_deref()
                .map(|raw| parse_decimal_str(raw, "markPrice").map(Price::new))
                .transpose()?;
            let event = BarterMarketEvent {
                source: request.source_id.clone(),
                mode: BarterMarketDataMode::Historical,
                exchange: normalize_exchange(&request.exchange),
                symbol: Symbol::new(&row.symbol),
                market_type: request.market_type,
                kind: BarterMarketDataKind::FundingRate,
                timestamp: event_time,
                received_at: TimestampNs::now(),
                payload: BarterMarketPayload::FundingRate(FundingRatePayload {
                    funding_rate,
                    funding_time: event_time,
                    mark_price,
                }),
                sequence: Some(row.funding_time_ms.to_string()),
                checkpoint: None,
            };
            envelopes.push(BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            ));
        }
        let complete = request
            .limit
            .map(|limit| envelopes.len() < limit)
            .unwrap_or(true);
        Ok(HistoricalBackfillPage {
            request,
            envelopes,
            next_cursor: None,
            complete,
        })
    }
}

pub fn binance_futures_usd_funding_rate_provider_from_response(
    response_body: &str,
) -> Result<BinanceFuturesFundingRateProvider> {
    BinanceFuturesFundingRateProvider::from_response_body(response_body)
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceFuturesOpenInterestRow {
    symbol: String,
    #[serde(rename = "openInterest")]
    open_interest: String,
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesOpenInterestProvider {
    capabilities: HistoricalProviderCapabilities,
    row: BinanceFuturesOpenInterestRow,
}

impl BinanceFuturesOpenInterestProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let row: BinanceFuturesOpenInterestRow = serde_json::from_str(response_body)
            .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))?;
        parse_decimal_str(&row.open_interest, "openInterest")?;
        Ok(Self {
            capabilities: binance_futures_usd_open_interest_capabilities(),
            row,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceFuturesOpenInterestProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;
        let received_at = TimestampNs::now();
        let event = BarterMarketEvent {
            source: request.source_id.clone(),
            mode: BarterMarketDataMode::Historical,
            exchange: normalize_exchange(&request.exchange),
            symbol: Symbol::new(&self.row.symbol),
            market_type: request.market_type,
            kind: BarterMarketDataKind::OpenInterest,
            timestamp: received_at,
            received_at,
            payload: BarterMarketPayload::OpenInterest(OpenInterestPayload {
                open_interest: parse_decimal_str(&self.row.open_interest, "openInterest")?,
                timestamp: received_at,
            }),
            sequence: None,
            checkpoint: None,
        };
        Ok(HistoricalBackfillPage {
            request: request.clone(),
            envelopes: vec![BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            )],
            next_cursor: None,
            complete: true,
        })
    }
}

pub fn binance_futures_usd_open_interest_provider_from_response(
    response_body: &str,
) -> Result<BinanceFuturesOpenInterestProvider> {
    BinanceFuturesOpenInterestProvider::from_response_body(response_body)
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceFuturesPremiumIndexRow {
    symbol: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "indexPrice")]
    index_price: Option<String>,
    #[serde(rename = "estimatedSettlePrice")]
    estimated_settle_price: Option<String>,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: Option<String>,
    #[serde(rename = "nextFundingTime")]
    next_funding_time_ms: Option<i64>,
    time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesMarkPriceProvider {
    capabilities: HistoricalProviderCapabilities,
    row: BinanceFuturesPremiumIndexRow,
}

impl BinanceFuturesMarkPriceProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let row: BinanceFuturesPremiumIndexRow = serde_json::from_str(response_body)
            .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))?;
        parse_decimal_str(&row.mark_price, "markPrice")?;
        if let Some(index_price) = &row.index_price {
            parse_decimal_str(index_price, "indexPrice")?;
        }
        if let Some(estimated_settle_price) = &row.estimated_settle_price {
            parse_decimal_str(estimated_settle_price, "estimatedSettlePrice")?;
        }
        if let Some(last_funding_rate) = &row.last_funding_rate {
            parse_decimal_str(last_funding_rate, "lastFundingRate")?;
        }
        Ok(Self {
            capabilities: binance_futures_usd_mark_price_capabilities(),
            row,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceFuturesMarkPriceProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;
        let event_time = self
            .row
            .time
            .map(millis_to_nanos)
            .map(TimestampNs::from_nanos)
            .unwrap_or_else(TimestampNs::now);
        let event = BarterMarketEvent {
            source: request.source_id.clone(),
            mode: BarterMarketDataMode::Historical,
            exchange: normalize_exchange(&request.exchange),
            symbol: Symbol::new(&self.row.symbol),
            market_type: request.market_type,
            kind: BarterMarketDataKind::MarkPrice,
            timestamp: event_time,
            received_at: TimestampNs::now(),
            payload: BarterMarketPayload::MarkPrice(MarkPricePayload {
                mark_price: Price::new(parse_decimal_str(&self.row.mark_price, "markPrice")?),
                index_price: self
                    .row
                    .index_price
                    .as_deref()
                    .map(|raw| parse_decimal_str(raw, "indexPrice").map(Price::new))
                    .transpose()?,
                estimated_settle_price: self
                    .row
                    .estimated_settle_price
                    .as_deref()
                    .map(|raw| parse_decimal_str(raw, "estimatedSettlePrice").map(Price::new))
                    .transpose()?,
                funding_rate: self
                    .row
                    .last_funding_rate
                    .as_deref()
                    .map(|raw| parse_decimal_str(raw, "lastFundingRate"))
                    .transpose()?,
                next_funding_time: self
                    .row
                    .next_funding_time_ms
                    .map(millis_to_nanos)
                    .map(TimestampNs::from_nanos),
            }),
            sequence: self.row.time.map(|time| time.to_string()),
            checkpoint: None,
        };
        Ok(HistoricalBackfillPage {
            request: request.clone(),
            envelopes: vec![BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            )],
            next_cursor: None,
            complete: true,
        })
    }
}

pub fn binance_futures_usd_mark_price_provider_from_response(
    response_body: &str,
) -> Result<BinanceFuturesMarkPriceProvider> {
    BinanceFuturesMarkPriceProvider::from_response_body(response_body)
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesOhlcvProvider {
    capabilities: HistoricalProviderCapabilities,
    rows: Vec<BinanceSpotKlineRow>,
}

impl BinanceFuturesOhlcvProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let rows = parse_binance_spot_klines_response(response_body)?;
        Ok(Self {
            capabilities: binance_futures_usd_ohlcv_capabilities(),
            rows,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceFuturesOhlcvProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;

        let interval = request.interval.clone();
        let mut envelopes = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let event = row.to_market_event(&request, interval.clone())?;
            envelopes.push(BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            ));
        }
        let complete = request
            .limit
            .map(|limit| envelopes.len() < limit)
            .unwrap_or(true);
        Ok(HistoricalBackfillPage {
            request,
            envelopes,
            next_cursor: None,
            complete,
        })
    }
}

pub fn binance_futures_usd_ohlcv_provider_from_response(
    response_body: &str,
) -> Result<BinanceFuturesOhlcvProvider> {
    BinanceFuturesOhlcvProvider::from_response_body(response_body)
}

/// Offline-testable Binance Spot OHLCV provider backed by parsed public klines response rows.
#[derive(Debug, Clone)]
pub struct BinanceSpotOhlcvProvider {
    capabilities: HistoricalProviderCapabilities,
    rows: Vec<BinanceSpotKlineRow>,
}

impl BinanceSpotOhlcvProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let rows = parse_binance_spot_klines_response(response_body)?;

        Ok(Self {
            capabilities: binance_spot_ohlcv_capabilities(),
            rows,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceSpotOhlcvProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;

        let interval = request.interval.clone();
        let mut envelopes = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let event = row.to_market_event(&request, interval.clone())?;
            envelopes.push(BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            ));
        }

        let complete = request
            .limit
            .map(|limit| envelopes.len() < limit)
            .unwrap_or(true);
        let next_cursor = if complete {
            None
        } else {
            self.rows.last().map(|row| {
                HistoricalCursor::next_start(
                    request.exchange.clone(),
                    request.symbol.clone(),
                    request.kind,
                    TimestampNs::from_nanos(millis_to_nanos(row.close_time_ms + 1)),
                )
            })
        };

        Ok(HistoricalBackfillPage {
            request,
            envelopes,
            next_cursor,
            complete,
        })
    }
}

/// Build an offline Binance Spot OHLCV provider from a raw `/api/v3/klines` JSON body.
pub fn binance_spot_ohlcv_provider_from_response(
    response_body: &str,
) -> Result<BinanceSpotOhlcvProvider> {
    BinanceSpotOhlcvProvider::from_response_body(response_body)
}

/// Execute a Binance Spot OHLCV REST descriptor through an injected executor and parse the response.
pub async fn execute_binance_spot_ohlcv_rest(
    executor: &dyn HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage> {
    let descriptor = binance_spot_ohlcv_rest_request_descriptor(&request)?;
    let response_body = executor.execute(&descriptor).await?;
    let provider = binance_spot_ohlcv_provider_from_response(&response_body)?;
    provider.fetch_page(request).await
}

pub async fn execute_binance_spot_historical_trades_rest(
    executor: &dyn HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage> {
    let descriptor = binance_spot_historical_trades_rest_request_descriptor(&request)?;
    let response_body = executor.execute(&descriptor).await?;
    let provider = binance_spot_historical_trades_provider_from_response(&response_body)?;
    provider.fetch_page(request).await
}

#[derive(Debug, Clone)]
pub struct BinanceSpotHistoricalTradesProvider {
    capabilities: HistoricalProviderCapabilities,
    rows: Vec<BinanceSpotAggTradeRow>,
}

impl BinanceSpotHistoricalTradesProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let rows = parse_binance_spot_agg_trades_response(response_body)?;
        Ok(Self {
            capabilities: binance_spot_historical_trades_capabilities(),
            rows,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceSpotHistoricalTradesProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;

        let mut envelopes = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let event = row.to_market_event(&request)?;
            envelopes.push(BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            ));
        }

        let complete = request
            .limit
            .map(|limit| envelopes.len() < limit)
            .unwrap_or(true);
        let next_cursor = if complete {
            None
        } else {
            self.rows.last().map(|row| {
                HistoricalCursor::next_start(
                    request.exchange.clone(),
                    request.symbol.clone(),
                    request.kind,
                    TimestampNs::from_nanos(millis_to_nanos(row.time_ms + 1)),
                )
            })
        };

        Ok(HistoricalBackfillPage {
            request,
            envelopes,
            next_cursor,
            complete,
        })
    }
}

pub fn binance_spot_historical_trades_provider_from_response(
    response_body: &str,
) -> Result<BinanceSpotHistoricalTradesProvider> {
    BinanceSpotHistoricalTradesProvider::from_response_body(response_body)
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceSpotAggTradeRow {
    #[serde(rename = "a")]
    aggregate_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    time_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

impl BinanceSpotAggTradeRow {
    fn to_market_event(&self, request: &HistoricalBackfillRequest) -> Result<BarterMarketEvent> {
        let event_time = TimestampNs::from_nanos(millis_to_nanos(self.time_ms));
        let cursor = HistoricalCursor::next_start(
            request.exchange.clone(),
            request.symbol.clone(),
            request.kind,
            TimestampNs::from_nanos(millis_to_nanos(self.time_ms + 1)),
        );

        Ok(BarterMarketEvent {
            source: request.source_id.clone(),
            mode: BarterMarketDataMode::Historical,
            exchange: normalize_exchange(&request.exchange),
            symbol: Symbol::new(&request.symbol),
            market_type: request.market_type,
            kind: BarterMarketDataKind::Trade,
            timestamp: event_time,
            received_at: TimestampNs::now(),
            payload: BarterMarketPayload::Trade(TradePayload {
                trade_id: Some(self.aggregate_trade_id.to_string()),
                price: Price::new(self.price.parse::<Decimal>().map_err(|_| {
                    BarterAdapterError::HistoricalRest(format!(
                        "invalid numeric value for field price: {}",
                        self.price
                    ))
                })?),
                quantity: self.quantity.parse::<Decimal>().map_err(|_| {
                    BarterAdapterError::HistoricalRest(format!(
                        "invalid numeric value for field quantity: {}",
                        self.quantity
                    ))
                })?,
                side: Some(if self.buyer_is_maker {
                    TradeSide::Sell
                } else {
                    TradeSide::Buy
                }),
            }),
            sequence: Some(self.aggregate_trade_id.to_string()),
            checkpoint: Some(crate::model::BarterCheckpoint {
                source_id: request.source_id.clone(),
                exchange: normalize_exchange(&request.exchange),
                symbol: request.symbol.to_ascii_uppercase(),
                kind: BarterMarketDataKind::Trade,
                mode: BarterMarketDataMode::Historical,
                last_event_time: event_time,
                cursor: Some(cursor),
                updated_at: TimestampNs::now(),
            }),
        })
    }
}

fn parse_binance_spot_agg_trades_response(
    response_body: &str,
) -> Result<Vec<BinanceSpotAggTradeRow>> {
    serde_json::from_str(response_body)
        .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))
}

#[derive(Debug, Clone)]
struct BinanceSpotKlineRow {
    open_time_ms: i64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
    close_time_ms: i64,
    quote_volume: Decimal,
    trade_count: u64,
}

impl BinanceSpotKlineRow {
    fn to_market_event(
        &self,
        request: &HistoricalBackfillRequest,
        interval: Option<String>,
    ) -> Result<BarterMarketEvent> {
        let open_time = TimestampNs::from_nanos(millis_to_nanos(self.open_time_ms));
        let close_time = TimestampNs::from_nanos(millis_to_nanos(self.close_time_ms));
        let cursor = HistoricalCursor::next_start(
            request.exchange.clone(),
            request.symbol.clone(),
            request.kind,
            TimestampNs::from_nanos(millis_to_nanos(self.close_time_ms + 1)),
        );

        Ok(BarterMarketEvent {
            source: request.source_id.clone(),
            mode: BarterMarketDataMode::Historical,
            exchange: normalize_exchange(&request.exchange),
            symbol: Symbol::new(&request.symbol),
            market_type: request.market_type,
            kind: BarterMarketDataKind::Candle,
            timestamp: open_time,
            received_at: TimestampNs::now(),
            payload: BarterMarketPayload::Candle(CandlePayload {
                interval,
                open_time,
                close_time,
                open: Price::new(self.open),
                high: Price::new(self.high),
                low: Price::new(self.low),
                close: Price::new(self.close),
                volume: self.volume,
                trade_count: Some(self.trade_count),
                quote_volume: Some(self.quote_volume),
            }),
            sequence: Some(self.open_time_ms.to_string()),
            checkpoint: Some(crate::model::BarterCheckpoint {
                source_id: request.source_id.clone(),
                exchange: normalize_exchange(&request.exchange),
                symbol: request.symbol.to_ascii_uppercase(),
                kind: BarterMarketDataKind::Candle,
                mode: BarterMarketDataMode::Historical,
                last_event_time: open_time,
                cursor: Some(cursor),
                updated_at: TimestampNs::now(),
            }),
        })
    }
}

fn parse_binance_spot_klines_response(response_body: &str) -> Result<Vec<BinanceSpotKlineRow>> {
    let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(response_body)
        .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))?;

    rows.iter()
        .enumerate()
        .map(|(index, row)| parse_binance_spot_kline_row(index, row))
        .collect()
}

fn parse_binance_spot_kline_row(
    index: usize,
    row: &[serde_json::Value],
) -> Result<BinanceSpotKlineRow> {
    if row.len() < 9 {
        return Err(BarterAdapterError::HistoricalRest(format!(
            "Binance kline row {index} has {} fields, expected at least 9",
            row.len()
        )));
    }

    Ok(BinanceSpotKlineRow {
        open_time_ms: value_i64(&row[0], "open_time")?,
        open: value_decimal(&row[1], "open")?,
        high: value_decimal(&row[2], "high")?,
        low: value_decimal(&row[3], "low")?,
        close: value_decimal(&row[4], "close")?,
        volume: value_decimal(&row[5], "volume")?,
        close_time_ms: value_i64(&row[6], "close_time")?,
        quote_volume: value_decimal(&row[7], "quote_volume")?,
        trade_count: value_u64(&row[8], "trade_count")?,
    })
}

fn value_i64(value: &serde_json::Value, field: &'static str) -> Result<i64> {
    value.as_i64().ok_or_else(|| {
        BarterAdapterError::HistoricalRest(format!("invalid integer value for field {field}"))
    })
}

fn value_u64(value: &serde_json::Value, field: &'static str) -> Result<u64> {
    value.as_u64().ok_or_else(|| {
        BarterAdapterError::HistoricalRest(format!("invalid integer value for field {field}"))
    })
}

fn value_decimal(value: &serde_json::Value, field: &'static str) -> Result<Decimal> {
    let raw = value.as_str().ok_or_else(|| {
        BarterAdapterError::HistoricalRest(format!("invalid numeric value for field {field}"))
    })?;

    parse_decimal_str(raw, field)
}

fn parse_decimal_str(raw: &str, field: &'static str) -> Result<Decimal> {
    raw.parse::<Decimal>().map_err(|_| {
        BarterAdapterError::HistoricalRest(format!(
            "invalid numeric value for field {field}: {raw}"
        ))
    })
}

/// Build a stable dedupe key for historical public trade records.
pub fn historical_trade_dedupe_key(event: &crate::model::BarterMarketEvent) -> Option<String> {
    let crate::model::BarterMarketPayload::Trade(trade) = &event.payload else {
        return None;
    };

    Some(match &trade.trade_id {
        Some(trade_id) => format!("{}:{}:{}", event.exchange, event.symbol.as_str(), trade_id),
        None => format!(
            "{}:{}:{}:{}:{}",
            event.exchange,
            event.symbol.as_str(),
            event.timestamp.as_nanos(),
            trade.price.to_f64(),
            trade.quantity
        ),
    })
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase().replace('-', "_")
}

fn nanos_to_millis(timestamp: TimestampNs) -> i64 {
    timestamp.as_nanos() / 1_000_000
}

fn millis_to_nanos(timestamp_ms: i64) -> i64 {
    timestamp_ms.saturating_mul(1_000_000)
}
