use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    model::{BarterMarketDataKind, BarterMarketType, HistoricalCursor},
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
