use serde::{Deserialize, Serialize};

use crate::model::BarterMarketDataKind;

/// Exchange endpoint rate limit rule used by historical REST implementations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitRule {
    pub exchange: String,
    pub endpoint: String,
    pub max_requests: u32,
    pub window_ms: u64,
    pub weight: u32,
}

impl RateLimitRule {
    pub fn new(
        exchange: impl Into<String>,
        endpoint: impl Into<String>,
        max_requests: u32,
        window_ms: u64,
        weight: u32,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            endpoint: endpoint.into(),
            max_requests,
            window_ms,
            weight,
        }
    }
}

/// Capability declaration for a Barter-backed crypto exchange source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterSourceCapabilities {
    pub exchange: String,
    pub supports_live: bool,
    pub supports_historical: bool,
    pub kinds: Vec<BarterMarketDataKind>,
    pub historical_kinds: Vec<BarterMarketDataKind>,
    pub rate_limits: Vec<RateLimitRule>,
}

impl BarterSourceCapabilities {
    pub fn crypto_exchange(
        exchange: impl Into<String>,
        live_kinds: Vec<BarterMarketDataKind>,
        historical_kinds: Vec<BarterMarketDataKind>,
        rate_limits: Vec<RateLimitRule>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            supports_live: !live_kinds.is_empty(),
            supports_historical: !historical_kinds.is_empty(),
            kinds: live_kinds,
            historical_kinds,
            rate_limits,
        }
    }
}
