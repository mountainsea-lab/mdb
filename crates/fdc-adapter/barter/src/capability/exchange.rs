use serde::{Deserialize, Serialize};

use crate::model::{BarterMarketDataKind, BarterMarketType};

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
    pub market_type: BarterMarketType,
    pub supports_live: bool,
    pub supports_historical: bool,
    pub kinds: Vec<BarterMarketDataKind>,
    pub historical_kinds: Vec<BarterMarketDataKind>,
    pub rate_limits: Vec<RateLimitRule>,
}

impl BarterSourceCapabilities {
    pub fn crypto_exchange(
        exchange: impl Into<String>,
        market_type: BarterMarketType,
        live_kinds: Vec<BarterMarketDataKind>,
        historical_kinds: Vec<BarterMarketDataKind>,
        rate_limits: Vec<RateLimitRule>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            market_type,
            supports_live: !live_kinds.is_empty(),
            supports_historical: !historical_kinds.is_empty(),
            kinds: live_kinds,
            historical_kinds,
            rate_limits,
        }
    }
}

pub fn supported_crypto_market_data_capabilities() -> Vec<BarterSourceCapabilities> {
    use BarterMarketDataKind::{Candle, Liquidation, OrderBook, OrderBookL1, Trade};
    use BarterMarketType::{Future, Option, Perpetual, Spot};

    vec![
        BarterSourceCapabilities::crypto_exchange(
            "binance_spot",
            Spot,
            vec![Trade, OrderBookL1, OrderBook],
            vec![Candle, Trade],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "binance_futures_usd",
            Perpetual,
            vec![Trade, OrderBookL1, OrderBook, Liquidation],
            vec![Candle],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "bybit_spot",
            Spot,
            vec![Trade, OrderBookL1, OrderBook],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "bybit_perpetuals_usd",
            Perpetual,
            vec![Trade, OrderBookL1, OrderBook],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "kraken",
            Spot,
            vec![Trade, OrderBookL1],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange("coinbase", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("bitfinex", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("bitmex", Perpetual, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange("gateio_spot", Spot, vec![Trade], vec![], vec![]),
        BarterSourceCapabilities::crypto_exchange(
            "gateio_futures_usd",
            Future,
            vec![Trade],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "gateio_futures_btc",
            Future,
            vec![Trade],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "gateio_perpetuals_usd",
            Perpetual,
            vec![Trade],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "gateio_perpetuals_btc",
            Perpetual,
            vec![Trade],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange(
            "gateio_options",
            Option,
            vec![Trade],
            vec![],
            vec![],
        ),
        BarterSourceCapabilities::crypto_exchange("okx", Spot, vec![Trade], vec![], vec![]),
    ]
}
