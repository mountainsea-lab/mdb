use serde::{Deserialize, Serialize};

use crate::model::BarterMarketDataMode;

/// Backward-compatible data retrieval mode requested from Barter-backed sources.
pub type BarterDataMode = BarterMarketDataMode;

/// First-stage adapter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterAdapterConfig {
    /// Requested data mode.
    pub mode: BarterDataMode,
    /// Safety switch: first-stage skeleton must not start real network streams by default.
    pub start_network_streams: bool,
    /// Upper bound used when grouping Barter subscriptions into one connection.
    pub max_subscriptions_per_connection: usize,
}

impl Default for BarterAdapterConfig {
    fn default() -> Self {
        Self {
            mode: BarterDataMode::Live,
            start_network_streams: false,
            max_subscriptions_per_connection: 100,
        }
    }
}
