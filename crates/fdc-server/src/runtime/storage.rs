use fdc_core::Result;
use fdc_storage::QueryableMarketDataStore;

use crate::{MarketDataStorageBackendConfig, MarketDataStorageRuntimeConfig};

pub async fn build_market_data_store_from_runtime_config(
    config: MarketDataStorageRuntimeConfig,
) -> Result<QueryableMarketDataStore> {
    match config.backend {
        MarketDataStorageBackendConfig::Memory => Ok(QueryableMarketDataStore::in_memory()),
        MarketDataStorageBackendConfig::Tiered => QueryableMarketDataStore::memory_tiered().await,
    }
}
