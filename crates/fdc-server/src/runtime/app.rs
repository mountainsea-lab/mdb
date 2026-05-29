use std::sync::Arc;

use axum::Router;
use fdc_storage::QueryableMarketDataStore;

use crate::{health::build_health_router, ServerRuntimeConfig};

#[derive(Clone)]
pub struct ProductionServerState {
    config: ServerRuntimeConfig,
    market_data_store: Arc<QueryableMarketDataStore>,
}

impl ProductionServerState {
    pub fn new(config: ServerRuntimeConfig) -> Self {
        Self {
            config,
            market_data_store: Arc::new(QueryableMarketDataStore::new()),
        }
    }

    pub fn config(&self) -> &ServerRuntimeConfig {
        &self.config
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }
}

pub fn build_production_router(state: ProductionServerState) -> Router {
    Router::new().merge(build_health_router(state))
}
