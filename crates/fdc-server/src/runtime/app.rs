use std::sync::Arc;

use axum::Router;
use fdc_core::Result;
use fdc_storage::QueryableMarketDataStore;

use crate::{
    health::build_health_router,
    market_data::{build_market_data_router, ingest_test_trade, supervisor::MarketDataSupervisor},
    ServerRuntimeConfig,
};

#[derive(Clone)]
pub struct ProductionServerState {
    config: ServerRuntimeConfig,
    market_data_store: Arc<QueryableMarketDataStore>,
    market_data_supervisor: Arc<MarketDataSupervisor>,
}

impl ProductionServerState {
    pub fn new(config: ServerRuntimeConfig) -> Self {
        Self {
            config,
            market_data_store: Arc::new(QueryableMarketDataStore::new()),
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
        }
    }

    pub fn with_market_data_store(
        config: ServerRuntimeConfig,
        market_data_store: Arc<QueryableMarketDataStore>,
    ) -> Self {
        Self {
            config,
            market_data_store,
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
        }
    }

    pub fn config(&self) -> &ServerRuntimeConfig {
        &self.config
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }

    pub fn market_data_supervisor(&self) -> Arc<MarketDataSupervisor> {
        Arc::clone(&self.market_data_supervisor)
    }

    pub async fn ingest_test_trade(&self, symbol: &str, trade_id: &str) -> Result<()> {
        ingest_test_trade(self, symbol, trade_id).await.map(|_| ())
    }

    pub async fn start_live_autostart_if_enabled(&self) -> Result<()> {
        if !(self.config.live_enabled && self.config.live_autostart) {
            return Ok(());
        }
        crate::market_data::service::start_background_live(
            self,
            crate::market_data::model::StartLiveMarketDataRequest {
                timeout_secs: None,
                max_envelopes: None,
            },
        )
        .await
        .map(|_| ())
        .map_err(fdc_core::error::Error::internal)
    }
}

pub fn build_production_router(state: ProductionServerState) -> Router {
    Router::new()
        .merge(build_health_router(state.clone()))
        .merge(build_market_data_router(state))
}
