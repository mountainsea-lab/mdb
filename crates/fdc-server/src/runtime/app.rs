use std::sync::Arc;

use axum::Router;
use fdc_barter::HistoricalPageFetcher;
use fdc_core::Result;
use fdc_storage::QueryableMarketDataStore;

use crate::{
    ServerRuntimeConfig, build_market_data_store_from_runtime_config,
    health::build_health_router,
    market_data::{
        build_market_data_router, ingest_test_candle, ingest_test_trade,
        maintenance_audit::MarketDataStorageMaintenanceAuditLog,
        maintenance_scheduler::{
            StorageMaintenanceSchedulerState, StorageMaintenanceSchedulerTaskHandle,
            spawn_storage_maintenance_scheduler_into_handle,
        },
        supervisor::MarketDataSupervisor,
    },
};

#[derive(Clone)]
pub struct ProductionServerState {
    config: ServerRuntimeConfig,
    market_data_store: Arc<QueryableMarketDataStore>,
    market_data_supervisor: Arc<MarketDataSupervisor>,
    market_data_storage_maintenance_audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    market_data_storage_maintenance_scheduler: StorageMaintenanceSchedulerState,
    market_data_storage_maintenance_scheduler_task: StorageMaintenanceSchedulerTaskHandle,
}

impl ProductionServerState {
    pub fn new(config: ServerRuntimeConfig) -> Self {
        let market_data_storage_maintenance_audit = market_data_storage_maintenance_audit(&config);
        let market_data_storage_maintenance_scheduler =
            StorageMaintenanceSchedulerState::from_config(&config);
        Self {
            config,
            market_data_store: Arc::new(QueryableMarketDataStore::new()),
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
            market_data_storage_maintenance_audit,
            market_data_storage_maintenance_scheduler,
            market_data_storage_maintenance_scheduler_task:
                StorageMaintenanceSchedulerTaskHandle::default(),
        }
    }

    pub async fn try_new(config: ServerRuntimeConfig) -> Result<Self> {
        let market_data_store =
            build_market_data_store_from_runtime_config(config.market_data_storage.clone()).await?;
        let market_data_store = Arc::new(market_data_store);
        let market_data_storage_maintenance_audit = market_data_storage_maintenance_audit(&config);
        let market_data_storage_maintenance_scheduler =
            StorageMaintenanceSchedulerState::from_config(&config);
        let market_data_storage_maintenance_scheduler_task =
            StorageMaintenanceSchedulerTaskHandle::default();
        spawn_storage_maintenance_scheduler_into_handle(
            config.clone(),
            Arc::clone(&market_data_store),
            Arc::clone(&market_data_storage_maintenance_audit),
            market_data_storage_maintenance_scheduler.clone(),
            market_data_storage_maintenance_scheduler_task.clone(),
        )
        .await;
        Ok(Self {
            config,
            market_data_store,
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
            market_data_storage_maintenance_audit,
            market_data_storage_maintenance_scheduler,
            market_data_storage_maintenance_scheduler_task,
        })
    }

    pub fn with_market_data_store(
        config: ServerRuntimeConfig,
        market_data_store: Arc<QueryableMarketDataStore>,
    ) -> Self {
        let market_data_storage_maintenance_audit = market_data_storage_maintenance_audit(&config);
        let market_data_storage_maintenance_scheduler =
            StorageMaintenanceSchedulerState::from_config(&config);
        Self {
            config,
            market_data_store,
            market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
            market_data_storage_maintenance_audit,
            market_data_storage_maintenance_scheduler,
            market_data_storage_maintenance_scheduler_task:
                StorageMaintenanceSchedulerTaskHandle::default(),
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

    pub fn market_data_storage_maintenance_audit(
        &self,
    ) -> Arc<MarketDataStorageMaintenanceAuditLog> {
        Arc::clone(&self.market_data_storage_maintenance_audit)
    }

    pub fn market_data_storage_maintenance_scheduler(&self) -> StorageMaintenanceSchedulerState {
        self.market_data_storage_maintenance_scheduler.clone()
    }

    pub fn market_data_storage_maintenance_scheduler_task(
        &self,
    ) -> StorageMaintenanceSchedulerTaskHandle {
        self.market_data_storage_maintenance_scheduler_task.clone()
    }

    pub async fn ingest_test_trade(&self, symbol: &str, trade_id: &str) -> Result<()> {
        ingest_test_trade(self, symbol, trade_id).await.map(|_| ())
    }

    pub async fn ingest_test_candle(&self, symbol: &str) -> Result<()> {
        ingest_test_candle(self, symbol).await.map(|_| ())
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

    pub async fn run_candle_acquisition_once_with_source<S>(
        &self,
        source: &S,
    ) -> Result<crate::market_data::candle_acquisition::CandleAcquisitionRunStatus>
    where
        S: HistoricalPageFetcher + ?Sized,
    {
        crate::market_data::candle_acquisition::run_candle_acquisition_once(
            &self.config.market_data_candle_acquisition,
            source,
            self.market_data_store.as_ref(),
        )
        .await
    }

    pub async fn start_candle_acquisition_autostart_if_enabled(
        &self,
    ) -> Result<crate::market_data::candle_acquisition::CandleAcquisitionRunStatus> {
        if !(self.config.market_data_candle_acquisition.enabled
            && self.config.market_data_candle_acquisition.autostart)
        {
            return Ok(crate::market_data::candle_acquisition::CandleAcquisitionRunStatus::default());
        }

        crate::market_data::candle_acquisition::run_binance_spot_candle_acquisition_once(
            &self.config.market_data_candle_acquisition,
            self.market_data_store.as_ref(),
        )
        .await
    }
}

fn market_data_storage_maintenance_audit(
    config: &ServerRuntimeConfig,
) -> Arc<MarketDataStorageMaintenanceAuditLog> {
    Arc::new(MarketDataStorageMaintenanceAuditLog::new(
        config.market_data_storage_maintenance_audit_capacity,
    ))
}

pub fn build_production_router(state: ProductionServerState) -> Router {
    Router::new()
        .merge(build_health_router(state.clone()))
        .merge(build_market_data_router(state))
}
