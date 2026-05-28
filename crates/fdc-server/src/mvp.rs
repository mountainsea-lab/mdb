use std::sync::Arc;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::Result;
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundedMarketDataMvpResult {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Clone)]
pub struct BoundedMarketDataMvpRunner {
    market_data_store: Arc<QueryableMarketDataStore>,
}

impl BoundedMarketDataMvpRunner {
    pub fn new(market_data_store: Arc<QueryableMarketDataStore>) -> Self {
        Self { market_data_store }
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }

    pub async fn run_barter_fixtures_once(
        &self,
        envelopes: Vec<BarterIngestionEnvelope>,
    ) -> Result<BoundedMarketDataMvpResult> {
        let pipeline_result =
            run_barter_envelopes_to_storage_once(envelopes, self.market_data_store.as_ref())
                .await?;
        let market_data_store_records = self
            .market_data_store
            .query(&MarketDataQuery::for_trades())
            .len();

        Ok(BoundedMarketDataMvpResult {
            envelopes_received: pipeline_result.envelopes_received,
            source_valid: pipeline_result.source_valid,
            source_invalid: pipeline_result.source_invalid,
            dto_mapped: pipeline_result.dto_mapped,
            storage_records_written: pipeline_result.storage_records_written,
            market_data_store_records,
        })
    }
}

pub async fn run_barter_fixture_mvp_once(
    envelopes: Vec<BarterIngestionEnvelope>,
    market_data_store: Arc<QueryableMarketDataStore>,
) -> Result<BoundedMarketDataMvpResult> {
    BoundedMarketDataMvpRunner::new(market_data_store)
        .run_barter_fixtures_once(envelopes)
        .await
}
