use std::sync::Arc;

use fdc_storage::{RecordingStorageSink, StorageWriteSink};

pub type MarketDataOrchestratorResult = fdc_orchestrator::pipeline::OrchestratorPipelineResult;

#[derive(Clone)]
pub struct ServerComponents {
    market_data_storage_sink: Arc<dyn StorageWriteSink>,
    uses_recording_storage_sink: bool,
}

impl ServerComponents {
    pub fn new(market_data_storage_sink: Arc<dyn StorageWriteSink>) -> Self {
        Self {
            market_data_storage_sink,
            uses_recording_storage_sink: false,
        }
    }

    pub fn with_recording_storage_sink() -> Self {
        Self {
            market_data_storage_sink: Arc::new(RecordingStorageSink::new()),
            uses_recording_storage_sink: true,
        }
    }

    pub fn market_data_storage_sink(&self) -> Arc<dyn StorageWriteSink> {
        Arc::clone(&self.market_data_storage_sink)
    }

    pub fn uses_recording_storage_sink(&self) -> bool {
        self.uses_recording_storage_sink
    }

    pub fn is_ready(&self) -> bool {
        true
    }
}

impl Default for ServerComponents {
    fn default() -> Self {
        Self::with_recording_storage_sink()
    }
}
