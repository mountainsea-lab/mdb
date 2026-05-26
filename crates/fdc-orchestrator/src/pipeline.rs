use fdc_barter::BarterIngestionEnvelope;
use fdc_core::{error::Error, Result};
use fdc_storage::StorageWriteSink;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrchestratorPipelineResult {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
}

pub async fn run_barter_envelopes_to_storage_once(
    _envelopes: Vec<BarterIngestionEnvelope>,
    _storage_sink: &dyn StorageWriteSink,
) -> Result<OrchestratorPipelineResult> {
    Err(Error::unimplemented(
        "finite orchestrator pipeline is implemented in Task 5",
    ))
}
