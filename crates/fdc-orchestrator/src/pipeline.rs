use fdc_barter::BarterIngestionEnvelope;
use fdc_core::Result;
use fdc_ingestion::SourceValidator;
use fdc_storage::{StorageWriteBatch, StorageWriteSink};

use crate::{
    barter::barter_envelope_to_source_envelope, market_data::barter_event_to_market_data_dto,
    storage::market_data_dto_to_storage_record,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrchestratorPipelineResult {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
}

pub async fn run_barter_envelopes_to_storage_once(
    envelopes: Vec<BarterIngestionEnvelope>,
    storage_sink: &dyn StorageWriteSink,
) -> Result<OrchestratorPipelineResult> {
    let validator = SourceValidator::default();
    let mut result = OrchestratorPipelineResult {
        envelopes_received: envelopes.len(),
        ..OrchestratorPipelineResult::default()
    };
    let mut records = Vec::new();

    for envelope in envelopes {
        let source = barter_envelope_to_source_envelope(envelope);
        let validation = validator.validate(&source).await;
        if !validation.is_valid {
            result.source_invalid += 1;
            continue;
        }

        result.source_valid += 1;
        let dto = barter_event_to_market_data_dto(&source)?;
        result.dto_mapped += 1;
        records.push(market_data_dto_to_storage_record(&dto)?);
    }

    if records.is_empty() {
        return Ok(result);
    }

    let outcome = storage_sink
        .write_batch(StorageWriteBatch::new(records))
        .await?;
    result.storage_records_written = outcome.accepted_records;
    Ok(result)
}
