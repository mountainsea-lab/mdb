use fdc_core::error::Result;

use super::{SourceBatchProcessor, SourceBatchResult, SourceEnvelope, SourceValidator};

#[derive(Debug, Clone, Default)]
pub struct SourcePipelineResult {
    pub input_count: usize,
    pub validation_success_count: usize,
    pub validation_failure_count: usize,
    pub batch_results: Vec<SourceBatchResult>,
}

impl SourcePipelineResult {
    pub fn processed_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.processed_count)
            .sum()
    }

    pub fn success_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.success_count)
            .sum()
    }

    pub fn failure_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.failure_count)
            .sum()
    }

    pub fn batch_count(&self) -> usize {
        self.batch_results.len()
    }
}

pub async fn run_source_pipeline_once<T, I>(
    envelopes: I,
    _validator: &SourceValidator,
    _processor: &SourceBatchProcessor<T>,
) -> Result<SourcePipelineResult>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = SourceEnvelope<T>>,
{
    let input_count = envelopes.into_iter().count();

    Ok(SourcePipelineResult {
        input_count,
        validation_success_count: 0,
        validation_failure_count: 0,
        batch_results: Vec::new(),
    })
}
