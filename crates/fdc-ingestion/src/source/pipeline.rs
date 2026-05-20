use fdc_core::error::Result;

use super::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchResult, SourceEnvelope, SourceValidator,
};

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
    validator: &SourceValidator,
    processor: &SourceBatchProcessor<T>,
) -> Result<SourcePipelineResult>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = SourceEnvelope<T>>,
{
    let mut result = SourcePipelineResult::default();

    for envelope in envelopes {
        result.input_count += 1;

        let validation_result = validator.validate(&envelope).await;
        if validation_result.is_valid {
            result.validation_success_count += 1;
        } else {
            result.validation_failure_count += 1;
        }

        let item = SourceBatchItem::new(envelope, validation_result);
        if let Some(batch_result) = processor.add_item(item).await? {
            result.batch_results.push(batch_result);
        }
    }

    if let Some(batch_result) = processor.flush().await? {
        result.batch_results.push(batch_result);
    }

    Ok(result)
}
