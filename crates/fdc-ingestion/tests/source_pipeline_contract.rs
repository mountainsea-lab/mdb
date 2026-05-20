use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::{error::Result, types::TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourceEnvelope, SourceType, SourceValidator,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

fn dummy_envelope(source_id: &str, symbol: &str) -> SourceEnvelope<DummyMarketEvent> {
    SourceEnvelope::new(
        source_id,
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: symbol.to_string(),
            price: 65000.25,
        },
    )
}

struct RecordingSourcePipelineSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> Default for RecordingSourcePipelineSink<T> {
    fn default() -> Self {
        Self {
            written: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> RecordingSourcePipelineSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourcePipelineSink<T>
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}

#[tokio::test]
async fn source_pipeline_empty_input_returns_zero_counts() {
    let sink = Arc::new(RecordingSourcePipelineSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(BatchConfig::default(), sink.clone());
    let validator = SourceValidator::default();

    let result = run_source_pipeline_once(Vec::new(), &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 0);
    assert_eq!(result.validation_success_count, 0);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.success_count(), 0);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(result.batch_count(), 0);
    assert_eq!(result.batch_results.len(), 0);
    assert_eq!(sink.written_count().await, 0);
}
