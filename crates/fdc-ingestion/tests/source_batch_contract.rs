use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    SourceBatchItem, SourceEnvelope, SourceType, SourceValidationResult, SourceValidator,
};

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

#[tokio::test]
async fn source_batch_item_preserves_envelope_and_validation_result() {
    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation: SourceValidationResult = SourceValidator::default().validate(&envelope).await;

    let item = SourceBatchItem::new(envelope, validation.clone());

    assert!(!item.item_id.is_empty());
    assert_eq!(item.envelope.source_id, "dummy-source");
    assert_eq!(item.envelope.payload.symbol, "BTCUSDT");
    assert_eq!(item.validation_result.is_valid, validation.is_valid);
    assert!(item.is_valid());
}


#[tokio::test]
async fn source_batch_item_validity_comes_from_validation_result() {
    let mut envelope = dummy_envelope("", "ETHUSDT");
    envelope.envelope_id = "envelope-with-empty-source".to_string();
    let validation = SourceValidator::default().validate(&envelope).await;

    let item = SourceBatchItem::new(envelope, validation);

    assert!(!item.is_valid());
    assert_eq!(item.validation_result.errors.len(), 1);
}

use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::error::Result;
use fdc_ingestion::{config::BatchConfig, SourceBatchProcessor, SourceBatchSink};
use tokio::sync::RwLock;

struct RecordingSourceBatchSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> Default for RecordingSourceBatchSink<T> {
    fn default() -> Self {
        Self {
            written: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> RecordingSourceBatchSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourceBatchSink<T>
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
async fn source_batch_processor_flush_writes_valid_items_to_sink() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );

    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation = SourceValidator::default().validate(&envelope).await;
    let item = SourceBatchItem::new(envelope, validation);

    assert!(processor.add_item(item).await.unwrap().is_none());

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 1);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 0);
    assert_eq!(result.batch_size, 1);
    assert!(result.errors.is_empty());
    assert!(!result.batch_id.is_empty());
    assert_eq!(sink.written_count().await, 1);
}

#[tokio::test]
async fn source_batch_processor_empty_flush_returns_none() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(BatchConfig::default(), sink);

    let result = processor.flush().await.unwrap();

    assert!(result.is_none());
}
