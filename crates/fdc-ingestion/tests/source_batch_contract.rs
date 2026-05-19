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

#[tokio::test]
async fn source_batch_processor_filters_invalid_items_before_sink() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );

    let valid_envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let valid_validation = SourceValidator::default().validate(&valid_envelope).await;
    let valid_item = SourceBatchItem::new(valid_envelope, valid_validation);

    let invalid_envelope = dummy_envelope("", "ETHUSDT");
    let invalid_validation = SourceValidator::default().validate(&invalid_envelope).await;
    let invalid_item = SourceBatchItem::new(invalid_envelope, invalid_validation);

    processor.add_item(valid_item).await.unwrap();
    processor.add_item(invalid_item).await.unwrap();

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 1);
    assert_eq!(sink.written_count().await, 1);
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].contains("invalid source batch item"));
}

#[tokio::test]
async fn source_batch_processor_processes_when_batch_size_is_reached() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 2,
            ..Default::default()
        },
        sink.clone(),
    );

    let first = dummy_envelope("dummy-source", "BTCUSDT");
    let first_validation = SourceValidator::default().validate(&first).await;
    let second = dummy_envelope("dummy-source", "ETHUSDT");
    let second_validation = SourceValidator::default().validate(&second).await;

    assert!(processor
        .add_item(SourceBatchItem::new(first, first_validation))
        .await
        .unwrap()
        .is_none());

    let result = processor
        .add_item(SourceBatchItem::new(second, second_validation))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 2);
    assert_eq!(result.failure_count, 0);
    assert_eq!(sink.written_count().await, 2);
    assert!(processor.flush().await.unwrap().is_none());
}

struct ShortWriteSourceBatchSink {
    accepted_count: usize,
}

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for ShortWriteSourceBatchSink {
    async fn write_batch(&self, _items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        Ok(self.accepted_count)
    }
}

struct FailingSourceBatchSink;

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for FailingSourceBatchSink {
    async fn write_batch(&self, _items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        Err(fdc_core::error::Error::storage("sink unavailable"))
    }
}

#[tokio::test]
async fn source_batch_processor_records_short_write_as_failure() {
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        Arc::new(ShortWriteSourceBatchSink { accepted_count: 1 }),
    );

    for symbol in ["BTCUSDT", "ETHUSDT"] {
        let envelope = dummy_envelope("dummy-source", symbol);
        let validation = SourceValidator::default().validate(&envelope).await;
        processor
            .add_item(SourceBatchItem::new(envelope, validation))
            .await
            .unwrap();
    }

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 1);
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].contains("source sink accepted 1 of 2 valid items"));
}

#[tokio::test]
async fn source_batch_processor_propagates_sink_errors() {
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 1,
            ..Default::default()
        },
        Arc::new(FailingSourceBatchSink),
    );

    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation = SourceValidator::default().validate(&envelope).await;

    let err = processor
        .add_item(SourceBatchItem::new(envelope, validation))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("sink unavailable"));

    let stats = processor.get_stats().await;
    assert_eq!(stats.batches_processed, 1);
    assert_eq!(stats.total_messages, 1);
    assert_eq!(stats.successful_messages, 0);
    assert_eq!(stats.failed_messages, 1);
}

#[tokio::test]
async fn source_batch_processor_records_and_resets_stats() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink,
    );

    let valid_envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let valid_validation = SourceValidator::default().validate(&valid_envelope).await;
    let invalid_envelope = dummy_envelope("", "ETHUSDT");
    let invalid_validation = SourceValidator::default().validate(&invalid_envelope).await;

    processor
        .add_item(SourceBatchItem::new(valid_envelope, valid_validation))
        .await
        .unwrap();
    processor
        .add_item(SourceBatchItem::new(invalid_envelope, invalid_validation))
        .await
        .unwrap();
    processor.flush().await.unwrap().unwrap();

    let stats = processor.get_stats().await;
    assert_eq!(stats.batches_processed, 1);
    assert_eq!(stats.total_messages, 2);
    assert_eq!(stats.successful_messages, 1);
    assert_eq!(stats.failed_messages, 1);
    assert_eq!(stats.avg_batch_size, 2.0);
    assert_eq!(stats.success_rate(), 0.5);

    processor.reset_stats().await;
    let reset = processor.get_stats().await;
    assert_eq!(reset.batches_processed, 0);
    assert_eq!(reset.total_messages, 0);
    assert_eq!(reset.success_rate(), 0.0);
}
