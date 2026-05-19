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
