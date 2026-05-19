use std::collections::BTreeMap;

use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    config::BufferConfig, DataBuffer, SourceCheckpoint, SourceEnvelope, SourceMetadata,
    SourcePartition, SourcePosition, SourceQualityFlags, SourceType,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

#[tokio::test]
async fn source_envelope_can_flow_through_data_buffer() {
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
            price: 65000.25,
        },
    )
    .with_metadata(SourceMetadata {
        adapter: Some("dummy".to_string()),
        exchange: Some("binance_spot".to_string()),
        symbol: Some("BTCUSDT".to_string()),
        kind: Some("Trade".to_string()),
        attributes: BTreeMap::new(),
    })
    .with_quality(SourceQualityFlags::default())
    .with_checkpoint(SourceCheckpoint {
        checkpoint_id: "cp-1".to_string(),
        source_id: "dummy-source".to_string(),
        partition: SourcePartition {
            exchange: Some("binance_spot".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            kind: Some("Trade".to_string()),
            shard: None,
        },
        position: SourcePosition::Timestamp(TimestampNs::from_nanos(1_000)),
        updated_at: TimestampNs::from_nanos(1_200),
    });

    let buffer = DataBuffer::new(BufferConfig {
        buffer_size: 2,
        ..Default::default()
    });

    buffer.enqueue(envelope.clone()).await.unwrap();
    let dequeued = buffer.dequeue().await.unwrap();

    assert_eq!(dequeued.source_id, "dummy-source");
    assert_eq!(dequeued.source_type, SourceType::MarketData);
    assert_eq!(dequeued.payload.symbol, "BTCUSDT");
    assert_eq!(dequeued.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(dequeued.checkpoint.as_ref().unwrap().checkpoint_id, "cp-1");
}

#[test]
fn source_envelope_can_attach_optional_checkpoint() {
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "ETHUSDT".to_string(),
            price: 3500.0,
        },
    )
    .with_optional_checkpoint(None);

    assert!(envelope.checkpoint.is_none());
    assert!(!envelope.envelope_id.is_empty());
}
