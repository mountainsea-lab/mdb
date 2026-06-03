use std::sync::Arc;

use chrono::{TimeZone, Utc};
use fdc_storage::{
    JsonStorageCodec, QueryableStorage, StorageCodec, StorageEngineType, StoragePlacementHint,
    StorageQuery, StorageTier, StorageTierScope, StorageTypeDescriptor, StorageWriteBatch,
    StorageWriteMetadata, StorageWriteRecord, StorageWriteSink, TierConfig, TierManager,
    TieredStorageStore,
};
use fdc_types::SerializationFormat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BusinessEvent {
    tenant: String,
    kind: String,
    sequence: u64,
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = StorageEngineType::Memory;
    config
}

async fn memory_tiered_store() -> TieredStorageStore {
    let mut manager = TierManager::new();
    manager.add_tier(memory_tier_config(StorageTier::L1));
    manager.add_tier(memory_tier_config(StorageTier::L2));
    manager.initialize().await.unwrap();
    TieredStorageStore::new(Arc::new(manager))
}

#[tokio::test]
async fn generic_storage_boundary_supports_business_like_dto_without_business_dependency() {
    let store = memory_tiered_store().await;
    let codec = JsonStorageCodec::<BusinessEvent>::new();
    let event = BusinessEvent {
        tenant: "tenant-a".to_string(),
        kind: "created".to_string(),
        sequence: 42,
    };
    let descriptor = StorageTypeDescriptor::new("business.event", "1", SerializationFormat::Json);
    let encoded = codec.encode(&event).unwrap();
    let mut metadata = StorageWriteMetadata {
        content_type: Some(codec.content_type().to_string()),
        schema: Some(descriptor.schema_name.clone()),
        schema_version: Some(descriptor.schema_version.clone()),
        source: Some("contract-test".to_string()),
        ..StorageWriteMetadata::default()
    };
    metadata
        .tags
        .insert("tenant".to_string(), event.tenant.clone());
    metadata.tags.insert("kind".to_string(), event.kind.clone());

    let record = StorageWriteRecord::new(
        "business_test",
        "events",
        b"tenant-a/000042".to_vec(),
        encoded,
    )
    .with_timestamp(Utc.timestamp_opt(42, 0).unwrap())
    .with_metadata(metadata)
    .with_placement(StoragePlacementHint::for_tier(StorageTier::L2));

    store
        .write_batch(StorageWriteBatch::new(vec![record]))
        .await
        .unwrap();

    let result = store
        .query_storage(
            &StorageQuery::new("business_test")
                .with_collection("events")
                .with_tag("tenant", "tenant-a")
                .with_tag("kind", "created")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].metadata.schema.as_deref(), Some("business.event"));
    assert_eq!(result[0].metadata.schema_version.as_deref(), Some("1"));
    let decoded = codec.decode(&result[0].value).unwrap();
    assert_eq!(decoded, event);
}
