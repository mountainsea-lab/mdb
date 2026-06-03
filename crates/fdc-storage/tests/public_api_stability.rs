use fdc_storage::{
    StorageEngineFeature, StorageEngineFeatureError, StorageEngineType, StorageMaintenanceReport,
    StoragePlacementHint, StorageQuery, StorageTier, StorageTierScope, TierLifecycleReport,
    TieredStorageStore,
};

#[tokio::test]
async fn public_storage_api_smoke_compiles_and_runs() {
    let store = TieredStorageStore::memory_only().await.unwrap();
    let query = StorageQuery::new("api_smoke")
        .with_collection("records")
        .with_tier_scope(StorageTierScope::Only(StorageTier::L1));
    query.validate().unwrap();

    let placement = StoragePlacementHint::for_tier(StorageTier::L1);
    assert_eq!(placement.target_tier, Some(StorageTier::L1));

    let lifecycle = TierLifecycleReport::default();
    assert_eq!(lifecycle.scanned_entries, 0);

    let feature = StorageEngineFeature::Compaction;
    let feature_error = StorageEngineFeatureError::unsupported(StorageEngineType::Memory, feature);
    assert_eq!(feature_error.feature, StorageEngineFeature::Compaction);

    let maintenance: StorageMaintenanceReport = store.run_maintenance_once().await.unwrap();
    assert!(maintenance.finished_at >= maintenance.started_at);
    assert!(maintenance.health.tiers.contains_key(&StorageTier::L1));
}
