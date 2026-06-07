use fdc_core::Result;
use fdc_storage::{
    QueryableMarketDataStore, StorageEngineType, StorageTier, StorageTieringPolicy, TierConfig,
};
use std::path::Path;

use crate::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig,
};

pub async fn build_market_data_store_from_runtime_config(
    config: MarketDataStorageRuntimeConfig,
) -> Result<QueryableMarketDataStore> {
    match config.backend {
        MarketDataStorageBackendConfig::Memory => Ok(QueryableMarketDataStore::in_memory()),
        MarketDataStorageBackendConfig::Tiered => {
            let policy = policy_from_runtime_config(config.policy_profile);
            let tier_configs = tier_configs_from_runtime_config(&config);
            QueryableMarketDataStore::tiered_with_policy_and_configs(policy, tier_configs).await
        }
    }
}

fn tier_configs_from_runtime_config(config: &MarketDataStorageRuntimeConfig) -> Vec<TierConfig> {
    let mut configs = vec![memory_tier_config(StorageTier::L1)];

    configs.push(match config.tiers.l2_redb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L2, StorageEngineType::Redb, path),
        None => memory_tier_config(StorageTier::L2),
    });
    configs.push(match config.tiers.l3_duckdb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L3, StorageEngineType::DuckDB, path),
        None => memory_tier_config(StorageTier::L3),
    });
    configs.push(match config.tiers.l4_rocksdb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L4, StorageEngineType::RocksDB, path),
        None => memory_tier_config(StorageTier::L4),
    });

    configs
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = StorageEngineType::Memory;
    config
}

fn durable_tier_config(
    tier: StorageTier,
    engine_type: StorageEngineType,
    path: &Path,
) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = engine_type;
    config
        .engine_config
        .insert("db_path".to_string(), path.display().to_string());
    config
}

fn policy_from_runtime_config(
    profile: MarketDataStoragePolicyProfileConfig,
) -> StorageTieringPolicy {
    match profile {
        MarketDataStoragePolicyProfileConfig::Compatibility => {
            StorageTieringPolicy::compatibility()
        }
        MarketDataStoragePolicyProfileConfig::GenericRealtime => {
            StorageTieringPolicy::generic_realtime()
        }
    }
}

#[cfg(test)]
mod tests {
    use fdc_storage::{
        QueryableStorage, StorageQuery, StorageTier, StorageTierScope, StorageWriteBatch,
        StorageWriteMetadata, StorageWriteRecord, StorageWriteSink,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::MarketDataStorageTierRuntimeConfig;

    fn tagged_record(key: &[u8], mode: &str) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata.tags.insert("mode".to_string(), mode.to_string());
        StorageWriteRecord::new("market_data", "trades", key.to_vec(), b"value".to_vec())
            .with_metadata(metadata)
    }

    #[tokio::test]
    async fn tiered_runtime_config_injects_generic_realtime_policy() {
        let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Tiered,
            policy_profile: MarketDataStoragePolicyProfileConfig::GenericRealtime,
            tiers: MarketDataStorageTierRuntimeConfig::default(),
        })
        .await
        .unwrap();

        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"live", "live"),
                tagged_record(b"backfill", "backfill"),
            ]))
            .await
            .unwrap();

        let hot = store
            .query_storage(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
            )
            .await
            .unwrap();
        let warm = store
            .query_storage(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
            )
            .await
            .unwrap();

        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].key, b"live".to_vec());
        assert_eq!(warm.len(), 1);
        assert_eq!(warm[0].key, b"backfill".to_vec());
    }

    fn unique_test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "fdc-server-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[tokio::test]
    async fn tiered_runtime_config_uses_configured_durable_tier_paths() {
        let root = unique_test_path("durable-tier-paths");
        let l2_path = root.join("l2.redb");
        let l3_path = root.join("l3.duckdb");
        let l4_path = root.join("l4-rocksdb");

        let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Tiered,
            policy_profile: MarketDataStoragePolicyProfileConfig::GenericRealtime,
            tiers: MarketDataStorageTierRuntimeConfig {
                l2_redb_path: Some(l2_path.clone()),
                l3_duckdb_path: Some(l3_path.clone()),
                l4_rocksdb_path: Some(l4_path.clone()),
            },
        })
        .await
        .unwrap();

        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"durable-live", "live"),
                tagged_record(b"durable-backfill", "backfill"),
            ]))
            .await
            .unwrap();

        let hot = store
            .query_storage(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
            )
            .await
            .unwrap();
        let warm = store
            .query_storage(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
            )
            .await
            .unwrap();

        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].key, b"durable-live".to_vec());
        assert_eq!(warm.len(), 1);
        assert_eq!(warm[0].key, b"durable-backfill".to_vec());
        assert!(
            l2_path.exists(),
            "redb file should exist at configured path"
        );
        assert!(
            l3_path.exists(),
            "duckdb file should exist at configured path"
        );
        assert!(
            l4_path.exists(),
            "rocksdb directory should exist at configured path"
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
