use fdc_core::Result;
use fdc_storage::{QueryableMarketDataStore, StorageTieringPolicy};

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
            QueryableMarketDataStore::memory_tiered_with_policy(policy_from_runtime_config(
                config.policy_profile,
            ))
            .await
        }
    }
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

    use super::*;

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
}
