//! Tier-aware storage-owned read/write store.
//!
//! This store bridges `StorageWriteSink` / `QueryableStorage` with `TierManager`
//! while keeping the storage boundary generic. It stores full
//! `StorageWriteRecord` values as bytes under a deterministic composite key.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageQuery,
    StorageTier, StorageWriteBatch, StorageWriteOutcome, StorageWriteRecord, StorageWriteSink,
    TierConfig, TierManager,
};

const KEY_SEPARATOR: u8 = 0;

#[derive(Clone)]
pub struct TieredStorageStore {
    tier_manager: Arc<TierManager>,
}

impl TieredStorageStore {
    pub fn new(tier_manager: Arc<TierManager>) -> Self {
        Self { tier_manager }
    }

    pub async fn memory_only() -> Result<Self> {
        let mut manager = TierManager::new();
        manager.add_tier(TierConfig::new(StorageTier::L1));
        manager.initialize().await?;
        Ok(Self::new(Arc::new(manager)))
    }

    pub fn tier_manager(&self) -> Arc<TierManager> {
        Arc::clone(&self.tier_manager)
    }

    pub fn storage_key_for_record(record: &StorageWriteRecord) -> Vec<u8> {
        storage_key(&record.namespace, &record.collection, &record.key)
    }
}

#[async_trait]
impl StorageWriteSink for TieredStorageStore {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;
        let batch_id = batch.batch_id;
        let record_count = batch.records.len();

        for record in batch.records {
            let key = Self::storage_key_for_record(&record);
            let value = encode_record(&record)?;
            self.tier_manager
                .put_with_placement(&key, &value, &record.placement)
                .await?;
        }

        Ok(StorageWriteOutcome::accepted(batch_id, record_count))
    }
}

#[async_trait]
impl QueryableStorage for TieredStorageStore {
    async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>> {
        query.validate()?;

        let prefix = query.collection.as_ref().map_or_else(
            || storage_namespace_prefix(&query.namespace),
            |collection| storage_collection_prefix(&query.namespace, collection),
        );

        let now = Utc::now();
        let mut seen = BTreeSet::new();
        let mut records = Vec::new();

        for (key, value) in self.tier_manager.scan_prefix(&prefix, None).await? {
            if !seen.insert(key) {
                continue;
            }

            let record = decode_record(&value)?;
            if record_is_expired(&record, now) {
                continue;
            }
            if record_matches_storage_query(&record, query) {
                records.push(record);
            }
        }

        Ok(apply_query_order_and_limit(records, query))
    }
}

fn storage_key(namespace: &str, collection: &str, key: &[u8]) -> Vec<u8> {
    let mut storage_key = Vec::with_capacity(namespace.len() + collection.len() + key.len() + 2);
    storage_key.extend_from_slice(namespace.as_bytes());
    storage_key.push(KEY_SEPARATOR);
    storage_key.extend_from_slice(collection.as_bytes());
    storage_key.push(KEY_SEPARATOR);
    storage_key.extend_from_slice(key);
    storage_key
}

fn storage_namespace_prefix(namespace: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(namespace.len() + 1);
    prefix.extend_from_slice(namespace.as_bytes());
    prefix.push(KEY_SEPARATOR);
    prefix
}

fn storage_collection_prefix(namespace: &str, collection: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(namespace.len() + collection.len() + 2);
    prefix.extend_from_slice(namespace.as_bytes());
    prefix.push(KEY_SEPARATOR);
    prefix.extend_from_slice(collection.as_bytes());
    prefix.push(KEY_SEPARATOR);
    prefix
}

fn encode_record(record: &StorageWriteRecord) -> Result<Vec<u8>> {
    bincode::serialize(record).map_err(Into::into)
}

fn decode_record(bytes: &[u8]) -> Result<StorageWriteRecord> {
    bincode::deserialize(bytes).map_err(Into::into)
}

fn record_is_expired(record: &StorageWriteRecord, now: DateTime<Utc>) -> bool {
    record
        .placement
        .ttl
        .map(|ttl| record.timestamp + ttl < now)
        .unwrap_or(false)
}

pub fn validate_storage_record_roundtrip(record: &StorageWriteRecord) -> Result<()> {
    let decoded = decode_record(&encode_record(record)?)?;
    if decoded != *record {
        return Err(Error::validation("storage record codec roundtrip mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};
    use std::collections::HashMap;

    use super::*;
    use crate::StorageEngineType;
    use crate::{StoragePlacementHint, StorageQueryOrder, StorageWriteMetadata};
    use tempfile::tempdir;

    fn tagged_record(key: &[u8], symbol: &str) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata
            .tags
            .insert("symbol".to_string(), symbol.to_string());
        StorageWriteRecord::new("market_data", "trades", key.to_vec(), b"value".to_vec())
            .with_metadata(metadata)
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L1))
    }

    #[test]
    fn record_codec_roundtrips_storage_write_record() {
        let record = tagged_record(b"1", "BTCUSDT");
        validate_storage_record_roundtrip(&record).unwrap();
    }

    #[tokio::test]
    async fn tiered_store_writes_and_queries_records_from_memory_tier() {
        let store = TieredStorageStore::memory_only().await.unwrap();
        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"1", "BTCUSDT"),
                tagged_record(b"2", "ETHUSDT"),
            ]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"1".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_applies_query_order_and_limit() {
        let store = TieredStorageStore::memory_only().await.unwrap();
        let first = tagged_record(b"1", "BTCUSDT").with_timestamp(Utc.timestamp_opt(1, 0).unwrap());
        let second =
            tagged_record(b"2", "BTCUSDT").with_timestamp(Utc.timestamp_opt(2, 0).unwrap());

        store
            .write_batch(StorageWriteBatch::new(vec![first, second]))
            .await
            .unwrap();

        let result = store
            .query_storage(
                &StorageQuery::new("market_data")
                    .with_order(StorageQueryOrder::TimestampDesc)
                    .with_limit(1),
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"2".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_filters_expired_ttl_records() {
        let store = TieredStorageStore::memory_only().await.unwrap();
        let expired = tagged_record(b"expired", "BTCUSDT")
            .with_timestamp(Utc::now() - Duration::seconds(10))
            .with_placement(
                StoragePlacementHint::for_tier(StorageTier::L1).with_ttl(Duration::seconds(1)),
            );
        let active = tagged_record(b"active", "BTCUSDT");

        store
            .write_batch(StorageWriteBatch::new(vec![expired, active]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"active".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_can_use_redb_l2_for_persistent_records() {
        let dir = tempdir().unwrap();
        let mut manager = TierManager::new();
        let mut config = TierConfig::new(StorageTier::L2);
        config.engine_type = StorageEngineType::Redb;
        config.engine_config = HashMap::from([(
            "db_path".to_string(),
            dir.path()
                .join("tiered-store.redb")
                .to_string_lossy()
                .to_string(),
        )]);
        manager.add_tier(config);
        manager.initialize().await.unwrap();

        let store = TieredStorageStore::new(Arc::new(manager));
        let record = tagged_record(b"l2", "BTCUSDT")
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L2));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"l2".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_can_use_duckdb_l3_for_warm_records() {
        let dir = tempdir().unwrap();
        let mut manager = TierManager::new();
        let mut config = TierConfig::new(StorageTier::L3);
        config.engine_type = StorageEngineType::DuckDB;
        config.engine_config = HashMap::from([(
            "db_path".to_string(),
            dir.path()
                .join("tiered-store.duckdb")
                .to_string_lossy()
                .to_string(),
        )]);
        manager.add_tier(config);
        manager.initialize().await.unwrap();

        let store = TieredStorageStore::new(Arc::new(manager));
        let record = tagged_record(b"l3", "BTCUSDT")
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L3));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"l3".to_vec());
        assert_eq!(result[0].value, b"value".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_can_use_rocksdb_l4_for_cold_records() {
        let dir = tempdir().unwrap();
        let mut manager = TierManager::new();
        let mut config = TierConfig::new(StorageTier::L4);
        config.engine_type = StorageEngineType::RocksDB;
        config.engine_config = HashMap::from([(
            "db_path".to_string(),
            dir.path()
                .join("tiered-store-rocksdb")
                .to_string_lossy()
                .to_string(),
        )]);
        manager.add_tier(config);
        manager.initialize().await.unwrap();

        let store = TieredStorageStore::new(Arc::new(manager));
        let record = tagged_record(b"l4", "BTCUSDT")
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L4));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"l4".to_vec());
        assert_eq!(result[0].value, b"value".to_vec());
    }
}
