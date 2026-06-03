//! Tier-aware storage-owned read/write store.
//!
//! This store bridges `StorageWriteSink` / `QueryableStorage` with `TierManager`
//! while keeping the storage boundary generic. It stores full
//! `StorageWriteRecord` values as bytes under a deterministic composite key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageQuery,
    StorageHealthSnapshot, StorageMaintenanceReport, StorageQueryMetrics, StorageQueryResult,
    StorageTier, StorageTierHealth, StorageTierHealthStatus, StorageWriteBatch, StorageWriteOutcome,
    StorageWriteRecord, StorageWriteSink, TierConfig, TierLifecycleAction, TierLifecycleReport,
    TierManager,
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

    pub async fn query_storage_with_metrics(
        &self,
        query: &StorageQuery,
    ) -> Result<StorageQueryResult> {
        query.validate()?;

        let prefix = query.collection.as_ref().map_or_else(
            || storage_namespace_prefix(&query.namespace),
            |collection| storage_collection_prefix(&query.namespace, collection),
        );

        let tiers = self.tier_manager.tiers_for_scope(&query.tier_scope);
        let now = Utc::now();
        let mut seen = BTreeSet::new();
        let mut records = Vec::new();
        let mut metrics = StorageQueryMetrics::default();

        for (tier, key, value) in self
            .tier_manager
            .scan_prefix_in_tiers(&prefix, &tiers, None)
            .await?
        {
            metrics.scanned_entries += 1;
            *metrics.tier_hits.entry(tier).or_insert(0) += 1;

            if !seen.insert(key) {
                continue;
            }

            let record = decode_record(&value)?;
            metrics.decoded_records += 1;
            if record_is_expired(&record, now) {
                continue;
            }
            if record_matches_storage_query(&record, query) {
                records.push(record);
            }
        }

        let records = apply_query_order_and_limit(records, query);
        metrics.returned_records = records.len();

        Ok(StorageQueryResult { records, metrics })
    }

    pub async fn run_lifecycle_once(&self) -> Result<TierLifecycleReport> {
        let now = Utc::now();
        let tiers = self
            .tier_manager
            .tiers_for_scope(&crate::StorageTierScope::All);
        let mut report = TierLifecycleReport::default();

        for tier in tiers {
            let entries = self
                .tier_manager
                .scan_prefix_in_tiers(&[], &[tier.clone()], None)
                .await?;

            for (_, key, value) in entries {
                report.record_scanned(tier.clone());

                let record = match decode_record(&value) {
                    Ok(record) => record,
                    Err(_) => {
                        report.record_decode_error(tier.clone());
                        continue;
                    }
                };

                if record_is_expired(&record, now) {
                    self.tier_manager.delete(&key).await?;
                    report.record_action(tier.clone(), TierLifecycleAction::TtlExpiredDelete);
                    continue;
                }

                let retention_expired = self
                    .tier_manager
                    .tier_config(&tier)
                    .and_then(|config| config.retention_duration)
                    .map(|retention| record.timestamp + retention < now)
                    .unwrap_or(false);

                if retention_expired {
                    if let Some(target_tier) = self.tier_manager.next_colder_available_tier(&tier) {
                        self.tier_manager
                            .put_to_specific_tier(&key, &value, &target_tier)
                            .await?;
                        self.tier_manager.delete_from_tier(&key, &tier).await?;
                        report.record_action(tier.clone(), TierLifecycleAction::RetentionDemote);
                    } else {
                        self.tier_manager.delete_from_tier(&key, &tier).await?;
                        report.record_action(
                            tier.clone(),
                            TierLifecycleAction::RetentionExpiredDelete,
                        );
                    }
                } else {
                    report.record_action(tier.clone(), TierLifecycleAction::Retain);
                }
            }
        }

        Ok(report)
    }

    pub async fn storage_health_snapshot(&self) -> Result<StorageHealthSnapshot> {
        let stats = self.tier_manager.get_tier_stats().await?;
        let initialized: BTreeSet<_> = self.tier_manager.initialized_tiers().into_iter().collect();
        let mut tiers = BTreeMap::new();

        for tier in self.tier_manager.configured_tiers() {
            let enabled = self
                .tier_manager
                .tier_config(&tier)
                .map(|config| config.enabled)
                .unwrap_or(false);
            let initialized_tier = initialized.contains(&tier);
            let tier_stats = stats.get(&tier).cloned();
            let status = if enabled && !initialized_tier {
                StorageTierHealthStatus::MissingEngine
            } else if initialized_tier && tier_stats.is_none() {
                StorageTierHealthStatus::StatsUnavailable
            } else {
                StorageTierHealthStatus::Healthy
            };
            tiers.insert(
                tier.clone(),
                StorageTierHealth {
                    tier,
                    enabled,
                    initialized: initialized_tier,
                    status,
                    stats: tier_stats,
                    error: None,
                },
            );
        }

        Ok(StorageHealthSnapshot {
            captured_at: Utc::now(),
            tiers,
            access_patterns: self.tier_manager.get_access_patterns_count().await,
            migration_queue_len: self.tier_manager.get_migration_queue_length().await,
        })
    }

    pub async fn run_maintenance_once(&self) -> Result<StorageMaintenanceReport> {
        let started_at = Utc::now();
        let lifecycle = self.run_lifecycle_once().await?;
        let mut compacted_tiers = Vec::new();
        let mut compaction_errors = BTreeMap::new();

        for tier in self.tier_manager.initialized_tiers() {
            match self.tier_manager.compact_tier(&tier).await {
                Ok(()) => compacted_tiers.push(tier),
                Err(error) => {
                    compaction_errors.insert(tier, error.to_string());
                }
            }
        }

        let health = self.storage_health_snapshot().await?;
        Ok(StorageMaintenanceReport {
            started_at,
            finished_at: Utc::now(),
            lifecycle,
            health,
            compacted_tiers,
            compaction_errors,
        })
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
        Ok(self.query_storage_with_metrics(query).await?.records)
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
    use crate::{StoragePlacementHint, StorageQueryOrder, StorageTierScope, StorageWriteMetadata};
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

    async fn memory_store_with_l1_to_l4() -> TieredStorageStore {
        let mut manager = TierManager::new();
        for tier in [
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ] {
            let mut config = TierConfig::new(tier);
            config.engine_type = StorageEngineType::Memory;
            manager.add_tier(config);
        }
        manager.initialize().await.unwrap();
        TieredStorageStore::new(Arc::new(manager))
    }

    async fn lifecycle_store_with_tiers(configs: Vec<TierConfig>) -> TieredStorageStore {
        let mut manager = TierManager::new();
        for config in configs {
            manager.add_tier(config);
        }
        manager.initialize().await.unwrap();
        TieredStorageStore::new(Arc::new(manager))
    }

    fn memory_tier_config(tier: StorageTier) -> TierConfig {
        let mut config = TierConfig::new(tier);
        config.engine_type = StorageEngineType::Memory;
        config
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
    async fn tiered_store_query_can_scope_to_only_one_tier() {
        let store = memory_store_with_l1_to_l4().await;
        let l1 = tagged_record(b"same", "BTCUSDT")
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
        let l3 = tagged_record(b"l3", "BTCUSDT")
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L3));

        store
            .write_batch(StorageWriteBatch::new(vec![l1, l3]))
            .await
            .unwrap();

        let result = store
            .query_storage_with_metrics(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
            )
            .await
            .unwrap();

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].key, b"l3".to_vec());
        assert_eq!(result.metrics.tier_hits.get(&StorageTier::L3), Some(&1));
        assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L1));
    }

    #[tokio::test]
    async fn tiered_store_query_hot_scope_reads_l1_l2_only() {
        let store = memory_store_with_l1_to_l4().await;
        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"l1", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L1)),
                tagged_record(b"l2", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L2)),
                tagged_record(b"l3", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L3)),
                tagged_record(b"l4", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L4)),
            ]))
            .await
            .unwrap();

        let result = store
            .query_storage_with_metrics(
                &StorageQuery::new("market_data")
                    .with_tier_scope(StorageTierScope::Hot)
                    .with_order(StorageQueryOrder::KeyAsc),
            )
            .await
            .unwrap();

        let keys: Vec<_> = result
            .records
            .iter()
            .map(|record| record.key.as_slice())
            .collect();
        assert_eq!(keys, vec![b"l1".as_slice(), b"l2".as_slice()]);
        assert_eq!(result.metrics.tier_hits.get(&StorageTier::L1), Some(&1));
        assert_eq!(result.metrics.tier_hits.get(&StorageTier::L2), Some(&1));
        assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L3));
        assert!(!result.metrics.tier_hits.contains_key(&StorageTier::L4));
    }

    #[tokio::test]
    async fn tiered_store_query_warm_and_cold_scopes_read_expected_tiers() {
        let store = memory_store_with_l1_to_l4().await;
        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"l3", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L3)),
                tagged_record(b"l4", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L4)),
            ]))
            .await
            .unwrap();

        let warm = store
            .query_storage_with_metrics(
                &StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Warm),
            )
            .await
            .unwrap();
        let cold = store
            .query_storage_with_metrics(
                &StorageQuery::new("market_data").with_tier_scope(StorageTierScope::Cold),
            )
            .await
            .unwrap();

        assert_eq!(warm.records.len(), 1);
        assert_eq!(warm.records[0].key, b"l3".to_vec());
        assert_eq!(cold.records.len(), 1);
        assert_eq!(cold.records[0].key, b"l4".to_vec());
    }

    #[tokio::test]
    async fn tiered_store_query_metrics_report_scanned_decoded_returned_and_tier_hits() {
        let store = memory_store_with_l1_to_l4().await;
        store
            .write_batch(StorageWriteBatch::new(vec![
                tagged_record(b"btc", "BTCUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L1)),
                tagged_record(b"eth", "ETHUSDT")
                    .with_placement(StoragePlacementHint::for_tier(StorageTier::L2)),
            ]))
            .await
            .unwrap();

        let result = store
            .query_storage_with_metrics(
                &StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"),
            )
            .await
            .unwrap();

        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].key, b"btc".to_vec());
        assert_eq!(result.metrics.scanned_entries, 2);
        assert_eq!(result.metrics.decoded_records, 2);
        assert_eq!(result.metrics.returned_records, 1);
        assert_eq!(result.metrics.tier_hits.get(&StorageTier::L1), Some(&1));
        assert_eq!(result.metrics.tier_hits.get(&StorageTier::L2), Some(&1));
    }

    #[tokio::test]
    async fn lifecycle_hard_deletes_ttl_expired_record_from_all_tiers() {
        let store = lifecycle_store_with_tiers(vec![
            memory_tier_config(StorageTier::L1),
            memory_tier_config(StorageTier::L2),
        ])
        .await;

        let record = tagged_record(b"ttl", "BTCUSDT")
            .with_timestamp(Utc::now() - Duration::seconds(10))
            .with_placement(
                StoragePlacementHint::for_tier(StorageTier::L1).with_ttl(Duration::seconds(1)),
            );
        let key = TieredStorageStore::storage_key_for_record(&record);
        let value = encode_record(&record).unwrap();

        store
            .tier_manager()
            .put_to_specific_tier(&key, &value, &StorageTier::L1)
            .await
            .unwrap();
        store
            .tier_manager()
            .put_to_specific_tier(&key, &value, &StorageTier::L2)
            .await
            .unwrap();

        let report = store.run_lifecycle_once().await.unwrap();

        assert_eq!(report.scanned_entries, 1);
        assert_eq!(report.ttl_deleted, 1);
        assert!(store.tier_manager().get(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lifecycle_demotes_retention_expired_record_to_next_colder_tier() {
        let mut l1 = memory_tier_config(StorageTier::L1);
        l1.retention_duration = Some(Duration::seconds(1));
        let l2 = memory_tier_config(StorageTier::L2);
        let store = lifecycle_store_with_tiers(vec![l1, l2]).await;

        let record = tagged_record(b"demote", "BTCUSDT")
            .with_timestamp(Utc::now() - Duration::seconds(10))
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let report = store.run_lifecycle_once().await.unwrap();
        let query = StorageQuery::new("market_data")
            .with_tier_scope(StorageTierScope::Only(StorageTier::L2));
        let result = store.query_storage_with_metrics(&query).await.unwrap();

        assert_eq!(report.retention_demoted, 1);
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0].key, b"demote".to_vec());
    }

    #[tokio::test]
    async fn lifecycle_deletes_retention_expired_record_when_no_colder_tier_exists() {
        let mut l4 = memory_tier_config(StorageTier::L4);
        l4.retention_duration = Some(Duration::seconds(1));
        let store = lifecycle_store_with_tiers(vec![l4]).await;

        let record = tagged_record(b"delete", "BTCUSDT")
            .with_timestamp(Utc::now() - Duration::seconds(10))
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L4));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let report = store.run_lifecycle_once().await.unwrap();
        let result = store
            .query_storage_with_metrics(&StorageQuery::new("market_data"))
            .await
            .unwrap();

        assert_eq!(report.retention_deleted, 1);
        assert!(result.records.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_report_counts_retained_and_per_tier_actions() {
        let mut l1 = memory_tier_config(StorageTier::L1);
        l1.retention_duration = Some(Duration::days(1));
        let store = lifecycle_store_with_tiers(vec![l1]).await;

        let record = tagged_record(b"keep", "BTCUSDT")
            .with_timestamp(Utc::now())
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let report = store.run_lifecycle_once().await.unwrap();

        assert_eq!(report.scanned_entries, 1);
        assert_eq!(report.retained, 1);
        assert_eq!(report.tier_reports[&StorageTier::L1].scanned_entries, 1);
        assert_eq!(report.tier_reports[&StorageTier::L1].retained, 1);
    }

    #[tokio::test]
    async fn storage_health_snapshot_reports_initialized_and_disabled_tiers() {
        let l1 = memory_tier_config(StorageTier::L1);
        let mut l2 = memory_tier_config(StorageTier::L2);
        l2.enabled = false;
        let store = lifecycle_store_with_tiers(vec![l1, l2]).await;

        let snapshot = store.storage_health_snapshot().await.unwrap();

        assert_eq!(
            snapshot.tiers[&StorageTier::L1].status,
            StorageTierHealthStatus::Healthy
        );
        assert!(snapshot.tiers[&StorageTier::L1].initialized);
        assert!(!snapshot.tiers[&StorageTier::L2].enabled);
        assert!(!snapshot.tiers[&StorageTier::L2].initialized);
    }

    #[tokio::test]
    async fn maintenance_pass_runs_lifecycle_and_records_compaction_errors() {
        let mut l1 = memory_tier_config(StorageTier::L1);
        l1.retention_duration = Some(Duration::seconds(1));
        let l2 = memory_tier_config(StorageTier::L2);
        let store = lifecycle_store_with_tiers(vec![l1, l2]).await;
        let record = tagged_record(b"maintenance-demote", "BTCUSDT")
            .with_timestamp(Utc::now() - Duration::seconds(10))
            .with_placement(StoragePlacementHint::for_tier(StorageTier::L1));
        store
            .write_batch(StorageWriteBatch::new(vec![record]))
            .await
            .unwrap();

        let report = store.run_maintenance_once().await.unwrap();

        assert_eq!(report.lifecycle.retention_demoted, 1);
        assert!(report.health.tiers.contains_key(&StorageTier::L1));
        assert!(report.compaction_errors.contains_key(&StorageTier::L1));
        assert!(report.compaction_errors.contains_key(&StorageTier::L2));
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
