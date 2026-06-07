//! Tier-aware storage-owned read/write store.
//!
//! This store bridges `StorageWriteSink` / `QueryableStorage` with `TierManager`
//! while keeping the storage boundary generic. It stores full
//! `StorageWriteRecord` values as bytes under a deterministic composite key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use tracing::{debug, info, instrument, warn};

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageEngineType,
    StorageHealthSnapshot, StorageMaintenanceAuditEntry, StorageMaintenanceErrorKind,
    StorageMaintenanceOptions, StorageMaintenanceReport, StorageQuery, StorageQueryMetrics,
    StorageQueryResult, StorageTier, StorageTierHealth, StorageTierHealthStatus,
    StorageTieringPolicy, StorageWriteBatch, StorageWriteOutcome, StorageWriteRecord,
    StorageWriteSink, TierConfig, TierLifecycleAction, TierLifecycleReport, TierManager,
};

const KEY_SEPARATOR: u8 = 0;

#[derive(Clone)]
pub struct TieredStorageStore {
    tier_manager: Arc<TierManager>,
    maintenance_running: Arc<AtomicBool>,
}

impl TieredStorageStore {
    pub fn new(tier_manager: Arc<TierManager>) -> Self {
        Self {
            tier_manager,
            maintenance_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn memory_only() -> Result<Self> {
        Self::memory_only_with_policy(StorageTieringPolicy::compatibility()).await
    }

    pub async fn memory_only_with_policy(policy: StorageTieringPolicy) -> Result<Self> {
        let mut manager = TierManager::with_policy(policy);
        for tier in [
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ] {
            manager.add_tier(memory_tier_config(tier));
        }
        manager.initialize().await?;
        Ok(Self::new(Arc::new(manager)))
    }

    pub fn tier_manager(&self) -> Arc<TierManager> {
        Arc::clone(&self.tier_manager)
    }

    pub fn storage_key_for_record(record: &StorageWriteRecord) -> Vec<u8> {
        storage_key(&record.namespace, &record.collection, &record.key)
    }

    #[instrument(skip(self, query), fields(
        namespace = %query.namespace,
        collection = query.collection.as_deref().unwrap_or(""),
        limit = ?query.limit,
        tier_scope = ?query.tier_scope
    ))]
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
        debug!(
            scanned_entries = metrics.scanned_entries,
            decoded_records = metrics.decoded_records,
            returned_records = metrics.returned_records,
            "storage query completed"
        );

        Ok(StorageQueryResult { records, metrics })
    }

    #[instrument(skip(self))]
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

        info!(
            scanned_entries = report.scanned_entries,
            ttl_deleted = report.ttl_deleted,
            retention_demoted = report.retention_demoted,
            retention_deleted = report.retention_deleted,
            retained = report.retained,
            decode_errors = report.decode_errors,
            "storage lifecycle pass completed"
        );

        Ok(report)
    }

    #[instrument(skip(self))]
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
        self.run_maintenance_once_with_options(StorageMaintenanceOptions::default())
            .await
    }

    #[instrument(skip(self, options), fields(
        timeout_ms = ?options.timeout.map(|timeout| timeout.as_millis())
    ))]
    pub async fn run_maintenance_once_with_options(
        &self,
        options: StorageMaintenanceOptions,
    ) -> Result<StorageMaintenanceReport> {
        let _guard = self.acquire_maintenance_guard()?;
        let timeout = options.timeout;
        if matches!(timeout, Some(timeout) if timeout.is_zero()) {
            return Err(StorageMaintenanceErrorKind::Timeout
                .storage_error("timed out before maintenance started"));
        }
        let maintenance = self.run_maintenance_once_inner();
        let report = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, maintenance)
                .await
                .map_err(|_| {
                    StorageMaintenanceErrorKind::Timeout
                        .storage_error(format!("timed out after {}ms", timeout.as_millis()))
                })??
        } else {
            maintenance.await?
        };

        if let Some(audit_sink) = options.audit_sink {
            let entry = StorageMaintenanceAuditEntry::from_report(&report);
            audit_sink
                .record_maintenance(entry)
                .await
                .map_err(|error| {
                    StorageMaintenanceErrorKind::AuditFailed.storage_error(error.to_string())
                })?;
        }

        Ok(report)
    }

    fn acquire_maintenance_guard(&self) -> Result<MaintenanceRunGuard> {
        self.maintenance_running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| {
                StorageMaintenanceErrorKind::AlreadyRunning
                    .storage_error("another maintenance run is already active")
            })?;
        Ok(MaintenanceRunGuard {
            running: Arc::clone(&self.maintenance_running),
        })
    }

    #[instrument(skip(self))]
    async fn run_maintenance_once_inner(&self) -> Result<StorageMaintenanceReport> {
        let started_at = Utc::now();
        let lifecycle = self.run_lifecycle_once().await?;
        let mut compacted_tiers = Vec::new();
        let mut compaction_errors = BTreeMap::new();
        let mut compaction_outcomes = Vec::new();
        let mut compaction_unsupported = 0;
        let mut compaction_failed = 0;

        for tier in self.tier_manager.initialized_tiers() {
            let outcome = self.tier_manager.compact_tier_with_outcome(&tier).await?;
            match outcome.kind {
                crate::StorageCompactionOutcomeKind::Compacted => {
                    compacted_tiers.push(tier.clone());
                    compaction_outcomes.push(outcome);
                }
                crate::StorageCompactionOutcomeKind::Unsupported => {
                    compaction_unsupported += 1;
                    compaction_outcomes.push(outcome);
                }
                crate::StorageCompactionOutcomeKind::Failed => {
                    compaction_failed += 1;
                    warn!(tier = ?tier, error = ?outcome.message, "storage compaction failed");
                    compaction_errors.insert(
                        tier.clone(),
                        outcome
                            .message
                            .clone()
                            .unwrap_or_else(|| "storage compaction failed".to_string()),
                    );
                    compaction_outcomes.push(outcome);
                }
            }
        }

        let health = self.storage_health_snapshot().await?;
        info!(
            compaction_compacted = compacted_tiers.len(),
            compaction_unsupported, compaction_failed, "storage maintenance pass completed"
        );
        Ok(StorageMaintenanceReport {
            started_at,
            finished_at: Utc::now(),
            lifecycle,
            health,
            compacted_tiers,
            compaction_errors,
            compaction_outcomes,
            compaction_unsupported,
            compaction_failed,
        })
    }
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = StorageEngineType::Memory;
    config
}

struct MaintenanceRunGuard {
    running: Arc<AtomicBool>,
}

impl Drop for MaintenanceRunGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

#[async_trait]
impl StorageWriteSink for TieredStorageStore {
    #[instrument(skip(self, batch), fields(batch_size = batch.records.len()))]
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;
        let batch_id = batch.batch_id;
        let record_count = batch.records.len();

        for record in batch.records {
            let key = Self::storage_key_for_record(&record);
            let value = encode_record(&record)?;
            let timestamp_age_seconds = Utc::now()
                .signed_duration_since(record.timestamp)
                .num_seconds();
            self.tier_manager
                .put_with_policy_context(
                    &key,
                    &value,
                    &record.namespace,
                    &record.collection,
                    &record.metadata.tags,
                    timestamp_age_seconds,
                    &record.placement,
                )
                .await?;
        }

        Ok(StorageWriteOutcome::accepted(batch_id, record_count))
    }
}

#[async_trait]
impl QueryableStorage for TieredStorageStore {
    #[instrument(skip(self, query), fields(namespace = %query.namespace, limit = ?query.limit))]
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
    use std::sync::Mutex;
    use std::time::Duration as StdDuration;

    use async_trait::async_trait;
    use fdc_core::Result;

    use super::*;
    use crate::StorageEngineType;
    use crate::{
        StorageMaintenanceAuditEntry, StorageMaintenanceAuditSink, StorageMaintenanceOptions,
        StoragePlacementHint, StorageQueryOrder, StorageTierScope, StorageTieringPolicy,
        StorageWriteMetadata,
    };
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

    async fn memory_store_with_l1_to_l4_and_policy(
        policy: StorageTieringPolicy,
    ) -> TieredStorageStore {
        let mut manager = TierManager::with_policy(policy);
        for tier in [
            StorageTier::L1,
            StorageTier::L2,
            StorageTier::L3,
            StorageTier::L4,
        ] {
            manager.add_tier(memory_tier_config(tier));
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
    async fn tiered_store_memory_only_with_policy_routes_generic_realtime_tags() {
        let store =
            memory_store_with_l1_to_l4_and_policy(StorageTieringPolicy::generic_realtime()).await;

        let mut live = tagged_record(b"live", "BTCUSDT");
        live.metadata
            .tags
            .insert("mode".to_string(), "live".to_string());
        live.placement = StoragePlacementHint::default();
        let mut backfill = tagged_record(b"backfill", "BTCUSDT");
        backfill
            .metadata
            .tags
            .insert("mode".to_string(), "backfill".to_string());
        backfill.placement = StoragePlacementHint::default();

        store
            .write_batch(StorageWriteBatch::new(vec![live.clone(), backfill.clone()]))
            .await
            .unwrap();

        let live_key = TieredStorageStore::storage_key_for_record(&live);
        let backfill_key = TieredStorageStore::storage_key_for_record(&backfill);
        assert!(store
            .tier_manager()
            .get_from_tier(&live_key, &StorageTier::L2)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .tier_manager()
            .get_from_tier(&backfill_key, &StorageTier::L3)
            .await
            .unwrap()
            .is_some());
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
    async fn lifecycle_ttl_delete_removes_duplicate_key_from_each_tier_copy() {
        let store = lifecycle_store_with_tiers(vec![
            memory_tier_config(StorageTier::L1),
            memory_tier_config(StorageTier::L2),
        ])
        .await;

        let record = tagged_record(b"ttl-duplicate", "BTCUSDT")
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

        assert!(store
            .tier_manager()
            .get_from_tier(&key, &StorageTier::L1)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .tier_manager()
            .get_from_tier(&key, &StorageTier::L2)
            .await
            .unwrap()
            .is_some());

        let report = store.run_lifecycle_once().await.unwrap();

        assert_eq!(report.ttl_deleted, 1);
        assert!(store
            .tier_manager()
            .get_from_tier(&key, &StorageTier::L1)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .tier_manager()
            .get_from_tier(&key, &StorageTier::L2)
            .await
            .unwrap()
            .is_none());
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
        assert!(report.compaction_errors.is_empty());
        assert_eq!(report.compaction_unsupported, 2);
        assert_eq!(report.compaction_failed, 0);
        assert_eq!(report.compaction_outcomes.len(), 2);
        assert!(report
            .metrics_snapshot()
            .to_prometheus_text()
            .contains("outcome=\"unsupported\""));
    }

    #[derive(Default)]
    struct RecordingAuditSink {
        entries: Mutex<Vec<StorageMaintenanceAuditEntry>>,
    }

    #[async_trait]
    impl StorageMaintenanceAuditSink for RecordingAuditSink {
        async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()> {
            self.entries.lock().unwrap().push(entry);
            Ok(())
        }
    }

    #[tokio::test]
    async fn maintenance_with_options_records_audit_entry() {
        let store = lifecycle_store_with_tiers(vec![memory_tier_config(StorageTier::L1)]).await;
        let audit = Arc::new(RecordingAuditSink::default());

        let report = store
            .run_maintenance_once_with_options(
                StorageMaintenanceOptions::new().with_audit_sink(audit.clone()),
            )
            .await
            .unwrap();

        let entries = audit.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].scanned_entries, report.lifecycle.scanned_entries);
        assert_eq!(
            entries[0].compaction_unsupported,
            report.compaction_unsupported
        );
    }

    #[tokio::test]
    async fn maintenance_timeout_releases_guard_for_next_run() {
        let store = lifecycle_store_with_tiers(vec![memory_tier_config(StorageTier::L1)]).await;

        let error = store
            .run_maintenance_once_with_options(
                StorageMaintenanceOptions::new().with_timeout(StdDuration::ZERO),
            )
            .await
            .expect_err("near-zero timeout should fail");

        assert!(error.to_string().contains("Timeout"));
        let report = store.run_maintenance_once().await.unwrap();
        assert!(report.finished_at >= report.started_at);
    }

    #[tokio::test]
    async fn maintenance_reentry_guard_rejects_concurrent_attempt() {
        let store = lifecycle_store_with_tiers(vec![memory_tier_config(StorageTier::L1)]).await;
        let _guard = store.acquire_maintenance_guard().unwrap();

        let error = store
            .run_maintenance_once()
            .await
            .expect_err("held guard should reject maintenance re-entry");

        assert!(error.to_string().contains("AlreadyRunning"));
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
