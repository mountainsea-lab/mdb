//! Generic storage query boundary.
//!
//! This module intentionally only understands storage-owned fields such as
//! namespace, collection, key, timestamp, and metadata tags. Business-specific
//! query wrappers should translate their filters into this generic model.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use serde::{Deserialize, Serialize};

use crate::StorageWriteRecord;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageQueryOrder {
    Insertion,
    TimestampAsc,
    TimestampDesc,
    KeyAsc,
    KeyDesc,
}

impl Default for StorageQueryOrder {
    fn default() -> Self {
        Self::Insertion
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQuery {
    pub namespace: String,
    pub collection: Option<String>,
    pub key_prefix: Option<Vec<u8>>,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub tags: BTreeMap<String, String>,
    pub limit: Option<usize>,
    pub order: StorageQueryOrder,
}

impl StorageQuery {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            collection: None,
            key_prefix: None,
            start_key: None,
            end_key: None,
            start_time: None,
            end_time: None,
            tags: BTreeMap::new(),
            limit: None,
            order: StorageQueryOrder::default(),
        }
    }

    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    pub fn with_key_prefix(mut self, key_prefix: Vec<u8>) -> Self {
        self.key_prefix = Some(key_prefix);
        self
    }

    pub fn with_key_range(mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        self.start_key = Some(start_key);
        self.end_key = Some(end_key);
        self
    }

    pub fn with_time_range(mut self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        self.start_time = Some(start_time);
        self.end_time = Some(end_time);
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_order(mut self, order: StorageQueryOrder) -> Self {
        self.order = order;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(Error::validation(
                "storage query namespace must not be empty",
            ));
        }
        if matches!(self.limit, Some(0)) {
            return Err(Error::validation(
                "storage query limit must be greater than zero",
            ));
        }
        if let (Some(start), Some(end)) = (&self.start_key, &self.end_key) {
            if start > end {
                return Err(Error::validation(
                    "storage query start_key must not be greater than end_key",
                ));
            }
        }
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            if start > end {
                return Err(Error::validation(
                    "storage query start_time must not be after end_time",
                ));
            }
        }
        for key in self.tags.keys() {
            if key.trim().is_empty() {
                return Err(Error::validation("storage query tag key must not be empty"));
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait QueryableStorage: Send + Sync {
    async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>>;
}

pub(crate) fn record_matches_storage_query(
    record: &StorageWriteRecord,
    query: &StorageQuery,
) -> bool {
    if record.namespace != query.namespace {
        return false;
    }
    if let Some(collection) = &query.collection {
        if &record.collection != collection {
            return false;
        }
    }
    if let Some(prefix) = &query.key_prefix {
        if !record.key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(start_key) = &query.start_key {
        if &record.key < start_key {
            return false;
        }
    }
    if let Some(end_key) = &query.end_key {
        if &record.key > end_key {
            return false;
        }
    }
    if let Some(start_time) = query.start_time {
        if record.timestamp < start_time {
            return false;
        }
    }
    if let Some(end_time) = query.end_time {
        if record.timestamp > end_time {
            return false;
        }
    }
    for (key, value) in &query.tags {
        if record.metadata.tags.get(key) != Some(value) {
            return false;
        }
    }
    true
}

pub(crate) fn apply_query_order_and_limit(
    mut records: Vec<StorageWriteRecord>,
    query: &StorageQuery,
) -> Vec<StorageWriteRecord> {
    match query.order {
        StorageQueryOrder::Insertion => {}
        StorageQueryOrder::TimestampAsc => records.sort_by_key(|record| record.timestamp),
        StorageQueryOrder::TimestampDesc => {
            records.sort_by_key(|record| std::cmp::Reverse(record.timestamp));
        }
        StorageQueryOrder::KeyAsc => records.sort_by(|left, right| left.key.cmp(&right.key)),
        StorageQueryOrder::KeyDesc => records.sort_by(|left, right| right.key.cmp(&left.key)),
    }

    if let Some(limit) = query.limit {
        records.truncate(limit);
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    use crate::{StorageWriteMetadata, StorageWriteRecord};

    fn record(
        namespace: &str,
        collection: &str,
        key: &[u8],
        timestamp_seconds: i64,
    ) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata
            .tags
            .insert("symbol".to_string(), "BTCUSDT".to_string());
        StorageWriteRecord::new(namespace, collection, key.to_vec(), b"value".to_vec())
            .with_timestamp(Utc.timestamp_opt(timestamp_seconds, 0).unwrap())
            .with_metadata(metadata)
    }

    #[test]
    fn storage_query_validation_rejects_empty_namespace() {
        let query = StorageQuery::new(" ");
        assert!(query.validate().is_err());
    }

    #[test]
    fn storage_query_matches_generic_filters() {
        let query = StorageQuery::new("market_data")
            .with_collection("trades")
            .with_key_prefix(b"BTC".to_vec())
            .with_time_range(
                Utc.timestamp_opt(10, 0).unwrap(),
                Utc.timestamp_opt(20, 0).unwrap(),
            )
            .with_tag("symbol", "BTCUSDT");

        let matching = record("market_data", "trades", b"BTC/001", 15);
        let wrong_symbol = record("market_data", "trades", b"ETH/001", 15);

        assert!(record_matches_storage_query(&matching, &query));
        assert!(!record_matches_storage_query(&wrong_symbol, &query));
    }

    #[test]
    fn query_order_and_limit_are_applied_after_filtering() {
        let query = StorageQuery::new("namespace")
            .with_order(StorageQueryOrder::TimestampDesc)
            .with_limit(2);

        let ordered = apply_query_order_and_limit(
            vec![
                record("namespace", "collection", b"1", 1),
                record("namespace", "collection", b"2", 3),
                record("namespace", "collection", b"3", 2),
            ],
            &query,
        );

        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].key, b"2".to_vec());
        assert_eq!(ordered[1].key, b"3".to_vec());
    }
}
