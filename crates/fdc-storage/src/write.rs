//! Storage-owned write boundary types.
//!
//! These types intentionally do not depend on ingestion, transform, or adapter
//! crates. Upstream orchestration code is responsible for mapping domain DTOs
//! into these generic storage write records.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use fdc_core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tier::StorageTier;

/// Generic storage-owned write record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWriteRecord {
    pub namespace: String,
    pub collection: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub metadata: StorageWriteMetadata,
    pub placement: StoragePlacementHint,
}

impl StorageWriteRecord {
    pub fn new(
        namespace: impl Into<String>,
        collection: impl Into<String>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            collection: collection.into(),
            key,
            value,
            timestamp: Utc::now(),
            metadata: StorageWriteMetadata::default(),
            placement: StoragePlacementHint::default(),
        }
    }

    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_metadata(mut self, metadata: StorageWriteMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_placement(mut self, placement: StoragePlacementHint) -> Self {
        self.placement = placement;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(Error::validation(
                "storage write record namespace must not be empty",
            ));
        }
        if self.collection.trim().is_empty() {
            return Err(Error::validation(
                "storage write record collection must not be empty",
            ));
        }
        if self.key.is_empty() {
            return Err(Error::validation(
                "storage write record key must not be empty",
            ));
        }
        if self.value.is_empty() {
            return Err(Error::validation(
                "storage write record value must not be empty",
            ));
        }
        self.metadata.validate()
    }
}

/// Generic descriptive metadata for a storage write record.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StorageWriteMetadata {
    pub content_type: Option<String>,
    pub schema: Option<String>,
    pub schema_version: Option<String>,
    pub source: Option<String>,
    pub tags: BTreeMap<String, String>,
}

impl StorageWriteMetadata {
    pub fn validate(&self) -> Result<()> {
        for key in self.tags.keys() {
            if key.trim().is_empty() {
                return Err(Error::validation(
                    "storage write metadata tag key must not be empty",
                ));
            }
        }
        Ok(())
    }
}

/// Advisory placement information for future tier and shard routing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StoragePlacementHint {
    pub target_tier: Option<StorageTier>,
    pub access_pattern: StorageAccessPatternHint,
    pub durability: StorageDurabilityHint,
    pub shard_key: Option<Vec<u8>>,
    pub ttl: Option<Duration>,
}

impl StoragePlacementHint {
    pub fn for_tier(tier: StorageTier) -> Self {
        Self {
            target_tier: Some(tier),
            ..Self::default()
        }
    }

    pub fn with_access_pattern(mut self, access_pattern: StorageAccessPatternHint) -> Self {
        self.access_pattern = access_pattern;
        self
    }

    pub fn with_durability(mut self, durability: StorageDurabilityHint) -> Self {
        self.durability = durability;
        self
    }

    pub fn with_shard_key(mut self, shard_key: Vec<u8>) -> Self {
        self.shard_key = Some(shard_key);
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

/// Advisory access heat category for future routing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StorageAccessPatternHint {
    UltraHot,
    Hot,
    Warm,
    Cold,
    #[default]
    Unspecified,
}

/// Advisory durability category for future routing.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StorageDurabilityHint {
    Ephemeral,
    Cached,
    Persistent,
    Archival,
    #[default]
    Unspecified,
}

/// Metadata for a bounded storage write batch.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StorageBatchMetadata {
    pub source: Option<String>,
    pub tags: BTreeMap<String, String>,
}

impl StorageBatchMetadata {
    pub fn validate(&self) -> Result<()> {
        for key in self.tags.keys() {
            if key.trim().is_empty() {
                return Err(Error::validation(
                    "storage write batch metadata tag key must not be empty",
                ));
            }
        }
        Ok(())
    }
}

/// Bounded group of storage-owned write records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWriteBatch {
    pub batch_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub records: Vec<StorageWriteRecord>,
    pub metadata: StorageBatchMetadata,
}

impl StorageWriteBatch {
    pub fn new(records: Vec<StorageWriteRecord>) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            created_at: Utc::now(),
            records,
            metadata: StorageBatchMetadata::default(),
        }
    }

    pub fn with_metadata(mut self, metadata: StorageBatchMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn validate(&self) -> Result<()> {
        if self.records.is_empty() {
            return Err(Error::validation("storage write batch must not be empty"));
        }
        self.metadata.validate()?;
        for record in &self.records {
            record.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_record() -> StorageWriteRecord {
        StorageWriteRecord::new(
            "namespace",
            "collection",
            b"key".to_vec(),
            b"value".to_vec(),
        )
    }

    #[test]
    fn record_validation_rejects_empty_namespace() {
        let record = StorageWriteRecord::new(" ", "collection", b"key".to_vec(), b"value".to_vec());
        let error = record.validate().expect_err("blank namespace should fail");
        assert!(error.to_string().contains("namespace must not be empty"));
    }

    #[test]
    fn record_validation_rejects_empty_collection() {
        let record = StorageWriteRecord::new("namespace", "", b"key".to_vec(), b"value".to_vec());
        let error = record.validate().expect_err("empty collection should fail");
        assert!(error.to_string().contains("collection must not be empty"));
    }

    #[test]
    fn record_validation_rejects_empty_value() {
        let record =
            StorageWriteRecord::new("namespace", "collection", b"key".to_vec(), Vec::new());
        let error = record.validate().expect_err("empty value should fail");
        assert!(error.to_string().contains("value must not be empty"));
    }

    #[test]
    fn metadata_validation_rejects_empty_tag_key() {
        let mut metadata = StorageWriteMetadata::default();
        metadata.tags.insert(" ".to_string(), "bad".to_string());
        let record = valid_record().with_metadata(metadata);

        let error = record
            .validate()
            .expect_err("blank metadata tag key should fail");
        assert!(error
            .to_string()
            .contains("metadata tag key must not be empty"));
    }

    #[test]
    fn batch_metadata_validation_rejects_empty_tag_key() {
        let mut metadata = StorageBatchMetadata::default();
        metadata.tags.insert("".to_string(), "bad".to_string());
        let batch = StorageWriteBatch::new(vec![valid_record()]).with_metadata(metadata);

        let error = batch
            .validate()
            .expect_err("blank batch tag key should fail");
        assert!(error
            .to_string()
            .contains("batch metadata tag key must not be empty"));
    }

    #[test]
    fn batch_helpers_report_record_count() {
        let batch = StorageWriteBatch::new(vec![valid_record(), valid_record()]);

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }
}
