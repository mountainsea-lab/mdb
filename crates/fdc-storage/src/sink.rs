//! Storage write sink boundary.
//!
//! B5 provides a deterministic in-memory recording sink. Production tier-aware
//! routing to TierManager, ShardManager, and concrete engines is future work.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::write::{StorageWriteBatch, StorageWriteRecord};

/// Outcome for an accepted storage write batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWriteOutcome {
    pub batch_id: Uuid,
    pub accepted_records: usize,
    pub rejected_records: usize,
    pub accepted_at: DateTime<Utc>,
}

impl StorageWriteOutcome {
    pub fn accepted(batch_id: Uuid, accepted_records: usize) -> Self {
        Self {
            batch_id,
            accepted_records,
            rejected_records: 0,
            accepted_at: Utc::now(),
        }
    }
}

/// Async sink for storage-owned write batches.
#[async_trait]
pub trait StorageWriteSink: Send + Sync {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome>;
}

/// File-free, database-free sink used for contracts and demos.
#[derive(Debug, Default)]
pub struct RecordingStorageSink {
    batches: RwLock<Vec<StorageWriteBatch>>,
}

impl RecordingStorageSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn recorded_batches(&self) -> Vec<StorageWriteBatch> {
        self.batches.read().clone()
    }

    pub fn recorded_records(&self) -> Vec<StorageWriteRecord> {
        self.batches
            .read()
            .iter()
            .flat_map(|batch| batch.records.iter().cloned())
            .collect()
    }

    pub fn recorded_batch_count(&self) -> usize {
        self.batches.read().len()
    }

    pub fn recorded_record_count(&self) -> usize {
        self.batches
            .read()
            .iter()
            .map(|batch| batch.records.len())
            .sum()
    }
}

#[async_trait]
impl StorageWriteSink for RecordingStorageSink {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;

        let outcome = StorageWriteOutcome::accepted(batch.batch_id, batch.records.len());
        self.batches.write().push(batch);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_batch() -> StorageWriteBatch {
        StorageWriteBatch::new(vec![StorageWriteRecord::new(
            "namespace",
            "collection",
            b"key".to_vec(),
            b"value".to_vec(),
        )])
    }

    #[tokio::test]
    async fn recording_sink_records_batches_in_order() {
        let sink = RecordingStorageSink::new();
        let first = valid_batch();
        let second = StorageWriteBatch::new(vec![StorageWriteRecord::new(
            "namespace",
            "collection",
            b"key-2".to_vec(),
            b"value-2".to_vec(),
        )]);
        let first_id = first.batch_id;
        let second_id = second.batch_id;

        sink.write_batch(first).await.expect("first batch should write");
        sink.write_batch(second).await.expect("second batch should write");

        let batches = sink.recorded_batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].batch_id, first_id);
        assert_eq!(batches[1].batch_id, second_id);
    }
}
