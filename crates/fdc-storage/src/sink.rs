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
