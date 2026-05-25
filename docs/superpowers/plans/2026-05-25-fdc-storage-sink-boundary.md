# B5 Tier-aware Storage Sink Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a tier-aware, storage-owned write boundary to `fdc-storage` without coupling storage to transform, ingestion, or adapter crates.

**Architecture:** `fdc-storage` gets two focused modules: `write.rs` for write records, batches, metadata, validation, and placement hints; `sink.rs` for the async sink trait, write outcome, and an in-memory recording sink. Placement hints align with the existing L1/L2/L3/L4 `StorageTier` and future shard routing, but B5 does not implement production `TierManager` routing or real database writes.

**Tech Stack:** Rust 1.95 workspace, `fdc-core::Result`, `fdc_core::error::Error`, `async-trait`, `chrono`, `uuid`, `parking_lot`, existing `fdc-storage::tier::StorageTier`.

---

## File Structure

- Create: `crates/fdc-storage/src/write.rs`  
  Owns `StorageWriteRecord`, `StorageWriteMetadata`, `StoragePlacementHint`, `StorageAccessPatternHint`, `StorageDurabilityHint`, `StorageWriteBatch`, `StorageBatchMetadata`, constructors, and validation.

- Create: `crates/fdc-storage/src/sink.rs`  
  Owns `StorageWriteSink`, `StorageWriteOutcome`, and `RecordingStorageSink`.

- Modify: `crates/fdc-storage/src/lib.rs`  
  Exports `write` and `sink` modules plus common public types.

- Create: `crates/fdc-storage/tests/storage_sink_boundary_contract.rs`  
  Contract tests for valid tier-aware writes, placement preservation, validation failures, atomic rejection, object-safe sink use, and dependency boundaries.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Adds B5 completion status, verification evidence, and next recommended slice.

---

## Task 1: Add failing storage sink boundary contract tests

**Files:**
- Create: `crates/fdc-storage/tests/storage_sink_boundary_contract.rs`

- [ ] **Step 1: Write the failing contract test file**

Create `crates/fdc-storage/tests/storage_sink_boundary_contract.rs` with this complete content:

```rust
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Duration;
use fdc_storage::{
    RecordingStorageSink, StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint,
    StorageTier, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord, StorageWriteSink,
};

fn sample_metadata() -> StorageWriteMetadata {
    let mut tags = BTreeMap::new();
    tags.insert("asset_class".to_string(), "crypto".to_string());
    tags.insert("granularity".to_string(), "trade".to_string());

    StorageWriteMetadata {
        content_type: Some("application/json".to_string()),
        schema: Some("generic.market_data.trade".to_string()),
        schema_version: Some("1".to_string()),
        source: Some("integration-test".to_string()),
        tags,
    }
}

fn tiered_placement() -> StoragePlacementHint {
    StoragePlacementHint {
        target_tier: Some(StorageTier::L2),
        access_pattern: StorageAccessPatternHint::Hot,
        durability: StorageDurabilityHint::Persistent,
        shard_key: Some(b"binance:btc-usdt".to_vec()),
        ttl: Some(Duration::hours(6)),
    }
}

fn sample_record(key: &[u8], value: &[u8]) -> StorageWriteRecord {
    StorageWriteRecord::new(
        "market_data",
        "trades",
        key.to_vec(),
        value.to_vec(),
    )
    .with_metadata(sample_metadata())
    .with_placement(tiered_placement())
}

#[tokio::test]
async fn recording_sink_accepts_valid_tier_aware_batch() {
    let sink = RecordingStorageSink::default();
    let batch = StorageWriteBatch::new(vec![sample_record(
        b"trade:binance:btc-usdt:1",
        br#"{"price":"100.00"}"#,
    )]);
    let batch_id = batch.batch_id;

    let outcome = sink.write_batch(batch).await.expect("valid batch should write");

    assert_eq!(outcome.batch_id, batch_id);
    assert_eq!(outcome.accepted_records, 1);
    assert_eq!(outcome.rejected_records, 0);
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn recording_sink_preserves_record_metadata_and_placement() {
    let sink = RecordingStorageSink::default();
    let record = sample_record(
        b"trade:binance:eth-usdt:1",
        br#"{"price":"200.00"}"#,
    );
    let batch = StorageWriteBatch::new(vec![record.clone()]);

    sink.write_batch(batch).await.expect("valid batch should write");

    let records = sink.recorded_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].namespace, "market_data");
    assert_eq!(records[0].collection, "trades");
    assert_eq!(records[0].key, b"trade:binance:eth-usdt:1".to_vec());
    assert_eq!(records[0].value, br#"{"price":"200.00"}"#.to_vec());
    assert_eq!(records[0].metadata.content_type.as_deref(), Some("application/json"));
    assert_eq!(records[0].metadata.schema.as_deref(), Some("generic.market_data.trade"));
    assert_eq!(records[0].metadata.schema_version.as_deref(), Some("1"));
    assert_eq!(records[0].metadata.source.as_deref(), Some("integration-test"));
    assert_eq!(
        records[0].metadata.tags.get("asset_class").map(String::as_str),
        Some("crypto")
    );
    assert_eq!(records[0].placement.target_tier, Some(StorageTier::L2));
    assert_eq!(records[0].placement.access_pattern, StorageAccessPatternHint::Hot);
    assert_eq!(records[0].placement.durability, StorageDurabilityHint::Persistent);
    assert_eq!(
        records[0].placement.shard_key.as_deref(),
        Some(&b"binance:btc-usdt"[..])
    );
    assert_eq!(records[0].placement.ttl, Some(Duration::hours(6)));
}

#[tokio::test]
async fn empty_batch_is_rejected_without_mutating_sink_state() {
    let sink = RecordingStorageSink::default();
    let valid_batch = StorageWriteBatch::new(vec![sample_record(b"valid", b"value")]);
    sink.write_batch(valid_batch).await.expect("seed batch should write");

    let empty_batch = StorageWriteBatch::new(Vec::new());
    let error = sink
        .write_batch(empty_batch)
        .await
        .expect_err("empty batch should be rejected");

    assert!(error.to_string().contains("storage write batch must not be empty"));
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn invalid_record_is_rejected_atomically() {
    let sink = RecordingStorageSink::default();
    let valid_seed = StorageWriteBatch::new(vec![sample_record(b"seed", b"seed-value")]);
    sink.write_batch(valid_seed).await.expect("seed batch should write");

    let invalid_record = StorageWriteRecord::new("market_data", "trades", Vec::new(), b"value".to_vec());
    let mixed_batch = StorageWriteBatch::new(vec![sample_record(b"valid-2", b"value-2"), invalid_record]);

    let error = sink
        .write_batch(mixed_batch)
        .await
        .expect_err("batch with empty record key should be rejected");

    assert!(error.to_string().contains("storage write record key must not be empty"));
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn sink_trait_is_usable_behind_arc_dyn_object() {
    let sink: Arc<dyn StorageWriteSink> = Arc::new(RecordingStorageSink::default());
    let batch = StorageWriteBatch::new(vec![sample_record(b"dyn-key", b"dyn-value")]);

    let outcome = sink.write_batch(batch).await.expect("dyn sink should write");

    assert_eq!(outcome.accepted_records, 1);
    assert_eq!(outcome.rejected_records, 0);
}

#[test]
fn default_placement_is_unspecified_and_engine_free() {
    let placement = StoragePlacementHint::default();

    assert_eq!(placement.target_tier, None);
    assert_eq!(placement.access_pattern, StorageAccessPatternHint::Unspecified);
    assert_eq!(placement.durability, StorageDurabilityHint::Unspecified);
    assert_eq!(placement.shard_key, None);
    assert_eq!(placement.ttl, None);
}

#[test]
fn placement_hints_can_reference_all_existing_storage_tiers() {
    let placements = [
        StoragePlacementHint::for_tier(StorageTier::L1),
        StoragePlacementHint::for_tier(StorageTier::L2),
        StoragePlacementHint::for_tier(StorageTier::L3),
        StoragePlacementHint::for_tier(StorageTier::L4),
    ];

    assert_eq!(placements[0].target_tier, Some(StorageTier::L1));
    assert_eq!(placements[1].target_tier, Some(StorageTier::L2));
    assert_eq!(placements[2].target_tier, Some(StorageTier::L3));
    assert_eq!(placements[3].target_tier, Some(StorageTier::L4));
}

#[test]
fn dependency_guard_storage_boundary_stays_decoupled_from_upstream_crates() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
    ];

    let forbidden = [
        "fdc-transform",
        "fdc_transform",
        "fdc-ingestion",
        "fdc_ingestion",
        "fdc-barter",
        "fdc_barter",
    ];
    let mut violations = Vec::new();

    for path in checked_paths {
        collect_forbidden_references(&path, &forbidden, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "fdc-storage must not depend on upstream pipeline or adapter crates; violations: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-storage should live two levels under workspace root")
        .to_path_buf()
}

fn collect_forbidden_references(path: &Path, forbidden: &[&str], violations: &mut Vec<String>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("failed to read directory for dependency guard") {
            let entry = entry.expect("failed to read directory entry");
            collect_forbidden_references(&entry.path(), forbidden, violations);
        }
        return;
    }

    if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
        && path.file_name().and_then(|file_name| file_name.to_str()) != Some("Cargo.toml")
    {
        return;
    }

    let content = std::fs::read_to_string(path).expect("failed to read dependency guard file");
    for needle in forbidden {
        if content.contains(needle) {
            violations.push(format!("{} contains {needle}", path.display()));
        }
    }
}
```

- [ ] **Step 2: Run the contract test to verify it fails before implementation**

Run:

```bash
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
```

Expected: FAIL with unresolved imports such as `no RecordingStorageSink in the root`, `no StorageWriteBatch in the root`, or similar missing type errors.

- [ ] **Step 3: Commit the failing test**

Run:

```bash
git add crates/fdc-storage/tests/storage_sink_boundary_contract.rs
git commit -m "test: define storage sink boundary contract"
```

---

## Task 2: Add storage write record, metadata, placement, and batch types

**Files:**
- Create: `crates/fdc-storage/src/write.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Implement `write.rs`**

Create `crates/fdc-storage/src/write.rs` with this complete content:

```rust
//! Storage-owned write boundary types.
//!
//! These types intentionally do not depend on ingestion, transform, or adapter
//! crates. Upstream orchestration code is responsible for mapping domain DTOs
//! into these generic storage write records.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use fdc_core::{
    error::{Error, Result},
};
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
            return Err(Error::validation("storage write record namespace must not be empty"));
        }
        if self.collection.trim().is_empty() {
            return Err(Error::validation("storage write record collection must not be empty"));
        }
        if self.key.is_empty() {
            return Err(Error::validation("storage write record key must not be empty"));
        }
        if self.value.is_empty() {
            return Err(Error::validation("storage write record value must not be empty"));
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
                return Err(Error::validation("storage write metadata tag key must not be empty"));
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
                return Err(Error::validation("storage write batch metadata tag key must not be empty"));
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
```

- [ ] **Step 2: Export the write module and types from `lib.rs`**

Modify `crates/fdc-storage/src/lib.rs` so the module section includes `write`:

```rust
pub mod engine;         // 存储引擎抽象
pub mod tier;           // 存储层级管理
pub mod shard;          // 数据分片
pub mod index;          // 索引系统
pub mod cache;          // 缓存管理
pub mod compression;    // 压缩算法
pub mod replication;    // 数据复制
pub mod backup;         // 备份恢复
pub mod metrics;        // 存储指标
pub mod config;         // 配置管理
pub mod write;          // storage-owned write records and placement hints
```

Add these re-exports near the existing public re-exports:

```rust
pub use write::{
    StorageAccessPatternHint, StorageBatchMetadata, StorageDurabilityHint, StoragePlacementHint,
    StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
};
```

- [ ] **Step 3: Run the contract test and confirm remaining failure is missing sink types**

Run:

```bash
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
```

Expected: FAIL with unresolved imports for `RecordingStorageSink` and `StorageWriteSink`, while write-related types resolve.

- [ ] **Step 4: Commit write boundary types**

Run:

```bash
git add crates/fdc-storage/src/write.rs crates/fdc-storage/src/lib.rs
git commit -m "feat: add storage write boundary types"
```

---

## Task 3: Add storage write sink trait and recording sink

**Files:**
- Create: `crates/fdc-storage/src/sink.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Implement `sink.rs`**

Create `crates/fdc-storage/src/sink.rs` with this complete content:

```rust
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
```

- [ ] **Step 2: Export the sink module and types from `lib.rs`**

Modify `crates/fdc-storage/src/lib.rs` so the module section includes `sink`:

```rust
pub mod engine;         // 存储引擎抽象
pub mod tier;           // 存储层级管理
pub mod shard;          // 数据分片
pub mod index;          // 索引系统
pub mod cache;          // 缓存管理
pub mod compression;    // 压缩算法
pub mod replication;    // 数据复制
pub mod backup;         // 备份恢复
pub mod metrics;        // 存储指标
pub mod config;         // 配置管理
pub mod write;          // storage-owned write records and placement hints
pub mod sink;           // storage write sink boundary
```

Add these re-exports near the write re-exports:

```rust
pub use sink::{RecordingStorageSink, StorageWriteOutcome, StorageWriteSink};
```

- [ ] **Step 3: Run the contract test and verify it passes**

Run:

```bash
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
```

Expected: PASS. The output should report 8 tests passing.

- [ ] **Step 4: Commit sink implementation**

Run:

```bash
git add crates/fdc-storage/src/sink.rs crates/fdc-storage/src/lib.rs
git commit -m "feat: add recording storage write sink"
```

---

## Task 4: Add focused unit tests for write validation edge cases

**Files:**
- Modify: `crates/fdc-storage/src/write.rs`
- Modify: `crates/fdc-storage/src/sink.rs`

- [ ] **Step 1: Add unit tests to `write.rs`**

Append this test module to the end of `crates/fdc-storage/src/write.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn valid_record() -> StorageWriteRecord {
        StorageWriteRecord::new("namespace", "collection", b"key".to_vec(), b"value".to_vec())
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
        let record = StorageWriteRecord::new("namespace", "collection", b"key".to_vec(), Vec::new());
        let error = record.validate().expect_err("empty value should fail");
        assert!(error.to_string().contains("value must not be empty"));
    }

    #[test]
    fn metadata_validation_rejects_empty_tag_key() {
        let mut metadata = StorageWriteMetadata::default();
        metadata.tags.insert(" ".to_string(), "bad".to_string());
        let record = valid_record().with_metadata(metadata);

        let error = record.validate().expect_err("blank metadata tag key should fail");
        assert!(error.to_string().contains("metadata tag key must not be empty"));
    }

    #[test]
    fn batch_metadata_validation_rejects_empty_tag_key() {
        let mut metadata = StorageBatchMetadata::default();
        metadata.tags.insert("".to_string(), "bad".to_string());
        let batch = StorageWriteBatch::new(vec![valid_record()]).with_metadata(metadata);

        let error = batch.validate().expect_err("blank batch tag key should fail");
        assert!(error.to_string().contains("batch metadata tag key must not be empty"));
    }

    #[test]
    fn batch_helpers_report_record_count() {
        let batch = StorageWriteBatch::new(vec![valid_record(), valid_record()]);

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }
}
```

- [ ] **Step 2: Add unit tests to `sink.rs`**

Append this test module to the end of `crates/fdc-storage/src/sink.rs`:

```rust
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
```

- [ ] **Step 3: Run package tests**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: PASS for all `fdc-storage` tests. If unrelated pre-existing engine tests fail, capture the exact failing test names and still verify the new tests with:

```bash
rtk cargo test -p fdc-storage write::tests sink::tests --lib
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
```

- [ ] **Step 4: Commit focused unit tests**

Run:

```bash
git add crates/fdc-storage/src/write.rs crates/fdc-storage/src/sink.rs
git commit -m "test: cover storage write validation"
```

---

## Task 5: Update development status documentation

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update B5 status section**

In `docs/DEVELOPMENT_STATUS.md`, replace the current `### Phase B5: Storage Sink Boundary` section under `## Next Recommended Development Slice` with this completed status section:

```markdown
### Phase B5: Tier-aware Storage Sink Boundary

Implemented in `crates/fdc-storage`.

Completed capabilities:

- Added storage-owned generic write boundary types:
  - `StorageWriteRecord`
  - `StorageWriteBatch`
  - `StorageWriteMetadata`
  - `StorageBatchMetadata`
- Added tier-aware placement hints aligned with existing L1/L2/L3/L4 storage architecture:
  - `StoragePlacementHint`
  - `StorageAccessPatternHint`
  - `StorageDurabilityHint`
  - optional shard routing key and TTL
- Added async `StorageWriteSink` trait.
- Added file-free, database-free `RecordingStorageSink` for contract tests and future examples.
- Preserved dependency boundaries: `fdc-storage` does not depend on `fdc-transform`, `fdc-ingestion`, or adapter crates.
- Deferred production `TierManager` / `ShardManager` / engine routing to a future storage runtime slice.
- Deferred `MarketDataDto -> StorageWriteRecord` mapping to a future orchestration/integration slice.

Contract tests:

- `crates/fdc-storage/tests/storage_sink_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-25-fdc-storage-sink-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-fdc-storage-sink-boundary.md`

Verification:

- `rtk cargo fmt --package fdc-storage --check`
- `rtk cargo test -p fdc-storage --test storage_sink_boundary_contract`
- `rtk cargo test -p fdc-storage`
- Dependency guard: no `fdc-transform`, `fdc_transform`, `fdc-ingestion`, `fdc_ingestion`, `fdc-barter`, or `fdc_barter` references in `crates/fdc-storage/Cargo.toml` or `crates/fdc-storage/src`.
```

Add this as the new next recommended slice immediately after the B5 section:

```markdown
## Next Recommended Development Slice

### Phase B6: Orchestration Glue Boundary Design

Goal: define where cross-layer glue lives without violating crate dependency direction.

Recommended scope:

- Decide whether glue belongs in `fdc-server`, a new integration crate, or a dedicated orchestrator module.
- Define bounded mapping responsibilities such as adapter envelope to ingestion source envelope, transform DTO to storage write record, and runtime wiring.
- Do not move adapter-specific mapping into `fdc-storage`, `fdc-transform`, or `fdc-ingestion` core.
- Keep production infinite stream lifecycle, checkpoint persistence, and real database writes as separate follow-up slices.
```

- [ ] **Step 2: Run a docs diff review**

Run:

```bash
rtk git diff -- docs/DEVELOPMENT_STATUS.md
```

Expected: diff shows B5 moved from recommended scope to completed status and B6 added as next recommended slice.

- [ ] **Step 3: Commit docs update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record storage sink boundary status"
```

---

## Task 6: Final verification and dependency guards

**Files:**
- No new files unless verification exposes issues.

- [ ] **Step 1: Run formatting check**

Run:

```bash
rtk cargo fmt --package fdc-storage --check
```

Expected: exit 0. If it fails, run:

```bash
rtk cargo fmt --package fdc-storage
git add crates/fdc-storage/src/write.rs crates/fdc-storage/src/sink.rs crates/fdc-storage/src/lib.rs crates/fdc-storage/tests/storage_sink_boundary_contract.rs
git commit -m "style: format storage sink boundary"
```

- [ ] **Step 2: Run B5 contract test**

Run:

```bash
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
```

Expected: PASS with 8 contract tests passing.

- [ ] **Step 3: Run full fdc-storage tests**

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected: PASS for all `fdc-storage` tests. If unrelated pre-existing tests fail, do not ignore them silently. Record the exact failures in `docs/DEVELOPMENT_STATUS.md`, then run the focused B5 tests again and report the limitation.

- [ ] **Step 4: Run dependency guard**

Run:

```bash
set -e
! grep -RInE "fdc-transform|fdc_transform|fdc-ingestion|fdc_ingestion|fdc-barter|fdc_barter" crates/fdc-storage/Cargo.toml crates/fdc-storage/src
```

Expected: command exits 0 with no output.

- [ ] **Step 5: Run branch status check**

Run:

```bash
rtk git status --short --branch
```

Expected: branch is `mdb-mqdev`; working tree is clean except for any intentional uncommitted user changes. If there are B5 changes, commit them before declaring complete.

- [ ] **Step 6: Push the completed branch only after verification succeeds**

Run:

```bash
git push origin mdb-mqdev
```

Expected: remote `origin/mdb-mqdev` advances successfully.

---

## Self-Review Checklist

- Spec coverage: Tasks implement storage-owned records, batch metadata, placement hints, async sink trait, recording sink, exports, contract tests, dependency guard, and development status update.
- Scope control: No production database writes, no `TierManager` routing, no `ShardManager` routing implementation, no transform DTO mapping, no adapter-specific schema.
- Type consistency: The plan consistently uses `StorageWriteRecord`, `StorageWriteMetadata`, `StoragePlacementHint`, `StorageAccessPatternHint`, `StorageDurabilityHint`, `StorageBatchMetadata`, `StorageWriteBatch`, `StorageWriteSink`, `StorageWriteOutcome`, and `RecordingStorageSink`.
- Verification: Final commands include format, contract tests, full `fdc-storage` tests, dependency guard, status check, and push.
