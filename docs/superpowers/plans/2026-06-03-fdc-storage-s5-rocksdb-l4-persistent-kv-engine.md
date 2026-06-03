# fdc-storage Phase S5 RocksDB L4 Persistent KV Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `RocksDBEngine` as the L4 cold persistent KV engine for `fdc-storage`.

**Architecture:** Replace the RocksDB skeleton with a real `rocksdb::DB` backed `StorageEngine` implementation. Keep the boundary generic by storing opaque key/value bytes, implement ordered scans with RocksDB iterators, and validate L4 placement through `TieredStorageStore`.

**Tech Stack:** Rust, `async_trait`, `rocksdb`, `parking_lot`, `tokio` tests, `tempfile`, `fdc-core`, `fdc-storage`.

---

## File Structure

- Modify: `crates/fdc-storage/src/engines/rocksdb.rs`
  - Replace skeleton with real RocksDB-backed `StorageEngine` implementation.
  - Add focused RocksDB tests.
- Modify: `crates/fdc-storage/src/tiered_store.rs`
  - Add L4 RocksDB integration test.
- Do not modify unrelated `fdc-server health` dirty files.

---

### Task 1: Create isolated worktree and verify baseline

**Files:**
- No source edits in this task.

- [ ] **Step 1: Create the S5 worktree**

Run from repo root:

```bash
git worktree add .worktrees/fdc-storage-s5-rocksdb -b fdc-storage-s5-rocksdb
cd .worktrees/fdc-storage-s5-rocksdb
```

Expected: new worktree on branch `fdc-storage-s5-rocksdb`.

- [ ] **Step 2: Run baseline storage tests**

```bash
rtk cargo test -p fdc-storage
```

Expected: `74 passed` or current full `fdc-storage` suite passes before S5 edits.

---

### Task 2: Add RocksDB engine tests first

**Files:**
- Modify: `crates/fdc-storage/src/engines/rocksdb.rs`

- [ ] **Step 1: Replace existing tests module with failing contract tests**

At the bottom of `crates/fdc-storage/src/engines/rocksdb.rs`, replace the current `#[cfg(test)] mod tests` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config_for(path: &std::path::Path) -> HashMap<String, String> {
        HashMap::from([(
            "db_path".to_string(),
            path.join("storage-rocksdb").to_string_lossy().to_string(),
        )])
    }

    #[tokio::test]
    async fn test_rocksdb_engine_creation() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        assert_eq!(engine.engine_type(), StorageEngineType::RocksDB);
        assert!(engine.db_path().ends_with("storage-rocksdb"));

        let caps = engine.capabilities();
        assert!(caps.supports_compression);
        assert!(caps.supports_replication);
    }

    #[tokio::test]
    async fn rocksdb_put_get_delete_roundtrip() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine.put(b"key", b"value").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));

        engine.put(b"key", b"updated").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"updated".to_vec()));

        engine.delete(b"key").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn rocksdb_persists_records_after_reopen() {
        let dir = tempdir().unwrap();
        let config = config_for(dir.path());
        {
            let mut engine = RocksDBEngine::new(config.clone()).await.unwrap();
            engine.initialize().await.unwrap();
            engine.put(b"persist", b"value").await.unwrap();
            engine.shutdown().await.unwrap();
        }

        let mut reopened = RocksDBEngine::new(config).await.unwrap();
        reopened.initialize().await.unwrap();
        assert_eq!(
            reopened.get(b"persist").await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn rocksdb_batch_and_ordered_scan_with_limit() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine
            .batch(vec![
                BatchOperation::Put {
                    key: b"key3".to_vec(),
                    value: b"value3".to_vec(),
                },
                BatchOperation::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                },
                BatchOperation::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                },
                BatchOperation::Delete {
                    key: b"key3".to_vec(),
                },
            ])
            .await
            .unwrap();

        let results = engine.scan(Some(b"key1"), Some(b"key9"), Some(2)).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"key1".to_vec());
        assert_eq!(results[1].0, b"key2".to_vec());
    }

    #[tokio::test]
    async fn rocksdb_scan_respects_inclusive_key_range() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine.put(b"a", b"1").await.unwrap();
        engine.put(b"b", b"2").await.unwrap();
        engine.put(b"c", b"3").await.unwrap();

        let results = engine.scan(Some(b"b"), Some(b"c"), None).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"b".to_vec());
        assert_eq!(results[1].0, b"c".to_vec());
    }

    #[tokio::test]
    async fn rocksdb_stats_track_key_count_and_size() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"a", b"one").await.unwrap();
        engine.put(b"bb", b"two").await.unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.total_size, 1 + 3 + 2 + 3);
        assert!(stats.writes >= 2);
    }

    #[tokio::test]
    async fn rocksdb_compact_succeeds() {
        let dir = tempdir().unwrap();
        let mut engine = RocksDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"compact", b"value").await.unwrap();

        engine.compact().await.unwrap();
    }
}
```

- [ ] **Step 2: Run focused tests and verify they fail**

```bash
rtk cargo test -p fdc-storage engines::rocksdb::tests -- --nocapture
```

Expected: tests fail because the skeleton still returns `RocksDB ... not implemented` for core operations.

---

### Task 3: Implement RocksDBEngine core

**Files:**
- Modify: `crates/fdc-storage/src/engines/rocksdb.rs`

- [ ] **Step 1: Replace imports and struct fields**

At the top of `rocksdb.rs`, use these imports and helpers:

```rust
use crate::engine::{
    BatchOperation, EngineCapabilities, StorageEngine, StorageEngineType, StorageOperation,
    StorageStats,
};
use async_trait::async_trait;
use fdc_core::error::{Error, Result};
use parking_lot::{Mutex, RwLock};
use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, DB};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

fn rocksdb_error(error: impl std::fmt::Display) -> Error {
    Error::storage(format!("RocksDB storage error: {error}"))
}
```

Replace `RocksDBEngine` with:

```rust
pub struct RocksDBEngine {
    db_path: PathBuf,
    db: Arc<Mutex<Option<Arc<DB>>>>,
    stats: Arc<RwLock<StorageStats>>,
}
```

- [ ] **Step 2: Add constructor and helpers**

Inside `impl RocksDBEngine`, use:

```rust
pub async fn new(config: HashMap<String, String>) -> Result<Self> {
    let db_path = config
        .get("db_path")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data/rocksdb"));

    Ok(Self {
        db_path,
        db: Arc::new(Mutex::new(None)),
        stats: Arc::new(RwLock::new(StorageStats::default())),
    })
}

pub fn db_path(&self) -> &PathBuf {
    &self.db_path
}

fn db(&self) -> Result<Arc<DB>> {
    self.db
        .lock()
        .as_ref()
        .cloned()
        .ok_or_else(|| Error::validation("RocksDB engine is not initialized"))
}

fn record_operation(&self, op: StorageOperation, latency_us: u64) {
    self.stats.write().record_operation(op, latency_us);
}

fn refresh_size_stats(&self) -> Result<()> {
    let db = self.db()?;
    let mut key_count = 0_u64;
    let mut total_size = 0_u64;

    for item in db.iterator(IteratorMode::Start) {
        let (key, value) = item.map_err(rocksdb_error)?;
        key_count += 1;
        total_size += key.len() as u64 + value.len() as u64;
    }

    let mut stats = self.stats.write();
    stats.key_count = key_count;
    stats.total_size = total_size;
    Ok(())
}
```

- [ ] **Step 3: Implement initialize and shutdown**

Replace `initialize` and `shutdown` with:

```rust
async fn initialize(&mut self) -> Result<()> {
    if let Some(parent) = self.db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_compression_type(DBCompressionType::Lz4);

    let db = DB::open(&options, &self.db_path).map_err(rocksdb_error)?;
    *self.db.lock() = Some(Arc::new(db));
    self.refresh_size_stats()
}

async fn shutdown(&mut self) -> Result<()> {
    *self.db.lock() = None;
    Ok(())
}
```

- [ ] **Step 4: Implement get/put/delete/batch**

Replace the corresponding trait methods with:

```rust
async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let start = Instant::now();
    let result = self.db()?.get(key).map_err(rocksdb_error)?;
    self.record_operation(StorageOperation::Get, start.elapsed().as_micros() as u64);
    Ok(result)
}

async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::validation("RocksDB key must not be empty"));
    }

    let start = Instant::now();
    self.db()?.put(key, value).map_err(rocksdb_error)?;
    self.record_operation(StorageOperation::Put, start.elapsed().as_micros() as u64);
    self.refresh_size_stats()
}

async fn delete(&self, key: &[u8]) -> Result<()> {
    let start = Instant::now();
    self.db()?.delete(key).map_err(rocksdb_error)?;
    self.record_operation(StorageOperation::Delete, start.elapsed().as_micros() as u64);
    self.refresh_size_stats()
}

async fn batch(&self, operations: Vec<BatchOperation>) -> Result<()> {
    let start = Instant::now();
    let mut batch = WriteBatch::default();

    for operation in operations {
        match operation {
            BatchOperation::Put { key, value } => {
                if key.is_empty() {
                    return Err(Error::validation("RocksDB batch key must not be empty"));
                }
                batch.put(key, value);
            }
            BatchOperation::Delete { key } => {
                batch.delete(key);
            }
        }
    }

    self.db()?.write(batch).map_err(rocksdb_error)?;
    self.record_operation(StorageOperation::Batch, start.elapsed().as_micros() as u64);
    self.refresh_size_stats()
}
```

- [ ] **Step 5: Implement scan/stats/compact**

Replace `scan`, `stats`, and `compact` with:

```rust
async fn scan(
    &self,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let start = Instant::now();
    let db = self.db()?;
    let mode = start_key
        .map(|key| IteratorMode::From(key, Direction::Forward))
        .unwrap_or(IteratorMode::Start);

    let mut results = Vec::new();
    for item in db.iterator(mode) {
        let (key, value) = item.map_err(rocksdb_error)?;
        if let Some(start_key) = start_key {
            if key.as_ref() < start_key {
                continue;
            }
        }
        if let Some(end_key) = end_key {
            if key.as_ref() > end_key {
                break;
            }
        }

        results.push((key.to_vec(), value.to_vec()));
        if let Some(limit) = limit {
            if results.len() >= limit {
                break;
            }
        }
    }

    self.record_operation(StorageOperation::Scan, start.elapsed().as_micros() as u64);
    Ok(results)
}

async fn stats(&self) -> Result<StorageStats> {
    self.refresh_size_stats()?;
    Ok(self.stats.read().clone())
}

async fn compact(&self) -> Result<()> {
    self.db()?.compact_range::<&[u8], &[u8]>(None, None);
    Ok(())
}
```

- [ ] **Step 6: Run focused tests**

```bash
rtk cargo test -p fdc-storage engines::rocksdb::tests -- --nocapture
```

Expected: all RocksDB focused tests pass.

- [ ] **Step 7: Commit core RocksDB implementation**

```bash
git add crates/fdc-storage/src/engines/rocksdb.rs
git commit -m "feat(storage): implement rocksdb l4 engine"
```

---

### Task 4: Add TieredStorageStore L4 integration test

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`

- [ ] **Step 1: Add the L4 integration test**

Inside `#[cfg(test)] mod tests` in `tiered_store.rs`, after the DuckDB L3 test, add:

```rust
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
```

- [ ] **Step 2: Run RocksDB filtered tests**

```bash
rtk cargo test -p fdc-storage rocksdb -- --nocapture
```

Expected: RocksDB engine tests and the L4 tiered integration test pass.

- [ ] **Step 3: Run exact L4 integration test**

```bash
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_can_use_rocksdb_l4_for_cold_records -- --nocapture
```

Expected: one test passes.

- [ ] **Step 4: Commit integration test**

```bash
git add crates/fdc-storage/src/tiered_store.rs
git commit -m "test(storage): cover rocksdb l4 tier placement"
```

---

### Task 5: Full verification, merge, and cleanup

**Files:**
- No new source edits unless verification exposes a bug.

- [ ] **Step 1: Format**

```bash
rtk cargo fmt -p fdc-storage
```

Expected: formatting completes without errors.

- [ ] **Step 2: Full worktree verification**

```bash
rtk cargo test -p fdc-storage
```

Expected: full `fdc-storage` test suite passes.

- [ ] **Step 3: Commit formatting if needed**

If `git status --short` shows formatting changes, run:

```bash
git add crates/fdc-storage/src/engines/rocksdb.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "style(storage): format rocksdb l4 engine"
```

If there are no formatting changes, skip this step.

- [ ] **Step 4: Merge back to main workspace**

From repo root:

```bash
cd ../..
git merge --ff-only fdc-storage-s5-rocksdb
rtk cargo test -p fdc-storage
```

Expected: fast-forward merge succeeds and full storage tests pass on `mdb-mqdev`.

- [ ] **Step 5: Clean worktree and branch**

```bash
git worktree remove .worktrees/fdc-storage-s5-rocksdb
git worktree prune
git branch -d fdc-storage-s5-rocksdb
```

Expected: worktree and branch are removed.

---

## Plan Self-Review

- Spec coverage: tasks cover RocksDB core operations, persistence, scan, stats, compact, and L4 tiered integration.
- Placeholder scan: no placeholder implementation steps remain.
- Type consistency: plan uses existing `StorageEngine`, `BatchOperation`, `StorageStats`, `StorageTier`, `TieredStorageStore`, and existing test helpers.
- Scope: storage module only, no pipeline glue, no business DTO coupling, no unrelated dirty file edits.
