//! redb storage engine (L2)

use crate::engine::{
    BatchOperation, EngineCapabilities, StorageEngine, StorageEngineType, StorageOperation,
    StorageStats,
};
use async_trait::async_trait;
use fdc_core::{error::Error, Result};
use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::task;

fn redb_error(error: impl std::fmt::Display) -> Error {
    Error::storage(format!("redb error: {error}"))
}

const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("fdc_storage_kv");

/// redb存储引擎
pub struct RedbEngine {
    db_path: PathBuf,
    db: Arc<Database>,
    stats: Arc<RwLock<StorageStats>>,
}

impl RedbEngine {
    /// 创建新的redb引擎
    pub async fn new(config: HashMap<String, String>) -> Result<Self> {
        let db_path = config
            .get("db_path")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/redb/fdc-storage.redb"));

        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(&db_path).map_err(redb_error)?;

        Ok(Self {
            db_path,
            db: Arc::new(db),
            stats: Arc::new(RwLock::new(StorageStats::default())),
        })
    }

    pub fn db_path(&self) -> &PathBuf {
        &self.db_path
    }

    fn record_operation(&self, op: StorageOperation, latency_us: u64) {
        self.stats.write().record_operation(op, latency_us);
    }

    async fn refresh_size_stats(&self) -> Result<()> {
        let db = Arc::clone(&self.db);
        let (key_count, total_size) = task::spawn_blocking(move || -> Result<(u64, u64)> {
            let read_txn = db.begin_read().map_err(redb_error)?;
            let table = match read_txn.open_table(KV_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((0, 0)),
                Err(error) => return Err(redb_error(error)),
            };

            let mut key_count = 0_u64;
            let mut total_size = 0_u64;
            for entry in table.iter().map_err(redb_error)? {
                let (key, value) = entry.map_err(redb_error)?;
                key_count += 1;
                total_size += key.value().len() as u64 + value.value().len() as u64;
            }
            Ok((key_count, total_size))
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        let mut stats = self.stats.write();
        stats.key_count = key_count;
        stats.total_size = total_size;
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for RedbEngine {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::Redb
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            supports_transactions: true,
            supports_indexes: false,
            supports_compression: false,
            supports_replication: false,
            supports_backup: true,
            supports_sql: false,
            supports_acid: true,
            max_data_size: Some(256 * 1024 * 1024 * 1024), // 256GB
            expected_latency_us: 50,
            expected_throughput_ops: 100_000,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        let db = Arc::clone(&self.db);
        task::spawn_blocking(move || -> Result<()> {
            let write_txn = db.begin_write().map_err(redb_error)?;
            {
                let _table = write_txn.open_table(KV_TABLE).map_err(redb_error)?;
            }
            write_txn.commit().map_err(redb_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;
        self.refresh_size_stats().await
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        let result = task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            let read_txn = db.begin_read().map_err(redb_error)?;
            let table = match read_txn.open_table(KV_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(redb_error(error)),
            };
            Ok(table
                .get(key.as_slice())
                .map_err(redb_error)?
                .map(|value| value.value().to_vec()))
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        self.record_operation(StorageOperation::Get, start.elapsed().as_micros() as u64);
        Ok(result)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::validation("redb key must not be empty"));
        }

        let start = Instant::now();
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        let value = value.to_vec();
        task::spawn_blocking(move || -> Result<()> {
            let write_txn = db.begin_write().map_err(redb_error)?;
            {
                let mut table = write_txn.open_table(KV_TABLE).map_err(redb_error)?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(redb_error)?;
            }
            write_txn.commit().map_err(redb_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        self.record_operation(StorageOperation::Put, start.elapsed().as_micros() as u64);
        self.refresh_size_stats().await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let start = Instant::now();
        let db = Arc::clone(&self.db);
        let key = key.to_vec();
        task::spawn_blocking(move || -> Result<()> {
            let write_txn = db.begin_write().map_err(redb_error)?;
            {
                let mut table = write_txn.open_table(KV_TABLE).map_err(redb_error)?;
                let _ = table.remove(key.as_slice()).map_err(redb_error)?;
            }
            write_txn.commit().map_err(redb_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        self.record_operation(StorageOperation::Delete, start.elapsed().as_micros() as u64);
        self.refresh_size_stats().await
    }

    async fn batch(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let start = Instant::now();
        let db = Arc::clone(&self.db);
        task::spawn_blocking(move || -> Result<()> {
            let write_txn = db.begin_write().map_err(redb_error)?;
            {
                let mut table = write_txn.open_table(KV_TABLE).map_err(redb_error)?;
                for operation in operations {
                    match operation {
                        BatchOperation::Put { key, value } => {
                            if key.is_empty() {
                                return Err(Error::validation("redb batch key must not be empty"));
                            }
                            table
                                .insert(key.as_slice(), value.as_slice())
                                .map_err(redb_error)?;
                        }
                        BatchOperation::Delete { key } => {
                            let _ = table.remove(key.as_slice()).map_err(redb_error)?;
                        }
                    }
                }
            }
            write_txn.commit().map_err(redb_error)?;
            Ok(())
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        self.record_operation(StorageOperation::Batch, start.elapsed().as_micros() as u64);
        self.refresh_size_stats().await
    }

    async fn scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();
        let db = Arc::clone(&self.db);
        let start_key = start_key.map(Vec::from);
        let end_key = end_key.map(Vec::from);
        let result = task::spawn_blocking(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let read_txn = db.begin_read().map_err(redb_error)?;
            let table = match read_txn.open_table(KV_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(redb_error(error)),
            };

            let mut results = Vec::new();
            for entry in table.iter().map_err(redb_error)? {
                let (key, value) = entry.map_err(redb_error)?;
                let key_bytes = key.value().to_vec();

                if let Some(start_key) = &start_key {
                    if key_bytes < *start_key {
                        continue;
                    }
                }
                if let Some(end_key) = &end_key {
                    if key_bytes > *end_key {
                        continue;
                    }
                }

                results.push((key_bytes, value.value().to_vec()));
                if let Some(limit) = limit {
                    if results.len() >= limit {
                        break;
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(|error| Error::internal(format!("redb task join error: {error}")))??;

        self.record_operation(StorageOperation::Scan, start.elapsed().as_micros() as u64);
        Ok(result)
    }

    async fn stats(&self) -> Result<StorageStats> {
        self.refresh_size_stats().await?;
        Ok(self.stats.read().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config_for(path: &std::path::Path) -> HashMap<String, String> {
        HashMap::from([(
            "db_path".to_string(),
            path.join("storage.redb").to_string_lossy().to_string(),
        )])
    }

    #[tokio::test]
    async fn test_redb_engine_creation() {
        let dir = tempdir().unwrap();
        let mut engine = RedbEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        assert_eq!(engine.engine_type(), StorageEngineType::Redb);
        assert!(engine.db_path().ends_with("storage.redb"));

        let caps = engine.capabilities();
        assert!(caps.supports_transactions);
        assert!(caps.supports_acid);
    }

    #[tokio::test]
    async fn redb_engine_put_get_delete_roundtrip() {
        let dir = tempdir().unwrap();
        let mut engine = RedbEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine.put(b"key-1", b"value-1").await.unwrap();
        assert_eq!(
            engine.get(b"key-1").await.unwrap(),
            Some(b"value-1".to_vec())
        );

        engine.delete(b"key-1").await.unwrap();
        assert_eq!(engine.get(b"key-1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn redb_engine_persists_after_reopen() {
        let dir = tempdir().unwrap();
        let config = config_for(dir.path());
        {
            let mut engine = RedbEngine::new(config.clone()).await.unwrap();
            engine.initialize().await.unwrap();
            engine.put(b"persisted", b"yes").await.unwrap();
        }

        let mut reopened = RedbEngine::new(config).await.unwrap();
        reopened.initialize().await.unwrap();
        assert_eq!(
            reopened.get(b"persisted").await.unwrap(),
            Some(b"yes".to_vec())
        );
    }

    #[tokio::test]
    async fn redb_engine_batch_and_scan_are_ordered() {
        let dir = tempdir().unwrap();
        let mut engine = RedbEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine
            .batch(vec![
                BatchOperation::Put {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                },
                BatchOperation::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                BatchOperation::Put {
                    key: b"c".to_vec(),
                    value: b"3".to_vec(),
                },
            ])
            .await
            .unwrap();

        let scanned = engine.scan(Some(b"a"), Some(b"b"), None).await.unwrap();
        assert_eq!(
            scanned,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );

        let limited = engine.scan(None, None, Some(2)).await.unwrap();
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].0, b"a".to_vec());
    }

    #[tokio::test]
    async fn redb_engine_stats_track_records() {
        let dir = tempdir().unwrap();
        let mut engine = RedbEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"key", b"value").await.unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.key_count, 1);
        assert!(stats.total_size >= 8);
        assert!(stats.writes >= 1);
    }
}
