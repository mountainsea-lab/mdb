//! DuckDB storage engine (L3)

use crate::engine::{
    BatchOperation, EngineCapabilities, StorageEngine, StorageEngineType, StorageOperation,
    StorageStats,
};
use async_trait::async_trait;
use duckdb::arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use duckdb::arrow::datatypes::DataType;
use duckdb::{params, Connection, OptionalExt};
use fdc_core::{
    error::{Error, Result},
    types::Value,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::task;

fn duckdb_error(error: impl std::fmt::Display) -> Error {
    Error::storage(format!("DuckDB storage error: {error}"))
}

const KV_TABLE: &str = "fdc_storage_kv";

/// DuckDB存储引擎
pub struct DuckDBEngine {
    db_path: PathBuf,
    connection: Arc<Mutex<Option<Connection>>>,
    stats: Arc<Mutex<StorageStats>>,
}

impl DuckDBEngine {
    /// 创建新的DuckDB引擎
    pub async fn new(config: HashMap<String, String>) -> Result<Self> {
        let db_path = config
            .get("db_path")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/duckdb.db"));

        Ok(Self {
            db_path,
            connection: Arc::new(Mutex::new(None)),
            stats: Arc::new(Mutex::new(StorageStats::default())),
        })
    }

    pub fn db_path(&self) -> &PathBuf {
        &self.db_path
    }

    fn record_operation(&self, op: StorageOperation, latency_us: u64) {
        self.stats.lock().record_operation(op, latency_us);
    }

    async fn refresh_size_stats(&self) -> Result<()> {
        let connection = Arc::clone(&self.connection);
        let (key_count, total_size) = task::spawn_blocking(move || -> Result<(u64, u64)> {
            with_connection(&connection, |conn| {
                let key_count: u64 = conn
                    .query_row(
                        &format!("SELECT COUNT(*) FROM {KV_TABLE}"),
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|count| count as u64)
                    .map_err(duckdb_error)?;

                let total_size: u64 = conn
                    .query_row(
                        &format!("SELECT COALESCE(SUM(OCTET_LENGTH(key) + OCTET_LENGTH(value)), 0) FROM {KV_TABLE}"),
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|size| size as u64)
                    .map_err(duckdb_error)?;

                Ok((key_count, total_size))
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        let mut stats = self.stats.lock();
        stats.key_count = key_count;
        stats.total_size = total_size;
        Ok(())
    }
}

fn with_connection<T>(
    connection: &Arc<Mutex<Option<Connection>>>,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let guard = connection.lock();
    let conn = guard
        .as_ref()
        .ok_or_else(|| Error::validation("DuckDB engine is not initialized"))?;
    f(conn)
}

#[async_trait]
impl StorageEngine for DuckDBEngine {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::DuckDB
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            supports_transactions: true,
            supports_indexes: true,
            supports_compression: true,
            supports_replication: false,
            supports_backup: true,
            supports_sql: true,
            supports_acid: true,
            max_data_size: None, // 无限制
            expected_latency_us: 100,
            expected_throughput_ops: 100_000,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db_path = self.db_path.clone();
        let connection = Arc::clone(&self.connection);
        task::spawn_blocking(move || -> Result<()> {
            let conn = Connection::open(db_path).map_err(duckdb_error)?;
            conn.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {KV_TABLE} (
                    key BLOB PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_fdc_storage_kv_key ON {KV_TABLE}(key);"
            ))
            .map_err(duckdb_error)?;
            *connection.lock() = Some(conn);
            Ok(())
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.refresh_size_stats().await
    }

    async fn shutdown(&mut self) -> Result<()> {
        *self.connection.lock() = None;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();
        let connection = Arc::clone(&self.connection);
        let key = key.to_vec();
        let result = task::spawn_blocking(move || -> Result<Option<Vec<u8>>> {
            with_connection(&connection, |conn| {
                conn.query_row(
                    &format!("SELECT value FROM {KV_TABLE} WHERE key = ?1"),
                    params![key],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()
                .map_err(duckdb_error)
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.record_operation(StorageOperation::Get, start.elapsed().as_micros() as u64);
        Ok(result)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::validation("DuckDB key must not be empty"));
        }

        let start = Instant::now();
        let connection = Arc::clone(&self.connection);
        let key = key.to_vec();
        let value = value.to_vec();
        task::spawn_blocking(move || -> Result<()> {
            with_connection(&connection, |conn| {
                let tx = conn.unchecked_transaction().map_err(duckdb_error)?;
                tx.execute(&format!("DELETE FROM {KV_TABLE} WHERE key = ?1"), params![key])
                    .map_err(duckdb_error)?;
                tx.execute(
                    &format!("INSERT INTO {KV_TABLE} (key, value, created_at, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"),
                    params![key, value],
                )
                .map_err(duckdb_error)?;
                tx.commit().map_err(duckdb_error)?;
                Ok(())
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.record_operation(StorageOperation::Put, start.elapsed().as_micros() as u64);
        self.refresh_size_stats().await
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let start = Instant::now();
        let connection = Arc::clone(&self.connection);
        let key = key.to_vec();
        task::spawn_blocking(move || -> Result<()> {
            with_connection(&connection, |conn| {
                conn.execute(
                    &format!("DELETE FROM {KV_TABLE} WHERE key = ?1"),
                    params![key],
                )
                .map_err(duckdb_error)?;
                Ok(())
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.record_operation(StorageOperation::Delete, start.elapsed().as_micros() as u64);
        self.refresh_size_stats().await
    }

    async fn batch(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let start = Instant::now();
        let connection = Arc::clone(&self.connection);
        task::spawn_blocking(move || -> Result<()> {
            with_connection(&connection, |conn| {
                let tx = conn.unchecked_transaction().map_err(duckdb_error)?;
                for operation in operations {
                    match operation {
                        BatchOperation::Put { key, value } => {
                            if key.is_empty() {
                                return Err(Error::validation("DuckDB batch key must not be empty"));
                            }
                            tx.execute(&format!("DELETE FROM {KV_TABLE} WHERE key = ?1"), params![key])
                                .map_err(duckdb_error)?;
                            tx.execute(
                                &format!("INSERT INTO {KV_TABLE} (key, value, created_at, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"),
                                params![key, value],
                            )
                            .map_err(duckdb_error)?;
                        }
                        BatchOperation::Delete { key } => {
                            tx.execute(&format!("DELETE FROM {KV_TABLE} WHERE key = ?1"), params![key])
                                .map_err(duckdb_error)?;
                        }
                    }
                }
                tx.commit().map_err(duckdb_error)?;
                Ok(())
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

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
        let connection = Arc::clone(&self.connection);
        let start_key = start_key.map(Vec::from);
        let end_key = end_key.map(Vec::from);
        let result = task::spawn_blocking(move || -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            with_connection(&connection, |conn| {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT key, value FROM {KV_TABLE} ORDER BY key ASC"
                    ))
                    .map_err(duckdb_error)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                    })
                    .map_err(duckdb_error)?;

                let mut results = Vec::new();
                for row in rows {
                    let (key, value) = row.map_err(duckdb_error)?;
                    if let Some(start_key) = &start_key {
                        if key < *start_key {
                            continue;
                        }
                    }
                    if let Some(end_key) = &end_key {
                        if key > *end_key {
                            continue;
                        }
                    }
                    results.push((key, value));
                    if let Some(limit) = limit {
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
                Ok(results)
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.record_operation(StorageOperation::Scan, start.elapsed().as_micros() as u64);
        Ok(result)
    }

    async fn stats(&self) -> Result<StorageStats> {
        self.refresh_size_stats().await?;
        Ok(self.stats.lock().clone())
    }

    async fn query(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        let start = Instant::now();
        let connection = Arc::clone(&self.connection);
        let sql = sql.to_string();
        let result = task::spawn_blocking(move || -> Result<Vec<HashMap<String, Value>>> {
            with_connection(&connection, |conn| {
                let mut stmt = conn.prepare(&sql).map_err(duckdb_error)?;
                let batches: Vec<_> = stmt.query_arrow([]).map_err(duckdb_error)?.collect();
                let mut results = Vec::new();

                for batch in batches {
                    let schema = batch.schema();
                    for row_index in 0..batch.num_rows() {
                        let mut row = HashMap::new();
                        for (column_index, field) in schema.fields().iter().enumerate() {
                            let column = batch.column(column_index);
                            row.insert(
                                field.name().clone(),
                                arrow_cell_to_value(column.as_ref(), row_index),
                            );
                        }
                        results.push(row);
                    }
                }

                Ok(results)
            })
        })
        .await
        .map_err(|error| Error::internal(format!("DuckDB task join error: {error}")))??;

        self.record_operation(StorageOperation::Query, start.elapsed().as_micros() as u64);
        Ok(result)
    }
}

fn arrow_cell_to_value(array: &dyn Array, row_index: usize) -> Value {
    if array.is_null(row_index) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| Value::Bool(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|array| Value::Int8(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|array| Value::Int16(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| Value::Int32(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| Value::Int64(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|array| Value::UInt8(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|array| Value::UInt16(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| Value::UInt32(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| Value::UInt64(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| Value::Float32(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| Value::Float64(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| Value::String(array.value(row_index).to_string()))
            .unwrap_or(Value::Null),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| Value::Binary(array.value(row_index).to_vec()))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn config_for(path: &std::path::Path) -> HashMap<String, String> {
        HashMap::from([(
            "db_path".to_string(),
            path.join("storage.duckdb").to_string_lossy().to_string(),
        )])
    }

    #[tokio::test]
    async fn test_duckdb_engine_creation() {
        let dir = tempdir().unwrap();
        let mut engine = DuckDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        assert_eq!(engine.engine_type(), StorageEngineType::DuckDB);
        assert!(engine.db_path().ends_with("storage.duckdb"));

        let caps = engine.capabilities();
        assert!(caps.supports_sql);
        assert!(caps.supports_compression);
    }

    #[tokio::test]
    async fn duckdb_put_get_delete_roundtrip() {
        let dir = tempdir().unwrap();
        let mut engine = DuckDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();

        engine.put(b"key", b"value").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"value".to_vec()));

        engine.put(b"key", b"updated").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), Some(b"updated".to_vec()));

        engine.delete(b"key").await.unwrap();
        assert_eq!(engine.get(b"key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn duckdb_persists_records_after_reopen() {
        let dir = tempdir().unwrap();
        let config = config_for(dir.path());
        {
            let mut engine = DuckDBEngine::new(config.clone()).await.unwrap();
            engine.initialize().await.unwrap();
            engine.put(b"persist", b"value").await.unwrap();
            engine.shutdown().await.unwrap();
        }

        let mut reopened = DuckDBEngine::new(config).await.unwrap();
        reopened.initialize().await.unwrap();
        assert_eq!(
            reopened.get(b"persist").await.unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn duckdb_batch_and_ordered_scan_with_limit() {
        let dir = tempdir().unwrap();
        let mut engine = DuckDBEngine::new(config_for(dir.path())).await.unwrap();
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

        let results = engine
            .scan(Some(b"key1"), Some(b"key9"), Some(2))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"key1".to_vec());
        assert_eq!(results[1].0, b"key2".to_vec());
    }

    #[tokio::test]
    async fn duckdb_stats_track_key_count_and_size() {
        let dir = tempdir().unwrap();
        let mut engine = DuckDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"a", b"one").await.unwrap();
        engine.put(b"bb", b"two").await.unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.total_size, 1 + 3 + 2 + 3);
        assert!(stats.writes >= 2);
    }

    #[tokio::test]
    async fn duckdb_query_returns_generic_values() {
        let dir = tempdir().unwrap();
        let mut engine = DuckDBEngine::new(config_for(dir.path())).await.unwrap();
        engine.initialize().await.unwrap();
        engine.put(b"sql-key", b"sql-value").await.unwrap();

        let rows = engine
            .query("SELECT COUNT(*) AS count FROM fdc_storage_kv")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("count"), Some(&Value::Int64(1)));

        let rows = engine
            .query("SELECT key, value FROM fdc_storage_kv")
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("key"),
            Some(&Value::Binary(b"sql-key".to_vec()))
        );
        assert_eq!(
            rows[0].get("value"),
            Some(&Value::Binary(b"sql-value".to_vec()))
        );
    }
}
