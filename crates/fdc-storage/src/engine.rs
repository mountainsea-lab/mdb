//! Storage engine abstraction

use async_trait::async_trait;
use fdc_core::{
    error::{Error, Result},
    types::Value,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// 存储引擎类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageEngineType {
    /// L1: 超热缓存 (内存)
    Memory,
    /// L2: 热数据缓存 (redb)
    Redb,
    /// L3: 温数据存储 (DuckDB)
    DuckDB,
    /// L4: 冷数据存储 (RocksDB)
    RocksDB,
    /// 自定义引擎
    Custom(String),
}

impl std::fmt::Display for StorageEngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngineType::Memory => write!(f, "memory"),
            StorageEngineType::Redb => write!(f, "redb"),
            StorageEngineType::DuckDB => write!(f, "duckdb"),
            StorageEngineType::RocksDB => write!(f, "rocksdb"),
            StorageEngineType::Custom(name) => write!(f, "custom_{}", name),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageEngineFeature {
    Compaction,
    Snapshot,
    Restore,
    SqlQuery,
}

impl std::fmt::Display for StorageEngineFeature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageEngineFeature::Compaction => write!(f, "compaction"),
            StorageEngineFeature::Snapshot => write!(f, "snapshot"),
            StorageEngineFeature::Restore => write!(f, "restore"),
            StorageEngineFeature::SqlQuery => write!(f, "sql_query"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEngineFeatureError {
    pub engine_type: StorageEngineType,
    pub feature: StorageEngineFeature,
    pub message: String,
}

impl StorageEngineFeatureError {
    pub fn unsupported(engine_type: StorageEngineType, feature: StorageEngineFeature) -> Self {
        Self {
            engine_type,
            feature,
            message: "feature is not supported by this storage engine".to_string(),
        }
    }

    pub fn into_error(self) -> Error {
        Error::unimplemented(format!(
            "unsupported storage engine feature: engine={}, feature={}, message={}",
            self.engine_type, self.feature, self.message
        ))
    }
}

/// 引擎能力
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineCapabilities {
    /// 是否支持事务
    pub supports_transactions: bool,
    /// 是否支持索引
    pub supports_indexes: bool,
    /// 是否支持压缩
    pub supports_compression: bool,
    /// 是否支持复制
    pub supports_replication: bool,
    /// 是否支持备份
    pub supports_backup: bool,
    /// 是否支持SQL查询
    pub supports_sql: bool,
    /// 是否支持ACID
    pub supports_acid: bool,
    /// 最大数据大小
    pub max_data_size: Option<usize>,
    /// 预期延迟（微秒）
    pub expected_latency_us: u64,
    /// 预期吞吐量（操作/秒）
    pub expected_throughput_ops: u64,
}

impl Default for EngineCapabilities {
    fn default() -> Self {
        Self {
            supports_transactions: false,
            supports_indexes: false,
            supports_compression: false,
            supports_replication: false,
            supports_backup: false,
            supports_sql: false,
            supports_acid: false,
            max_data_size: None,
            expected_latency_us: 1000,
            expected_throughput_ops: 1000,
        }
    }
}

/// 存储操作类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageOperation {
    Get,
    Put,
    Delete,
    Scan,
    Query,
    Batch,
}

/// 存储统计信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageStats {
    /// 读取次数
    pub reads: u64,
    /// 写入次数
    pub writes: u64,
    /// 删除次数
    pub deletes: u64,
    /// 扫描次数
    pub scans: u64,
    /// 查询次数
    pub queries: u64,
    /// 总数据大小
    pub total_size: u64,
    /// 键数量
    pub key_count: u64,
    /// 平均延迟（微秒）
    pub avg_latency_us: f64,
    /// 缓存命中率
    pub cache_hit_rate: f64,
    /// 压缩率
    pub compression_ratio: f64,
}

impl StorageStats {
    /// 记录操作
    pub fn record_operation(&mut self, op: StorageOperation, latency_us: u64) {
        match op {
            StorageOperation::Get => self.reads += 1,
            StorageOperation::Put => self.writes += 1,
            StorageOperation::Delete => self.deletes += 1,
            StorageOperation::Scan => self.scans += 1,
            StorageOperation::Query => self.queries += 1,
            StorageOperation::Batch => {
                // 批量操作可能包含多种操作类型
                self.writes += 1;
            }
        }

        // 更新平均延迟
        let total_ops = self.reads + self.writes + self.deletes + self.scans + self.queries;
        if total_ops > 0 {
            self.avg_latency_us = (self.avg_latency_us * (total_ops - 1) as f64
                + latency_us as f64)
                / total_ops as f64;
        }
    }

    /// 获取总操作数
    pub fn total_operations(&self) -> u64 {
        self.reads + self.writes + self.deletes + self.scans + self.queries
    }

    /// 获取读写比
    pub fn read_write_ratio(&self) -> f64 {
        if self.writes == 0 {
            f64::INFINITY
        } else {
            self.reads as f64 / self.writes as f64
        }
    }
}

/// 存储引擎特征
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// 获取引擎类型
    fn engine_type(&self) -> StorageEngineType;

    /// 获取引擎能力
    fn capabilities(&self) -> EngineCapabilities;

    fn supports_feature(&self, feature: StorageEngineFeature) -> bool {
        let capabilities = self.capabilities();
        match feature {
            StorageEngineFeature::Compaction => capabilities.supports_compression,
            StorageEngineFeature::Snapshot => capabilities.supports_backup,
            StorageEngineFeature::Restore => capabilities.supports_backup,
            StorageEngineFeature::SqlQuery => capabilities.supports_sql,
        }
    }

    /// 初始化引擎
    async fn initialize(&mut self) -> Result<()>;

    /// 关闭引擎
    async fn shutdown(&mut self) -> Result<()>;

    /// 获取值
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// 设置值
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// 删除值
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// 批量操作
    async fn batch(&self, operations: Vec<BatchOperation>) -> Result<()>;

    /// 扫描键值对
    async fn scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// 检查键是否存在
    async fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    /// 获取统计信息
    async fn stats(&self) -> Result<StorageStats>;

    /// 压缩数据
    async fn compact(&self) -> Result<()> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Compaction,
        )
        .into_error())
    }

    /// 创建快照
    async fn snapshot(&self) -> Result<String> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Snapshot,
        )
        .into_error())
    }

    /// 恢复快照
    async fn restore(&self, _snapshot_id: &str) -> Result<()> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::Restore,
        )
        .into_error())
    }

    /// 执行SQL查询（如果支持）
    async fn query(&self, _sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        Err(StorageEngineFeatureError::unsupported(
            self.engine_type(),
            StorageEngineFeature::SqlQuery,
        )
        .into_error())
    }

    /// 健康检查
    async fn health_check(&self) -> Result<bool> {
        // 默认实现：尝试读取一个不存在的键
        match self.get(b"__health_check__").await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

/// 批量操作
#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// 存储引擎工厂
pub struct StorageEngineFactory;

impl StorageEngineFactory {
    /// 创建存储引擎
    pub async fn create_engine(
        engine_type: StorageEngineType,
        config: HashMap<String, String>,
    ) -> Result<Box<dyn StorageEngine>> {
        match engine_type {
            StorageEngineType::Memory => {
                let engine = crate::engines::memory::MemoryEngine::new(config).await?;
                Ok(Box::new(engine))
            }
            StorageEngineType::Redb => {
                let engine = crate::engines::redb::RedbEngine::new(config).await?;
                Ok(Box::new(engine))
            }
            StorageEngineType::DuckDB => {
                let engine = crate::engines::duckdb::DuckDBEngine::new(config).await?;
                Ok(Box::new(engine))
            }
            StorageEngineType::RocksDB => {
                let engine = crate::engines::rocksdb::RocksDBEngine::new(config).await?;
                Ok(Box::new(engine))
            }
            StorageEngineType::Custom(name) => Err(Error::unimplemented(format!(
                "Custom engine not implemented: {}",
                name
            ))),
        }
    }

    /// 获取引擎能力
    pub fn get_capabilities(engine_type: &StorageEngineType) -> EngineCapabilities {
        match engine_type {
            StorageEngineType::Memory => EngineCapabilities {
                supports_transactions: false,
                supports_indexes: false,
                supports_compression: false,
                supports_replication: false,
                supports_backup: false,
                supports_sql: false,
                supports_acid: false,
                max_data_size: Some(16 * 1024 * 1024 * 1024), // 16GB
                expected_latency_us: 1,
                expected_throughput_ops: 10_000_000,
            },
            StorageEngineType::Redb => EngineCapabilities {
                supports_transactions: true,
                supports_indexes: true,
                supports_compression: false,
                supports_replication: false,
                supports_backup: true,
                supports_sql: false,
                supports_acid: true,
                max_data_size: Some(256 * 1024 * 1024 * 1024), // 256GB
                expected_latency_us: 5,
                expected_throughput_ops: 1_000_000,
            },
            StorageEngineType::DuckDB => EngineCapabilities {
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
            },
            StorageEngineType::RocksDB => EngineCapabilities {
                supports_transactions: true,
                supports_indexes: false,
                supports_compression: true,
                supports_replication: true,
                supports_backup: true,
                supports_sql: false,
                supports_acid: false,
                max_data_size: None, // 无限制
                expected_latency_us: 10_000,
                expected_throughput_ops: 10_000,
            },
            StorageEngineType::Custom(_) => EngineCapabilities::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_type_display() {
        assert_eq!(StorageEngineType::Memory.to_string(), "memory");
        assert_eq!(StorageEngineType::Redb.to_string(), "redb");
        assert_eq!(StorageEngineType::DuckDB.to_string(), "duckdb");
        assert_eq!(StorageEngineType::RocksDB.to_string(), "rocksdb");
        assert_eq!(
            StorageEngineType::Custom("test".to_string()).to_string(),
            "custom_test"
        );
    }

    #[test]
    fn test_storage_stats() {
        let mut stats = StorageStats::default();

        stats.record_operation(StorageOperation::Get, 100);
        stats.record_operation(StorageOperation::Put, 200);

        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.total_operations(), 2);
        assert_eq!(stats.avg_latency_us, 150.0);
        assert_eq!(stats.read_write_ratio(), 1.0);
    }

    #[test]
    fn test_engine_capabilities() {
        let memory_caps = StorageEngineFactory::get_capabilities(&StorageEngineType::Memory);
        assert_eq!(memory_caps.expected_latency_us, 1);
        assert_eq!(memory_caps.expected_throughput_ops, 10_000_000);
        assert!(!memory_caps.supports_sql);

        let duckdb_caps = StorageEngineFactory::get_capabilities(&StorageEngineType::DuckDB);
        assert!(duckdb_caps.supports_sql);
        assert!(duckdb_caps.supports_compression);
    }

    #[test]
    fn engine_feature_support_maps_from_capabilities() {
        assert!(!StorageEngineFactory::get_capabilities(&StorageEngineType::Memory).supports_sql);
        assert!(StorageEngineFactory::get_capabilities(&StorageEngineType::DuckDB).supports_sql);

        struct DummyEngine {
            engine_type: StorageEngineType,
            capabilities: EngineCapabilities,
        }

        #[async_trait]
        impl StorageEngine for DummyEngine {
            fn engine_type(&self) -> StorageEngineType {
                self.engine_type.clone()
            }

            fn capabilities(&self) -> EngineCapabilities {
                self.capabilities.clone()
            }

            async fn initialize(&mut self) -> Result<()> {
                Ok(())
            }

            async fn shutdown(&mut self) -> Result<()> {
                Ok(())
            }

            async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
                Ok(None)
            }

            async fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
                Ok(())
            }

            async fn delete(&self, _key: &[u8]) -> Result<()> {
                Ok(())
            }

            async fn batch(&self, _operations: Vec<BatchOperation>) -> Result<()> {
                Ok(())
            }

            async fn scan(
                &self,
                _start_key: Option<&[u8]>,
                _end_key: Option<&[u8]>,
                _limit: Option<usize>,
            ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
                Ok(Vec::new())
            }

            async fn stats(&self) -> Result<StorageStats> {
                Ok(StorageStats::default())
            }
        }

        let unsupported = DummyEngine {
            engine_type: StorageEngineType::Memory,
            capabilities: EngineCapabilities::default(),
        };
        assert!(!unsupported.supports_feature(StorageEngineFeature::Compaction));
        assert!(!unsupported.supports_feature(StorageEngineFeature::SqlQuery));

        let supported = DummyEngine {
            engine_type: StorageEngineType::RocksDB,
            capabilities: EngineCapabilities {
                supports_compression: true,
                supports_sql: true,
                supports_backup: true,
                ..EngineCapabilities::default()
            },
        };
        assert!(supported.supports_feature(StorageEngineFeature::Compaction));
        assert!(supported.supports_feature(StorageEngineFeature::SqlQuery));
        assert!(supported.supports_feature(StorageEngineFeature::Snapshot));
        assert!(supported.supports_feature(StorageEngineFeature::Restore));
    }

    #[test]
    fn unsupported_feature_error_formats_stable_message() {
        let error = StorageEngineFeatureError::unsupported(
            StorageEngineType::Memory,
            StorageEngineFeature::Compaction,
        )
        .into_error();

        let message = error.to_string();
        assert!(message.contains("unsupported storage engine feature"));
        assert!(message.contains("memory"));
        assert!(message.contains("compaction"));
    }
}
