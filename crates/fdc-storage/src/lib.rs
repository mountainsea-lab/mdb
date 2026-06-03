//! # Financial Data Center Multi-Engine Storage System
//!
//! This crate provides a comprehensive multi-engine storage system for the
//! Financial Data Center, featuring L1-L4 storage tiers, data sharding,
//! index optimization, and high-performance data access.

pub mod backup; // 备份恢复
pub mod cache; // 缓存管理
pub mod codec; // generic typed storage codecs
pub mod compression; // 压缩算法
pub mod config; // 配置管理
pub mod engine; // 存储引擎抽象
pub mod index; // 索引系统
pub mod lifecycle; // tier lifecycle reports and maintenance result types
pub mod metrics; // 存储指标
pub mod query; // generic storage query boundary
pub mod queryable; // queryable in-memory market-data storage boundary
pub mod replication; // 数据复制
pub mod shard; // 数据分片
pub mod sink; // storage write sink boundary
pub mod tier; // 存储层级管理
pub mod tiered_store; // tier-aware storage sink/query store
pub mod typed; // typed storage facade over raw records
pub mod write; // storage-owned write records and placement hints

// 具体存储引擎实现
pub mod engines {
    pub mod duckdb; // L3: DuckDB存储
    pub mod memory; // L1: 内存存储
    pub mod redb; // L2: redb存储
    pub mod rocksdb; // L4: RocksDB存储
}

// 重新导出常用类型
pub use backup::{BackupConfig, BackupManager, RestoreConfig};
pub use cache::{CacheManager, CachePolicy, CacheStats};
pub use codec::{BincodeStorageCodec, JsonStorageCodec, StorageCodec};
pub use compression::{CompressionAlgorithm, CompressionManager};
pub use config::StorageConfig;
pub use engine::{EngineCapabilities, StorageEngine, StorageEngineType};
pub use index::{IndexConfig, IndexManager, IndexType};
pub use lifecycle::{TierLifecycleAction, TierLifecycleReport, TierLifecycleTierReport};
pub use metrics::StorageMetrics;
pub(crate) use query::{apply_query_order_and_limit, record_matches_storage_query};
pub use query::{
    QueryableStorage, StorageQuery, StorageQueryMetrics, StorageQueryOrder, StorageQueryResult,
    StorageTierScope,
};
pub use queryable::{InMemoryQueryableStorage, MarketDataQuery, QueryableMarketDataStore};
pub use replication::{ReplicationConfig, ReplicationManager};
pub use shard::{ShardKey, ShardManager, ShardStrategy};
pub use sink::{RecordingStorageSink, StorageWriteOutcome, StorageWriteSink};
pub use tier::{StorageTier, TierConfig, TierManager};
pub use tiered_store::{validate_storage_record_roundtrip, TieredStorageStore};
pub use typed::{
    decode_storage_record, query_typed_storage, StorageTypeDescriptor, TypedStorageReadRecord,
    TypedStorageRecord,
};
pub use write::{
    StorageAccessPatternHint, StorageBatchMetadata, StorageDurabilityHint, StoragePlacementHint,
    StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
};

/// 库版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 库名称
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// 默认L1缓存大小 (1GB)
pub const DEFAULT_L1_CACHE_SIZE: usize = 1024 * 1024 * 1024;

/// 默认L2缓存大小 (8GB)
pub const DEFAULT_L2_CACHE_SIZE: usize = 8 * 1024 * 1024 * 1024;

/// 默认分片数量
pub const DEFAULT_SHARD_COUNT: usize = 16;

/// 默认压缩算法
pub const DEFAULT_COMPRESSION: CompressionAlgorithm = CompressionAlgorithm::Lz4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "fdc-storage");
        assert_eq!(DEFAULT_L1_CACHE_SIZE, 1024 * 1024 * 1024);
        assert_eq!(DEFAULT_L2_CACHE_SIZE, 8 * 1024 * 1024 * 1024);
        assert_eq!(DEFAULT_SHARD_COUNT, 16);
    }
}
