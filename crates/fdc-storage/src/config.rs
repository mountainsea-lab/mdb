//! Storage configuration

use crate::{
    cache::CachePolicy,
    compression::CompressionAlgorithm,
    shard::ShardStrategy,
    tier::{StorageTier, TierConfig},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 层级配置
    pub tiers: HashMap<StorageTier, TierConfig>,
    /// 默认压缩算法
    pub default_compression: CompressionAlgorithm,
    /// 缓存策略
    pub cache_policy: CachePolicy,
    /// 分片策略
    pub shard_strategy: ShardStrategy,
    /// 分片数量
    pub shard_count: u32,
    /// 是否启用复制
    pub enable_replication: bool,
    /// 复制因子
    pub replication_factor: u8,
    /// 是否启用备份
    pub enable_backup: bool,
    /// 备份路径
    pub backup_path: Option<String>,
    /// 是否启用指标收集
    pub enable_metrics: bool,
    /// 数据目录
    pub data_dir: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let mut tiers = HashMap::new();

        // L1: 内存缓存 (1GB)
        tiers.insert(
            StorageTier::L1,
            TierConfig::new(StorageTier::L1)
                .with_max_size(crate::DEFAULT_L1_CACHE_SIZE)
                .with_migration_threshold(0.9),
        );

        // L2: redb热数据 (8GB)
        tiers.insert(
            StorageTier::L2,
            TierConfig::new(StorageTier::L2)
                .with_max_size(crate::DEFAULT_L2_CACHE_SIZE)
                .with_migration_threshold(0.8),
        );

        // L3: DuckDB温数据
        tiers.insert(
            StorageTier::L3,
            TierConfig::new(StorageTier::L3).with_migration_threshold(0.7),
        );

        // L4: RocksDB冷数据
        tiers.insert(
            StorageTier::L4,
            TierConfig::new(StorageTier::L4).with_migration_threshold(0.6),
        );

        Self {
            tiers,
            default_compression: crate::DEFAULT_COMPRESSION,
            cache_policy: CachePolicy::LRU,
            shard_strategy: ShardStrategy::Hash,
            shard_count: crate::DEFAULT_SHARD_COUNT as u32,
            enable_replication: false,
            replication_factor: 1,
            enable_backup: true,
            backup_path: Some("./backups".to_string()),
            enable_metrics: true,
            data_dir: "./data".to_string(),
        }
    }
}

impl StorageConfig {
    /// 创建新的存储配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置数据目录
    pub fn with_data_dir(mut self, data_dir: String) -> Self {
        self.data_dir = data_dir;
        self
    }

    /// 设置分片数量
    pub fn with_shard_count(mut self, count: u32) -> Self {
        self.shard_count = count;
        self
    }

    /// 启用复制
    pub fn with_replication(mut self, factor: u8) -> Self {
        self.enable_replication = true;
        self.replication_factor = factor;
        self
    }

    /// 设置备份路径
    pub fn with_backup_path(mut self, path: String) -> Self {
        self.backup_path = Some(path);
        self.enable_backup = true;
        self
    }

    /// 禁用备份
    pub fn without_backup(mut self) -> Self {
        self.enable_backup = false;
        self.backup_path = None;
        self
    }

    /// 设置压缩算法
    pub fn with_compression(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.default_compression = algorithm;
        self
    }

    /// 设置缓存策略
    pub fn with_cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    /// 验证配置
    pub fn validate(&self) -> Result<(), String> {
        if self.data_dir.is_empty() {
            return Err("Data directory cannot be empty".to_string());
        }

        if self.shard_count == 0 {
            return Err("Shard count must be greater than 0".to_string());
        }

        if self.enable_replication && self.replication_factor < 2 {
            return Err(
                "Replication factor must be at least 2 when replication is enabled".to_string(),
            );
        }

        if self.tiers.is_empty() {
            return Err("At least one storage tier must be configured".to_string());
        }

        Ok(())
    }

    /// 获取层级配置
    pub fn get_tier_config(&self, tier: &StorageTier) -> Option<&TierConfig> {
        self.tiers.get(tier)
    }

    /// 添加层级配置
    pub fn add_tier_config(&mut self, config: TierConfig) {
        self.tiers.insert(config.tier.clone(), config);
    }

    /// 移除层级配置
    pub fn remove_tier_config(&mut self, tier: &StorageTier) {
        self.tiers.remove(tier);
    }

    /// 获取启用的层级
    pub fn enabled_tiers(&self) -> Vec<StorageTier> {
        self.tiers
            .values()
            .filter(|config| config.enabled)
            .map(|config| config.tier.clone())
            .collect()
    }

    /// 生成配置摘要
    pub fn summary(&self) -> String {
        let mut summary = String::new();
        summary.push_str(&format!("Storage Configuration Summary:\n"));
        summary.push_str(&format!("  Data Directory: {}\n", self.data_dir));
        summary.push_str(&format!("  Shard Count: {}\n", self.shard_count));
        summary.push_str(&format!("  Compression: {:?}\n", self.default_compression));
        summary.push_str(&format!("  Cache Policy: {:?}\n", self.cache_policy));
        summary.push_str(&format!(
            "  Replication: {} (factor: {})\n",
            self.enable_replication, self.replication_factor
        ));
        summary.push_str(&format!(
            "  Backup: {} (path: {:?})\n",
            self.enable_backup, self.backup_path
        ));
        summary.push_str(&format!("  Metrics: {}\n", self.enable_metrics));

        summary.push_str("  Tiers:\n");
        for (tier, config) in &self.tiers {
            if config.enabled {
                summary.push_str(&format!(
                    "    {}: {:?} (max: {:?})\n",
                    tier.name(),
                    config.engine_type,
                    config.max_size
                ));
            }
        }

        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StorageConfig::default();

        assert_eq!(config.data_dir, "./data");
        assert_eq!(config.shard_count, crate::DEFAULT_SHARD_COUNT as u32);
        assert_eq!(config.default_compression, crate::DEFAULT_COMPRESSION);
        assert!(!config.enable_replication);
        assert!(config.enable_backup);
        assert!(config.enable_metrics);

        // 应该有4个层级
        assert_eq!(config.tiers.len(), 4);
        assert!(config.tiers.contains_key(&StorageTier::L1));
        assert!(config.tiers.contains_key(&StorageTier::L2));
        assert!(config.tiers.contains_key(&StorageTier::L3));
        assert!(config.tiers.contains_key(&StorageTier::L4));
    }

    #[test]
    fn test_config_validation() {
        let config = StorageConfig::default();
        assert!(config.validate().is_ok());

        let mut invalid_config = config.clone();
        invalid_config.data_dir = String::new();
        assert!(invalid_config.validate().is_err());

        let mut invalid_config2 = config.clone();
        invalid_config2.shard_count = 0;
        assert!(invalid_config2.validate().is_err());

        let mut invalid_config3 = config.clone();
        invalid_config3.enable_replication = true;
        invalid_config3.replication_factor = 1;
        assert!(invalid_config3.validate().is_err());
    }

    #[test]
    fn test_config_builder() {
        let config = StorageConfig::new()
            .with_data_dir("/custom/data".to_string())
            .with_shard_count(32)
            .with_replication(3)
            .with_compression(CompressionAlgorithm::Zstd)
            .without_backup();

        assert_eq!(config.data_dir, "/custom/data");
        assert_eq!(config.shard_count, 32);
        assert!(config.enable_replication);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.default_compression, CompressionAlgorithm::Zstd);
        assert!(!config.enable_backup);
        assert!(config.backup_path.is_none());
    }

    #[test]
    fn test_enabled_tiers() {
        let config = StorageConfig::default();
        let enabled_tiers = config.enabled_tiers();

        // 默认情况下所有层级都应该启用
        assert_eq!(enabled_tiers.len(), 4);
        assert!(enabled_tiers.contains(&StorageTier::L1));
        assert!(enabled_tiers.contains(&StorageTier::L2));
        assert!(enabled_tiers.contains(&StorageTier::L3));
        assert!(enabled_tiers.contains(&StorageTier::L4));
    }

    #[test]
    fn test_config_summary() {
        let config = StorageConfig::default();
        let summary = config.summary();

        assert!(summary.contains("Storage Configuration Summary"));
        assert!(summary.contains("Data Directory: ./data"));
        assert!(summary.contains("Shard Count: 16"));
        assert!(summary.contains("L1-UltraHot"));
        assert!(summary.contains("L2-Hot"));
        assert!(summary.contains("L3-Warm"));
        assert!(summary.contains("L4-Cold"));
    }
}
