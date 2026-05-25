//! Data sharding system

use ahash::AHasher;
use fdc_core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// 分片键
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    pub key: Vec<u8>,
    pub shard_id: u32,
}

impl ShardKey {
    pub fn new(key: Vec<u8>, shard_count: u32) -> Self {
        let shard_id = Self::calculate_shard_id(&key, shard_count);
        Self { key, shard_id }
    }

    fn calculate_shard_id(key: &[u8], shard_count: u32) -> u32 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() % shard_count as u64) as u32
    }
}

/// 分片策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardStrategy {
    Hash,
    Range,
    Custom(String),
}

/// 分片管理器
pub struct ShardManager {
    shard_count: u32,
    strategy: ShardStrategy,
    shard_map: HashMap<u32, String>,
}

impl ShardManager {
    pub fn new(shard_count: u32, strategy: ShardStrategy) -> Self {
        Self {
            shard_count,
            strategy,
            shard_map: HashMap::new(),
        }
    }

    pub fn get_shard_key(&self, key: &[u8]) -> ShardKey {
        ShardKey::new(key.to_vec(), self.shard_count)
    }

    pub fn get_shard_count(&self) -> u32 {
        self.shard_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_key() {
        let key = b"test_key";
        let shard_key = ShardKey::new(key.to_vec(), 16);
        assert_eq!(shard_key.key, key);
        assert!(shard_key.shard_id < 16);
    }

    #[test]
    fn test_shard_manager() {
        let manager = ShardManager::new(16, ShardStrategy::Hash);
        assert_eq!(manager.get_shard_count(), 16);

        let shard_key = manager.get_shard_key(b"test");
        assert!(shard_key.shard_id < 16);
    }
}
