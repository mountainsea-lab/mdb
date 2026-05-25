//! Cache management system

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 缓存策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    LRU,
    LFU,
    FIFO,
    Random,
}

/// 缓存统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub capacity: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// 缓存管理器
pub struct CacheManager {
    policy: CachePolicy,
    capacity: usize,
    stats: CacheStats,
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl CacheManager {
    pub fn new(policy: CachePolicy, capacity: usize) -> Self {
        Self {
            policy,
            capacity,
            stats: CacheStats::default(),
            data: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.data.get(key) {
            self.stats.hits += 1;
            Some(value.clone())
        } else {
            self.stats.misses += 1;
            None
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.data.len() >= self.capacity {
            self.evict_one();
        }
        self.data.insert(key, value);
        self.stats.size = self.data.len();
    }

    fn evict_one(&mut self) {
        if let Some(key) = self.data.keys().next().cloned() {
            self.data.remove(&key);
            self.stats.evictions += 1;
        }
    }

    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_manager() {
        let mut cache = CacheManager::new(CachePolicy::LRU, 2);

        cache.put(b"key1".to_vec(), b"value1".to_vec());
        cache.put(b"key2".to_vec(), b"value2".to_vec());

        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(cache.get(b"key3"), None);

        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hit_rate(), 0.5);
    }
}
