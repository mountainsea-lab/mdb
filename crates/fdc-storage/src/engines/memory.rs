//! Memory storage engine (L1)

use crate::engine::{
    BatchOperation, EngineCapabilities, StorageEngine, StorageEngineType, StorageStats,
};
use async_trait::async_trait;
use fdc_core::{
    error::{Error, Result},
    types::Value,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// 内存存储引擎
pub struct MemoryEngine {
    /// 数据存储
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// 统计信息
    stats: Arc<RwLock<StorageStats>>,
    /// 最大大小
    max_size: Option<usize>,
    /// 当前大小
    current_size: Arc<RwLock<usize>>,
}

impl MemoryEngine {
    /// 创建新的内存引擎
    pub async fn new(config: HashMap<String, String>) -> Result<Self> {
        let max_size = config.get("max_size").and_then(|s| s.parse().ok());

        Ok(Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(StorageStats::default())),
            max_size,
            current_size: Arc::new(RwLock::new(0)),
        })
    }

    /// 检查容量
    fn check_capacity(&self, additional_size: usize) -> Result<()> {
        if let Some(max_size) = self.max_size {
            let current_size = *self.current_size.read();
            if current_size + additional_size > max_size {
                return Err(Error::resource_exhausted("Memory engine capacity exceeded"));
            }
        }
        Ok(())
    }

    /// 更新大小
    fn update_size(&self, size_delta: isize) {
        let mut current_size = self.current_size.write();
        if size_delta < 0 {
            *current_size = current_size.saturating_sub((-size_delta) as usize);
        } else {
            *current_size += size_delta as usize;
        }
    }
}

#[async_trait]
impl StorageEngine for MemoryEngine {
    fn engine_type(&self) -> StorageEngineType {
        StorageEngineType::Memory
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            supports_transactions: false,
            supports_indexes: false,
            supports_compression: false,
            supports_replication: false,
            supports_backup: false,
            supports_sql: false,
            supports_acid: false,
            max_data_size: self.max_size,
            expected_latency_us: 1,
            expected_throughput_ops: 10_000_000,
        }
    }

    async fn initialize(&mut self) -> Result<()> {
        // 内存引擎无需初始化
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        // 清空数据
        self.data.write().clear();
        *self.current_size.write() = 0;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();

        let data = self.data.read();
        let result = data.get(key).cloned();

        // 更新统计
        let latency_us = start.elapsed().as_micros() as u64;
        let mut stats = self.stats.write();
        stats.record_operation(crate::engine::StorageOperation::Get, latency_us);

        Ok(result)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let start = Instant::now();

        // 检查容量
        self.check_capacity(key.len() + value.len())?;

        let mut data = self.data.write();
        let old_size = data.get(key).map(|v| key.len() + v.len()).unwrap_or(0);
        let new_size = key.len() + value.len();

        data.insert(key.to_vec(), value.to_vec());

        // 更新大小
        self.update_size(new_size as isize - old_size as isize);

        // 更新统计
        let latency_us = start.elapsed().as_micros() as u64;
        let mut stats = self.stats.write();
        stats.record_operation(crate::engine::StorageOperation::Put, latency_us);
        stats.total_size = *self.current_size.read() as u64;
        stats.key_count = data.len() as u64;

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let start = Instant::now();

        let mut data = self.data.write();
        if let Some(old_value) = data.remove(key) {
            let old_size = key.len() + old_value.len();
            self.update_size(-(old_size as isize));
        }

        // 更新统计
        let latency_us = start.elapsed().as_micros() as u64;
        let mut stats = self.stats.write();
        stats.record_operation(crate::engine::StorageOperation::Delete, latency_us);
        stats.total_size = *self.current_size.read() as u64;
        stats.key_count = data.len() as u64;

        Ok(())
    }

    async fn batch(&self, operations: Vec<BatchOperation>) -> Result<()> {
        let start = Instant::now();

        let mut data = self.data.write();
        let mut size_delta = 0isize;

        for op in operations {
            match op {
                BatchOperation::Put { key, value } => {
                    let old_size = data.get(&key).map(|v| key.len() + v.len()).unwrap_or(0);
                    let new_size = key.len() + value.len();
                    data.insert(key, value);
                    size_delta += new_size as isize - old_size as isize;
                }
                BatchOperation::Delete { key } => {
                    if let Some(old_value) = data.remove(&key) {
                        let old_size = key.len() + old_value.len();
                        size_delta -= old_size as isize;
                    }
                }
            }
        }

        // 更新大小
        self.update_size(size_delta);

        // 更新统计
        let latency_us = start.elapsed().as_micros() as u64;
        let mut stats = self.stats.write();
        stats.record_operation(crate::engine::StorageOperation::Batch, latency_us);
        stats.total_size = *self.current_size.read() as u64;
        stats.key_count = data.len() as u64;

        Ok(())
    }

    async fn scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();

        let data = self.data.read();
        let mut results = Vec::new();

        for (key, value) in data.iter() {
            // 检查范围
            if let Some(start_key) = start_key {
                if key.as_slice() < start_key {
                    continue;
                }
            }
            if let Some(end_key) = end_key {
                if key.as_slice() >= end_key {
                    continue;
                }
            }

            results.push((key.clone(), value.clone()));

            // 检查限制
            if let Some(limit) = limit {
                if results.len() >= limit {
                    break;
                }
            }
        }

        // 排序结果
        results.sort_by(|a, b| a.0.cmp(&b.0));

        // 更新统计
        let latency_us = start.elapsed().as_micros() as u64;
        let mut stats = self.stats.write();
        stats.record_operation(crate::engine::StorageOperation::Scan, latency_us);

        Ok(results)
    }

    async fn stats(&self) -> Result<StorageStats> {
        let stats = self.stats.read().clone();
        Ok(stats)
    }

    async fn health_check(&self) -> Result<bool> {
        // 内存引擎总是健康的
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_engine_basic_operations() {
        let mut engine = MemoryEngine::new(HashMap::new()).await.unwrap();
        engine.initialize().await.unwrap();

        // 测试put和get
        let key = b"test_key";
        let value = b"test_value";

        engine.put(key, value).await.unwrap();
        let result = engine.get(key).await.unwrap();
        assert_eq!(result, Some(value.to_vec()));

        // 测试delete
        engine.delete(key).await.unwrap();
        let result = engine.get(key).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_memory_engine_batch_operations() {
        let mut engine = MemoryEngine::new(HashMap::new()).await.unwrap();
        engine.initialize().await.unwrap();

        let operations = vec![
            BatchOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            },
            BatchOperation::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            },
            BatchOperation::Delete {
                key: b"key1".to_vec(),
            },
        ];

        engine.batch(operations).await.unwrap();

        assert_eq!(engine.get(b"key1").await.unwrap(), None);
        assert_eq!(engine.get(b"key2").await.unwrap(), Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_engine_scan() {
        let mut engine = MemoryEngine::new(HashMap::new()).await.unwrap();
        engine.initialize().await.unwrap();

        // 插入测试数据
        engine.put(b"key1", b"value1").await.unwrap();
        engine.put(b"key2", b"value2").await.unwrap();
        engine.put(b"key3", b"value3").await.unwrap();

        // 扫描所有数据
        let results = engine.scan(None, None, None).await.unwrap();
        assert_eq!(results.len(), 3);

        // 限制扫描结果
        let results = engine.scan(None, None, Some(2)).await.unwrap();
        assert_eq!(results.len(), 2);

        // 范围扫描
        let results = engine
            .scan(Some(b"key2"), Some(b"key3"), None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"key2");
    }

    #[tokio::test]
    async fn test_memory_engine_capacity() {
        let mut config = HashMap::new();
        config.insert("max_size".to_string(), "100".to_string());

        let mut engine = MemoryEngine::new(config).await.unwrap();
        engine.initialize().await.unwrap();

        // 应该能插入小数据
        engine.put(b"small", b"data").await.unwrap();

        // 应该拒绝大数据
        let large_value = vec![0u8; 200];
        let result = engine.put(b"large", &large_value).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_engine_stats() {
        let mut engine = MemoryEngine::new(HashMap::new()).await.unwrap();
        engine.initialize().await.unwrap();

        engine.put(b"key1", b"value1").await.unwrap();
        engine.get(b"key1").await.unwrap();
        engine.delete(b"key1").await.unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.deletes, 1);
    }
}
