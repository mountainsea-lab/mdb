//! Storage metrics collection

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// 存储指标
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageMetrics {
    /// 读取操作数
    pub reads: u64,
    /// 写入操作数
    pub writes: u64,
    /// 删除操作数
    pub deletes: u64,
    /// 总数据大小
    pub total_size: u64,
    /// 平均读取延迟（微秒）
    pub avg_read_latency_us: f64,
    /// 平均写入延迟（微秒）
    pub avg_write_latency_us: f64,
    /// 缓存命中率
    pub cache_hit_rate: f64,
    /// 压缩率
    pub compression_ratio: f64,
    /// 按层级统计
    pub tier_metrics: HashMap<String, TierMetrics>,
    /// 启动时间
    pub start_time: Option<SystemTime>,
}

/// 层级指标
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierMetrics {
    pub reads: u64,
    pub writes: u64,
    pub size: u64,
    pub hit_rate: f64,
}

impl StorageMetrics {
    pub fn new() -> Self {
        Self {
            start_time: Some(SystemTime::now()),
            ..Default::default()
        }
    }

    pub fn record_read(&mut self, latency_us: u64) {
        self.reads += 1;
        let count = self.reads;
        Self::update_avg_latency(&mut self.avg_read_latency_us, latency_us, count);
    }

    pub fn record_write(&mut self, latency_us: u64) {
        self.writes += 1;
        let count = self.writes;
        Self::update_avg_latency(&mut self.avg_write_latency_us, latency_us, count);
    }

    fn update_avg_latency(avg: &mut f64, new_latency: u64, count: u64) {
        *avg = (*avg * (count - 1) as f64 + new_latency as f64) / count as f64;
    }

    pub fn uptime(&self) -> Option<Duration> {
        self.start_time
            .and_then(|start| SystemTime::now().duration_since(start).ok())
    }

    pub fn operations_per_second(&self) -> f64 {
        if let Some(uptime) = self.uptime() {
            let seconds = uptime.as_secs_f64();
            if seconds > 0.0 {
                return (self.reads + self.writes + self.deletes) as f64 / seconds;
            }
        }
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_metrics() {
        let mut metrics = StorageMetrics::new();

        metrics.record_read(100);
        metrics.record_write(200);

        assert_eq!(metrics.reads, 1);
        assert_eq!(metrics.writes, 1);
        assert_eq!(metrics.avg_read_latency_us, 100.0);
        assert_eq!(metrics.avg_write_latency_us, 200.0);

        assert!(metrics.uptime().is_some());
    }
}
