//! API metrics collection

use crate::config::MetricsConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// API指标
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiMetrics {
    /// 请求总数
    pub requests_total: u64,
    /// 成功请求数
    pub requests_success: u64,
    /// 失败请求数
    pub requests_failed: u64,
    /// 平均响应时间（毫秒）
    pub avg_response_time_ms: f64,
    /// 按端点统计
    pub endpoint_stats: HashMap<String, EndpointStats>,
    /// 按状态码统计
    pub status_code_stats: HashMap<u16, u64>,
}

/// 端点统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EndpointStats {
    /// 请求数
    pub requests: u64,
    /// 平均响应时间
    pub avg_response_time_ms: f64,
    /// 错误数
    pub errors: u64,
}

/// 指标收集器
pub struct MetricsCollector {
    config: MetricsConfig,
    metrics: Arc<RwLock<ApiMetrics>>,
}

impl MetricsCollector {
    /// 创建新的指标收集器
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(RwLock::new(ApiMetrics::default())),
        }
    }

    /// 记录请求
    pub async fn record_request(&self, endpoint: &str, status_code: u16, response_time_ms: u64) {
        if !self.config.enabled {
            return;
        }

        let mut metrics = self.metrics.write().await;
        metrics.requests_total += 1;

        if status_code < 400 {
            metrics.requests_success += 1;
        } else {
            metrics.requests_failed += 1;
        }

        // 更新端点统计
        let endpoint_stats = metrics
            .endpoint_stats
            .entry(endpoint.to_string())
            .or_default();
        endpoint_stats.requests += 1;
        endpoint_stats.avg_response_time_ms = (endpoint_stats.avg_response_time_ms
            * (endpoint_stats.requests - 1) as f64
            + response_time_ms as f64)
            / endpoint_stats.requests as f64;

        if status_code >= 400 {
            endpoint_stats.errors += 1;
        }

        // 更新状态码统计
        *metrics.status_code_stats.entry(status_code).or_insert(0) += 1;

        // 更新平均响应时间
        metrics.avg_response_time_ms = (metrics.avg_response_time_ms
            * (metrics.requests_total - 1) as f64
            + response_time_ms as f64)
            / metrics.requests_total as f64;
    }

    /// 获取指标
    pub async fn get_metrics(&self) -> ApiMetrics {
        self.metrics.read().await.clone()
    }

    /// 重置指标
    pub async fn reset_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        *metrics = ApiMetrics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector() {
        let config = MetricsConfig::default();
        let collector = MetricsCollector::new(config);

        collector.record_request("/health", 200, 10).await;
        collector.record_request("/query", 400, 50).await;

        let metrics = collector.get_metrics().await;
        assert_eq!(metrics.requests_total, 2);
        assert_eq!(metrics.requests_success, 1);
        assert_eq!(metrics.requests_failed, 1);
        assert_eq!(metrics.endpoint_stats.len(), 2);
    }
}
