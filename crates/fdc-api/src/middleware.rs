//! API middleware

use crate::config::MiddlewareConfig;

/// 中间件管理器
pub struct MiddlewareManager {
    config: MiddlewareConfig,
}

impl MiddlewareManager {
    /// 创建新的中间件管理器
    pub fn new(config: MiddlewareConfig) -> Self {
        Self { config }
    }

    /// 检查是否启用日志中间件
    pub fn is_logging_enabled(&self) -> bool {
        self.config.logging_enabled
    }

    /// 检查是否启用指标中间件
    pub fn is_metrics_enabled(&self) -> bool {
        self.config.metrics_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_middleware_manager_creation() {
        let config = MiddlewareConfig::default();
        let manager = MiddlewareManager::new(config);
        assert!(manager.is_logging_enabled());
        assert!(manager.is_metrics_enabled());
    }
}
