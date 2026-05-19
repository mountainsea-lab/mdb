//! # Financial Data Center High-Performance Data Ingestion System
//!
//! This crate provides a comprehensive data ingestion system for the Financial Data Center,
//! featuring high-performance network receivers, data parsing, validation, and batch processing.

pub mod receiver;       // 网络数据接收器
pub mod parser;         // 数据解析器
pub mod validator;      // 数据验证器
pub mod buffer;         // 数据缓冲区
pub mod batch;          // 批量处理器
pub mod backpressure;   // 背压控制
pub mod recovery;       // 错误恢复
pub mod metrics;        // 接入指标
pub mod config;         // 配置管理
pub mod protocols;      // 协议支持
pub mod source;         // 结构化 source path

// 重新导出常用类型
pub use receiver::{DataReceiver, ReceiverType};
pub use parser::{DataParser, ParsedData};
pub use validator::{DataValidator, ValidationRule, ValidationResult};
pub use buffer::{DataBuffer, BufferStats};
pub use batch::{BatchProcessor, BatchResult};
pub use backpressure::BackpressureController;
pub use recovery::{RecoveryManager, RecoveryStrategy};
pub use metrics::IngestionMetrics;
pub use config::{
    IngestionConfig, ReceiverConfig, ParserConfig, ValidatorConfig,
    BufferConfig, BatchConfig, BackpressureConfig, RecoveryConfig, MetricsConfig
};
pub use source::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchProcessorStats, SourceBatchResult,
    SourceBatchSink, SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition,
    SourcePosition, SourceQualityFlags, SourceType, SourceValidationError, SourceValidationErrorType,
    SourceValidationResult, SourceValidationWarning, SourceValidator, SourceValidatorStats,
};

/// 库版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 库名称
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// 默认接收缓冲区大小 (64MB)
pub const DEFAULT_RECEIVE_BUFFER_SIZE: usize = 64 * 1024 * 1024;

/// 默认批量大小
pub const DEFAULT_BATCH_SIZE: usize = 10000;

/// 默认批量超时时间 (毫秒)
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 100;

/// 默认最大并发连接数
pub const DEFAULT_MAX_CONNECTIONS: usize = 1000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "fdc-ingestion");
        assert_eq!(DEFAULT_RECEIVE_BUFFER_SIZE, 64 * 1024 * 1024);
        assert_eq!(DEFAULT_BATCH_SIZE, 10000);
        assert_eq!(DEFAULT_BATCH_TIMEOUT_MS, 100);
        assert_eq!(DEFAULT_MAX_CONNECTIONS, 1000);
    }
}
