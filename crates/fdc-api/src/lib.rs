//! # Financial Data Center API Layer
//!
//! This crate provides comprehensive API interfaces for the Financial Data Center,
//! including REST, gRPC, GraphQL, and WebSocket APIs for data access and management.

pub mod auth; // 认证和授权
pub mod config; // API配置
pub mod errors; // API错误处理
pub mod graphql; // GraphQL API实现
pub mod grpc; // gRPC API实现
pub mod handlers; // 请求处理器
pub mod market_data; // bounded market-data query route
pub mod metrics; // API指标
pub mod middleware; // 中间件
pub mod models; // API数据模型
pub mod rest; // REST API实现
pub mod runner_control;
pub mod runner_status;
pub mod server; // 服务器管理
pub mod state;
pub mod websocket; // WebSocket API实现 // API应用状态边界

// 重新导出常用类型
pub use config::ApiConfig;
pub use errors::{ApiError, ApiResult};
pub use market_data::{
    build_market_data_router, query_market_data_trades, MarketDataTradeQueryParams,
    MarketDataTradeRecord, MarketDataTradesResponse,
};
pub use models::{ApiResponse, QueryRequest, QueryResponse};
pub use runner_control::{
    build_runner_control_router, cancel_runner_from_state, start_fixture_runner_from_state,
    RunnerFixtureTradeInput, RunnerStartFixtureRequest,
};
pub use runner_status::{
    build_runner_status_router, runner_status_response_from_state, ApiRunnerLastResultProjection,
    ApiRunnerLifecycleStatus, ApiRunnerStatusProjection,
};
pub use server::{ApiServer, ServerConfig};
pub use state::{
    readiness_response_from_state, server_environment_label, server_lifecycle_state_label,
    ApiAppState, ApiReadinessProjection, ApiReadinessStatus,
};

/// 库版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 库名称
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// 默认REST API端口
pub const DEFAULT_REST_PORT: u16 = 8080;

/// 默认gRPC端口
pub const DEFAULT_GRPC_PORT: u16 = 9090;

/// 默认GraphQL端点
pub const DEFAULT_GRAPHQL_ENDPOINT: &str = "/graphql";

/// 默认WebSocket端点
pub const DEFAULT_WEBSOCKET_ENDPOINT: &str = "/ws";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "fdc-api");
        assert_eq!(DEFAULT_REST_PORT, 8080);
        assert_eq!(DEFAULT_GRPC_PORT, 9090);
        assert_eq!(DEFAULT_GRAPHQL_ENDPOINT, "/graphql");
        assert_eq!(DEFAULT_WEBSOCKET_ENDPOINT, "/ws");
    }
}
