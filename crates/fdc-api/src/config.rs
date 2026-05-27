//! API configuration management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// API配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// 服务器配置
    pub server: ServerConfig,
    /// REST API配置
    pub rest: RestConfig,
    /// gRPC配置
    pub grpc: GrpcConfig,
    /// GraphQL配置
    pub graphql: GraphQLConfig,
    /// WebSocket配置
    pub websocket: WebSocketConfig,
    /// 认证配置
    pub auth: AuthConfig,
    /// 中间件配置
    pub middleware: MiddlewareConfig,
    /// 指标配置
    pub metrics: MetricsConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            rest: RestConfig::default(),
            grpc: GrpcConfig::default(),
            graphql: GraphQLConfig::default(),
            websocket: WebSocketConfig::default(),
            auth: AuthConfig::default(),
            middleware: MiddlewareConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// 服务器主机
    pub host: String,
    /// 工作线程数
    pub workers: usize,
    /// 最大连接数
    pub max_connections: usize,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 请求超时时间
    pub request_timeout: Duration,
    /// 是否启用TLS
    pub tls_enabled: bool,
    /// TLS证书路径
    pub tls_cert_path: Option<String>,
    /// TLS私钥路径
    pub tls_key_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            workers: num_cpus::get(),
            max_connections: 10000,
            connection_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

/// REST API配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestConfig {
    /// 监听端口
    pub port: u16,
    /// API版本前缀
    pub version_prefix: String,
    /// 最大请求体大小
    pub max_body_size: usize,
    /// 是否启用压缩
    pub compression_enabled: bool,
    /// CORS配置
    pub cors: CorsConfig,
    /// 限流配置
    pub rate_limit: RateLimitConfig,
}

impl Default for RestConfig {
    fn default() -> Self {
        Self {
            port: crate::DEFAULT_REST_PORT,
            version_prefix: "/api/v1".to_string(),
            max_body_size: 16 * 1024 * 1024, // 16MB
            compression_enabled: true,
            cors: CorsConfig::default(),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

/// CORS配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    /// 允许的源
    pub allowed_origins: Vec<String>,
    /// 允许的方法
    pub allowed_methods: Vec<String>,
    /// 允许的头部
    pub allowed_headers: Vec<String>,
    /// 是否允许凭证
    pub allow_credentials: bool,
    /// 预检请求缓存时间
    pub max_age: Duration,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allowed_origins: vec!["*".to_string()],
            allowed_methods: vec![
                "GET".to_string(),
                "POST".to_string(),
                "PUT".to_string(),
                "DELETE".to_string(),
                "OPTIONS".to_string(),
            ],
            allowed_headers: vec![
                "Content-Type".to_string(),
                "Authorization".to_string(),
                "X-Requested-With".to_string(),
            ],
            allow_credentials: false,
            max_age: Duration::from_secs(3600),
        }
    }
}

/// 限流配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// 是否启用限流
    pub enabled: bool,
    /// 每秒请求数限制
    pub requests_per_second: u32,
    /// 突发请求数限制
    pub burst_size: u32,
    /// 限流窗口大小
    pub window_size: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_second: 1000,
            burst_size: 100,
            window_size: Duration::from_secs(1),
        }
    }
}

/// gRPC配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// 监听端口
    pub port: u16,
    /// 最大消息大小
    pub max_message_size: usize,
    /// 连接保活时间
    pub keepalive_time: Duration,
    /// 连接保活超时
    pub keepalive_timeout: Duration,
    /// 是否启用反射
    pub reflection_enabled: bool,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            port: crate::DEFAULT_GRPC_PORT,
            max_message_size: 4 * 1024 * 1024, // 4MB
            keepalive_time: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(5),
            reflection_enabled: true,
        }
    }
}

/// GraphQL配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLConfig {
    /// GraphQL端点
    pub endpoint: String,
    /// Playground端点
    pub playground_endpoint: String,
    /// 是否启用Playground
    pub playground_enabled: bool,
    /// 查询复杂度限制
    pub max_complexity: usize,
    /// 查询深度限制
    pub max_depth: usize,
    /// 是否启用订阅
    pub subscriptions_enabled: bool,
}

impl Default for GraphQLConfig {
    fn default() -> Self {
        Self {
            endpoint: crate::DEFAULT_GRAPHQL_ENDPOINT.to_string(),
            playground_endpoint: "/playground".to_string(),
            playground_enabled: true,
            max_complexity: 1000,
            max_depth: 10,
            subscriptions_enabled: true,
        }
    }
}

/// WebSocket配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// WebSocket端点
    pub endpoint: String,
    /// 最大连接数
    pub max_connections: usize,
    /// 心跳间隔
    pub heartbeat_interval: Duration,
    /// 连接超时时间
    pub connection_timeout: Duration,
    /// 最大消息大小
    pub max_message_size: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            endpoint: crate::DEFAULT_WEBSOCKET_ENDPOINT.to_string(),
            max_connections: 1000,
            heartbeat_interval: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(60),
            max_message_size: 1024 * 1024, // 1MB
        }
    }
}

/// 认证配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// 是否启用认证
    pub enabled: bool,
    /// JWT密钥
    pub jwt_secret: String,
    /// JWT过期时间
    pub jwt_expiration: Duration,
    /// API密钥
    pub api_keys: Vec<String>,
    /// OAuth配置
    pub oauth: HashMap<String, String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            jwt_secret: "default-secret-change-in-production".to_string(),
            jwt_expiration: Duration::from_secs(3600), // 1小时
            api_keys: Vec::new(),
            oauth: HashMap::new(),
        }
    }
}

/// 中间件配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareConfig {
    /// 是否启用日志中间件
    pub logging_enabled: bool,
    /// 是否启用指标中间件
    pub metrics_enabled: bool,
    /// 是否启用追踪中间件
    pub tracing_enabled: bool,
    /// 自定义中间件
    pub custom_middleware: Vec<String>,
}

impl Default for MiddlewareConfig {
    fn default() -> Self {
        Self {
            logging_enabled: true,
            metrics_enabled: true,
            tracing_enabled: true,
            custom_middleware: Vec::new(),
        }
    }
}

/// 指标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// 是否启用指标收集
    pub enabled: bool,
    /// 指标端点
    pub endpoint: String,
    /// 收集间隔
    pub collection_interval: Duration,
    /// 导出器配置
    pub exporters: Vec<String>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".to_string(),
            collection_interval: Duration::from_secs(10),
            exporters: vec!["prometheus".to_string()],
        }
    }
}

impl ApiConfig {
    /// 从文件加载配置
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// 保存配置到文件
    pub fn save_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// 验证配置
    pub fn validate(&self) -> Result<(), String> {
        if self.server.workers == 0 {
            return Err("workers must be greater than 0".to_string());
        }

        if self.server.max_connections == 0 {
            return Err("max_connections must be greater than 0".to_string());
        }

        if self.rest.max_body_size == 0 {
            return Err("max_body_size must be greater than 0".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ApiConfig::default();
        assert!(config.validate().is_ok());
        assert_eq!(config.rest.port, crate::DEFAULT_REST_PORT);
        assert_eq!(config.grpc.port, crate::DEFAULT_GRPC_PORT);
    }

    #[test]
    fn test_config_validation() {
        let mut config = ApiConfig::default();

        // 测试无效的workers
        config.server.workers = 0;
        assert!(config.validate().is_err());

        // 恢复有效值
        config.server.workers = 4;
        assert!(config.validate().is_ok());
    }
}
