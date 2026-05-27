//! API server management

use crate::{config::ApiConfig, errors::ApiResult};
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

/// API服务器配置
pub type ServerConfig = crate::config::ServerConfig;

/// API服务器
pub struct ApiServer {
    /// 配置
    config: Arc<ApiConfig>,
    /// 路由器
    router: Option<Router>,
}

impl ApiServer {
    /// 创建新的API服务器
    pub fn new(config: ApiConfig) -> Self {
        Self {
            config: Arc::new(config),
            router: None,
        }
    }

    /// 构建路由器
    pub fn build_router(&mut self) -> ApiResult<()> {
        let router = Router::new()
            // 健康检查端点
            .route("/health", get(health_handler))
            .route("/ready", get(readiness_handler))
            // API版本信息
            .route("/version", get(version_handler))
            // 查询端点
            .route("/query", post(query_handler))
            // 数据插入端点
            .route("/insert", post(insert_handler))
            // 指标端点
            .route(&self.config.metrics.endpoint, get(metrics_handler))
            // 中间件层
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(CorsLayer::permissive()),
            );

        self.router = Some(router);
        Ok(())
    }

    /// 启动服务器
    pub async fn start(&mut self) -> ApiResult<()> {
        if self.router.is_none() {
            self.build_router()?;
        }

        let router = self.router.take().unwrap();
        let addr = format!("{}:{}", self.config.server.host, self.config.rest.port);

        info!("Starting API server on {}", addr);

        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            crate::errors::ApiError::internal(format!("Failed to bind to {}: {}", addr, e))
        })?;

        info!("API server listening on {}", addr);

        axum::serve(listener, router)
            .await
            .map_err(|e| crate::errors::ApiError::internal(format!("Server error: {}", e)))?;

        Ok(())
    }

    /// 获取配置
    pub fn config(&self) -> &ApiConfig {
        &self.config
    }
}

/// 健康检查处理器
async fn health_handler() -> axum::Json<crate::models::HealthResponse> {
    use crate::models::{ComponentStatus, HealthResponse, SystemInfo};
    use std::collections::HashMap;

    let mut components = HashMap::new();
    components.insert("database".to_string(), ComponentStatus::healthy());
    components.insert("query_engine".to_string(), ComponentStatus::healthy());
    components.insert("storage".to_string(), ComponentStatus::healthy());

    let health = HealthResponse {
        status: "healthy".to_string(),
        version: crate::VERSION.to_string(),
        uptime: "0d 0h 0m 0s".to_string(), // 简化实现
        system: SystemInfo {
            cpu_usage: 0.0,
            memory_used: 0,
            memory_total: 0,
            disk_used: 0,
            disk_total: 0,
        },
        components,
    };

    axum::Json(health)
}

/// 就绪检查处理器
async fn readiness_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ready",
        "timestamp": chrono::Utc::now()
    }))
}

/// 版本信息处理器
async fn version_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name": crate::NAME,
        "version": crate::VERSION,
        "build_time": "unknown",
        "git_hash": "unknown"
    }))
}

/// 查询处理器
async fn query_handler(
    axum::Json(request): axum::Json<crate::models::QueryRequest>,
) -> Result<
    axum::Json<crate::models::ApiResponse<crate::models::QueryResponse>>,
    crate::errors::ApiError,
> {
    use crate::models::{ColumnInfo, QueryResponse, QueryStats};
    use std::collections::HashMap;

    // 简化实现：模拟查询处理
    info!("Processing query: {}", request.query);

    let response = QueryResponse {
        results: vec![{
            let mut row = HashMap::new();
            row.insert(
                "symbol".to_string(),
                serde_json::Value::String("AAPL".to_string()),
            );
            row.insert(
                "price".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(150.25).unwrap()),
            );
            row
        }],
        columns: vec![
            ColumnInfo {
                name: "symbol".to_string(),
                data_type: "string".to_string(),
                nullable: false,
                description: Some("Stock symbol".to_string()),
            },
            ColumnInfo {
                name: "price".to_string(),
                data_type: "float64".to_string(),
                nullable: false,
                description: Some("Stock price".to_string()),
            },
        ],
        stats: QueryStats {
            execution_time_ms: 10,
            rows_returned: 1,
            rows_scanned: 1000,
            memory_used: 1024,
            cpu_time_us: 5000,
        },
        plan: None,
    };

    Ok(axum::Json(crate::models::ApiResponse::success(response)))
}

/// 插入处理器
async fn insert_handler(
    axum::Json(request): axum::Json<crate::models::InsertRequest>,
) -> Result<
    axum::Json<crate::models::ApiResponse<crate::models::InsertResponse>>,
    crate::errors::ApiError,
> {
    use crate::models::{InsertResponse, InsertStats};

    // 简化实现：模拟插入处理
    info!("Processing insert into table: {}", request.table);

    let response = InsertResponse {
        rows_inserted: request.data.len() as u64,
        rows_skipped: 0,
        rows_errored: 0,
        stats: InsertStats {
            processing_time_ms: 5,
            validation_time_ms: 2,
            write_time_ms: 3,
        },
    };

    Ok(axum::Json(crate::models::ApiResponse::success(response)))
}

/// 指标处理器
async fn metrics_handler() -> String {
    // 简化实现：返回Prometheus格式的指标
    format!(
        "# HELP fdc_api_requests_total Total number of API requests\n\
         # TYPE fdc_api_requests_total counter\n\
         fdc_api_requests_total{{method=\"GET\",endpoint=\"/health\"}} 1\n\
         \n\
         # HELP fdc_api_request_duration_seconds Request duration in seconds\n\
         # TYPE fdc_api_request_duration_seconds histogram\n\
         fdc_api_request_duration_seconds_bucket{{le=\"0.1\"}} 1\n\
         fdc_api_request_duration_seconds_sum 0.05\n\
         fdc_api_request_duration_seconds_count 1\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_creation() {
        let config = ApiConfig::default();
        let server = ApiServer::new(config.clone());
        assert_eq!(server.config.rest.port, config.rest.port);
    }

    #[tokio::test]
    async fn test_health_handler() {
        let response = health_handler().await;
        assert_eq!(response.status, "healthy");
        assert_eq!(response.version, crate::VERSION);
    }

    #[tokio::test]
    async fn test_version_handler() {
        let response = version_handler().await;
        let value = response.0;
        assert_eq!(value["name"], crate::NAME);
        assert_eq!(value["version"], crate::VERSION);
    }
}
