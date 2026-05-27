//! API data models

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// 通用API响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    /// 响应数据
    pub data: T,
    /// 响应状态
    pub status: String,
    /// 响应消息
    pub message: Option<String>,
    /// 时间戳
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// 请求ID
    pub request_id: String,
    /// 元数据
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl<T> ApiResponse<T> {
    /// 创建成功响应
    pub fn success(data: T) -> Self {
        Self {
            data,
            status: "success".to_string(),
            message: None,
            timestamp: chrono::Utc::now(),
            request_id: Uuid::new_v4().to_string(),
            metadata: None,
        }
    }

    /// 创建带消息的成功响应
    pub fn success_with_message(data: T, message: String) -> Self {
        Self {
            data,
            status: "success".to_string(),
            message: Some(message),
            timestamp: chrono::Utc::now(),
            request_id: Uuid::new_v4().to_string(),
            metadata: None,
        }
    }

    /// 设置请求ID
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = request_id;
        self
    }

    /// 设置元数据
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// 分页信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    /// 当前页码
    pub page: u32,
    /// 每页大小
    pub page_size: u32,
    /// 总记录数
    pub total: u64,
    /// 总页数
    pub total_pages: u32,
    /// 是否有下一页
    pub has_next: bool,
    /// 是否有上一页
    pub has_prev: bool,
}

impl Pagination {
    /// 创建分页信息
    pub fn new(page: u32, page_size: u32, total: u64) -> Self {
        let total_pages = ((total as f64) / (page_size as f64)).ceil() as u32;
        Self {
            page,
            page_size,
            total,
            total_pages,
            has_next: page < total_pages,
            has_prev: page > 1,
        }
    }
}

/// 分页响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    /// 数据列表
    pub items: Vec<T>,
    /// 分页信息
    pub pagination: Pagination,
}

impl<T> PaginatedResponse<T> {
    /// 创建分页响应
    pub fn new(items: Vec<T>, page: u32, page_size: u32, total: u64) -> Self {
        Self {
            items,
            pagination: Pagination::new(page, page_size, total),
        }
    }
}

/// 查询请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// SQL查询语句
    pub query: String,
    /// 查询参数
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    /// 查询选项
    pub options: Option<QueryOptions>,
}

/// 查询选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
    /// 查询超时时间（秒）
    pub timeout: Option<u64>,
    /// 最大返回行数
    pub limit: Option<u64>,
    /// 是否返回查询计划
    pub explain: Option<bool>,
    /// 查询格式
    pub format: Option<String>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Some(30),
            limit: Some(10000),
            explain: Some(false),
            format: Some("json".to_string()),
        }
    }
}

/// 查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// 查询结果
    pub results: Vec<HashMap<String, serde_json::Value>>,
    /// 列信息
    pub columns: Vec<ColumnInfo>,
    /// 查询统计
    pub stats: QueryStats,
    /// 查询计划（如果请求）
    pub plan: Option<String>,
}

/// 列信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// 列名
    pub name: String,
    /// 数据类型
    pub data_type: String,
    /// 是否可空
    pub nullable: bool,
    /// 列描述
    pub description: Option<String>,
}

/// 查询统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    /// 执行时间（毫秒）
    pub execution_time_ms: u64,
    /// 返回行数
    pub rows_returned: u64,
    /// 扫描行数
    pub rows_scanned: u64,
    /// 使用的内存（字节）
    pub memory_used: u64,
    /// CPU时间（微秒）
    pub cpu_time_us: u64,
}

/// 数据插入请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
    /// 表名
    pub table: String,
    /// 数据行
    pub data: Vec<HashMap<String, serde_json::Value>>,
    /// 插入选项
    pub options: Option<InsertOptions>,
}

/// 插入选项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertOptions {
    /// 是否忽略重复
    pub ignore_duplicates: Option<bool>,
    /// 是否替换现有数据
    pub replace_existing: Option<bool>,
    /// 批量大小
    pub batch_size: Option<usize>,
}

/// 插入响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResponse {
    /// 插入的行数
    pub rows_inserted: u64,
    /// 跳过的行数
    pub rows_skipped: u64,
    /// 错误的行数
    pub rows_errored: u64,
    /// 插入统计
    pub stats: InsertStats,
}

/// 插入统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertStats {
    /// 处理时间（毫秒）
    pub processing_time_ms: u64,
    /// 验证时间（毫秒）
    pub validation_time_ms: u64,
    /// 写入时间（毫秒）
    pub write_time_ms: u64,
}

/// 健康检查响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// 服务状态
    pub status: String,
    /// 版本信息
    pub version: String,
    /// 启动时间
    pub uptime: String,
    /// 系统信息
    pub system: SystemInfo,
    /// 组件状态
    pub components: HashMap<String, ComponentStatus>,
}

/// 系统信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// CPU使用率
    pub cpu_usage: f64,
    /// 内存使用量（字节）
    pub memory_used: u64,
    /// 总内存（字节）
    pub memory_total: u64,
    /// 磁盘使用量（字节）
    pub disk_used: u64,
    /// 总磁盘空间（字节）
    pub disk_total: u64,
}

/// 组件状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentStatus {
    /// 状态
    pub status: String,
    /// 最后检查时间
    pub last_check: chrono::DateTime<chrono::Utc>,
    /// 错误信息（如果有）
    pub error: Option<String>,
    /// 额外信息
    pub details: Option<HashMap<String, serde_json::Value>>,
}

impl ComponentStatus {
    /// 创建健康状态
    pub fn healthy() -> Self {
        Self {
            status: "healthy".to_string(),
            last_check: chrono::Utc::now(),
            error: None,
            details: None,
        }
    }

    /// 创建不健康状态
    pub fn unhealthy(error: String) -> Self {
        Self {
            status: "unhealthy".to_string(),
            last_check: chrono::Utc::now(),
            error: Some(error),
            details: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_creation() {
        let response = ApiResponse::success("test data");
        assert_eq!(response.status, "success");
        assert_eq!(response.data, "test data");
        assert!(response.message.is_none());
    }

    #[test]
    fn test_pagination() {
        let pagination = Pagination::new(2, 10, 25);
        assert_eq!(pagination.page, 2);
        assert_eq!(pagination.page_size, 10);
        assert_eq!(pagination.total, 25);
        assert_eq!(pagination.total_pages, 3);
        assert!(pagination.has_next);
        assert!(pagination.has_prev);
    }

    #[test]
    fn test_component_status() {
        let healthy = ComponentStatus::healthy();
        assert_eq!(healthy.status, "healthy");
        assert!(healthy.error.is_none());

        let unhealthy = ComponentStatus::unhealthy("Connection failed".to_string());
        assert_eq!(unhealthy.status, "unhealthy");
        assert_eq!(unhealthy.error, Some("Connection failed".to_string()));
    }
}
