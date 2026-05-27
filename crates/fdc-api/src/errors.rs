//! API error handling

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// API结果类型
pub type ApiResult<T> = Result<T, ApiError>;

/// API错误类型
#[derive(Debug, Error)]
pub enum ApiError {
    /// 内部服务器错误
    #[error("Internal server error: {message}")]
    Internal { message: String },

    /// 请求验证错误
    #[error("Validation error: {message}")]
    Validation { message: String },

    /// 认证错误
    #[error("Authentication error: {message}")]
    Authentication { message: String },

    /// 授权错误
    #[error("Authorization error: {message}")]
    Authorization { message: String },

    /// 资源未找到
    #[error("Resource not found: {resource}")]
    NotFound { resource: String },

    /// 资源冲突
    #[error("Resource conflict: {message}")]
    Conflict { message: String },

    /// 请求过于频繁
    #[error("Too many requests")]
    TooManyRequests,

    /// 请求体过大
    #[error("Request body too large")]
    PayloadTooLarge,

    /// 不支持的媒体类型
    #[error("Unsupported media type")]
    UnsupportedMediaType,

    /// 查询错误
    #[error("Query error: {message}")]
    Query { message: String },

    /// 数据库错误
    #[error("Database error: {message}")]
    Database { message: String },

    /// 网络错误
    #[error("Network error: {message}")]
    Network { message: String },

    /// 超时错误
    #[error("Timeout error")]
    Timeout,

    /// 服务不可用
    #[error("Service unavailable")]
    ServiceUnavailable,
}

impl ApiError {
    /// 创建内部错误
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// 创建验证错误
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
        }
    }

    /// 创建认证错误
    pub fn authentication(message: impl Into<String>) -> Self {
        Self::Authentication {
            message: message.into(),
        }
    }

    /// 创建授权错误
    pub fn authorization(message: impl Into<String>) -> Self {
        Self::Authorization {
            message: message.into(),
        }
    }

    /// 创建未找到错误
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::NotFound {
            resource: resource.into(),
        }
    }

    /// 创建冲突错误
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    /// 创建查询错误
    pub fn query(message: impl Into<String>) -> Self {
        Self::Query {
            message: message.into(),
        }
    }

    /// 创建数据库错误
    pub fn database(message: impl Into<String>) -> Self {
        Self::Database {
            message: message.into(),
        }
    }

    /// 创建网络错误
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
        }
    }

    /// 获取HTTP状态码
    pub fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Validation { .. } => StatusCode::BAD_REQUEST,
            ApiError::Authentication { .. } => StatusCode::UNAUTHORIZED,
            ApiError::Authorization { .. } => StatusCode::FORBIDDEN,
            ApiError::NotFound { .. } => StatusCode::NOT_FOUND,
            ApiError::Conflict { .. } => StatusCode::CONFLICT,
            ApiError::TooManyRequests => StatusCode::TOO_MANY_REQUESTS,
            ApiError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ApiError::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ApiError::Query { .. } => StatusCode::BAD_REQUEST,
            ApiError::Database { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::Network { .. } => StatusCode::BAD_GATEWAY,
            ApiError::Timeout => StatusCode::REQUEST_TIMEOUT,
            ApiError::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// 获取错误代码
    pub fn error_code(&self) -> &'static str {
        match self {
            ApiError::Internal { .. } => "INTERNAL_ERROR",
            ApiError::Validation { .. } => "VALIDATION_ERROR",
            ApiError::Authentication { .. } => "AUTHENTICATION_ERROR",
            ApiError::Authorization { .. } => "AUTHORIZATION_ERROR",
            ApiError::NotFound { .. } => "NOT_FOUND",
            ApiError::Conflict { .. } => "CONFLICT",
            ApiError::TooManyRequests => "TOO_MANY_REQUESTS",
            ApiError::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
            ApiError::UnsupportedMediaType => "UNSUPPORTED_MEDIA_TYPE",
            ApiError::Query { .. } => "QUERY_ERROR",
            ApiError::Database { .. } => "DATABASE_ERROR",
            ApiError::Network { .. } => "NETWORK_ERROR",
            ApiError::Timeout => "TIMEOUT",
            ApiError::ServiceUnavailable => "SERVICE_UNAVAILABLE",
        }
    }
}

/// API错误响应
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// 错误代码
    pub error: String,
    /// 错误消息
    pub message: String,
    /// 时间戳
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// 请求ID（如果有）
    pub request_id: Option<String>,
}

impl ErrorResponse {
    /// 创建新的错误响应
    pub fn new(error: String, message: String) -> Self {
        Self {
            error,
            message,
            timestamp: chrono::Utc::now(),
            request_id: None,
        }
    }

    /// 设置请求ID
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_response = ErrorResponse::new(self.error_code().to_string(), self.to_string());

        (status, Json(error_response)).into_response()
    }
}

// 从fdc-core错误转换
impl From<fdc_core::error::Error> for ApiError {
    fn from(err: fdc_core::error::Error) -> Self {
        match err {
            fdc_core::error::Error::Validation { message } => ApiError::validation(message),
            fdc_core::error::Error::Query { message } => ApiError::query(message),
            fdc_core::error::Error::Storage { message } => ApiError::database(message),
            fdc_core::error::Error::Network { message } => ApiError::network(message),
            fdc_core::error::Error::NotFound { resource } => ApiError::not_found(resource),
            fdc_core::error::Error::AlreadyExists { resource } => {
                ApiError::conflict(format!("Resource already exists: {}", resource))
            }
            fdc_core::error::Error::Timeout { .. } => ApiError::Timeout,
            _ => ApiError::internal(err.to_string()),
        }
    }
}

// 从serde_json错误转换
impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        ApiError::validation(format!("JSON parsing error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let error = ApiError::validation("Invalid input");
        assert_eq!(error.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(error.error_code(), "VALIDATION_ERROR");
    }

    #[test]
    fn test_error_response() {
        let response = ErrorResponse::new("TEST_ERROR".to_string(), "Test message".to_string());

        assert_eq!(response.error, "TEST_ERROR");
        assert_eq!(response.message, "Test message");
        assert!(response.request_id.is_none());
    }

    #[test]
    fn test_error_response_with_request_id() {
        let response = ErrorResponse::new("TEST_ERROR".to_string(), "Test message".to_string())
            .with_request_id("req-123".to_string());

        assert_eq!(response.request_id, Some("req-123".to_string()));
    }

    #[test]
    fn test_from_fdc_core_error() {
        let core_error = fdc_core::error::Error::validation("Core validation error");
        let api_error: ApiError = core_error.into();

        match api_error {
            ApiError::Validation { message } => {
                assert_eq!(message, "Core validation error");
            }
            _ => panic!("Expected validation error"),
        }
    }
}
