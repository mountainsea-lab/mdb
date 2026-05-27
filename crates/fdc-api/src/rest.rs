//! REST API implementation

use crate::{config::RestConfig, errors::ApiResult};
use axum::{routing::get, Router};

/// REST API路由器
pub struct RestRouter {
    _config: RestConfig, // 添加下划线前缀避免未使用警告
}

impl RestRouter {
    /// 创建新的REST路由器
    pub fn new(config: RestConfig) -> Self {
        Self { _config: config }
    }

    /// 构建路由
    pub fn build_routes(&self) -> ApiResult<Router> {
        let router = Router::new().route("/", get(|| async { "FDC REST API" }));

        Ok(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_router_creation() {
        let config = RestConfig::default();
        let _router = RestRouter::new(config);
    }
}
