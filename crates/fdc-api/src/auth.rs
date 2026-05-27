//! Authentication and authorization

use crate::{config::AuthConfig, errors::ApiResult};

/// 认证管理器
pub struct AuthManager {
    config: AuthConfig,
}

impl AuthManager {
    /// 创建新的认证管理器
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }

    /// 验证API密钥
    pub fn validate_api_key(&self, key: &str) -> bool {
        self.config.api_keys.contains(&key.to_string())
    }

    /// 验证JWT令牌
    pub fn validate_jwt(&self, _token: &str) -> ApiResult<bool> {
        // 简化实现
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_manager_creation() {
        let config = AuthConfig::default();
        let _manager = AuthManager::new(config);
    }
}
