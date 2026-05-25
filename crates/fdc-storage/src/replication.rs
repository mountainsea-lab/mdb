//! Data replication system

use serde::{Deserialize, Serialize};

/// 复制配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    pub factor: u8,
    pub strategy: ReplicationStrategy,
}

/// 复制策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    Sync,
    Async,
    Quorum,
}

/// 复制管理器
pub struct ReplicationManager {
    config: ReplicationConfig,
}

impl ReplicationManager {
    pub fn new(config: ReplicationConfig) -> Self {
        Self { config }
    }

    pub fn get_config(&self) -> &ReplicationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_manager() {
        let config = ReplicationConfig {
            factor: 3,
            strategy: ReplicationStrategy::Sync,
        };

        let manager = ReplicationManager::new(config);
        assert_eq!(manager.get_config().factor, 3);
    }
}
