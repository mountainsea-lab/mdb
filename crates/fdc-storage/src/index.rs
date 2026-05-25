//! Index management system

use fdc_core::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 索引类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    LSM,
    Bitmap,
}

/// 索引配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    pub index_type: IndexType,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// 索引管理器
pub struct IndexManager {
    indexes: HashMap<String, IndexConfig>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, config: IndexConfig) -> Result<()> {
        self.indexes.insert(config.name.clone(), config);
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        self.indexes.remove(name);
        Ok(())
    }

    pub fn list_indexes(&self) -> Vec<&IndexConfig> {
        self.indexes.values().collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_manager() {
        let mut manager = IndexManager::new();

        let config = IndexConfig {
            name: "test_index".to_string(),
            index_type: IndexType::BTree,
            columns: vec!["col1".to_string()],
            unique: false,
        };

        assert!(manager.create_index(config).is_ok());
        assert_eq!(manager.list_indexes().len(), 1);

        assert!(manager.drop_index("test_index").is_ok());
        assert_eq!(manager.list_indexes().len(), 0);
    }
}
