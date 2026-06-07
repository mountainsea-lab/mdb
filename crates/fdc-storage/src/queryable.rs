use async_trait::async_trait;
use fdc_core::Result;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageQuery,
    StorageTierScope, StorageTieringPolicy, StorageWriteBatch, StorageWriteOutcome,
    StorageWriteRecord, StorageWriteSink, TieredStorageStore,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataQuery {
    pub namespace: String,
    pub collection: Option<String>,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub limit: Option<usize>,
}

impl MarketDataQuery {
    pub fn new() -> Self {
        Self {
            namespace: "market_data".to_string(),
            collection: None,
            symbol: None,
            kind: None,
            limit: None,
        }
    }

    pub fn for_trades() -> Self {
        Self::new().with_collection("trades").with_kind("trade")
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    pub fn with_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbol = Some(symbol.into());
        self
    }

    pub fn with_kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn to_storage_query(&self) -> StorageQuery {
        let mut query = StorageQuery::new(self.namespace.clone());
        query.collection = self.collection.clone();
        if let Some(symbol) = &self.symbol {
            query.tags.insert("symbol".to_string(), symbol.clone());
        }
        if let Some(kind) = &self.kind {
            query.tags.insert("kind".to_string(), kind.clone());
        }
        query.limit = self.limit;
        query
    }
}

impl Default for MarketDataQuery {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct InMemoryQueryableStorage {
    records: RwLock<Vec<StorageWriteRecord>>,
}

impl InMemoryQueryableStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all_records(&self) -> Vec<StorageWriteRecord> {
        self.records.read().clone()
    }

    pub fn record_count(&self) -> usize {
        self.records.read().len()
    }
}

#[async_trait]
impl StorageWriteSink for InMemoryQueryableStorage {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;

        let outcome = StorageWriteOutcome::accepted(batch.batch_id, batch.records.len());
        self.records.write().extend(batch.records);
        Ok(outcome)
    }
}

#[async_trait]
impl QueryableStorage for InMemoryQueryableStorage {
    async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>> {
        query.validate()?;

        let matches = self
            .records
            .read()
            .iter()
            .filter(|record| record_matches_storage_query(record, query))
            .cloned()
            .collect();

        Ok(apply_query_order_and_limit(matches, query))
    }
}

enum QueryableMarketDataBackend {
    InMemory(InMemoryQueryableStorage),
    Tiered(Arc<TieredStorageStore>),
}

pub struct QueryableMarketDataStore {
    backend: QueryableMarketDataBackend,
}

impl Default for QueryableMarketDataStore {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl QueryableMarketDataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_memory() -> Self {
        Self {
            backend: QueryableMarketDataBackend::InMemory(InMemoryQueryableStorage::new()),
        }
    }

    pub fn from_tiered_store(store: Arc<TieredStorageStore>) -> Self {
        Self {
            backend: QueryableMarketDataBackend::Tiered(store),
        }
    }

    pub async fn memory_tiered() -> Result<Self> {
        Self::memory_tiered_with_policy(StorageTieringPolicy::compatibility()).await
    }

    pub async fn memory_tiered_with_policy(policy: StorageTieringPolicy) -> Result<Self> {
        let store = TieredStorageStore::memory_only_with_policy(policy).await?;
        Ok(Self::from_tiered_store(Arc::new(store)))
    }

    pub fn query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => {
                futures::executor::block_on(inner.query_storage(&query.to_storage_query()))
                    .expect("market data query should be valid")
            }
            QueryableMarketDataBackend::Tiered(store) => {
                futures::executor::block_on(store.query_storage(&query.to_storage_query()))
                    .expect("market data query should be valid")
            }
        }
    }

    pub fn all_records(&self) -> Vec<StorageWriteRecord> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => inner.all_records(),
            QueryableMarketDataBackend::Tiered(store) => {
                futures::executor::block_on(store.query_storage(
                    &StorageQuery::new("market_data").with_tier_scope(StorageTierScope::All),
                ))
                .expect("market data record scan should be valid")
            }
        }
    }

    pub fn record_count(&self) -> usize {
        self.all_records().len()
    }
}

#[async_trait]
impl StorageWriteSink for QueryableMarketDataStore {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => inner.write_batch(batch).await,
            QueryableMarketDataBackend::Tiered(store) => store.write_batch(batch).await,
        }
    }
}

#[async_trait]
impl QueryableStorage for QueryableMarketDataStore {
    async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>> {
        match &self.backend {
            QueryableMarketDataBackend::InMemory(inner) => inner.query_storage(query).await,
            QueryableMarketDataBackend::Tiered(store) => store.query_storage(query).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageTieringPolicy, StorageWriteBatch, StorageWriteMetadata};

    fn record(
        namespace: &str,
        collection: &str,
        key: &[u8],
        symbol: &str,
        kind: &str,
    ) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata
            .tags
            .insert("symbol".to_string(), symbol.to_string());
        metadata.tags.insert("kind".to_string(), kind.to_string());
        StorageWriteRecord::new(namespace, collection, key.to_vec(), b"value".to_vec())
            .with_metadata(metadata)
    }

    #[tokio::test]
    async fn in_memory_queryable_storage_filters_generic_query() {
        let store = InMemoryQueryableStorage::new();
        store
            .write_batch(StorageWriteBatch::new(vec![
                record("market_data", "trades", b"1", "BTCUSDT", "trade"),
                record("market_data", "trades", b"2", "ETHUSDT", "trade"),
            ]))
            .await
            .unwrap();

        let result = store
            .query_storage(&StorageQuery::new("market_data").with_tag("symbol", "BTCUSDT"))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, b"1".to_vec());
    }

    #[tokio::test]
    async fn market_data_query_remains_compatible() {
        let store = QueryableMarketDataStore::new();
        store
            .write_batch(StorageWriteBatch::new(vec![
                record("market_data", "trades", b"1", "BTCUSDT", "trade"),
                record("market_data", "candles", b"2", "BTCUSDT", "candle"),
            ]))
            .await
            .unwrap();

        let result = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].collection, "trades");
    }

    #[tokio::test]
    async fn queryable_market_data_store_can_use_policy_configured_tiered_backend() {
        let store = QueryableMarketDataStore::memory_tiered_with_policy(
            StorageTieringPolicy::generic_realtime(),
        )
        .await
        .unwrap();

        store
            .write_batch(StorageWriteBatch::new(vec![record(
                "market_data",
                "trades",
                b"policy",
                "BTCUSDT",
                "trade",
            )]))
            .await
            .unwrap();

        assert_eq!(store.record_count(), 1);
    }
}
