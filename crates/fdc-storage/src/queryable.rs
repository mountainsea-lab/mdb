use async_trait::async_trait;
use fdc_core::Result;
use parking_lot::RwLock;

use crate::{StorageWriteBatch, StorageWriteOutcome, StorageWriteRecord, StorageWriteSink};

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
}

impl Default for MarketDataQuery {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct QueryableMarketDataStore {
    records: RwLock<Vec<StorageWriteRecord>>,
}

impl QueryableMarketDataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord> {
        let records = self.records.read();
        let mut matches = Vec::new();

        for record in records.iter() {
            if !record_matches_query(record, query) {
                continue;
            }

            matches.push(record.clone());

            if let Some(limit) = query.limit {
                if matches.len() >= limit {
                    break;
                }
            }
        }

        matches
    }

    pub fn all_records(&self) -> Vec<StorageWriteRecord> {
        self.records.read().clone()
    }

    pub fn record_count(&self) -> usize {
        self.records.read().len()
    }
}

#[async_trait]
impl StorageWriteSink for QueryableMarketDataStore {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        batch.validate()?;

        let outcome = StorageWriteOutcome::accepted(batch.batch_id, batch.records.len());
        self.records.write().extend(batch.records);
        Ok(outcome)
    }
}

fn record_matches_query(record: &StorageWriteRecord, query: &MarketDataQuery) -> bool {
    if record.namespace != query.namespace {
        return false;
    }

    if let Some(collection) = &query.collection {
        if &record.collection != collection {
            return false;
        }
    }

    if let Some(symbol) = &query.symbol {
        if record.metadata.tags.get("symbol") != Some(symbol) {
            return false;
        }
    }

    if let Some(kind) = &query.kind {
        if record.metadata.tags.get("kind") != Some(kind) {
            return false;
        }
    }

    true
}
