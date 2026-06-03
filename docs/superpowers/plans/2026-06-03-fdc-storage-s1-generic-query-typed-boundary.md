# fdc-storage Phase S1 Generic Query and Typed Storage Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a generic storage query boundary and typed storage facade inside `fdc-storage`, reusing `fdc-types` for type/schema/serialization metadata.

**Architecture:** Keep `StorageWriteRecord` as the raw bytes boundary. Add `StorageQuery`/`QueryableStorage` for generic raw reads, then add `StorageCodec<T>` and typed helpers that encode/decode business DTOs without making storage market-data-specific. Preserve `MarketDataQuery` as a compatibility wrapper over the new generic query model.

**Tech Stack:** Rust, `async_trait`, `serde`, `serde_json`, `bincode`, `chrono`, `fdc-core`, `fdc-types`, `tokio` tests.

---

## File Structure

- Create: `crates/fdc-storage/src/query.rs`
  - Defines `StorageQuery`, `StorageQueryOrder`, `QueryableStorage`, and record matching/sorting helpers.
- Modify: `crates/fdc-storage/src/queryable.rs`
  - Adds `InMemoryQueryableStorage` generic store.
  - Keeps `QueryableMarketDataStore` as a wrapper/alias-compatible type.
- Create: `crates/fdc-storage/src/codec.rs`
  - Defines `StorageCodec<T>`, `JsonStorageCodec<T>`, `BincodeStorageCodec<T>`.
- Create: `crates/fdc-storage/src/typed.rs`
  - Defines `StorageTypeDescriptor`, `TypedStorageRecord<T>`, `TypedStorageReadRecord<T>`, and conversion/query helpers.
- Modify: `crates/fdc-storage/src/lib.rs`
  - Exports new modules/types.
- Modify: `crates/fdc-storage/docs/generic-tiered-storage-query-design-analysis.md`
  - Already updated to include `fdc-types` reuse requirement. Do not remove it.

---

### Task 1: Add Generic StorageQuery Model

**Files:**
- Create: `crates/fdc-storage/src/query.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Write failing tests in `query.rs`**

Create `crates/fdc-storage/src/query.rs` with tests first:

```rust
use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use serde::{Deserialize, Serialize};

use crate::StorageWriteRecord;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageQueryOrder {
    Insertion,
    TimestampAsc,
    TimestampDesc,
    KeyAsc,
    KeyDesc,
}

impl Default for StorageQueryOrder {
    fn default() -> Self {
        Self::Insertion
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQuery {
    pub namespace: String,
    pub collection: Option<String>,
    pub key_prefix: Option<Vec<u8>>,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub tags: BTreeMap<String, String>,
    pub limit: Option<usize>,
    pub order: StorageQueryOrder,
}

impl StorageQuery {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            collection: None,
            key_prefix: None,
            start_key: None,
            end_key: None,
            start_time: None,
            end_time: None,
            tags: BTreeMap::new(),
            limit: None,
            order: StorageQueryOrder::default(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.namespace.trim().is_empty() {
            return Err(Error::validation("storage query namespace must not be empty"));
        }
        if matches!(self.limit, Some(0)) {
            return Err(Error::validation("storage query limit must be greater than zero"));
        }
        if let (Some(start), Some(end)) = (&self.start_key, &self.end_key) {
            if start > end {
                return Err(Error::validation("storage query start_key must not be greater than end_key"));
            }
        }
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            if start > end {
                return Err(Error::validation("storage query start_time must not be after end_time"));
            }
        }
        for key in self.tags.keys() {
            if key.trim().is_empty() {
                return Err(Error::validation("storage query tag key must not be empty"));
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait QueryableStorage: Send + Sync {
    async fn query_storage(&self, query: &StorageQuery) -> Result<Vec<StorageWriteRecord>>;
}

pub(crate) fn record_matches_storage_query(record: &StorageWriteRecord, query: &StorageQuery) -> bool {
    if record.namespace != query.namespace {
        return false;
    }
    if let Some(collection) = &query.collection {
        if &record.collection != collection {
            return false;
        }
    }
    if let Some(prefix) = &query.key_prefix {
        if !record.key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(start_key) = &query.start_key {
        if &record.key < start_key {
            return false;
        }
    }
    if let Some(end_key) = &query.end_key {
        if &record.key > end_key {
            return false;
        }
    }
    if let Some(start_time) = query.start_time {
        if record.timestamp < start_time {
            return false;
        }
    }
    if let Some(end_time) = query.end_time {
        if record.timestamp > end_time {
            return false;
        }
    }
    for (key, value) in &query.tags {
        if record.metadata.tags.get(key) != Some(value) {
            return false;
        }
    }
    true
}

pub(crate) fn apply_query_order_and_limit(
    mut records: Vec<StorageWriteRecord>,
    query: &StorageQuery,
) -> Vec<StorageWriteRecord> {
    match query.order {
        StorageQueryOrder::Insertion => {}
        StorageQueryOrder::TimestampAsc => records.sort_by_key(|record| record.timestamp),
        StorageQueryOrder::TimestampDesc => records.sort_by_key(|record| std::cmp::Reverse(record.timestamp)),
        StorageQueryOrder::KeyAsc => records.sort_by(|left, right| left.key.cmp(&right.key)),
        StorageQueryOrder::KeyDesc => records.sort_by(|left, right| right.key.cmp(&left.key)),
    }

    if let Some(limit) = query.limit {
        records.truncate(limit);
    }
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    use crate::{StorageWriteMetadata, StorageWriteRecord};

    fn record(namespace: &str, collection: &str, key: &[u8], timestamp_seconds: i64) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata.tags.insert("symbol".to_string(), "BTCUSDT".to_string());
        StorageWriteRecord::new(namespace, collection, key.to_vec(), b"value".to_vec())
            .with_timestamp(Utc.timestamp_opt(timestamp_seconds, 0).unwrap())
            .with_metadata(metadata)
    }

    #[test]
    fn storage_query_validation_rejects_empty_namespace() {
        let query = StorageQuery::new(" ");
        assert!(query.validate().is_err());
    }

    #[test]
    fn storage_query_matches_generic_filters() {
        let query = StorageQuery::new("market_data")
            .with_collection("trades")
            .with_key_prefix(b"BTC".to_vec())
            .with_time_range(
                Utc.timestamp_opt(10, 0).unwrap(),
                Utc.timestamp_opt(20, 0).unwrap(),
            )
            .with_tag("symbol", "BTCUSDT");

        let matching = record("market_data", "trades", b"BTC/001", 15);
        let wrong_symbol = record("market_data", "trades", b"ETH/001", 15);

        assert!(record_matches_storage_query(&matching, &query));
        assert!(!record_matches_storage_query(&wrong_symbol, &query));
    }

    #[test]
    fn query_order_and_limit_are_applied_after_filtering() {
        let mut query = StorageQuery::new("namespace");
        query.order = StorageQueryOrder::TimestampDesc;
        query.limit = Some(2);

        let ordered = apply_query_order_and_limit(
            vec![
                record("namespace", "collection", b"1", 1),
                record("namespace", "collection", b"2", 3),
                record("namespace", "collection", b"3", 2),
            ],
            &query,
        );

        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].key, b"2".to_vec());
        assert_eq!(ordered[1].key, b"3".to_vec());
    }
}
```

- [ ] **Step 2: Add builder methods needed by tests**

Add these methods inside `impl StorageQuery` before running tests:

```rust
    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = Some(collection.into());
        self
    }

    pub fn with_key_prefix(mut self, key_prefix: Vec<u8>) -> Self {
        self.key_prefix = Some(key_prefix);
        self
    }

    pub fn with_key_range(mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        self.start_key = Some(start_key);
        self.end_key = Some(end_key);
        self
    }

    pub fn with_time_range(mut self, start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        self.start_time = Some(start_time);
        self.end_time = Some(end_time);
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_order(mut self, order: StorageQueryOrder) -> Self {
        self.order = order;
        self
    }
```

- [ ] **Step 3: Export the new module in `lib.rs`**

Modify `crates/fdc-storage/src/lib.rs`:

```rust
pub mod query; // generic storage query boundary
```

Add exports:

```rust
pub use query::{QueryableStorage, StorageQuery, StorageQueryOrder};
```

- [ ] **Step 4: Run focused test**

Run:

```bash
rtk cargo test -p fdc-storage query::tests -- --nocapture
```

Expected: `3 passed` for the new query tests.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-storage/src/query.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add generic storage query model"
```

---

### Task 2: Generalize In-Memory Queryable Store

**Files:**
- Modify: `crates/fdc-storage/src/queryable.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Replace queryable implementation with generic store plus market-data wrapper**

Use this structure in `crates/fdc-storage/src/queryable.rs`:

```rust
use async_trait::async_trait;
use fdc_core::Result;
use parking_lot::RwLock;

use crate::{
    apply_query_order_and_limit, record_matches_storage_query, QueryableStorage, StorageQuery,
    StorageWriteBatch, StorageWriteOutcome, StorageWriteRecord, StorageWriteSink,
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

#[derive(Debug, Default)]
pub struct QueryableMarketDataStore {
    inner: InMemoryQueryableStorage,
}

impl QueryableMarketDataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query(&self, query: &MarketDataQuery) -> Vec<StorageWriteRecord> {
        futures::executor::block_on(self.inner.query_storage(&query.to_storage_query()))
            .expect("market data query should be valid")
    }

    pub fn all_records(&self) -> Vec<StorageWriteRecord> {
        self.inner.all_records()
    }

    pub fn record_count(&self) -> usize {
        self.inner.record_count()
    }
}

#[async_trait]
impl StorageWriteSink for QueryableMarketDataStore {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome> {
        self.inner.write_batch(batch).await
    }
}
```

- [ ] **Step 2: Add tests at bottom of `queryable.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageWriteBatch, StorageWriteMetadata};

    fn record(namespace: &str, collection: &str, key: &[u8], symbol: &str, kind: &str) -> StorageWriteRecord {
        let mut metadata = StorageWriteMetadata::default();
        metadata.tags.insert("symbol".to_string(), symbol.to_string());
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
}
```

- [ ] **Step 3: Export `InMemoryQueryableStorage` and helper visibility**

In `crates/fdc-storage/src/lib.rs`, update exports:

```rust
pub use queryable::{InMemoryQueryableStorage, MarketDataQuery, QueryableMarketDataStore};
pub(crate) use query::{apply_query_order_and_limit, record_matches_storage_query};
```

- [ ] **Step 4: Run focused test**

```bash
rtk cargo test -p fdc-storage queryable::tests -- --nocapture
```

Expected: queryable tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-storage/src/queryable.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add generic in-memory queryable storage"
```

---

### Task 3: Add Storage Codecs Reusing fdc-types SerializationFormat

**Files:**
- Create: `crates/fdc-storage/src/codec.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Create `codec.rs`**

```rust
use std::marker::PhantomData;

use fdc_core::Result;
use fdc_types::SerializationFormat;
use serde::{de::DeserializeOwned, Serialize};

pub trait StorageCodec<T>: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn serialization_format(&self) -> SerializationFormat;
    fn encode(&self, value: &T) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<T>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct JsonStorageCodec<T> {
    _marker: PhantomData<T>,
}

impl<T> JsonStorageCodec<T> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<T> StorageCodec<T> for JsonStorageCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn serialization_format(&self) -> SerializationFormat {
        SerializationFormat::Json
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode(&self, bytes: &[u8]) -> Result<T> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct BincodeStorageCodec<T> {
    _marker: PhantomData<T>,
}

impl<T> BincodeStorageCodec<T> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<T> StorageCodec<T> for BincodeStorageCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn content_type(&self) -> &'static str {
        "application/octet-stream"
    }

    fn serialization_format(&self) -> SerializationFormat {
        SerializationFormat::Binary
    }

    fn encode(&self, value: &T) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }

    fn decode(&self, bytes: &[u8]) -> Result<T> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct DemoValue {
        symbol: String,
        sequence: u64,
    }

    #[test]
    fn json_codec_roundtrips_value() {
        let codec = JsonStorageCodec::<DemoValue>::new();
        let value = DemoValue { symbol: "BTCUSDT".to_string(), sequence: 7 };

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, value);
        assert_eq!(codec.serialization_format(), SerializationFormat::Json);
    }

    #[test]
    fn bincode_codec_roundtrips_value() {
        let codec = BincodeStorageCodec::<DemoValue>::new();
        let value = DemoValue { symbol: "ETHUSDT".to_string(), sequence: 9 };

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, value);
        assert_eq!(codec.serialization_format(), SerializationFormat::Binary);
    }
}
```

- [ ] **Step 2: Export codec module in `lib.rs`**

```rust
pub mod codec; // generic typed storage codecs
pub use codec::{BincodeStorageCodec, JsonStorageCodec, StorageCodec};
```

- [ ] **Step 3: Run focused test**

```bash
rtk cargo test -p fdc-storage codec::tests -- --nocapture
```

Expected: 2 codec tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-storage/src/codec.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add typed storage codecs"
```

---

### Task 4: Add Typed Storage Records and fdc-types Descriptor

**Files:**
- Create: `crates/fdc-storage/src/typed.rs`
- Modify: `crates/fdc-storage/src/lib.rs`

- [ ] **Step 1: Create `typed.rs`**

```rust
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use fdc_types::{SerializationFormat, TypeDefinition, TypeSchema};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    QueryableStorage, StorageCodec, StoragePlacementHint, StorageQuery, StorageWriteMetadata,
    StorageWriteRecord,
};

#[derive(Debug, Clone, PartialEq)]
pub struct StorageTypeDescriptor {
    pub type_definition: Option<TypeDefinition>,
    pub schema: Option<TypeSchema>,
    pub schema_name: String,
    pub schema_version: String,
    pub serialization_format: SerializationFormat,
}

impl StorageTypeDescriptor {
    pub fn new(
        schema_name: impl Into<String>,
        schema_version: impl Into<String>,
        serialization_format: SerializationFormat,
    ) -> Self {
        Self {
            type_definition: None,
            schema: None,
            schema_name: schema_name.into(),
            schema_version: schema_version.into(),
            serialization_format,
        }
    }

    pub fn from_type_definition(type_definition: TypeDefinition, serialization_format: SerializationFormat) -> Self {
        Self {
            schema_name: type_definition.name.clone(),
            schema_version: type_definition.version.clone(),
            type_definition: Some(type_definition),
            schema: None,
            serialization_format,
        }
    }

    pub fn from_schema(schema: TypeSchema, serialization_format: SerializationFormat) -> Self {
        Self {
            schema_name: schema.name.clone(),
            schema_version: schema.version.clone(),
            type_definition: None,
            schema: Some(schema),
            serialization_format,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_name.trim().is_empty() {
            return Err(Error::validation("storage type descriptor schema_name must not be empty"));
        }
        if self.schema_version.trim().is_empty() {
            return Err(Error::validation("storage type descriptor schema_version must not be empty"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedStorageRecord<T> {
    pub namespace: String,
    pub collection: String,
    pub key: Vec<u8>,
    pub value: T,
    pub timestamp: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub placement: StoragePlacementHint,
}

impl<T> TypedStorageRecord<T> {
    pub fn new(
        namespace: impl Into<String>,
        collection: impl Into<String>,
        key: Vec<u8>,
        value: T,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            collection: collection.into(),
            key,
            value,
            timestamp: Utc::now(),
            tags: BTreeMap::new(),
            placement: StoragePlacementHint::default(),
        }
    }

    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn with_placement(mut self, placement: StoragePlacementHint) -> Self {
        self.placement = placement;
        self
    }

    pub fn encode<C>(&self, codec: &C, descriptor: &StorageTypeDescriptor) -> Result<StorageWriteRecord>
    where
        C: StorageCodec<T>,
    {
        descriptor.validate()?;
        if self.namespace.trim().is_empty() {
            return Err(Error::validation("typed storage record namespace must not be empty"));
        }
        if self.collection.trim().is_empty() {
            return Err(Error::validation("typed storage record collection must not be empty"));
        }
        if self.key.is_empty() {
            return Err(Error::validation("typed storage record key must not be empty"));
        }

        let mut metadata = StorageWriteMetadata::default();
        metadata.content_type = Some(codec.content_type().to_string());
        metadata.schema = Some(descriptor.schema_name.clone());
        metadata.schema_version = Some(descriptor.schema_version.clone());
        metadata.tags = self.tags.clone();
        metadata.tags.insert(
            "serialization_format".to_string(),
            format!("{:?}", descriptor.serialization_format),
        );

        Ok(StorageWriteRecord::new(
            self.namespace.clone(),
            self.collection.clone(),
            self.key.clone(),
            codec.encode(&self.value)?,
        )
        .with_timestamp(self.timestamp)
        .with_metadata(metadata)
        .with_placement(self.placement.clone()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedStorageReadRecord<T> {
    pub raw: StorageWriteRecord,
    pub value: T,
}

pub fn decode_storage_record<T, C>(record: StorageWriteRecord, codec: &C) -> Result<TypedStorageReadRecord<T>>
where
    C: StorageCodec<T>,
{
    let value = codec.decode(&record.value)?;
    Ok(TypedStorageReadRecord { raw: record, value })
}

pub async fn query_typed_storage<T, C, S>(
    store: &S,
    query: &StorageQuery,
    codec: &C,
) -> Result<Vec<TypedStorageReadRecord<T>>>
where
    T: Serialize + DeserializeOwned + Send + Sync,
    C: StorageCodec<T>,
    S: QueryableStorage,
{
    let records = store.query_storage(query).await?;
    records
        .into_iter()
        .map(|record| decode_storage_record(record, codec))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdc_types::{PrimitiveType, TypeDefinition, TypeKind};
    use serde::{Deserialize, Serialize};

    use crate::{InMemoryQueryableStorage, JsonStorageCodec, StorageWriteBatch, StorageWriteSink};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct DemoValue {
        symbol: String,
        sequence: u64,
    }

    #[test]
    fn typed_record_encodes_with_fdc_types_descriptor_metadata() {
        let type_definition = TypeDefinition::new(
            "DemoValue".to_string(),
            TypeKind::Primitive(PrimitiveType::Bytes),
        );
        let descriptor = StorageTypeDescriptor::from_type_definition(
            type_definition,
            SerializationFormat::Json,
        );
        let codec = JsonStorageCodec::<DemoValue>::new();
        let typed = TypedStorageRecord::new(
            "demo",
            "values",
            b"demo/1".to_vec(),
            DemoValue { symbol: "BTCUSDT".to_string(), sequence: 1 },
        )
        .with_tag("symbol", "BTCUSDT");

        let raw = typed.encode(&codec, &descriptor).unwrap();

        assert_eq!(raw.namespace, "demo");
        assert_eq!(raw.collection, "values");
        assert_eq!(raw.metadata.content_type.as_deref(), Some("application/json"));
        assert_eq!(raw.metadata.schema.as_deref(), Some("DemoValue"));
        assert_eq!(raw.metadata.schema_version.as_deref(), Some("1.0.0"));
        assert_eq!(raw.metadata.tags.get("symbol"), Some(&"BTCUSDT".to_string()));
    }

    #[tokio::test]
    async fn typed_query_decodes_business_values() {
        let store = InMemoryQueryableStorage::new();
        let codec = JsonStorageCodec::<DemoValue>::new();
        let descriptor = StorageTypeDescriptor::new("DemoValue", "1.0.0", SerializationFormat::Json);
        let typed = TypedStorageRecord::new(
            "demo",
            "values",
            b"demo/1".to_vec(),
            DemoValue { symbol: "BTCUSDT".to_string(), sequence: 42 },
        )
        .with_tag("symbol", "BTCUSDT");
        let raw = typed.encode(&codec, &descriptor).unwrap();

        store.write_batch(StorageWriteBatch::new(vec![raw])).await.unwrap();

        let results = query_typed_storage(
            &store,
            &StorageQuery::new("demo").with_tag("symbol", "BTCUSDT"),
            &codec,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value.sequence, 42);
    }
}
```

- [ ] **Step 2: Export typed module in `lib.rs`**

```rust
pub mod typed; // typed storage facade over raw records
pub use typed::{
    decode_storage_record, query_typed_storage, StorageTypeDescriptor, TypedStorageReadRecord,
    TypedStorageRecord,
};
```

- [ ] **Step 3: Run focused test**

```bash
rtk cargo test -p fdc-storage typed::tests -- --nocapture
```

Expected: typed tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fdc-storage/src/typed.rs crates/fdc-storage/src/lib.rs
git commit -m "feat(storage): add typed storage facade"
```

---

### Task 5: Full Storage Verification and Documentation Check

**Files:**
- Modify only if needed: `docs/superpowers/specs/2026-06-03-fdc-storage-s1-generic-query-typed-boundary-design.md`
- Modify only if needed: `crates/fdc-storage/docs/generic-tiered-storage-query-design-analysis.md`

- [ ] **Step 1: Run full storage tests**

```bash
rtk cargo test -p fdc-storage
```

Expected: all non-ignored `fdc-storage` tests pass.

- [ ] **Step 2: Confirm no unrelated health files were touched**

```bash
rtk git status --short
```

Expected: changes are limited to `crates/fdc-storage/src/*`, storage docs/specs/plans, and pre-existing unrelated `fdc-server health` dirty files remain untouched.

- [ ] **Step 3: Run formatting if tests fail due to formatting/lints or before final commit**

```bash
rtk cargo fmt -p fdc-storage
rtk cargo test -p fdc-storage
```

Expected: formatting succeeds and tests pass.

- [ ] **Step 4: Final commit if any formatting/doc cleanup remains**

```bash
git add crates/fdc-storage/src docs/superpowers/specs/2026-06-03-fdc-storage-s1-generic-query-typed-boundary-design.md crates/fdc-storage/docs/generic-tiered-storage-query-design-analysis.md
git commit -m "test(storage): verify generic typed storage boundary"
```

Skip this commit if there are no remaining changes after previous task commits.

---

## Plan Self-Review

- Spec coverage: covers generic query, queryable storage, typed codec/facade, `fdc-types` reuse, market-data compatibility, and storage-only scope.
- Placeholder scan: no placeholder tasks or unfinished sections.
- Type consistency: `StorageQuery`, `QueryableStorage`, `StorageCodec<T>`, `StorageTypeDescriptor`, `TypedStorageRecord<T>`, and `TypedStorageReadRecord<T>` are introduced before use.
- Scope: does not implement pipeline glue, redb, DuckDB, RocksDB, or SQL planner.
- Safety: explicitly avoids unrelated `fdc-server health` dirty files.
