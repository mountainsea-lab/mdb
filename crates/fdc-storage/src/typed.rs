//! Typed storage facade over raw storage records.
//!
//! Storage remains responsible for raw bytes. Business modules provide concrete
//! DTO types plus codecs and reusable `fdc-types` descriptors.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use fdc_core::{error::Error, Result};
use fdc_types::{SerializationFormat, TypeDefinition, TypeSchema};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    QueryableStorage, StorageCodec, StoragePlacementHint, StorageQuery, StorageWriteMetadata,
    StorageWriteRecord,
};

#[derive(Debug, Clone)]
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

    pub fn from_type_definition(
        type_definition: TypeDefinition,
        serialization_format: SerializationFormat,
    ) -> Self {
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
            return Err(Error::validation(
                "storage type descriptor schema_name must not be empty",
            ));
        }
        if self.schema_version.trim().is_empty() {
            return Err(Error::validation(
                "storage type descriptor schema_version must not be empty",
            ));
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
            return Err(Error::validation(
                "typed storage record namespace must not be empty",
            ));
        }
        if self.collection.trim().is_empty() {
            return Err(Error::validation(
                "typed storage record collection must not be empty",
            ));
        }
        if self.key.is_empty() {
            return Err(Error::validation("typed storage record key must not be empty"));
        }

        let mut metadata = StorageWriteMetadata {
            content_type: Some(codec.content_type().to_string()),
            schema: Some(descriptor.schema_name.clone()),
            schema_version: Some(descriptor.schema_version.clone()),
            source: None,
            tags: self.tags.clone(),
        };
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

pub fn decode_storage_record<T, C>(
    record: StorageWriteRecord,
    codec: &C,
) -> Result<TypedStorageReadRecord<T>>
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
    use fdc_types::{definition::PrimitiveType, TypeDefinition, TypeKind};
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
        let descriptor =
            StorageTypeDescriptor::from_type_definition(type_definition, SerializationFormat::Json);
        let codec = JsonStorageCodec::<DemoValue>::new();
        let typed = TypedStorageRecord::new(
            "demo",
            "values",
            b"demo/1".to_vec(),
            DemoValue {
                symbol: "BTCUSDT".to_string(),
                sequence: 1,
            },
        )
        .with_tag("symbol", "BTCUSDT");

        let raw = typed.encode(&codec, &descriptor).unwrap();

        assert_eq!(raw.namespace, "demo");
        assert_eq!(raw.collection, "values");
        assert_eq!(raw.metadata.content_type.as_deref(), Some("application/json"));
        assert_eq!(raw.metadata.schema.as_deref(), Some("DemoValue"));
        assert_eq!(raw.metadata.schema_version.as_deref(), Some("1.0.0"));
        assert_eq!(
            raw.metadata.tags.get("symbol"),
            Some(&"BTCUSDT".to_string())
        );
        assert_eq!(
            raw.metadata.tags.get("serialization_format"),
            Some(&"Json".to_string())
        );
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
            DemoValue {
                symbol: "BTCUSDT".to_string(),
                sequence: 42,
            },
        )
        .with_tag("symbol", "BTCUSDT");
        let raw = typed.encode(&codec, &descriptor).unwrap();

        store
            .write_batch(StorageWriteBatch::new(vec![raw]))
            .await
            .unwrap();

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
