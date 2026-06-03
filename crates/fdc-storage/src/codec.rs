//! Generic typed storage codecs.

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
        Self {
            _marker: PhantomData,
        }
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
        Self {
            _marker: PhantomData,
        }
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
        let value = DemoValue {
            symbol: "BTCUSDT".to_string(),
            sequence: 7,
        };

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, value);
        assert!(matches!(codec.serialization_format(), SerializationFormat::Json));
    }

    #[test]
    fn bincode_codec_roundtrips_value() {
        let codec = BincodeStorageCodec::<DemoValue>::new();
        let value = DemoValue {
            symbol: "ETHUSDT".to_string(),
            sequence: 9,
        };

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, value);
        assert!(matches!(codec.serialization_format(), SerializationFormat::Binary));
    }
}
