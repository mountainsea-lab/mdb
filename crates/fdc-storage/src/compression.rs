//! Compression algorithms

use fdc_core::error::{Error, Result};
use serde::{Deserialize, Serialize};

/// 压缩算法
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    Zstd,
    Snappy,
}

/// 压缩管理器
pub struct CompressionManager {
    algorithm: CompressionAlgorithm,
}

impl CompressionManager {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self { algorithm }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            CompressionAlgorithm::Zstd => zstd::bulk::compress(data, 3)
                .map_err(|e| Error::compression(format!("Zstd compression failed: {}", e))),
            CompressionAlgorithm::Snappy => {
                Err(Error::unimplemented("Snappy compression not implemented"))
            }
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| Error::compression(format!("LZ4 decompression failed: {}", e))),
            CompressionAlgorithm::Zstd => zstd::bulk::decompress(data, 1024 * 1024)
                .map_err(|e| Error::compression(format!("Zstd decompression failed: {}", e))),
            CompressionAlgorithm::Snappy => {
                Err(Error::unimplemented("Snappy decompression not implemented"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let manager = CompressionManager::new(CompressionAlgorithm::None);
        let data = b"test data";

        let compressed = manager.compress(data).unwrap();
        let decompressed = manager.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_lz4_compression() {
        let manager = CompressionManager::new(CompressionAlgorithm::Lz4);
        let data = b"test data that should compress well with repeated patterns";

        let compressed = manager.compress(data).unwrap();
        let decompressed = manager.decompress(&compressed).unwrap();

        assert_eq!(data, decompressed.as_slice());
    }
}
