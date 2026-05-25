use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::market_data::MarketDataDto;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketDataTransformSinkResult {
    pub accepted_count: usize,
    pub rejected_count: usize,
}

#[async_trait]
pub trait MarketDataTransformSink: Send + Sync {
    async fn write_market_data_batch(
        &self,
        items: Vec<MarketDataDto>,
    ) -> fdc_core::error::Result<MarketDataTransformSinkResult>;
}

#[derive(Debug, Clone, Default)]
pub struct RecordingMarketDataSink {
    written: Arc<RwLock<Vec<MarketDataDto>>>,
}

impl RecordingMarketDataSink {
    pub async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }

    pub async fn snapshot(&self) -> Vec<MarketDataDto> {
        self.written.read().await.clone()
    }
}

#[async_trait]
impl MarketDataTransformSink for RecordingMarketDataSink {
    async fn write_market_data_batch(
        &self,
        items: Vec<MarketDataDto>,
    ) -> fdc_core::error::Result<MarketDataTransformSinkResult> {
        let accepted_count = items.len();
        self.written.write().await.extend(items);
        Ok(MarketDataTransformSinkResult {
            accepted_count,
            rejected_count: 0,
        })
    }
}
