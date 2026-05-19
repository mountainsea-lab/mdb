//! Barter-rs adapter for Financial Data Center.
//!
//! This crate is the integration boundary between Barter's exchange market data
//! streams and mdb's internal data pipeline. It intentionally keeps storage,
//! query, and trading execution concerns out of this adapter.

pub mod capability;
pub mod config;
pub mod error;
pub mod ingestion;
pub mod mapper;
pub mod model;

pub use config::{BarterAdapterConfig, BarterDataMode};
pub use error::{BarterAdapterError, Result};
pub use ingestion::{BarterIngestionEnvelope, DataQualityFlags};
pub use mapper::event::map_market_event;
pub use model::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterMarketEvent, BarterSourceState, BarterSourceStatus, HistoricalCursor,
    HistoricalPageRequest,
};

#[cfg(test)]
mod tests {
    use crate::mapper::event::volume_from_f64;

    #[test]
    fn volume_rejects_negative_values() {
        assert!(volume_from_f64("amount", -1.0).is_err());
    }
}
