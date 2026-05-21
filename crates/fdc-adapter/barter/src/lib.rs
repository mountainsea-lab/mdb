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

pub use capability::{BarterSourceCapabilities, RateLimitRule};
pub use config::{BarterAdapterConfig, BarterDataMode};
pub use error::{BarterAdapterError, Result};
pub use ingestion::{BarterIngestionEnvelope, DataQualityFlags, IntoSourceEnvelope};
pub use mapper::event::map_market_event;
pub use model::{
    BarterCheckpoint, BarterMarketDataKind, BarterMarketDataMode, BarterMarketDataRequest,
    BarterMarketEvent, BarterMarketPayload, BarterSourceState, BarterSourceStatus, CandlePayload,
    DecimalQuantity, HistoricalCursor, HistoricalPageRequest, OrderBookL1Payload, RawPayload,
    TradePayload, TradeSide,
};
