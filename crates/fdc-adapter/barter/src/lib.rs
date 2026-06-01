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

pub use capability::{
    supported_crypto_market_data_capabilities, BarterSourceCapabilities, RateLimitRule,
};
pub use config::{BarterAdapterConfig, BarterDataMode};
pub use error::{BarterAdapterError, Result};
pub use ingestion::{
    collect_live_market_data_envelopes, collect_live_trade_envelopes,
    default_binance_futures_usd_market_data_subscriptions,
    default_binance_spot_market_data_subscriptions, default_binance_spot_trade_subscriptions,
    historical_trade_dedupe_key, init_binance_futures_usd_market_data,
    init_binance_spot_market_data, init_binance_spot_public_trades, map_live_market_data_result,
    map_live_trade_result, public_trade_result_to_data_kind, validate_historical_backfill_request,
    BarterIngestionEnvelope, DataQualityFlags, HistoricalBackfillPage, HistoricalBackfillRequest,
    HistoricalBackfillSource, HistoricalPageOutcome, LiveExchange, LiveMarketDataSubscription,
    LiveTradeSubscription,
};
pub use mapper::event::map_market_event;
pub use model::{
    event_latency_ns, BarterCheckpoint, BarterKindCounters, BarterMarketDataKind,
    BarterMarketDataMode, BarterMarketDataRequest, BarterMarketEvent, BarterMarketPayload,
    BarterMarketType, BarterRuntimeObservation, BarterSourceState, BarterSourceStatus,
    CandlePayload, DecimalQuantity, HistoricalCursor, HistoricalPageRequest, LiquidationPayload,
    OrderBookL1Payload, OrderBookLevelPayload, OrderBookPayload, OrderBookUpdateKind, RawPayload,
    TradePayload, TradeSide,
};
