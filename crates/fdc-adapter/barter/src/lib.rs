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
    binance_futures_usd_funding_rate_capabilities,
    binance_futures_usd_funding_rate_provider_from_response,
    binance_futures_usd_funding_rate_rest_request_descriptor,
    binance_futures_usd_mark_price_capabilities,
    binance_futures_usd_mark_price_provider_from_response,
    binance_futures_usd_mark_price_rest_request_descriptor, binance_futures_usd_ohlcv_capabilities,
    binance_futures_usd_ohlcv_provider_from_response,
    binance_futures_usd_ohlcv_rest_request_descriptor,
    binance_futures_usd_open_interest_capabilities,
    binance_futures_usd_open_interest_provider_from_response,
    binance_futures_usd_open_interest_rest_request_descriptor,
    binance_spot_historical_trades_capabilities,
    binance_spot_historical_trades_provider_from_response,
    binance_spot_historical_trades_rest_request_descriptor, binance_spot_ohlcv_capabilities,
    binance_spot_ohlcv_provider_from_response, binance_spot_ohlcv_rest_request_descriptor,
    collect_live_envelopes_with_summary, collect_live_market_data_envelopes,
    collect_live_trade_envelopes, default_binance_futures_usd_market_data_subscriptions,
    default_binance_spot_market_data_subscriptions, default_binance_spot_trade_subscriptions,
    execute_binance_spot_historical_trades_rest, execute_binance_spot_ohlcv_rest,
    historical_trade_dedupe_key, init_binance_futures_usd_market_data,
    init_binance_spot_market_data, init_binance_spot_public_trades, map_live_market_data_result,
    map_live_trade_result, public_trade_result_to_data_kind, run_historical_backfill_pages,
    validate_historical_backfill_request, BarterIngestionEnvelope,
    BarterIntegrationHistoricalRestExecutor, BinanceSpotHistoricalTradesProvider,
    BinanceSpotOhlcvHistoricalPageFetcher, BinanceSpotOhlcvProvider,
    BinanceSpotTradesHistoricalPageFetcher, DataQualityFlags, HistoricalBackfillPage,
    HistoricalBackfillRequest, HistoricalBackfillRunOutcome, HistoricalBackfillRunRequest,
    HistoricalBackfillSource, HistoricalBackfillStopReason, HistoricalExchangeProvider,
    HistoricalPageFetcher, HistoricalPageOutcome, HistoricalProviderCapabilities,
    HistoricalProviderRegistry, HistoricalRestExecutor, HistoricalRestRequestDescriptor,
    LiveCollectionOutcome, LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
    LiveTradeSubscription,
};
pub use mapper::event::map_market_event;
pub use model::{
    event_latency_ns, BarterCheckpoint, BarterKindCounters, BarterMarketDataKind,
    BarterMarketDataMode, BarterMarketDataRequest, BarterMarketEvent, BarterMarketPayload,
    BarterMarketType, BarterRuntimeObservation, BarterSourceState, BarterSourceStatus,
    CandlePayload, DecimalQuantity, FundingRatePayload, HistoricalCursor, HistoricalPageRequest,
    IndexPricePayload, LiquidationPayload, MarkPricePayload, OpenInterestPayload,
    OrderBookL1Payload, OrderBookLevelPayload, OrderBookPayload, OrderBookUpdateKind, RawPayload,
    TradePayload, TradeSide,
};
