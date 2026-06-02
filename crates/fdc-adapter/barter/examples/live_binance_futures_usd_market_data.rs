use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use fdc_barter::{
    collect_live_envelopes_with_summary, init_binance_futures_usd_market_data,
    BarterMarketDataKind, LiveCollectionRequest, LiveExchange, LiveMarketDataSubscription,
};
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, EnvFilter};

const EXAMPLE_NAME: &str = "live_binance_futures_usd_market_data";
const ENABLE_ENV: &str = "FDC_BARTER_LIVE_EXAMPLE";
const LIMIT: usize = 5;
const TIMEOUT_SECS: u64 = 30;

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}

fn live_example_enabled() -> bool {
    std::env::var(ENABLE_ENV).as_deref() == Ok("1")
}

// Manual run commands for troubleshooting:
//
// Safe dry run, no network access:
//   cargo run --example live_binance_futures_usd_market_data
//
// Live network run:
//   FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_futures_usd_market_data
//
// Debug live network run:
//   RUST_LOG=debug FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_futures_usd_market_data
//
// IDE main run:
//   Add environment variable FDC_BARTER_LIVE_EXAMPLE=1 to the run configuration.
//   Add RUST_LOG=debug as well when detailed subscription diagnostics are needed.
#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    init_tracing();

    info!(
        example = EXAMPLE_NAME,
        env = ENABLE_ENV,
        enabled = live_example_enabled(),
        "starting live example"
    );

    if !live_example_enabled() {
        info!(
            example = EXAMPLE_NAME,
            env = ENABLE_ENV,
            command = "FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_futures_usd_market_data",
            "live network access disabled; set env to run this example"
        );
        return Ok(());
    }

    let subscriptions = [
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Trade,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBookL1,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::OrderBook,
        ),
        LiveMarketDataSubscription::new(
            LiveExchange::BinanceFuturesUsd,
            "btc",
            "usdt",
            MarketDataInstrumentKind::Perpetual,
            BarterMarketDataKind::Liquidation,
        ),
    ];

    for subscription in &subscriptions {
        info!(
            example = EXAMPLE_NAME,
            exchange = ?subscription.exchange,
            base = %subscription.base,
            quote = %subscription.quote,
            instrument_kind = ?subscription.instrument_kind,
            kind = ?subscription.kind,
            "configured live subscription"
        );
    }
    debug!(example = EXAMPLE_NAME, subscriptions = ?subscriptions, "subscription detail");

    info!(example = EXAMPLE_NAME, "initializing live streams");
    let streams = match init_binance_futures_usd_market_data(subscriptions).await {
        Ok(streams) => streams,
        Err(error) => {
            error!(example = EXAMPLE_NAME, %error, "failed to initialize live streams");
            return Err(error);
        }
    };

    info!(
        example = EXAMPLE_NAME,
        limit = LIMIT,
        timeout_secs = TIMEOUT_SECS,
        "streams initialized; collecting records"
    );

    let outcome = collect_live_envelopes_with_summary(
        LiveCollectionRequest {
            source_id: "example-binance-futures-usd-market-data".to_string(),
            limit: LIMIT,
            timeout: Some(std::time::Duration::from_secs(TIMEOUT_SECS)),
        },
        streams.select_all(),
    )
    .await?;

    info!(
        example = EXAMPLE_NAME,
        records_received = outcome.records_received,
        requested_limit = outcome.requested_limit,
        complete = outcome.complete,
        skipped_reconnects = outcome.skipped_reconnects,
        "collection finished"
    );

    for (index, envelope) in outcome.envelopes.into_iter().enumerate() {
        info!(
            example = EXAMPLE_NAME,
            index,
            kind = ?envelope.event.kind,
            exchange = %envelope.event.exchange,
            symbol = %envelope.event.symbol.as_str(),
            sequence = ?envelope.event.sequence,
            timestamp_ns = envelope.event.timestamp.as_nanos(),
            "received live envelope"
        );
    }

    Ok(())
}
