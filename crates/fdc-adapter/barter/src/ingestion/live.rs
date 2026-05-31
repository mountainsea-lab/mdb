use barter_data::{
    event::{DataKind, MarketEvent},
    exchange::binance::spot::BinanceSpot,
    streams::{consumer::MarketStreamResult, reconnect, Streams},
    subscription::trade::{PublicTrade, PublicTrades},
};
use barter_instrument::instrument::market_data::{
    kind::MarketDataInstrumentKind, MarketDataInstrument,
};
use futures::{Stream, StreamExt};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
    mapper::event::map_market_event,
    model::BarterMarketDataKind,
};

/// Live exchange variants supported by the first acquisition slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LiveExchange {
    BinanceSpot,
}

/// Public-trade subscription accepted by the live acquisition adapter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveTradeSubscription {
    pub exchange: LiveExchange,
    pub base: String,
    pub quote: String,
}

impl LiveTradeSubscription {
    pub fn new(exchange: LiveExchange, base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            exchange,
            base: base.into(),
            quote: quote.into(),
        }
    }
}

/// Generic market-data subscription accepted by expanded live acquisition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LiveMarketDataSubscription {
    pub exchange: LiveExchange,
    pub base: String,
    pub quote: String,
    pub instrument_kind: MarketDataInstrumentKind,
    pub kind: BarterMarketDataKind,
}

impl LiveMarketDataSubscription {
    pub fn new(
        exchange: LiveExchange,
        base: impl Into<String>,
        quote: impl Into<String>,
        instrument_kind: MarketDataInstrumentKind,
        kind: BarterMarketDataKind,
    ) -> Self {
        Self {
            exchange,
            base: base.into(),
            quote: quote.into(),
            instrument_kind,
            kind,
        }
    }
}

/// Default first-slice live subscriptions: Binance Spot BTC/USDT and ETH/USDT public trades.
pub fn default_binance_spot_trade_subscriptions() -> Vec<LiveTradeSubscription> {
    vec![
        LiveTradeSubscription::new(LiveExchange::BinanceSpot, "btc", "usdt"),
        LiveTradeSubscription::new(LiveExchange::BinanceSpot, "eth", "usdt"),
    ]
}

/// Default first expanded subscriptions for Binance Spot BTC/USDT and ETH/USDT.
pub fn default_binance_spot_market_data_subscriptions() -> Vec<LiveMarketDataSubscription> {
    ["btc", "eth"]
        .into_iter()
        .flat_map(|base| {
            [
                BarterMarketDataKind::Trade,
                BarterMarketDataKind::OrderBookL1,
                BarterMarketDataKind::OrderBook,
            ]
            .into_iter()
            .map(move |kind| {
                LiveMarketDataSubscription::new(
                    LiveExchange::BinanceSpot,
                    base,
                    "usdt",
                    MarketDataInstrumentKind::Spot,
                    kind,
                )
            })
        })
        .collect()
}

/// Start Barter-rs Binance Spot public trade streams.
///
/// Reconnect behavior is owned by Barter-rs. This adapter only builds the subscription list.
pub async fn init_binance_spot_public_trades(
    subscriptions: impl IntoIterator<Item = LiveTradeSubscription>,
) -> Result<Streams<MarketStreamResult<MarketDataInstrument, PublicTrade>>> {
    let mut barter_subscriptions = Vec::new();

    for subscription in subscriptions {
        if subscription.exchange != LiveExchange::BinanceSpot {
            return Err(BarterAdapterError::UnsupportedLiveSubscription(format!(
                "{:?}:{}{}",
                subscription.exchange, subscription.base, subscription.quote
            )));
        }

        barter_subscriptions.push((
            BinanceSpot::default(),
            subscription.base,
            subscription.quote,
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        ));
    }

    Streams::<PublicTrades>::builder()
        .subscribe(barter_subscriptions)
        .init()
        .await
        .map_err(|error| BarterAdapterError::LiveStreamInit(error.to_string()))
}

/// Convert Barter's native PublicTrade stream result into the adapter's DataKind stream result.
pub fn public_trade_result_to_data_kind(
    result: MarketStreamResult<MarketDataInstrument, PublicTrade>,
) -> MarketStreamResult<MarketDataInstrument, DataKind> {
    result.map_ok(|event| MarketEvent {
        time_exchange: event.time_exchange,
        time_received: event.time_received,
        exchange: event.exchange,
        instrument: event.instrument,
        kind: DataKind::Trade(event.kind),
    })
}

/// Map one live Barter stream result into an optional ingestion envelope.
///
/// Reconnect notifications are observable at this boundary but do not emit envelopes.
pub fn map_live_market_data_result(
    source_id: &str,
    result: MarketStreamResult<MarketDataInstrument, DataKind>,
) -> Result<Option<BarterIngestionEnvelope>> {
    match result {
        reconnect::Event::Reconnecting(_exchange) => Ok(None),
        reconnect::Event::Item(Ok(event)) => {
            let event = map_market_event(event)?;
            Ok(Some(BarterIngestionEnvelope::from_event(source_id, event)))
        }
        reconnect::Event::Item(Err(error)) => {
            Err(BarterAdapterError::LiveStreamItem(error.to_string()))
        }
    }
}

/// Backward-compatible wrapper for the original trade-only name.
pub fn map_live_trade_result(
    source_id: &str,
    result: MarketStreamResult<MarketDataInstrument, DataKind>,
) -> Result<Option<BarterIngestionEnvelope>> {
    map_live_market_data_result(source_id, result)
}

/// Collect the next `limit` emitted live market-data envelopes from a stream.
///
/// Reconnect events are skipped. Stream item errors are returned to the caller.
pub async fn collect_live_market_data_envelopes<S>(
    source_id: &str,
    mut stream: S,
    limit: usize,
) -> Result<Vec<BarterIngestionEnvelope>>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    let mut envelopes = Vec::with_capacity(limit);

    while envelopes.len() < limit {
        let Some(result) = stream.next().await else {
            break;
        };

        if let Some(envelope) = map_live_market_data_result(source_id, result)? {
            envelopes.push(envelope);
        }
    }

    Ok(envelopes)
}

/// Backward-compatible wrapper for the original trade-only name.
pub async fn collect_live_trade_envelopes<S>(
    source_id: &str,
    stream: S,
    limit: usize,
) -> Result<Vec<BarterIngestionEnvelope>>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    collect_live_market_data_envelopes(source_id, stream, limit).await
}
