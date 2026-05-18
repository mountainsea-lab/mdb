use barter_data::{
    event::{DataKind, MarketEvent},
    subscription::trade::PublicTrade,
};
use barter_instrument::{
    exchange::ExchangeId as BarterExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::Utc;
use fdc_barter::{BarterAdapterConfig, BarterDataMode, BarterMarketDataKind, BarterMarketEvent};

#[test]
fn default_config_is_live_and_disables_real_network_startup() {
    let config = BarterAdapterConfig::default();

    assert_eq!(config.mode, BarterDataMode::Live);
    assert!(!config.start_network_streams);
    assert_eq!(config.max_subscriptions_per_connection, 100);
}

#[test]
fn public_trade_event_maps_to_mdb_standard_event() {
    let barter_event = MarketEvent {
        time_exchange: Utc::now(),
        time_received: Utc::now(),
        exchange: BarterExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new("btc", "usdt", MarketDataInstrumentKind::Spot),
        kind: DataKind::Trade(PublicTrade {
            id: "trade-1".to_string(),
            price: 65000.25,
            amount: 2.5,
            side: Side::Buy,
        }),
    };

    let event = BarterMarketEvent::try_from(barter_event).expect("trade should map");

    assert_eq!(event.exchange, "binance_spot");
    assert_eq!(event.symbol.as_str(), "BTCUSDT");
    assert_eq!(event.kind, BarterMarketDataKind::Trade);
    assert_eq!(event.price.expect("price").to_f64(), 65000.25);
    assert_eq!(event.volume.expect("volume").as_u64(), 2);
    assert_eq!(event.source, "barter-rs");
}
