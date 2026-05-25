use std::{fs, path::PathBuf};

use barter_data::{
    error::DataError,
    event::{DataKind, MarketEvent},
    streams::{consumer::MarketStreamResult, reconnect},
    subscription::trade::PublicTrade,
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{kind::MarketDataInstrumentKind, MarketDataInstrument},
    Side,
};
use chrono::{TimeZone, Utc};
use fdc_barter::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, map_live_trade_result, BarterMarketDataKind,
    BarterMarketDataMode, BarterMarketPayload, LiveExchange, LiveTradeSubscription, TradeSide,
};
use futures::{stream, StreamExt};

const SOURCE_ID: &str = "barter-binance-spot-live-trades";

fn barter_trade_event(
    base: &str,
    quote: &str,
    trade_id: &str,
) -> MarketStreamResult<MarketDataInstrument, DataKind> {
    reconnect::Event::Item(Ok(MarketEvent {
        time_exchange: Utc.timestamp_nanos(1_700_000_000_000_000_000),
        time_received: Utc.timestamp_nanos(1_700_000_000_000_001_000),
        exchange: ExchangeId::BinanceSpot,
        instrument: MarketDataInstrument::new(base, quote, MarketDataInstrumentKind::Spot),
        kind: DataKind::Trade(PublicTrade {
            id: trade_id.to_string(),
            price: 65_000.25,
            amount: 0.5,
            side: Side::Buy,
        }),
    }))
}

#[test]
fn default_binance_spot_trade_subscriptions_are_btc_and_eth_usdt() {
    let subscriptions = default_binance_spot_trade_subscriptions();

    assert_eq!(
        subscriptions,
        vec![
            LiveTradeSubscription::new(LiveExchange::BinanceSpot, "btc", "usdt"),
            LiveTradeSubscription::new(LiveExchange::BinanceSpot, "eth", "usdt"),
        ]
    );
}

#[test]
fn live_trade_result_maps_to_ingestion_envelope() {
    let envelope = map_live_trade_result(SOURCE_ID, barter_trade_event("btc", "usdt", "trade-1"))
        .expect("trade result should map")
        .expect("trade result should emit an envelope");

    assert_eq!(envelope.source_id, SOURCE_ID);
    assert_eq!(envelope.event.source, "barter-rs");
    assert_eq!(envelope.event.mode, BarterMarketDataMode::Live);
    assert_eq!(envelope.event.exchange, "binance_spot");
    assert_eq!(envelope.event.symbol.to_string(), "BTCUSDT");
    assert_eq!(envelope.event.kind, BarterMarketDataKind::Trade);
    assert_eq!(
        envelope.event.timestamp.as_nanos(),
        1_700_000_000_000_000_000
    );
    assert_eq!(
        envelope.event.received_at.as_nanos(),
        1_700_000_000_000_001_000
    );
    match &envelope.event.payload {
        BarterMarketPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price.to_f64(), 65_000.25);
            assert_eq!(trade.quantity.to_string(), "0.5");
            assert_eq!(trade.side, Some(TradeSide::Buy));
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
    assert_eq!(envelope.checkpoint, None);
    assert!(!envelope.quality.is_replay);
    assert!(!envelope.quality.is_backfill);
}

#[tokio::test]
async fn bounded_collection_returns_requested_number_of_envelopes() {
    let input = stream::iter(vec![
        barter_trade_event("btc", "usdt", "trade-1"),
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
        barter_trade_event("eth", "usdt", "trade-2"),
        barter_trade_event("btc", "usdt", "trade-3"),
    ]);

    let envelopes = collect_live_trade_envelopes(SOURCE_ID, input, 2)
        .await
        .expect("bounded collection should succeed");

    assert_eq!(envelopes.len(), 2);
    assert_eq!(envelopes[0].event.symbol.to_string(), "BTCUSDT");
    assert_eq!(envelopes[1].event.symbol.to_string(), "ETHUSDT");
}

#[test]
fn reconnect_event_is_observable_but_does_not_emit_envelope() {
    let mapped = map_live_trade_result(
        SOURCE_ID,
        reconnect::Event::Reconnecting(ExchangeId::BinanceSpot),
    )
    .expect("reconnect events should not fail mapping");

    assert_eq!(mapped, None);
}

#[tokio::test]
async fn stream_item_error_is_returned_instead_of_panicking() {
    let input = stream::iter(vec![reconnect::Event::Item(Err(
        DataError::SubscriptionsEmpty,
    ))]);

    let error = collect_live_trade_envelopes(SOURCE_ID, input, 1)
        .await
        .expect_err("stream item error should be returned");

    assert!(error.to_string().contains("live stream item error"));
}

#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_can_collect_one_binance_spot_trade() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .expect("live Binance Spot stream should initialize");
    let stream = streams
        .select_all()
        .map(fdc_barter::public_trade_result_to_data_kind);
    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        collect_live_trade_envelopes(SOURCE_ID, stream, 1),
    )
    .await
    .expect("should receive one live trade within timeout")
    .expect("live collection should succeed");

    for (index, envelope) in envelopes.iter().enumerate() {
        eprintln!("live envelope #{index}: {envelope:#?}");
    }

    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].event.exchange, "binance_spot");
    assert_eq!(envelopes[0].event.kind, BarterMarketDataKind::Trade);
}

#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_prints_realtime_binance_spot_trades_for_review() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let duration = std::env::var("FDC_BARTER_LIVE_PRINT_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(100));

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .expect("live Binance Spot stream should initialize");
    let mut stream = streams
        .select_all()
        .map(fdc_barter::public_trade_result_to_data_kind);
    let deadline = tokio::time::Instant::now() + duration;
    let mut received_count = 0usize;

    eprintln!(
        "collecting realtime Binance Spot trades for {}s; set FDC_BARTER_LIVE_PRINT_SECONDS to change the window",
        duration.as_secs()
    );

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        let item = match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(item)) => item,
            Ok(None) => break,
            Err(_) => break,
        };

        if let Some(envelope) = map_live_trade_result(SOURCE_ID, item)
            .expect("live trade item should map to an envelope")
        {
            received_count += 1;
            eprintln!("realtime live envelope #{received_count}: {envelope:#?}");
        }
    }

    assert!(
        received_count > 0,
        "expected at least one realtime Binance Spot trade during the review window"
    );
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter() {
    let adapter_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = adapter_manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .expect("fdc-barter should live under crates/fdc-adapter/barter");
    let files_to_scan = [
        workspace_root.join("Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
    ];
    let ingestion_src_dir = workspace_root.join("crates/fdc-ingestion/src");
    let mut violations = Vec::new();

    for file in files_to_scan {
        collect_fdc_barter_references(&file, &mut violations);
    }
    collect_fdc_barter_references_recursively(&ingestion_src_dir, &mut violations);

    assert!(
        violations.is_empty(),
        "fdc-ingestion must not reference fdc-barter; violations: {violations:#?}"
    );
}

fn collect_fdc_barter_references_recursively(path: &PathBuf, violations: &mut Vec<String>) {
    for entry in fs::read_dir(path)
        .unwrap_or_else(|error| panic!("failed to read directory {}: {error}", path.display()))
    {
        let entry = entry.expect("failed to read fdc-ingestion directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_fdc_barter_references_recursively(&path, violations);
        } else if path.is_file() {
            collect_fdc_barter_references(&path, violations);
        }
    }
}

fn collect_fdc_barter_references(path: &PathBuf, violations: &mut Vec<String>) {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));

    for (line_number, line) in contents.lines().enumerate() {
        if line.contains("fdc-barter") || line.contains("fdc_barter") {
            violations.push(format!("{}:{}:{line}", path.display(), line_number + 1));
        }
    }
}
