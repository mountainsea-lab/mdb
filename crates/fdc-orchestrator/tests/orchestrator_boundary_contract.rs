use std::path::{Path, PathBuf};

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, BarterMarketType, CandlePayload, DataQualityFlags, DecimalQuantity,
    FundingRatePayload, IndexPricePayload, MarkPricePayload, OpenInterestPayload, TradePayload,
    TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_ingestion::{SourceEnvelope, SourceType};
use fdc_orchestrator::{
    barter::barter_envelope_to_source_envelope, market_data::barter_event_to_market_data_dto,
    pipeline::run_barter_envelopes_to_storage_once, storage::market_data_dto_to_storage_record,
};
use fdc_storage::{RecordingStorageSink, StorageAccessPatternHint, StorageDurabilityHint};
use fdc_transform::{MarketDataKind, MarketDataPayload, TradeSide as DtoTradeSide};
use rust_decimal::Decimal;

fn sample_trade_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("trade-1".to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_envelope() -> BarterIngestionEnvelope {
    let mut envelope =
        BarterIngestionEnvelope::from_event("barter:binance_spot", sample_trade_event());
    envelope.envelope_id = "env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: true,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

fn sample_envelope_with_quality(quality: DataQualityFlags) -> BarterIngestionEnvelope {
    let mut envelope = sample_envelope();
    envelope.quality = quality;
    envelope
}

fn sample_candle_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Candle,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Candle(CandlePayload {
            interval: Some("1m".to_string()),
            open_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            close_time: TimestampNs::from_nanos(1_700_000_060_000_000_000),
            open: Price::new(Decimal::new(42_000_00, 2)),
            high: Price::new(Decimal::new(42_100_00, 2)),
            low: Price::new(Decimal::new(41_900_00, 2)),
            close: Price::new(Decimal::new(42_050_00, 2)),
            volume: Decimal::new(25, 1),
            trade_count: Some(100),
            quote_volume: Some(Decimal::new(1_000_000, 2)),
        }),
        sequence: Some("candle-seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_candle_envelope() -> BarterIngestionEnvelope {
    let mut envelope =
        BarterIngestionEnvelope::from_event("barter:binance_spot", sample_candle_event());
    envelope.envelope_id = "candle-env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

fn sample_funding_rate_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_futures_usd".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Perpetual,
        kind: BarterMarketDataKind::FundingRate,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::FundingRate(FundingRatePayload {
            funding_rate: Decimal::new(125, 6),
            funding_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            mark_price: Some(Price::new(Decimal::new(42_000_00, 2))),
        }),
        sequence: Some("funding-seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_funding_rate_envelope() -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(
        "barter:binance_futures_usd",
        sample_funding_rate_event(),
    );
    envelope.envelope_id = "funding-env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

fn sample_open_interest_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_futures_usd".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Perpetual,
        kind: BarterMarketDataKind::OpenInterest,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_100),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_110),
        payload: BarterMarketPayload::OpenInterest(OpenInterestPayload {
            open_interest: Decimal::new(987_654_321, 3),
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_100),
        }),
        sequence: Some("open-interest-seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_mark_price_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_futures_usd".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Perpetual,
        kind: BarterMarketDataKind::MarkPrice,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_200),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_210),
        payload: BarterMarketPayload::MarkPrice(MarkPricePayload {
            mark_price: Price::new(Decimal::new(42_010_00, 2)),
            index_price: Some(Price::new(Decimal::new(42_000_00, 2))),
            estimated_settle_price: Some(Price::new(Decimal::new(42_020_00, 2))),
            funding_rate: Some(Decimal::new(126, 6)),
            next_funding_time: Some(TimestampNs::from_nanos(1_700_028_800_000_000_000)),
        }),
        sequence: Some("mark-price-seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_index_price_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_futures_usd".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Perpetual,
        kind: BarterMarketDataKind::IndexPrice,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_300),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_310),
        payload: BarterMarketPayload::IndexPrice(IndexPricePayload {
            index_price: Price::new(Decimal::new(42_000_00, 2)),
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_300),
        }),
        sequence: Some("index-price-seq-1".to_string()),
        checkpoint: None,
    }
}

fn historical_derivatives_envelope(
    source_id: &str,
    envelope_id: &str,
    event: BarterMarketEvent,
) -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(source_id, event);
    envelope.envelope_id = envelope_id.to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[test]
fn barter_envelope_maps_to_source_envelope() {
    let envelope = sample_envelope();

    let source: SourceEnvelope<BarterMarketEvent> =
        barter_envelope_to_source_envelope(envelope.clone());

    assert_eq!(source.envelope_id, "env-1");
    assert_eq!(source.source_id, "barter:binance_spot");
    assert_eq!(source.source_type, SourceType::MarketData);
    assert_eq!(source.sequence.as_deref(), Some("seq-1"));
    assert_eq!(source.event_time, envelope.event.timestamp);
    assert_eq!(source.received_at, envelope.event.received_at);
    assert_eq!(source.emitted_at, envelope.emitted_at);
    assert_eq!(source.payload, envelope.event);
    assert!(source.quality.is_duplicate_candidate);
    assert_eq!(source.metadata.adapter.as_deref(), Some("barter"));
    assert_eq!(source.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(source.metadata.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(source.metadata.kind.as_deref(), Some("trade"));
}

#[test]
fn barter_trade_event_maps_to_market_data_dto() {
    let source = barter_envelope_to_source_envelope(sample_envelope());

    let dto = barter_event_to_market_data_dto(&source).expect("trade should map to DTO");

    assert_eq!(dto.event_id, "env-1");
    assert_eq!(dto.source_id, "barter:binance_spot");
    assert_eq!(dto.adapter, "barter");
    assert_eq!(dto.exchange, "binance_spot");
    assert_eq!(dto.symbol.as_str(), "BTCUSDT");
    assert_eq!(dto.kind, MarketDataKind::Trade);
    assert_eq!(dto.source_sequence.as_deref(), Some("seq-1"));
    assert!(dto.quality.is_duplicate_candidate);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price, Price::new(Decimal::new(42_000_00, 2)));
            assert_eq!(trade.quantity, Decimal::new(125, 3));
            assert_eq!(trade.side, DtoTradeSide::Buy);
        }
        other => panic!("expected trade payload, got {other:?}"),
    }
}

#[test]
fn market_data_dto_maps_to_storage_write_record() {
    let source = barter_envelope_to_source_envelope(sample_envelope());
    let dto = barter_event_to_market_data_dto(&source).expect("trade should map to DTO");

    let record = market_data_dto_to_storage_record(&dto).expect("DTO should map to storage record");

    assert_eq!(record.namespace, "market_data");
    assert_eq!(record.collection, "trades");
    assert_eq!(
        record.key,
        b"barter:binance_spot:BTCUSDT:trade:1700000000000000001".to_vec()
    );
    assert_eq!(
        record.metadata.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(record.metadata.schema.as_deref(), Some("market_data.trade"));
    assert_eq!(
        record.metadata.source.as_deref(),
        Some("barter:binance_spot")
    );
    assert_eq!(
        record.placement.access_pattern,
        StorageAccessPatternHint::Hot
    );
    assert_eq!(
        record.placement.durability,
        StorageDurabilityHint::Persistent
    );
    assert_eq!(
        record.placement.shard_key.as_deref(),
        Some(&b"barter:binance_spot:BTCUSDT"[..])
    );
    assert_eq!(
        record.metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(
        record.metadata.tags.get("data.kind").map(String::as_str),
        Some("event")
    );
    assert_eq!(
        record.metadata.tags.get("record.kind").map(String::as_str),
        Some("trade")
    );
    assert_eq!(record.metadata.tags.get("quality.is_replay"), None);

    let json: serde_json::Value =
        serde_json::from_slice(&record.value).expect("record value should be JSON");
    assert_eq!(json["kind"], "Trade");
    assert_eq!(json["symbol"], "BTCUSDT");
}

#[test]
fn market_data_dto_quality_maps_to_generic_storage_tags() {
    let backfill_source =
        barter_envelope_to_source_envelope(sample_envelope_with_quality(DataQualityFlags {
            is_replay: false,
            is_backfill: true,
            is_duplicate_candidate: false,
            has_gap_before: false,
            is_out_of_order: false,
        }));
    let backfill_dto = barter_event_to_market_data_dto(&backfill_source)
        .expect("backfill trade should map to DTO");
    let backfill_record = market_data_dto_to_storage_record(&backfill_dto)
        .expect("backfill DTO should map to storage record");

    assert_eq!(
        backfill_record
            .metadata
            .tags
            .get("mode")
            .map(String::as_str),
        Some("backfill")
    );

    let replay_source =
        barter_envelope_to_source_envelope(sample_envelope_with_quality(DataQualityFlags {
            is_replay: true,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: true,
            is_out_of_order: true,
        }));
    let replay_dto =
        barter_event_to_market_data_dto(&replay_source).expect("replay trade should map to DTO");
    let replay_record = market_data_dto_to_storage_record(&replay_dto)
        .expect("replay DTO should map to storage record");

    assert_eq!(
        replay_record.metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_replay")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_duplicate_candidate")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.has_gap_before")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_out_of_order")
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn market_data_candle_maps_to_generic_aggregate_storage_tags() {
    let source = barter_envelope_to_source_envelope(sample_candle_envelope());
    let dto = barter_event_to_market_data_dto(&source).expect("candle should map to DTO");
    let record = market_data_dto_to_storage_record(&dto).expect("candle DTO should map");

    assert_eq!(dto.kind, MarketDataKind::Candle);
    assert_eq!(record.collection, "candles");
    assert_eq!(
        record.metadata.tags.get("mode").map(String::as_str),
        Some("backfill")
    );
    assert_eq!(
        record.metadata.tags.get("data.kind").map(String::as_str),
        Some("aggregate")
    );
    assert_eq!(
        record.metadata.tags.get("record.kind").map(String::as_str),
        Some("candle")
    );
}

#[test]
fn derivatives_payload_maps_to_structured_market_data() {
    let source = barter_envelope_to_source_envelope(sample_funding_rate_envelope());

    let dto = barter_event_to_market_data_dto(&source).expect("funding rate should map to DTO");

    assert_eq!(dto.kind, MarketDataKind::FundingRate);
    assert_eq!(dto.exchange, "binance_futures_usd");
    assert_eq!(dto.symbol.as_str(), "BTCUSDT");
    assert!(dto.quality.is_backfill);
    match dto.payload {
        MarketDataPayload::FundingRate(funding) => {
            assert_eq!(funding.funding_rate, Decimal::new(125, 6));
            assert_eq!(
                funding.mark_price,
                Some(Price::new(Decimal::new(42_000_00, 2)))
            );
        }
        other => panic!("expected funding rate payload, got {other:?}"),
    }
}

#[test]
fn derivatives_payloads_map_to_structured_market_data_and_storage() {
    let cases = [
        (
            historical_derivatives_envelope(
                "barter:binance_futures_usd",
                "funding-env-structured",
                sample_funding_rate_event(),
            ),
            MarketDataKind::FundingRate,
            "funding_rates",
            "funding_rate",
        ),
        (
            historical_derivatives_envelope(
                "barter:binance_futures_usd",
                "open-interest-env-structured",
                sample_open_interest_event(),
            ),
            MarketDataKind::OpenInterest,
            "open_interest",
            "open_interest",
        ),
        (
            historical_derivatives_envelope(
                "barter:binance_futures_usd",
                "mark-price-env-structured",
                sample_mark_price_event(),
            ),
            MarketDataKind::MarkPrice,
            "mark_prices",
            "mark_price",
        ),
        (
            historical_derivatives_envelope(
                "barter:binance_futures_usd",
                "index-price-env-structured",
                sample_index_price_event(),
            ),
            MarketDataKind::IndexPrice,
            "index_prices",
            "index_price",
        ),
    ];

    for (envelope, expected_kind, expected_collection, expected_record_kind) in cases {
        let source = barter_envelope_to_source_envelope(envelope);
        let dto = barter_event_to_market_data_dto(&source).expect("derivative should map to DTO");
        let record = market_data_dto_to_storage_record(&dto).expect("derivative DTO should map");

        assert_eq!(dto.kind, expected_kind);
        assert_eq!(record.collection, expected_collection);
        assert_eq!(
            record.metadata.schema.as_deref(),
            Some(format!("market_data.{expected_record_kind}").as_str())
        );
        assert_eq!(
            record.metadata.tags.get("data.kind").map(String::as_str),
            Some("derivative")
        );
        assert_eq!(
            record.metadata.tags.get("record.kind").map(String::as_str),
            Some(expected_record_kind)
        );
        assert_eq!(
            record.placement.durability,
            StorageDurabilityHint::Persistent
        );

        match (expected_kind, dto.payload) {
            (MarketDataKind::FundingRate, MarketDataPayload::FundingRate(payload)) => {
                assert_eq!(payload.funding_rate, Decimal::new(125, 6));
                assert_eq!(
                    payload.mark_price,
                    Some(Price::new(Decimal::new(42_000_00, 2)))
                );
            }
            (MarketDataKind::OpenInterest, MarketDataPayload::OpenInterest(payload)) => {
                assert_eq!(payload.open_interest, Decimal::new(987_654_321, 3));
            }
            (MarketDataKind::MarkPrice, MarketDataPayload::MarkPrice(payload)) => {
                assert_eq!(payload.mark_price, Price::new(Decimal::new(42_010_00, 2)));
                assert_eq!(payload.funding_rate, Some(Decimal::new(126, 6)));
            }
            (MarketDataKind::IndexPrice, MarketDataPayload::IndexPrice(payload)) => {
                assert_eq!(payload.index_price, Price::new(Decimal::new(42_000_00, 2)));
            }
            (_, other) => panic!("expected structured derivative payload, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn finite_barter_fixture_flows_into_recording_storage_sink() {
    let sink = RecordingStorageSink::new();

    let result = run_barter_envelopes_to_storage_once(vec![sample_envelope()], &sink)
        .await
        .expect("finite fixture should write");

    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.source_valid, 1);
    assert_eq!(result.source_invalid, 0);
    assert_eq!(result.dto_mapped, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[test]
fn dependency_guard_core_crates_do_not_reference_orchestrator() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-adapter/barter/Cargo.toml"),
        workspace_root.join("crates/fdc-adapter/barter/src"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/src"),
        workspace_root.join("crates/fdc-transform/Cargo.toml"),
        workspace_root.join("crates/fdc-transform/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(
            &path,
            &["fdc-orchestrator", "fdc_orchestrator"],
            &mut violations,
        );
    }

    assert!(
        violations.is_empty(),
        "core crates must not depend on orchestrator: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-orchestrator should live two levels under workspace root")
        .to_path_buf()
}

fn collect_forbidden_references(path: &Path, forbidden: &[&str], violations: &mut Vec<String>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("failed to read directory") {
            collect_forbidden_references(
                &entry.expect("failed to read entry").path(),
                forbidden,
                violations,
            );
        }
        return;
    }

    if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
        && path.file_name().and_then(|file_name| file_name.to_str()) != Some("Cargo.toml")
    {
        return;
    }

    let content = std::fs::read_to_string(path).expect("failed to read dependency guard file");
    for needle in forbidden {
        if content.contains(needle) {
            violations.push(format!("{} contains {needle}", path.display()));
        }
    }
}
