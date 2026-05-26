use std::path::{Path, PathBuf};

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
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

    let json: serde_json::Value =
        serde_json::from_slice(&record.value).expect("record value should be JSON");
    assert_eq!(json["kind"], "Trade");
    assert_eq!(json["symbol"], "BTCUSDT");
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
