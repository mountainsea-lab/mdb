use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use fdc_barter::{
    BarterCheckpoint, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode,
    BarterMarketEvent, BarterMarketPayload, DataQualityFlags, HistoricalCursor, IntoSourceEnvelope,
    TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourcePosition, SourceType, SourceValidator,
};
use rust_decimal::Decimal;
use tokio::sync::RwLock;

fn trade_event(mode: BarterMarketDataMode) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_000),
        received_at: TimestampNs::from_nanos(1_100),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("trade-1".to_string()),
            price: Price::from_f64(65_000.25).unwrap(),
            quantity: Decimal::new(25, 1),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("seq-42".to_string()),
        checkpoint: None,
    }
}

fn live_envelope() -> BarterIngestionEnvelope {
    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-1".to_string(),
        source_id: "barter-live-binance-btcusdt".to_string(),
        emitted_at: TimestampNs::from_nanos(1_200),
        event: trade_event(BarterMarketDataMode::Live),
        checkpoint: None,
        quality: DataQualityFlags {
            is_replay: false,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: true,
            is_out_of_order: false,
        },
    }
}

fn checkpoint_with_cursor(cursor: HistoricalCursor) -> BarterCheckpoint {
    BarterCheckpoint {
        source_id: "barter-historical-binance-btcusdt".to_string(),
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        mode: BarterMarketDataMode::Historical,
        last_event_time: TimestampNs::from_nanos(2_000),
        cursor: Some(cursor),
        updated_at: TimestampNs::from_nanos(2_100),
    }
}

fn historical_envelope_with_checkpoint(checkpoint: BarterCheckpoint) -> BarterIngestionEnvelope {
    let mut event = trade_event(BarterMarketDataMode::Historical);
    event.checkpoint = Some(checkpoint.clone());

    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-historical-1".to_string(),
        source_id: "barter-historical-binance-btcusdt".to_string(),
        emitted_at: TimestampNs::from_nanos(2_200),
        event,
        checkpoint: Some(checkpoint),
        quality: DataQualityFlags::default(),
    }
}

#[test]
fn live_trade_envelope_maps_identity_timing_payload_quality_and_metadata() {
    let source = live_envelope();
    let converted = source.into_source_envelope();

    assert_eq!(converted.envelope_id, "barter-envelope-1");
    assert_eq!(converted.source_id, "barter-live-binance-btcusdt");
    assert_eq!(converted.source_type, SourceType::MarketData);
    assert_eq!(converted.sequence.as_deref(), Some("seq-42"));
    assert_eq!(converted.event_time, TimestampNs::from_nanos(1_000));
    assert_eq!(converted.received_at, TimestampNs::from_nanos(1_100));
    assert_eq!(converted.emitted_at, TimestampNs::from_nanos(1_200));

    assert_eq!(converted.payload.exchange, "binance_spot");
    assert_eq!(converted.payload.symbol.to_string(), "BTCUSDT");
    assert_eq!(converted.payload.kind, BarterMarketDataKind::Trade);

    assert!(converted.quality.is_duplicate_candidate);
    assert!(converted.quality.has_gap_before);
    assert!(!converted.quality.is_backfill);
    assert!(!converted.quality.is_replay);
    assert!(!converted.quality.is_out_of_order);

    assert_eq!(converted.metadata.adapter.as_deref(), Some("barter-rs"));
    assert_eq!(converted.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(converted.metadata.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(converted.metadata.kind.as_deref(), Some("Trade"));
    assert_eq!(
        converted
            .metadata
            .attributes
            .get("mode")
            .map(String::as_str),
        Some("Live")
    );
    assert_eq!(
        converted
            .metadata
            .attributes
            .get("payload_kind")
            .map(String::as_str),
        Some("Trade")
    );
}

#[test]
fn historical_envelope_maps_to_replay_backfill_source_semantics() {
    let cursor = HistoricalCursor {
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        next_start: Some(TimestampNs::from_nanos(3_000)),
        page_token: None,
        last_seen_exchange_id: None,
    };
    let source = historical_envelope_with_checkpoint(checkpoint_with_cursor(cursor));

    let converted = source.into_source_envelope();

    assert_eq!(converted.source_type, SourceType::Replay);
    assert!(converted.quality.is_backfill);
    assert_eq!(
        converted
            .metadata
            .attributes
            .get("mode")
            .map(String::as_str),
        Some("Historical")
    );
}

#[test]
fn barter_checkpoint_maps_to_source_checkpoint_partition_and_page_token_position() {
    let cursor = HistoricalCursor {
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        next_start: Some(TimestampNs::from_nanos(3_000)),
        page_token: Some("page-token-1".to_string()),
        last_seen_exchange_id: Some("trade-99".to_string()),
    };
    let source = historical_envelope_with_checkpoint(checkpoint_with_cursor(cursor));

    let converted = source.into_source_envelope();
    let checkpoint = converted.checkpoint.expect("checkpoint should map");

    assert_eq!(
        checkpoint.checkpoint_id,
        "barter-historical-binance-btcusdt:binance_spot:BTCUSDT:Trade:Historical:2000"
    );
    assert_eq!(checkpoint.source_id, "barter-historical-binance-btcusdt");
    assert_eq!(
        checkpoint.partition.exchange.as_deref(),
        Some("binance_spot")
    );
    assert_eq!(checkpoint.partition.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(checkpoint.partition.kind.as_deref(), Some("Trade"));
    assert_eq!(checkpoint.partition.shard, None);
    assert_eq!(
        checkpoint.position,
        SourcePosition::PageToken("page-token-1".to_string())
    );
    assert_eq!(checkpoint.updated_at, TimestampNs::from_nanos(2_100));
}

struct RecordingSourceBridgeSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> Default for RecordingSourceBridgeSink<T> {
    fn default() -> Self {
        Self {
            written: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> RecordingSourceBridgeSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourceBridgeSink<T>
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> fdc_core::error::Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}

#[tokio::test]
async fn bridged_barter_envelopes_run_through_bounded_source_pipeline() {
    let sink = Arc::new(RecordingSourceBridgeSink::<BarterMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        live_envelope().into_source_envelope(),
        live_envelope().into_source_envelope(),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 2);
    assert_eq!(result.validation_success_count, 2);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(result.success_count(), 2);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(result.batch_count(), 1);
    assert_eq!(sink.written_count().await, 2);
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("workspace root should be three levels above fdc-barter crate");
    let output = Command::new("grep")
        .args([
            "-R",
            "fdc-barter\\|fdc_barter",
            "-n",
            "crates/fdc-ingestion",
            "Cargo.toml",
            "crates/fdc-ingestion/Cargo.toml",
        ])
        .current_dir(workspace_root)
        .output()
        .expect("grep should run");

    assert!(
        !output.status.success(),
        "fdc-ingestion must not reference fdc-barter"
    );
}
