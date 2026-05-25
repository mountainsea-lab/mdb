use std::{fs, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, IntoMarketDataDto, IntoSourceEnvelope, TradePayload,
    TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourceValidator,
};
use fdc_transform::{
    MarketDataKind, MarketDataPayload, MarketDataTransformSink, RecordingMarketDataSink,
    TradeSide as TransformTradeSide,
};
use rust_decimal::Decimal;

fn live_trade_envelope() -> BarterIngestionEnvelope {
    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-1".to_string(),
        source_id: "barter-binance-spot-live-trades".to_string(),
        emitted_at: TimestampNs::from_nanos(1_700_000_000_000_002_000),
        event: BarterMarketEvent {
            source: "barter-rs".to_string(),
            mode: BarterMarketDataMode::Live,
            exchange: "binance_spot".to_string(),
            symbol: Symbol::new("BTCUSDT"),
            kind: BarterMarketDataKind::Trade,
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000),
            payload: BarterMarketPayload::Trade(TradePayload {
                trade_id: Some("trade-1".to_string()),
                price: Price::from_f64(65_000.25).unwrap(),
                quantity: Decimal::new(5, 1),
                side: Some(TradeSide::Buy),
            }),
            sequence: Some("seq-42".to_string()),
            checkpoint: None,
        },
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

#[test]
fn barter_live_trade_envelope_maps_to_neutral_market_data_dto() {
    let dto = live_trade_envelope()
        .into_market_data_dto()
        .expect("live trade should map to neutral DTO");

    assert_eq!(dto.event_id, "barter-envelope-1");
    assert_eq!(dto.source_id, "barter-binance-spot-live-trades");
    assert_eq!(dto.adapter, "barter-rs");
    assert_eq!(dto.exchange, "binance_spot");
    assert_eq!(dto.symbol.to_string(), "BTCUSDT");
    assert_eq!(dto.kind, MarketDataKind::Trade);
    assert_eq!(dto.event_time.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(dto.received_at.as_nanos(), 1_700_000_000_000_001_000);
    assert_eq!(dto.emitted_at.as_nanos(), 1_700_000_000_000_002_000);
    assert_eq!(dto.source_sequence.as_deref(), Some("seq-42"));
    assert_eq!(dto.ingestion_sequence.as_deref(), Some("barter-envelope-1"));
    assert!(dto.quality.is_duplicate_candidate);
    assert!(dto.quality.has_gap_before);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price.to_f64(), 65_000.25);
            assert_eq!(trade.quantity.to_string(), "0.5");
            assert_eq!(trade.side, TransformTradeSide::Buy);
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
}

struct TransformForwardingSink {
    transform_sink: Arc<RecordingMarketDataSink>,
}

#[async_trait]
impl SourceBatchSink<BarterMarketEvent> for TransformForwardingSink {
    async fn write_batch(
        &self,
        items: Vec<SourceBatchItem<BarterMarketEvent>>,
    ) -> fdc_core::error::Result<usize> {
        let mut dtos = Vec::with_capacity(items.len());
        for item in items {
            let envelope = BarterIngestionEnvelope {
                envelope_id: item.envelope.envelope_id,
                source_id: item.envelope.source_id,
                emitted_at: item.envelope.emitted_at,
                event: item.envelope.payload,
                checkpoint: None,
                quality: DataQualityFlags {
                    is_replay: item.envelope.quality.is_replay,
                    is_backfill: item.envelope.quality.is_backfill,
                    is_duplicate_candidate: item.envelope.quality.is_duplicate_candidate,
                    has_gap_before: item.envelope.quality.has_gap_before,
                    is_out_of_order: item.envelope.quality.is_out_of_order,
                },
            };
            let dto = envelope
                .into_market_data_dto()
                .map_err(|error| fdc_core::error::Error::validation(error.to_string()))?;
            dtos.push(dto);
        }
        let result = self.transform_sink.write_market_data_batch(dtos).await?;
        Ok(result.accepted_count)
    }
}

#[tokio::test]
async fn validated_barter_source_batch_can_forward_to_transform_sink() {
    let transform_sink = Arc::new(RecordingMarketDataSink::default());
    let forwarding_sink = Arc::new(TransformForwardingSink {
        transform_sink: transform_sink.clone(),
    });
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        forwarding_sink,
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        live_trade_envelope().into_source_envelope(),
        live_trade_envelope().into_source_envelope(),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .expect("validated source pipeline should forward to transform sink");

    assert_eq!(result.input_count, 2);
    assert_eq!(result.validation_success_count, 2);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(transform_sink.written_count().await, 2);
    assert_eq!(transform_sink.snapshot().await[0].exchange, "binance_spot");
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter_or_fdc_transform_after_b4() {
    let adapter_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = adapter_manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .expect("fdc-barter should live under crates/fdc-adapter/barter");

    let files_to_scan = [
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/src/lib.rs"),
    ];
    let ingestion_src_dir = workspace_root.join("crates/fdc-ingestion/src/source");

    let mut haystack = String::new();
    for file in files_to_scan {
        haystack.push_str(&fs::read_to_string(&file).expect("manifest/source should be readable"));
    }
    for entry in
        fs::read_dir(ingestion_src_dir).expect("fdc-ingestion source dir should be readable")
    {
        let entry = entry.expect("dir entry should be readable");
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            haystack.push_str(&fs::read_to_string(path).expect("source file should be readable"));
        }
    }

    assert!(!haystack.contains("fdc-barter"));
    assert!(!haystack.contains("fdc_barter"));
    assert!(!haystack.contains("fdc-transform"));
    assert!(!haystack.contains("fdc_transform"));
}
