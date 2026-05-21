use std::collections::BTreeMap;

use fdc_ingestion::{
    SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition,
    SourceQualityFlags, SourceType,
};

use crate::{
    ingestion::{BarterIngestionEnvelope, DataQualityFlags},
    model::{BarterCheckpoint, BarterMarketDataMode, BarterMarketEvent},
};

pub trait IntoSourceEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent>;
}

impl IntoSourceEnvelope for BarterIngestionEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent> {
        let BarterIngestionEnvelope {
            envelope_id,
            source_id,
            emitted_at,
            event,
            checkpoint,
            quality,
        } = self;

        let source_type = match event.mode {
            BarterMarketDataMode::Live => SourceType::MarketData,
            BarterMarketDataMode::Historical => SourceType::Replay,
        };
        let source_quality = map_quality(quality, event.mode);
        let metadata = metadata_for_event(&event);
        let event_time = event.timestamp;
        let received_at = event.received_at;
        let sequence = event.sequence.clone();
        let checkpoint = checkpoint.map(map_checkpoint);

        SourceEnvelope {
            envelope_id,
            source_id,
            source_type,
            sequence,
            event_time,
            received_at,
            emitted_at,
            payload: event,
            checkpoint,
            quality: source_quality,
            metadata,
        }
    }
}

fn map_quality(quality: DataQualityFlags, mode: BarterMarketDataMode) -> SourceQualityFlags {
    SourceQualityFlags {
        is_replay: quality.is_replay,
        is_backfill: quality.is_backfill || matches!(mode, BarterMarketDataMode::Historical),
        is_duplicate_candidate: quality.is_duplicate_candidate,
        has_gap_before: quality.has_gap_before,
        is_out_of_order: quality.is_out_of_order,
    }
}

fn metadata_for_event(event: &BarterMarketEvent) -> SourceMetadata {
    let mut attributes = BTreeMap::new();
    attributes.insert("mode".to_string(), format!("{:?}", event.mode));
    attributes.insert(
        "payload_kind".to_string(),
        format!("{:?}", event.payload.kind()),
    );

    SourceMetadata {
        adapter: Some(event.source.clone()),
        exchange: Some(event.exchange.clone()),
        symbol: Some(event.symbol.to_string()),
        kind: Some(format!("{:?}", event.kind)),
        attributes,
    }
}

fn map_checkpoint(checkpoint: BarterCheckpoint) -> SourceCheckpoint {
    let checkpoint_id = format!(
        "{}:{}:{}:{:?}:{:?}:{}",
        checkpoint.source_id,
        checkpoint.exchange,
        checkpoint.symbol,
        checkpoint.kind,
        checkpoint.mode,
        checkpoint.last_event_time.as_nanos()
    );

    let position = checkpoint
        .cursor
        .as_ref()
        .and_then(|cursor| {
            cursor
                .page_token
                .clone()
                .map(SourcePosition::PageToken)
                .or_else(|| cursor.next_start.map(SourcePosition::Timestamp))
                .or_else(|| {
                    cursor
                        .last_seen_exchange_id
                        .clone()
                        .map(SourcePosition::Sequence)
                })
        })
        .unwrap_or(SourcePosition::Timestamp(checkpoint.last_event_time));

    SourceCheckpoint {
        checkpoint_id,
        source_id: checkpoint.source_id,
        partition: SourcePartition {
            exchange: Some(checkpoint.exchange),
            symbol: Some(checkpoint.symbol),
            kind: Some(format!("{:?}", checkpoint.kind)),
            shard: None,
        },
        position,
        updated_at: checkpoint.updated_at,
    }
}
