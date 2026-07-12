use fdc_barter::{
    BarterCheckpoint, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketEvent,
    DataQualityFlags,
};
use fdc_ingestion::{
    SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition,
    SourceQualityFlags, SourceType,
};

pub fn barter_envelope_to_source_envelope(
    envelope: BarterIngestionEnvelope,
) -> SourceEnvelope<BarterMarketEvent> {
    let event = envelope.event.clone();
    let sequence = event.sequence.clone();
    let checkpoint = envelope
        .checkpoint
        .as_ref()
        .map(barter_checkpoint_to_source_checkpoint);

    let source = SourceEnvelope::new(
        envelope.source_id.clone(),
        SourceType::MarketData,
        event.timestamp,
        event.received_at,
        envelope.event,
    )
    .with_optional_checkpoint(checkpoint)
    .with_quality(data_quality_to_source_quality(envelope.quality))
    .with_metadata(SourceMetadata {
        adapter: Some("barter".to_string()),
        exchange: Some(event.exchange.clone()),
        symbol: Some(event.symbol.as_str().to_string()),
        kind: Some(barter_kind_label(event.kind).to_string()),
        attributes: Default::default(),
    });

    let mut source = if let Some(sequence) = sequence {
        source.with_sequence(sequence)
    } else {
        source
    };

    source.envelope_id = envelope.envelope_id;
    source.emitted_at = envelope.emitted_at;
    source
}

pub fn data_quality_to_source_quality(quality: DataQualityFlags) -> SourceQualityFlags {
    SourceQualityFlags {
        is_replay: quality.is_replay,
        is_backfill: quality.is_backfill,
        is_duplicate_candidate: quality.is_duplicate_candidate,
        has_gap_before: quality.has_gap_before,
        is_out_of_order: quality.is_out_of_order,
    }
}

pub fn barter_kind_label(kind: BarterMarketDataKind) -> &'static str {
    match kind {
        BarterMarketDataKind::Trade => "trade",
        BarterMarketDataKind::OrderBookL1 => "order_book_l1",
        BarterMarketDataKind::OrderBook => "order_book",
        BarterMarketDataKind::Candle => "candle",
        BarterMarketDataKind::Liquidation => "liquidation",
        BarterMarketDataKind::FundingRate => "funding_rate",
        BarterMarketDataKind::OpenInterest => "open_interest",
        BarterMarketDataKind::MarkPrice => "mark_price",
        BarterMarketDataKind::IndexPrice => "index_price",
    }
}

fn barter_checkpoint_to_source_checkpoint(checkpoint: &BarterCheckpoint) -> SourceCheckpoint {
    SourceCheckpoint {
        checkpoint_id: format!(
            "{}:{}:{}:{}",
            checkpoint.source_id,
            checkpoint.exchange,
            checkpoint.symbol,
            checkpoint.last_event_time.as_nanos()
        ),
        source_id: checkpoint.source_id.clone(),
        partition: SourcePartition {
            exchange: Some(checkpoint.exchange.clone()),
            symbol: Some(checkpoint.symbol.clone()),
            kind: Some(barter_kind_label(checkpoint.kind).to_string()),
            shard: None,
        },
        position: checkpoint
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.page_token.clone())
            .map(SourcePosition::PageToken)
            .unwrap_or(SourcePosition::Timestamp(checkpoint.last_event_time)),
        updated_at: checkpoint.updated_at,
    }
}
