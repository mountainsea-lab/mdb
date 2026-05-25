use std::sync::Arc;

use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_transform::{
    MarketDataDto, MarketDataKind, MarketDataPayload, MarketDataTransformSink,
    RecordingMarketDataSink, TradeDto, TradeSide, TransformQualityFlags,
};
use rust_decimal::Decimal;

fn trade_dto() -> MarketDataDto {
    MarketDataDto {
        event_id: "barter-envelope-1".to_string(),
        source_id: "barter-binance-spot-live-trades".to_string(),
        adapter: "barter-rs".to_string(),
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: MarketDataKind::Trade,
        event_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000),
        emitted_at: TimestampNs::from_nanos(1_700_000_000_000_002_000),
        source_sequence: Some("seq-42".to_string()),
        ingestion_sequence: Some("ingest-7".to_string()),
        quality: TransformQualityFlags {
            is_replay: false,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: false,
            is_out_of_order: false,
        },
        payload: MarketDataPayload::Trade(TradeDto {
            trade_id: Some("trade-1".to_string()),
            price: Price::from_f64(65_000.25).unwrap(),
            quantity: Decimal::new(5, 1),
            side: TradeSide::Buy,
        }),
    }
}

#[test]
fn trade_market_data_dto_preserves_identity_timing_payload_and_quality() {
    let dto = trade_dto();

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
    assert_eq!(dto.ingestion_sequence.as_deref(), Some("ingest-7"));
    assert!(dto.quality.is_duplicate_candidate);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price.to_f64(), 65_000.25);
            assert_eq!(trade.quantity.to_string(), "0.5");
            assert_eq!(trade.side, TradeSide::Buy);
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
}

#[tokio::test]
async fn recording_market_data_sink_accepts_and_records_bounded_batches() {
    let sink = Arc::new(RecordingMarketDataSink::default());
    let result = sink
        .write_market_data_batch(vec![trade_dto(), trade_dto()])
        .await
        .expect("recording sink should accept valid DTOs");

    assert_eq!(result.accepted_count, 2);
    assert_eq!(result.rejected_count, 0);
    assert_eq!(sink.written_count().await, 2);
    assert_eq!(sink.snapshot().await[0].symbol.to_string(), "BTCUSDT");
}
