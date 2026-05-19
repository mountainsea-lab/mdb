use fdc_barter::{BarterCheckpoint, BarterMarketDataKind, HistoricalCursor, HistoricalPageRequest};
use fdc_core::types::TimestampNs;

#[test]
fn historical_checkpoint_captures_resume_cursor() {
    let request = HistoricalPageRequest::new(
        "barter-binance-history",
        "binance_spot",
        "BTCUSDT",
        BarterMarketDataKind::Candle,
        TimestampNs::from_nanos(1_000),
        Some(TimestampNs::from_nanos(2_000)),
        Some(1000),
    );

    let cursor = HistoricalCursor::next_start(
        "binance_spot",
        "BTCUSDT",
        BarterMarketDataKind::Candle,
        TimestampNs::from_nanos(1_500),
    );

    let checkpoint = BarterCheckpoint::from_historical_page(
        &request,
        TimestampNs::from_nanos(1_499),
        cursor.clone(),
    );

    assert_eq!(checkpoint.source_id, "barter-binance-history");
    assert_eq!(checkpoint.exchange, "binance_spot");
    assert_eq!(checkpoint.symbol, "BTCUSDT");
    assert_eq!(checkpoint.kind, BarterMarketDataKind::Candle);
    assert_eq!(checkpoint.cursor, Some(cursor));
}
