use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, TradePayload, TradeSide,
};
use fdc_core::{
    types::{Price, Symbol, TimestampNs},
    Result,
};
use fdc_storage::{MarketDataQuery, StorageWriteRecord};
use rust_decimal::Decimal;

use crate::{
    market_data::model::{
        MarketDataLiveState, MarketDataTradeRecord, MarketDataTradesResponse,
        StartLiveMarketDataResponse,
    },
    run_realtime_barter_envelope_stream, ProductionServerState, RealtimeMarketDataMvpConfig,
};

pub fn live_status(
    state: &ProductionServerState,
) -> crate::market_data::model::LiveMarketDataStatusResponse {
    state.market_data_supervisor().status()
}

pub fn start_live_disabled(state: &ProductionServerState) -> (StartLiveMarketDataResponse, String) {
    let status = state.market_data_supervisor().status();
    (
        StartLiveMarketDataResponse {
            state: status.state,
            envelopes_received: 0,
            storage_records_written: 0,
            market_data_store_records: state.market_data_store().record_count(),
        },
        "live market-data acquisition requires FDC_LIVE_ENABLED=1".to_string(),
    )
}

pub fn query_trades(
    state: &ProductionServerState,
    symbol: Option<String>,
    limit: Option<usize>,
) -> MarketDataTradesResponse {
    let mut query = MarketDataQuery::for_trades();
    if let Some(symbol) = symbol {
        query = query.with_symbol(symbol);
    }
    if let Some(limit) = limit {
        query = query.with_limit(limit);
    }

    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_trade_record)
        .collect();

    MarketDataTradesResponse {
        returned_records: records.len(),
        records,
    }
}

pub async fn ingest_test_trade(
    state: &ProductionServerState,
    symbol: &str,
    trade_id: &str,
) -> Result<StartLiveMarketDataResponse> {
    let envelope = test_trade_envelope(symbol, trade_id);
    let summary = run_realtime_barter_envelope_stream(
        futures::stream::iter(vec![envelope]),
        state.market_data_store(),
        RealtimeMarketDataMvpConfig::default(),
    )
    .await?;

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}

fn record_to_trade_record(record: StorageWriteRecord) -> MarketDataTradeRecord {
    let payload = serde_json::from_slice(&record.value).unwrap_or_else(|_| serde_json::Value::Null);
    MarketDataTradeRecord {
        key: String::from_utf8_lossy(&record.key).to_string(),
        symbol: record.metadata.tags.get("symbol").cloned(),
        kind: record.metadata.tags.get("kind").cloned(),
        source: record.metadata.source.clone(),
        payload,
    }
}

fn test_trade_envelope(symbol: &str, trade_id: &str) -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::now(),
        received_at: TimestampNs::now(),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: Decimal::new(1, 0),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(format!("seq-{trade_id}")),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.quality = DataQualityFlags::default();
    envelope
}
