use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use fdc_storage::{MarketDataQuery, StorageWriteRecord};
use serde::{Deserialize, Serialize};

use crate::{ApiAppState, ApiResponse};

const DEFAULT_TRADE_LIMIT: usize = 100;

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct MarketDataTradeQueryParams {
    pub symbol: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataTradeRecord {
    pub key: String,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub source: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataTradesResponse {
    pub records: Vec<MarketDataTradeRecord>,
    pub returned_records: usize,
}

pub fn query_market_data_trades(
    state: &ApiAppState,
    params: MarketDataTradeQueryParams,
) -> ApiResponse<MarketDataTradesResponse> {
    let mut query =
        MarketDataQuery::for_trades().with_limit(params.limit.unwrap_or(DEFAULT_TRADE_LIMIT));

    if let Some(symbol) = params.symbol {
        query = query.with_symbol(symbol);
    }

    let records: Vec<MarketDataTradeRecord> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(storage_record_to_trade_record)
        .collect();

    ApiResponse::success(MarketDataTradesResponse {
        returned_records: records.len(),
        records,
    })
}

pub fn build_market_data_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/market-data/trades", get(market_data_trades_handler))
        .with_state(state)
}

async fn market_data_trades_handler(
    State(state): State<ApiAppState>,
    Query(params): Query<MarketDataTradeQueryParams>,
) -> Json<ApiResponse<MarketDataTradesResponse>> {
    Json(query_market_data_trades(&state, params))
}

fn storage_record_to_trade_record(record: StorageWriteRecord) -> MarketDataTradeRecord {
    let payload = serde_json::from_slice(&record.value).unwrap_or(serde_json::Value::Null);

    MarketDataTradeRecord {
        key: String::from_utf8_lossy(&record.key).to_string(),
        symbol: record.metadata.tags.get("symbol").cloned(),
        kind: record.metadata.tags.get("kind").cloned(),
        source: record.metadata.source,
        payload,
    }
}
