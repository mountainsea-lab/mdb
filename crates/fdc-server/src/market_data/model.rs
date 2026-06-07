use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataLiveState {
    Idle,
    Starting,
    Running,
    Completed,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveMarketDataRequest {
    pub timeout_secs: Option<u64>,
    pub max_envelopes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveMarketDataStatusResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub started_at_ns: Option<u64>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
    pub subscriptions: Vec<String>,
    pub last_record_at_ns: Option<u64>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
    pub last_result: Option<StartLiveMarketDataResponse>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageStatusResponse {
    pub backend: String,
    pub policy_profile: String,
    pub tiers: Vec<MarketDataStorageTierStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageTierStatus {
    pub tier: String,
    pub engine: String,
    pub durable_path_configured: bool,
    pub path_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataTradesResponse {
    pub returned_records: usize,
    pub records: Vec<MarketDataTradeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataTradeRecord {
    pub key: String,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub source: Option<String>,
    pub payload: Value,
}
