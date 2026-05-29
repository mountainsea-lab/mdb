use crate::market_data::model::{
    LiveMarketDataStatusResponse, MarketDataLiveState, StartLiveMarketDataResponse,
};

#[derive(Debug, Clone)]
pub struct MarketDataSupervisor {
    state: MarketDataLiveState,
    last_result: Option<StartLiveMarketDataResponse>,
    failure_message: Option<String>,
}

impl MarketDataSupervisor {
    pub fn new() -> Self {
        Self {
            state: MarketDataLiveState::Idle,
            last_result: None,
            failure_message: None,
        }
    }

    pub fn status(&self) -> LiveMarketDataStatusResponse {
        LiveMarketDataStatusResponse {
            state: self.state,
            last_result: self.last_result.clone(),
            failure_message: self.failure_message.clone(),
        }
    }
}

impl Default for MarketDataSupervisor {
    fn default() -> Self {
        Self::new()
    }
}
