use fdc_core::{error::Error, Result};
use std::sync::Mutex;

use crate::market_data::model::{
    LiveMarketDataStatusResponse, MarketDataLiveState, StartLiveMarketDataResponse,
};

#[derive(Debug)]
pub struct MarketDataSupervisor {
    inner: Mutex<MarketDataSupervisorInner>,
}

#[derive(Debug, Clone)]
struct MarketDataSupervisorInner {
    state: MarketDataLiveState,
    last_result: Option<StartLiveMarketDataResponse>,
    failure_message: Option<String>,
}

impl MarketDataSupervisor {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MarketDataSupervisorInner {
                state: MarketDataLiveState::Idle,
                last_result: None,
                failure_message: None,
            }),
        }
    }

    pub fn try_start(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        match inner.state {
            MarketDataLiveState::Idle
            | MarketDataLiveState::Completed
            | MarketDataLiveState::Failed
            | MarketDataLiveState::Stopped => {
                inner.state = MarketDataLiveState::Starting;
                inner.failure_message = None;
                Ok(())
            }
            MarketDataLiveState::Starting => Err(Error::validation(
                "market-data live runner is already starting",
            )),
            MarketDataLiveState::Running => Err(Error::validation(
                "market-data live runner is already running",
            )),
            MarketDataLiveState::Stopping => Err(Error::validation(
                "market-data live runner is already stopping",
            )),
        }
    }

    pub fn complete(&self, result: StartLiveMarketDataResponse) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.state = MarketDataLiveState::Completed;
        inner.last_result = Some(result);
        inner.failure_message = None;
    }

    pub fn fail(&self, message: impl Into<String>) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.state = MarketDataLiveState::Failed;
        inner.failure_message = Some(message.into());
    }

    pub fn status(&self) -> LiveMarketDataStatusResponse {
        let inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        LiveMarketDataStatusResponse {
            state: inner.state,
            last_result: inner.last_result.clone(),
            failure_message: inner.failure_message.clone(),
        }
    }
}

impl Default for MarketDataSupervisor {
    fn default() -> Self {
        Self::new()
    }
}
