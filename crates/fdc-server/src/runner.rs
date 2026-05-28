use std::sync::Arc;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::{error::Error, Result};
use fdc_storage::QueryableMarketDataStore;

use crate::{run_barter_fixture_mvp_once, BoundedMarketDataMvpResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedRunnerState {
    Created,
    Running,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedRunnerFailure {
    pub message: String,
}

#[derive(Clone)]
pub struct BoundedMarketDataRunnerHandle {
    market_data_store: Arc<QueryableMarketDataStore>,
    state: BoundedRunnerState,
    last_result: Option<BoundedMarketDataMvpResult>,
    failure: Option<BoundedRunnerFailure>,
}

impl BoundedMarketDataRunnerHandle {
    pub fn new(market_data_store: Arc<QueryableMarketDataStore>) -> Self {
        Self {
            market_data_store,
            state: BoundedRunnerState::Created,
            last_result: None,
            failure: None,
        }
    }

    pub fn state(&self) -> BoundedRunnerState {
        self.state
    }

    pub fn last_result(&self) -> Option<&BoundedMarketDataMvpResult> {
        self.last_result.as_ref()
    }

    pub fn failure(&self) -> Option<&BoundedRunnerFailure> {
        self.failure.as_ref()
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }

    pub fn cancel(&mut self) -> Result<()> {
        match self.state {
            BoundedRunnerState::Created | BoundedRunnerState::Cancelled => {
                self.state = BoundedRunnerState::Cancelled;
                Ok(())
            }
            BoundedRunnerState::Running => Err(Error::validation(
                "bounded market-data runner cannot cancel while running",
            )),
            BoundedRunnerState::Completed => Err(Error::validation(
                "bounded market-data runner cannot cancel after completed",
            )),
            BoundedRunnerState::Failed => Err(Error::validation(
                "bounded market-data runner cannot cancel after failed",
            )),
        }
    }

    pub async fn start_once(
        &mut self,
        envelopes: Vec<BarterIngestionEnvelope>,
    ) -> Result<BoundedMarketDataMvpResult> {
        match self.state {
            BoundedRunnerState::Created => {}
            BoundedRunnerState::Running => {
                return Err(Error::validation(
                    "bounded market-data runner is already running",
                ));
            }
            BoundedRunnerState::Completed => {
                return Err(Error::validation(
                    "bounded market-data runner already completed",
                ));
            }
            BoundedRunnerState::Cancelled => {
                return Err(Error::validation(
                    "bounded market-data runner was cancelled",
                ));
            }
            BoundedRunnerState::Failed => {
                return Err(Error::validation(
                    "bounded market-data runner already failed",
                ));
            }
        }

        self.state = BoundedRunnerState::Running;
        self.failure = None;

        match run_barter_fixture_mvp_once(envelopes, Arc::clone(&self.market_data_store)).await {
            Ok(result) => {
                self.state = BoundedRunnerState::Completed;
                self.last_result = Some(result.clone());
                Ok(result)
            }
            Err(error) => {
                self.state = BoundedRunnerState::Failed;
                self.failure = Some(BoundedRunnerFailure {
                    message: error.to_string(),
                });
                Err(error)
            }
        }
    }
}
