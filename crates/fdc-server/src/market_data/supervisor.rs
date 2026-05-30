use fdc_core::{error::Error, types::TimestampNs, Result};
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
    next_task_sequence: u64,
    task_id: Option<String>,
    started_at_ns: Option<u64>,
    stopped_at_ns: Option<u64>,
    stop_reason: Option<String>,
    subscriptions: Vec<String>,
    last_record_at_ns: Option<u64>,
    envelopes_received: usize,
    storage_records_written: usize,
    market_data_store_records: usize,
    stop_requested: bool,
    last_result: Option<StartLiveMarketDataResponse>,
    failure_message: Option<String>,
}

impl MarketDataSupervisor {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(MarketDataSupervisorInner {
                state: MarketDataLiveState::Idle,
                next_task_sequence: 0,
                task_id: None,
                started_at_ns: None,
                stopped_at_ns: None,
                stop_reason: None,
                subscriptions: Vec::new(),
                last_record_at_ns: None,
                envelopes_received: 0,
                storage_records_written: 0,
                market_data_store_records: 0,
                stop_requested: false,
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
                inner.stop_reason = None;
                inner.stop_requested = false;
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

    pub fn start_background(&self, subscriptions: Vec<String>) -> Result<String> {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        match inner.state {
            MarketDataLiveState::Idle
            | MarketDataLiveState::Completed
            | MarketDataLiveState::Failed
            | MarketDataLiveState::Stopped => {
                inner.next_task_sequence += 1;
                let task_id = format!("market-data-live-{}", inner.next_task_sequence);
                inner.state = MarketDataLiveState::Running;
                inner.task_id = Some(task_id.clone());
                inner.started_at_ns = Some(now_ns());
                inner.stopped_at_ns = None;
                inner.stop_reason = None;
                inner.subscriptions = subscriptions;
                inner.last_record_at_ns = None;
                inner.envelopes_received = 0;
                inner.storage_records_written = 0;
                inner.market_data_store_records = 0;
                inner.last_result = None;
                inner.failure_message = None;
                inner.stop_requested = false;
                Ok(task_id)
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
        inner.task_id = result.task_id.clone();
        inner.envelopes_received = result.envelopes_received;
        inner.storage_records_written = result.storage_records_written;
        inner.market_data_store_records = result.market_data_store_records;
        inner.last_result = Some(result);
        inner.failure_message = None;
        inner.stop_requested = false;
    }

    pub fn fail(&self, message: impl Into<String>) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.state = MarketDataLiveState::Failed;
        inner.failure_message = Some(message.into());
        inner.stop_requested = false;
    }

    pub fn record_progress(
        &self,
        envelopes: usize,
        storage_records: usize,
        store_records: usize,
        last_record_at_ns: Option<u64>,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.envelopes_received += envelopes;
        inner.storage_records_written += storage_records;
        inner.market_data_store_records = store_records;
        if last_record_at_ns.is_some() {
            inner.last_record_at_ns = last_record_at_ns;
        }
    }

    pub fn request_stop(&self, reason: impl Into<String>) -> bool {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        match inner.state {
            MarketDataLiveState::Running | MarketDataLiveState::Starting => {
                inner.state = MarketDataLiveState::Stopping;
                inner.stop_requested = true;
                inner.stop_reason = Some(reason.into());
                true
            }
            MarketDataLiveState::Stopping => true,
            _ => false,
        }
    }

    pub fn stop_requested(&self) -> bool {
        self.inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned")
            .stop_requested
    }

    pub fn stopped(&self, reason: impl Into<String>) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.state = MarketDataLiveState::Stopped;
        inner.stopped_at_ns = Some(now_ns());
        inner.stop_reason = Some(reason.into());
        inner.stop_requested = false;
    }

    pub fn status(&self) -> LiveMarketDataStatusResponse {
        let inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        LiveMarketDataStatusResponse {
            state: inner.state,
            task_id: inner.task_id.clone(),
            started_at_ns: inner.started_at_ns,
            stopped_at_ns: inner.stopped_at_ns,
            stop_reason: inner.stop_reason.clone(),
            subscriptions: inner.subscriptions.clone(),
            last_record_at_ns: inner.last_record_at_ns,
            envelopes_received: inner.envelopes_received,
            storage_records_written: inner.storage_records_written,
            market_data_store_records: inner.market_data_store_records,
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

fn now_ns() -> u64 {
    TimestampNs::now().as_nanos().max(0) as u64
}
