use fdc_core::{error::Error, types::TimestampNs, Result};
use std::sync::Mutex;

use crate::market_data::model::{
    LiveMarketDataStatusResponse, MarketDataLiveState, StartLiveMarketDataResponse,
};

#[derive(Debug)]
pub struct MarketDataSupervisor {
    inner: Mutex<MarketDataSupervisorInner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveResumeOutcome {
    pub resumed: bool,
    pub running: bool,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub retry_count: u64,
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
    consecutive_failures: u32,
    retry_count: u64,
    last_error: Option<String>,
    last_error_at_ns: Option<u64>,
    next_retry_at_ns: Option<u64>,
    suppressed_reason: Option<String>,
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
                consecutive_failures: 0,
                retry_count: 0,
                last_error: None,
                last_error_at_ns: None,
                next_retry_at_ns: None,
                suppressed_reason: None,
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
            MarketDataLiveState::Suppressed => {
                Err(Error::validation("market-data live runner is suppressed"))
            }
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
                inner.last_error = None;
                inner.last_error_at_ns = None;
                inner.next_retry_at_ns = None;
                inner.suppressed_reason = None;
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
            MarketDataLiveState::Suppressed => {
                Err(Error::validation("market-data live runner is suppressed"))
            }
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
        inner.consecutive_failures = 0;
        inner.last_error = None;
        inner.last_error_at_ns = None;
        inner.next_retry_at_ns = None;
        inner.suppressed_reason = None;
        inner.stop_requested = false;
    }

    pub fn fail(&self, message: impl Into<String>) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.state = MarketDataLiveState::Failed;
        let message = message.into();
        inner.failure_message = Some(message.clone());
        inner.last_error = Some(message);
        inner.last_error_at_ns = Some(now_ns());
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
        inner.consecutive_failures = 0;
        inner.last_error = None;
        inner.last_error_at_ns = None;
        inner.next_retry_at_ns = None;
        inner.suppressed_reason = None;
    }

    pub fn record_failure_for_retry(
        &self,
        message: impl Into<String>,
        max_consecutive_failures: u32,
        next_retry_at_ns: Option<u64>,
    ) {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        inner.retry_count = inner.retry_count.saturating_add(1);
        inner.last_error = Some(sanitize_live_error(message.into()));
        inner.failure_message = inner.last_error.clone();
        inner.last_error_at_ns = Some(now_ns());
        if inner.consecutive_failures >= max_consecutive_failures {
            inner.state = MarketDataLiveState::Suppressed;
            inner.suppressed_reason = Some("suppressed_after_failures".to_string());
            inner.next_retry_at_ns = None;
            inner.stop_requested = false;
        } else {
            inner.next_retry_at_ns = next_retry_at_ns;
        }
    }

    pub fn prepare_resume(&self, reason: String) -> LiveResumeOutcome {
        let mut inner = self
            .inner
            .lock()
            .expect("market-data supervisor mutex should not be poisoned");
        let previous_consecutive_failures = inner.consecutive_failures;
        match inner.state {
            MarketDataLiveState::Starting
            | MarketDataLiveState::Running
            | MarketDataLiveState::Stopping => LiveResumeOutcome {
                resumed: false,
                running: true,
                previous_consecutive_failures,
                consecutive_failures: inner.consecutive_failures,
                retry_count: inner.retry_count,
            },
            _ => {
                inner.state = MarketDataLiveState::Idle;
                inner.consecutive_failures = 0;
                inner.retry_count = 0;
                inner.last_error = None;
                inner.failure_message = None;
                inner.last_error_at_ns = None;
                inner.next_retry_at_ns = None;
                inner.suppressed_reason = None;
                inner.stop_requested = false;
                inner.stop_reason = Some(reason);
                LiveResumeOutcome {
                    resumed: true,
                    running: false,
                    previous_consecutive_failures,
                    consecutive_failures: 0,
                    retry_count: 0,
                }
            }
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
            consecutive_failures: inner.consecutive_failures,
            retry_count: inner.retry_count,
            last_error: inner.last_error.clone(),
            last_error_at_ns: inner.last_error_at_ns,
            next_retry_at_ns: inner.next_retry_at_ns,
            suppressed_reason: inner.suppressed_reason.clone(),
            resume_enabled: false,
        }
    }
}

impl Default for MarketDataSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

fn sanitize_live_error(message: String) -> String {
    let single_line = message.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX_LEN: usize = 256;
    if single_line.len() > MAX_LEN {
        format!("{}...", &single_line[..MAX_LEN])
    } else {
        single_line
    }
}

fn now_ns() -> u64 {
    TimestampNs::now().as_nanos().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_supervisor_records_failure_and_suppresses_at_threshold() {
        let supervisor = MarketDataSupervisor::new();
        supervisor
            .start_background(vec!["test:BTCUSDT:trade".to_string()])
            .unwrap();

        supervisor.record_failure_for_retry("first\nerror", 2, Some(123));
        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Running);
        assert_eq!(status.consecutive_failures, 1);
        assert_eq!(status.retry_count, 1);
        assert_eq!(status.last_error.as_deref(), Some("first error"));
        assert_eq!(status.next_retry_at_ns, Some(123));

        supervisor.record_failure_for_retry("second error", 2, None);
        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Suppressed);
        assert_eq!(status.consecutive_failures, 2);
        assert_eq!(
            status.suppressed_reason.as_deref(),
            Some("suppressed_after_failures")
        );
        assert!(status.next_retry_at_ns.is_none());
    }

    #[test]
    fn live_supervisor_success_resets_failure_state() {
        let supervisor = MarketDataSupervisor::new();
        supervisor
            .start_background(vec!["test:BTCUSDT:trade".to_string()])
            .unwrap();
        supervisor.record_failure_for_retry("temporary", 3, Some(456));
        supervisor.record_progress(1, 1, 1, Some(789));

        let status = supervisor.status();
        assert_eq!(status.consecutive_failures, 0);
        assert!(status.last_error.is_none());
        assert!(status.last_error_at_ns.is_none());
        assert!(status.next_retry_at_ns.is_none());
        assert!(status.suppressed_reason.is_none());
    }

    #[test]
    fn live_supervisor_prepare_resume_clears_suppression_when_safe() {
        let supervisor = MarketDataSupervisor::new();
        supervisor
            .start_background(vec!["test:BTCUSDT:trade".to_string()])
            .unwrap();
        supervisor.record_failure_for_retry("boom", 1, None);

        let outcome = supervisor.prepare_resume("operator".to_string());
        assert!(outcome.resumed);
        assert!(!outcome.running);
        assert_eq!(outcome.consecutive_failures, 0);

        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Idle);
        assert_eq!(status.stop_reason.as_deref(), Some("operator"));
        assert!(status.last_error.is_none());
    }

    #[test]
    fn live_supervisor_prepare_resume_rejects_running_state() {
        let supervisor = MarketDataSupervisor::new();
        supervisor
            .start_background(vec!["test:BTCUSDT:trade".to_string()])
            .unwrap();

        let outcome = supervisor.prepare_resume("operator".to_string());
        assert!(!outcome.resumed);
        assert!(outcome.running);
        assert_eq!(supervisor.status().state, MarketDataLiveState::Running);
    }
}
