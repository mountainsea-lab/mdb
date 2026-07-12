use std::sync::Arc;
use std::time::Duration;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::{Result, error::Error, types::TimestampNs};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::QueryableMarketDataStore;
use futures::{Stream, StreamExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeMarketDataMvpConfig {
    pub runtime_window: Duration,
    pub idle_timeout: Duration,
    pub max_errors: usize,
}

impl Default for RealtimeMarketDataMvpConfig {
    fn default() -> Self {
        Self {
            runtime_window: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(2),
            max_errors: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeMarketDataMvpSummary {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
    pub errors_observed: usize,
    pub started_at: TimestampNs,
    pub stopped_at: TimestampNs,
}

pub async fn run_realtime_barter_envelope_stream<S>(
    mut stream: S,
    market_data_store: Arc<QueryableMarketDataStore>,
    config: RealtimeMarketDataMvpConfig,
) -> Result<RealtimeMarketDataMvpSummary>
where
    S: Stream<Item = BarterIngestionEnvelope> + Unpin,
{
    if config.runtime_window.is_zero() {
        return Err(Error::validation(
            "realtime market-data MVP runtime_window must be greater than zero",
        ));
    }
    if config.idle_timeout.is_zero() {
        return Err(Error::validation(
            "realtime market-data MVP idle_timeout must be greater than zero",
        ));
    }

    let started_at = TimestampNs::now();
    let deadline = tokio::time::Instant::now() + config.runtime_window;
    let mut summary = RealtimeMarketDataMvpSummary {
        envelopes_received: 0,
        source_valid: 0,
        source_invalid: 0,
        dto_mapped: 0,
        storage_records_written: 0,
        market_data_store_records: 0,
        errors_observed: 0,
        started_at,
        stopped_at: started_at,
    };

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        let wait_for = remaining.min(config.idle_timeout);
        match tokio::time::timeout(wait_for, stream.next()).await {
            Ok(Some(envelope)) => {
                summary.envelopes_received += 1;
                let pipeline_result = run_barter_envelopes_to_storage_once(
                    vec![envelope],
                    market_data_store.as_ref(),
                )
                .await?;
                summary.source_valid += pipeline_result.source_valid;
                summary.source_invalid += pipeline_result.source_invalid;
                summary.dto_mapped += pipeline_result.dto_mapped;
                summary.storage_records_written += pipeline_result.storage_records_written;
            }
            Ok(None) | Err(_) => break,
        }
    }

    summary.market_data_store_records = market_data_store.record_count();
    summary.stopped_at = TimestampNs::now();
    Ok(summary)
}
