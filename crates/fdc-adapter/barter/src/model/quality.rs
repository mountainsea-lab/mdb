use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::{BarterMarketDataKind, BarterMarketEvent};

/// Adapter-owned runtime observations that are not market-data envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarterRuntimeObservation {
    Reconnect { exchange: String },
    StreamItemError { message: String },
}

/// Per-kind counters for emitted adapter market-data events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarterKindCounters {
    pub emitted_by_kind: BTreeMap<BarterMarketDataKind, u64>,
}

impl BarterKindCounters {
    pub fn record_event(&mut self, event: &BarterMarketEvent) {
        *self.emitted_by_kind.entry(event.kind).or_insert(0) += 1;
    }

    pub fn count(&self, kind: BarterMarketDataKind) -> u64 {
        self.emitted_by_kind.get(&kind).copied().unwrap_or(0)
    }
}

/// Event latency in nanoseconds, measured as adapter receive time minus exchange event time.
pub fn event_latency_ns(event: &BarterMarketEvent) -> Option<i128> {
    let latency = event.received_at.as_nanos() as i128 - event.timestamp.as_nanos() as i128;
    (latency >= 0).then_some(latency)
}
