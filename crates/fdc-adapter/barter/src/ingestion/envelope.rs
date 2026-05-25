use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{BarterCheckpoint, BarterMarketEvent};

/// Data quality markers attached to an ingestion envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DataQualityFlags {
    pub is_replay: bool,
    pub is_backfill: bool,
    pub is_duplicate_candidate: bool,
    pub has_gap_before: bool,
    pub is_out_of_order: bool,
}

/// Stable adapter handoff object produced by `fdc-barter` for downstream pipeline bridges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BarterIngestionEnvelope {
    pub envelope_id: String,
    pub source_id: String,
    pub emitted_at: TimestampNs,
    pub event: BarterMarketEvent,
    pub checkpoint: Option<BarterCheckpoint>,
    pub quality: DataQualityFlags,
}

impl BarterIngestionEnvelope {
    pub fn from_event(source_id: impl Into<String>, event: BarterMarketEvent) -> Self {
        Self {
            envelope_id: Uuid::new_v4().to_string(),
            source_id: source_id.into(),
            emitted_at: TimestampNs::now(),
            checkpoint: event.checkpoint.clone(),
            quality: DataQualityFlags::default(),
            event,
        }
    }
}
