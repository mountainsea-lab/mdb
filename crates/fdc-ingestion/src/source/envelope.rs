use std::collections::BTreeMap;

use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{SourceCheckpoint, SourceQualityFlags};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceType {
    MarketData,
    ReferenceData,
    Replay,
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub adapter: Option<String>,
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceEnvelope<T> {
    pub envelope_id: String,
    pub source_id: String,
    pub source_type: SourceType,
    pub sequence: Option<String>,
    pub event_time: TimestampNs,
    pub received_at: TimestampNs,
    pub emitted_at: TimestampNs,
    pub payload: T,
    pub checkpoint: Option<SourceCheckpoint>,
    pub quality: SourceQualityFlags,
    pub metadata: SourceMetadata,
}

impl<T> SourceEnvelope<T> {
    pub fn new(
        source_id: impl Into<String>,
        source_type: SourceType,
        event_time: TimestampNs,
        received_at: TimestampNs,
        payload: T,
    ) -> Self {
        Self {
            envelope_id: Uuid::new_v4().to_string(),
            source_id: source_id.into(),
            source_type,
            sequence: None,
            event_time,
            received_at,
            emitted_at: TimestampNs::now(),
            payload,
            checkpoint: None,
            quality: SourceQualityFlags::default(),
            metadata: SourceMetadata::default(),
        }
    }

    pub fn with_sequence(mut self, sequence: impl Into<String>) -> Self {
        self.sequence = Some(sequence.into());
        self
    }

    pub fn with_optional_checkpoint(mut self, checkpoint: Option<SourceCheckpoint>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    pub fn with_checkpoint(mut self, checkpoint: SourceCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    pub fn with_quality(mut self, quality: SourceQualityFlags) -> Self {
        self.quality = quality;
        self
    }

    pub fn with_metadata(mut self, metadata: SourceMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}
