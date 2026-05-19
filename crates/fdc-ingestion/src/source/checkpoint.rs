use fdc_core::types::TimestampNs;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    pub checkpoint_id: String,
    pub source_id: String,
    pub partition: SourcePartition,
    pub position: SourcePosition,
    pub updated_at: TimestampNs,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SourcePartition {
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub shard: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourcePosition {
    Timestamp(TimestampNs),
    Sequence(String),
    PageToken(String),
    Opaque(serde_json::Value),
}
