use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::SourceEnvelope;

#[derive(Debug, Clone, Default)]
pub struct SourceValidatorStats {
    pub messages_validated: u64,
    pub validation_successes: u64,
    pub validation_failures: u64,
    pub total_validation_time_us: u64,
}

impl SourceValidatorStats {
    pub fn record_validation(&mut self, result: &SourceValidationResult) {
        self.messages_validated += 1;
        self.total_validation_time_us += result.validation_time_us;
        if result.is_valid {
            self.validation_successes += 1;
        } else {
            self.validation_failures += 1;
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.messages_validated == 0 {
            0.0
        } else {
            self.validation_successes as f64 / self.messages_validated as f64
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceValidationErrorType {
    EmptySourceId,
    EmptyEnvelopeId,
    InvalidEventTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceValidationError {
    pub error_type: SourceValidationErrorType,
    pub field_path: String,
    pub message: String,
}

impl SourceValidationError {
    fn empty_source_id() -> Self {
        Self {
            error_type: SourceValidationErrorType::EmptySourceId,
            field_path: "source_id".to_string(),
            message: "source_id must not be empty".to_string(),
        }
    }

    fn empty_envelope_id() -> Self {
        Self {
            error_type: SourceValidationErrorType::EmptyEnvelopeId,
            field_path: "envelope_id".to_string(),
            message: "envelope_id must not be empty".to_string(),
        }
    }

    fn invalid_event_time() -> Self {
        Self {
            error_type: SourceValidationErrorType::InvalidEventTime,
            field_path: "event_time".to_string(),
            message: "event_time must be greater than zero nanoseconds".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceValidationWarning {
    pub warning_type: String,
    pub field_path: String,
    pub message: String,
}

impl SourceValidationWarning {
    fn duplicate_candidate() -> Self {
        Self {
            warning_type: "duplicate_candidate".to_string(),
            field_path: "quality.is_duplicate_candidate".to_string(),
            message: "source marked this envelope as a duplicate candidate".to_string(),
        }
    }

    fn gap_before() -> Self {
        Self {
            warning_type: "gap_before".to_string(),
            field_path: "quality.has_gap_before".to_string(),
            message: "source marked a gap before this envelope".to_string(),
        }
    }

    fn out_of_order() -> Self {
        Self {
            warning_type: "out_of_order".to_string(),
            field_path: "quality.is_out_of_order".to_string(),
            message: "source marked this envelope as out of order".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceValidationResult {
    pub is_valid: bool,
    pub errors: Vec<SourceValidationError>,
    pub warnings: Vec<SourceValidationWarning>,
    pub validation_time_us: u64,
    pub validated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct SourceValidator {
    stats: Arc<RwLock<SourceValidatorStats>>,
}

impl SourceValidator {
    pub async fn validate<T>(&self, envelope: &SourceEnvelope<T>) -> SourceValidationResult {
        let start = Instant::now();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        if envelope.envelope_id.trim().is_empty() {
            errors.push(SourceValidationError::empty_envelope_id());
        }

        if envelope.source_id.trim().is_empty() {
            errors.push(SourceValidationError::empty_source_id());
        }

        if envelope.event_time.as_nanos() <= 0 {
            errors.push(SourceValidationError::invalid_event_time());
        }

        if envelope.quality.is_duplicate_candidate {
            warnings.push(SourceValidationWarning::duplicate_candidate());
        }

        if envelope.quality.has_gap_before {
            warnings.push(SourceValidationWarning::gap_before());
        }

        if envelope.quality.is_out_of_order {
            warnings.push(SourceValidationWarning::out_of_order());
        }

        let result = SourceValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            validation_time_us: start.elapsed().as_micros() as u64,
            validated_at: Utc::now(),
        };

        self.stats.write().await.record_validation(&result);
        result
    }

    pub async fn get_stats(&self) -> SourceValidatorStats {
        self.stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        *self.stats.write().await = SourceValidatorStats::default();
    }
}
