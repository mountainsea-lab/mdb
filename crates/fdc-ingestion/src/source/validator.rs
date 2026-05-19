use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct SourceValidatorStats {
    pub messages_validated: u64,
    pub validation_successes: u64,
    pub validation_failures: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceValidationWarning {
    pub warning_type: String,
    pub field_path: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceValidationResult {
    pub is_valid: bool,
    pub errors: Vec<SourceValidationError>,
    pub warnings: Vec<SourceValidationWarning>,
    pub validation_time_us: u64,
    pub validated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Default)]
pub struct SourceValidator;
