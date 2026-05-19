use uuid::Uuid;

use super::{SourceEnvelope, SourceValidationResult};

#[derive(Debug, Clone)]
pub struct SourceBatchItem<T> {
    pub item_id: String,
    pub envelope: SourceEnvelope<T>,
    pub validation_result: SourceValidationResult,
}

impl<T> SourceBatchItem<T> {
    pub fn new(envelope: SourceEnvelope<T>, validation_result: SourceValidationResult) -> Self {
        Self {
            item_id: Uuid::new_v4().to_string(),
            envelope,
            validation_result,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.validation_result.is_valid
    }
}
