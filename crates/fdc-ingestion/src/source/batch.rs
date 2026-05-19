use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fdc_core::error::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::config::BatchConfig;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBatchResult {
    pub batch_id: String,
    pub processed_count: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub batch_size: usize,
    pub processing_time_ms: u64,
    pub processed_at: DateTime<Utc>,
    pub errors: Vec<String>,
}

#[async_trait]
pub trait SourceBatchSink<T>: Send + Sync {
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize>;
}

pub struct SourceBatchProcessor<T> {
    config: BatchConfig,
    sink: Arc<dyn SourceBatchSink<T>>,
    current_batch: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
    batch_timer: Arc<RwLock<Option<Instant>>>,
}

impl<T> SourceBatchProcessor<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(config: BatchConfig, sink: Arc<dyn SourceBatchSink<T>>) -> Self {
        Self {
            config,
            sink,
            current_batch: Arc::new(RwLock::new(Vec::new())),
            batch_timer: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn add_item(&self, item: SourceBatchItem<T>) -> Result<Option<SourceBatchResult>> {
        let mut batch = self.current_batch.write().await;
        let mut timer = self.batch_timer.write().await;

        if batch.is_empty() {
            *timer = Some(Instant::now());
        }

        batch.push(item);

        let reached_size = batch.len() >= self.config.batch_size;
        let reached_timeout = timer
            .as_ref()
            .map(|started| started.elapsed() >= self.config.batch_timeout)
            .unwrap_or(false);

        if reached_size || reached_timeout {
            let items = batch.drain(..).collect::<Vec<_>>();
            *timer = None;
            drop(batch);
            drop(timer);
            return Ok(Some(self.process_batch(items).await?));
        }

        Ok(None)
    }

    pub async fn flush(&self) -> Result<Option<SourceBatchResult>> {
        let mut batch = self.current_batch.write().await;
        let mut timer = self.batch_timer.write().await;

        if batch.is_empty() {
            return Ok(None);
        }

        let items = batch.drain(..).collect::<Vec<_>>();
        *timer = None;
        drop(batch);
        drop(timer);

        Ok(Some(self.process_batch(items).await?))
    }

    async fn process_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<SourceBatchResult> {
        let batch_id = Uuid::new_v4().to_string();
        let batch_size = items.len();
        let start = Instant::now();

        let mut valid_items = Vec::new();
        let mut invalid_count = 0usize;
        let mut errors = Vec::new();

        for item in items {
            if item.is_valid() {
                valid_items.push(item);
            } else {
                invalid_count += 1;
                errors.push(format!("invalid source batch item: {}", item.item_id));
            }
        }

        let valid_count = valid_items.len();
        let written_count = if valid_items.is_empty() {
            0
        } else {
            self.sink.write_batch(valid_items).await?
        };

        if written_count < valid_count {
            errors.push(format!(
                "source sink accepted {} of {} valid items",
                written_count, valid_count
            ));
        }

        let failure_count = invalid_count + valid_count.saturating_sub(written_count);

        Ok(SourceBatchResult {
            batch_id,
            processed_count: batch_size,
            success_count: written_count,
            failure_count,
            batch_size,
            processing_time_ms: start.elapsed().as_millis() as u64,
            processed_at: Utc::now(),
            errors,
        })
    }
}
