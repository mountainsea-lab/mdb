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

#[derive(Debug, Clone, Default)]
pub struct SourceBatchProcessorStats {
    pub batches_processed: u64,
    pub total_messages: u64,
    pub successful_messages: u64,
    pub failed_messages: u64,
    pub total_processing_time_ms: u64,
    pub avg_batch_size: f64,
    pub avg_processing_time_ms: f64,
    pub throughput_msg_per_sec: f64,
}

impl SourceBatchProcessorStats {
    pub fn record_batch(&mut self, result: &SourceBatchResult) {
        self.batches_processed += 1;
        self.total_messages += result.processed_count as u64;
        self.successful_messages += result.success_count as u64;
        self.failed_messages += result.failure_count as u64;
        self.total_processing_time_ms += result.processing_time_ms;

        self.avg_batch_size = self.total_messages as f64 / self.batches_processed as f64;
        self.avg_processing_time_ms =
            self.total_processing_time_ms as f64 / self.batches_processed as f64;

        if self.total_processing_time_ms > 0 {
            self.throughput_msg_per_sec =
                (self.total_messages as f64 * 1000.0) / self.total_processing_time_ms as f64;
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_messages == 0 {
            0.0
        } else {
            self.successful_messages as f64 / self.total_messages as f64
        }
    }
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
    stats: Arc<RwLock<SourceBatchProcessorStats>>,
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
            stats: Arc::new(RwLock::new(SourceBatchProcessorStats::default())),
        }
    }

    pub async fn get_stats(&self) -> SourceBatchProcessorStats {
        self.stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        *self.stats.write().await = SourceBatchProcessorStats::default();
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
            match self.sink.write_batch(valid_items).await {
                Ok(written_count) => written_count,
                Err(error) => {
                    let result = SourceBatchResult {
                        batch_id,
                        processed_count: batch_size,
                        success_count: 0,
                        failure_count: batch_size,
                        batch_size,
                        processing_time_ms: start.elapsed().as_millis() as u64,
                        processed_at: Utc::now(),
                        errors,
                    };

                    self.stats.write().await.record_batch(&result);

                    return Err(error);
                }
            }
        };

        if written_count < valid_count {
            errors.push(format!(
                "source sink accepted {} of {} valid items",
                written_count, valid_count
            ));
        }

        let failure_count = invalid_count + valid_count.saturating_sub(written_count);

        let result = SourceBatchResult {
            batch_id,
            processed_count: batch_size,
            success_count: written_count,
            failure_count,
            batch_size,
            processing_time_ms: start.elapsed().as_millis() as u64,
            processed_at: Utc::now(),
            errors,
        };

        self.stats.write().await.record_batch(&result);

        Ok(result)
    }
}
