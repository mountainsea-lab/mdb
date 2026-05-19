# fdc-ingestion Phase B1b Source Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the second slice of the `fdc-ingestion` structured source path: source batch items, source batch sink abstraction, source batch processor, batch result, and source batch processor stats.

**Architecture:** Phase B1b adds `source::batch` alongside the B1a source envelope and validator modules. The processor accepts already validated `SourceBatchItem<T>`, filters invalid items before sink handoff, writes valid items to a generic `SourceBatchSink<T>`, and records local batch stats. It keeps the existing network byte `BatchProcessor` unchanged and keeps `fdc-ingestion` independent from `fdc-barter`.

**Tech Stack:** Rust 2021, `fdc-core::error::Result`, `fdc-core::types::TimestampNs`, `serde`, `chrono`, `uuid`, `tokio`, `async-trait`, existing `fdc-ingestion::config::BatchConfig`.

---

## Review Input

Read before implementation:

- `docs/architecture/fdc-ingestion-phase-b1b-source-batch-pseudocode-review.md`
- `docs/architecture/fdc-ingestion-phase-b1-source-path-pseudocode-review.md`
- `crates/fdc-ingestion/src/source/envelope.rs`
- `crates/fdc-ingestion/src/source/validator.rs`
- `crates/fdc-ingestion/src/source/mod.rs`
- `crates/fdc-ingestion/src/batch.rs`
- `crates/fdc-ingestion/src/config.rs`
- `crates/fdc-ingestion/src/lib.rs`

## Scope

### In Scope

- Create `crates/fdc-ingestion/src/source/batch.rs`.
- Add `SourceBatchItem<T>`.
- Add `SourceBatchSink<T>` async trait.
- Add `SourceBatchResult`.
- Add `SourceBatchProcessorStats`.
- Add `SourceBatchProcessor<T>`.
- Re-export source batch types from `source::mod` and crate root.
- Add contract tests for source batch item, sink filtering, size-trigger flush, explicit flush, short writes, sink errors, and stats.
- Verify `fdc-ingestion` has no direct `fdc-barter` / `fdc_barter` dependency.

### Out of Scope

- No source pipeline runner. That is B1c.
- No `fdc-transform` sink.
- No storage sink.
- No `fdc-barter` dependency from `fdc-ingestion`.
- No checkpoint persistence.
- No dead-letter queue.
- No dedupe cache or stateful gap detection.
- No real WebSocket, REST, storage, or network I/O.
- No changes to existing `crates/fdc-ingestion/src/batch.rs` behavior.

## File Structure

### Create

- `crates/fdc-ingestion/src/source/batch.rs`
  Owns source batch item/result/sink/processor/stats. This file depends on B1a source envelope and validation result, but not on existing network byte `BatchItem`.
- `crates/fdc-ingestion/tests/source_batch_contract.rs`
  Integration tests proving the public source batch API works with dummy payloads and in-memory sinks.

### Modify

- `crates/fdc-ingestion/src/source/mod.rs`
  Add `pub mod batch;` and re-export source batch public types.
- `crates/fdc-ingestion/src/lib.rs`
  Re-export source batch public types from the crate root.

## Public API Target

After B1b, this should compile:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::{error::Result, types::TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, SourceBatchItem, SourceBatchProcessor, SourceBatchSink, SourceEnvelope,
    SourceType, SourceValidator,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

struct NoopSink;

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for NoopSink {
    async fn write_batch(&self, items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        Ok(items.len())
    }
}

#[tokio::test]
async fn source_batch_processor_accepts_valid_items() {
    let processor = SourceBatchProcessor::new(
        BatchConfig { batch_size: 1, ..Default::default() },
        Arc::new(NoopSink),
    );

    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent { symbol: "BTCUSDT".to_string(), price: 65000.25 },
    );
    let validation = SourceValidator::default().validate(&envelope).await;
    let item = SourceBatchItem::new(envelope, validation);

    let result = processor.add_item(item).await.unwrap().unwrap();

    assert_eq!(result.processed_count, 1);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 0);
}
```

## Task 1: Source Batch Item and Public Exports

**Files:**
- Create: `crates/fdc-ingestion/src/source/batch.rs`
- Modify: `crates/fdc-ingestion/src/source/mod.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`
- Test: `crates/fdc-ingestion/tests/source_batch_contract.rs`

- [ ] **Step 1: Write failing source batch item contract test**

Create `crates/fdc-ingestion/tests/source_batch_contract.rs`:

```rust
use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    SourceBatchItem, SourceEnvelope, SourceType, SourceValidationResult, SourceValidator,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

fn dummy_envelope(source_id: &str, symbol: &str) -> SourceEnvelope<DummyMarketEvent> {
    SourceEnvelope::new(
        source_id,
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: symbol.to_string(),
            price: 65000.25,
        },
    )
}

#[tokio::test]
async fn source_batch_item_preserves_envelope_and_validation_result() {
    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation: SourceValidationResult = SourceValidator::default().validate(&envelope).await;

    let item = SourceBatchItem::new(envelope, validation.clone());

    assert!(!item.item_id.is_empty());
    assert_eq!(item.envelope.source_id, "dummy-source");
    assert_eq!(item.envelope.payload.symbol, "BTCUSDT");
    assert_eq!(item.validation_result.is_valid, validation.is_valid);
    assert!(item.is_valid());
}

#[tokio::test]
async fn source_batch_item_validity_comes_from_validation_result() {
    let mut envelope = dummy_envelope("", "ETHUSDT");
    envelope.envelope_id = "envelope-with-empty-source".to_string();
    let validation = SourceValidator::default().validate(&envelope).await;

    let item = SourceBatchItem::new(envelope, validation);

    assert!(!item.is_valid());
    assert_eq!(item.validation_result.errors.len(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: FAIL because `SourceBatchItem` is not exported by `fdc_ingestion`.

- [ ] **Step 3: Implement minimal source batch item and exports**

Create `crates/fdc-ingestion/src/source/batch.rs`:

```rust
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
```

Modify `crates/fdc-ingestion/src/source/mod.rs` to include the batch module and item export:

```rust
pub mod batch;
pub mod checkpoint;
pub mod envelope;
pub mod quality;
pub mod validator;

pub use batch::SourceBatchItem;
pub use checkpoint::{SourceCheckpoint, SourcePartition, SourcePosition};
pub use envelope::{SourceEnvelope, SourceMetadata, SourceType};
pub use quality::SourceQualityFlags;
pub use validator::{
    SourceValidationError, SourceValidationErrorType, SourceValidationResult,
    SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

Modify the `pub use source::{ ... }` block in `crates/fdc-ingestion/src/lib.rs`:

```rust
pub use source::{
    SourceBatchItem, SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition,
    SourcePosition, SourceQualityFlags, SourceType, SourceValidationError,
    SourceValidationErrorType, SourceValidationResult, SourceValidationWarning, SourceValidator,
    SourceValidatorStats,
};
```

- [ ] **Step 4: Run source batch item tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS with 2 tests.

- [ ] **Step 5: Commit source batch item**

Run:

```bash
git add crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/src/source/mod.rs \
        crates/fdc-ingestion/src/lib.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
git commit -m "feat: add fdc-ingestion source batch item"
```

## Task 2: Source Batch Result, Sink, Processor, and Valid Flush

**Files:**
- Modify: `crates/fdc-ingestion/src/source/batch.rs`
- Modify: `crates/fdc-ingestion/src/source/mod.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`
- Test: `crates/fdc-ingestion/tests/source_batch_contract.rs`

- [ ] **Step 1: Extend contract test with recording sink and valid flush behavior**

Append to `crates/fdc-ingestion/tests/source_batch_contract.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::error::Result;
use fdc_ingestion::{
    config::BatchConfig, SourceBatchProcessor, SourceBatchSink,
};
use tokio::sync::RwLock;

#[derive(Default)]
struct RecordingSourceBatchSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> RecordingSourceBatchSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourceBatchSink<T>
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}

#[tokio::test]
async fn source_batch_processor_flush_writes_valid_items_to_sink() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );

    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation = SourceValidator::default().validate(&envelope).await;
    let item = SourceBatchItem::new(envelope, validation);

    assert!(processor.add_item(item).await.unwrap().is_none());

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 1);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 0);
    assert_eq!(result.batch_size, 1);
    assert!(result.errors.is_empty());
    assert!(!result.batch_id.is_empty());
    assert_eq!(sink.written_count().await, 1);
}

#[tokio::test]
async fn source_batch_processor_empty_flush_returns_none() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(BatchConfig::default(), sink);

    let result = processor.flush().await.unwrap();

    assert!(result.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: FAIL because `SourceBatchProcessor`, `SourceBatchSink`, and `SourceBatchResult` are not defined or exported.

- [ ] **Step 3: Implement source batch result, sink trait, and processor valid flush path**

Replace `crates/fdc-ingestion/src/source/batch.rs` with:

```rust
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
```

Modify `crates/fdc-ingestion/src/source/mod.rs` batch export line:

```rust
pub use batch::{SourceBatchItem, SourceBatchProcessor, SourceBatchResult, SourceBatchSink};
```

Modify the `pub use source::{ ... }` block in `crates/fdc-ingestion/src/lib.rs`:

```rust
pub use source::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchResult, SourceBatchSink, SourceCheckpoint,
    SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition, SourceQualityFlags,
    SourceType, SourceValidationError, SourceValidationErrorType, SourceValidationResult,
    SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

- [ ] **Step 4: Run valid flush tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS with 4 tests.

- [ ] **Step 5: Commit processor valid flush path**

Run:

```bash
git add crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/src/source/mod.rs \
        crates/fdc-ingestion/src/lib.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
git commit -m "feat: add fdc-ingestion source batch processor"
```

## Task 3: Invalid Filtering, Size Trigger, Short Writes, and Sink Errors

**Files:**
- Modify: `crates/fdc-ingestion/src/source/batch.rs`
- Test: `crates/fdc-ingestion/tests/source_batch_contract.rs`

- [ ] **Step 1: Add failing tests for invalid filtering and size trigger**

Append to `crates/fdc-ingestion/tests/source_batch_contract.rs`:

```rust
#[tokio::test]
async fn source_batch_processor_filters_invalid_items_before_sink() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );

    let valid_envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let valid_validation = SourceValidator::default().validate(&valid_envelope).await;
    let valid_item = SourceBatchItem::new(valid_envelope, valid_validation);

    let invalid_envelope = dummy_envelope("", "ETHUSDT");
    let invalid_validation = SourceValidator::default().validate(&invalid_envelope).await;
    let invalid_item = SourceBatchItem::new(invalid_envelope, invalid_validation);

    processor.add_item(valid_item).await.unwrap();
    processor.add_item(invalid_item).await.unwrap();

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 1);
    assert_eq!(sink.written_count().await, 1);
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].contains("invalid source batch item"));
}

#[tokio::test]
async fn source_batch_processor_processes_when_batch_size_is_reached() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 2,
            ..Default::default()
        },
        sink.clone(),
    );

    let first = dummy_envelope("dummy-source", "BTCUSDT");
    let first_validation = SourceValidator::default().validate(&first).await;
    let second = dummy_envelope("dummy-source", "ETHUSDT");
    let second_validation = SourceValidator::default().validate(&second).await;

    assert!(processor
        .add_item(SourceBatchItem::new(first, first_validation))
        .await
        .unwrap()
        .is_none());

    let result = processor
        .add_item(SourceBatchItem::new(second, second_validation))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 2);
    assert_eq!(result.failure_count, 0);
    assert_eq!(sink.written_count().await, 2);
    assert!(processor.flush().await.unwrap().is_none());
}
```

- [ ] **Step 2: Run tests and confirm current behavior**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS if Task 2 already implemented the full `process_batch` pseudocode. If the invalid filtering or size trigger behavior is missing, the new tests fail and identify the missing behavior.

- [ ] **Step 3: Add failing tests for short writes and sink errors**

Append to `crates/fdc-ingestion/tests/source_batch_contract.rs`:

```rust
struct ShortWriteSourceBatchSink {
    accepted_count: usize,
}

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for ShortWriteSourceBatchSink {
    async fn write_batch(&self, _items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        Ok(self.accepted_count)
    }
}

struct FailingSourceBatchSink;

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for FailingSourceBatchSink {
    async fn write_batch(&self, _items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        Err(fdc_core::error::Error::storage("sink unavailable"))
    }
}

#[tokio::test]
async fn source_batch_processor_records_short_write_as_failure() {
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        Arc::new(ShortWriteSourceBatchSink { accepted_count: 1 }),
    );

    for symbol in ["BTCUSDT", "ETHUSDT"] {
        let envelope = dummy_envelope("dummy-source", symbol);
        let validation = SourceValidator::default().validate(&envelope).await;
        processor
            .add_item(SourceBatchItem::new(envelope, validation))
            .await
            .unwrap();
    }

    let result = processor.flush().await.unwrap().unwrap();

    assert_eq!(result.processed_count, 2);
    assert_eq!(result.success_count, 1);
    assert_eq!(result.failure_count, 1);
    assert_eq!(result.errors.len(), 1);
    assert!(result.errors[0].contains("source sink accepted 1 of 2 valid items"));
}

#[tokio::test]
async fn source_batch_processor_propagates_sink_errors() {
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 1,
            ..Default::default()
        },
        Arc::new(FailingSourceBatchSink),
    );

    let envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let validation = SourceValidator::default().validate(&envelope).await;

    let err = processor
        .add_item(SourceBatchItem::new(envelope, validation))
        .await
        .unwrap_err();

    assert!(err.to_string().contains("sink unavailable"));
}
```

- [ ] **Step 4: Run short write and sink error tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract source_batch_processor_records_short_write_as_failure -- --exact
cargo test -p fdc-ingestion --test source_batch_contract source_batch_processor_propagates_sink_errors -- --exact
```

Expected: PASS. If `fdc_core::error::Error::storage` does not exist in this workspace, inspect `crates/fdc-core/src/error.rs` and replace the test error constructor with the closest existing constructor that formats to include `sink unavailable`.

- [ ] **Step 5: Run all source batch contract tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS with 8 tests.

- [ ] **Step 6: Commit batch filtering and error semantics**

Run:

```bash
git add crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
git commit -m "feat: define source batch filtering semantics"
```

## Task 4: Source Batch Processor Stats

**Files:**
- Modify: `crates/fdc-ingestion/src/source/batch.rs`
- Modify: `crates/fdc-ingestion/src/source/mod.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`
- Test: `crates/fdc-ingestion/tests/source_batch_contract.rs`

- [ ] **Step 1: Add failing stats contract test**

Append to `crates/fdc-ingestion/tests/source_batch_contract.rs`:

```rust
#[tokio::test]
async fn source_batch_processor_records_and_resets_stats() {
    let sink = Arc::new(RecordingSourceBatchSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink,
    );

    let valid_envelope = dummy_envelope("dummy-source", "BTCUSDT");
    let valid_validation = SourceValidator::default().validate(&valid_envelope).await;
    let invalid_envelope = dummy_envelope("", "ETHUSDT");
    let invalid_validation = SourceValidator::default().validate(&invalid_envelope).await;

    processor
        .add_item(SourceBatchItem::new(valid_envelope, valid_validation))
        .await
        .unwrap();
    processor
        .add_item(SourceBatchItem::new(invalid_envelope, invalid_validation))
        .await
        .unwrap();
    processor.flush().await.unwrap().unwrap();

    let stats = processor.get_stats().await;
    assert_eq!(stats.batches_processed, 1);
    assert_eq!(stats.total_messages, 2);
    assert_eq!(stats.successful_messages, 1);
    assert_eq!(stats.failed_messages, 1);
    assert_eq!(stats.avg_batch_size, 2.0);
    assert_eq!(stats.success_rate(), 0.5);

    processor.reset_stats().await;
    let reset = processor.get_stats().await;
    assert_eq!(reset.batches_processed, 0);
    assert_eq!(reset.total_messages, 0);
    assert_eq!(reset.success_rate(), 0.0);
}
```

- [ ] **Step 2: Run stats test to verify it fails**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract source_batch_processor_records_and_resets_stats -- --exact
```

Expected: FAIL because `SourceBatchProcessorStats`, `get_stats`, and `reset_stats` are not implemented or exported.

- [ ] **Step 3: Implement stats**

Add this type to `crates/fdc-ingestion/src/source/batch.rs` after `SourceBatchResult`:

```rust
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
```

Modify `SourceBatchProcessor<T>` in `crates/fdc-ingestion/src/source/batch.rs` to include stats:

```rust
pub struct SourceBatchProcessor<T> {
    config: BatchConfig,
    sink: Arc<dyn SourceBatchSink<T>>,
    stats: Arc<RwLock<SourceBatchProcessorStats>>,
    current_batch: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
    batch_timer: Arc<RwLock<Option<Instant>>>,
}
```

Modify `SourceBatchProcessor::new`:

```rust
pub fn new(config: BatchConfig, sink: Arc<dyn SourceBatchSink<T>>) -> Self {
    Self {
        config,
        sink,
        stats: Arc::new(RwLock::new(SourceBatchProcessorStats::default())),
        current_batch: Arc::new(RwLock::new(Vec::new())),
        batch_timer: Arc::new(RwLock::new(None)),
    }
}
```

Add methods inside the existing `impl<T> SourceBatchProcessor<T>` block:

```rust
pub async fn get_stats(&self) -> SourceBatchProcessorStats {
    self.stats.read().await.clone()
}

pub async fn reset_stats(&self) {
    *self.stats.write().await = SourceBatchProcessorStats::default();
}
```

Modify the end of `process_batch` to record stats before returning:

```rust
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
```

Modify `crates/fdc-ingestion/src/source/mod.rs` batch export line:

```rust
pub use batch::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchProcessorStats, SourceBatchResult,
    SourceBatchSink,
};
```

Modify the `pub use source::{ ... }` block in `crates/fdc-ingestion/src/lib.rs`:

```rust
pub use source::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchProcessorStats, SourceBatchResult,
    SourceBatchSink, SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition,
    SourcePosition, SourceQualityFlags, SourceType, SourceValidationError,
    SourceValidationErrorType, SourceValidationResult, SourceValidationWarning, SourceValidator,
    SourceValidatorStats,
};
```

- [ ] **Step 4: Run stats test**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract source_batch_processor_records_and_resets_stats -- --exact
```

Expected: PASS.

- [ ] **Step 5: Run all source batch contract tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS with 9 tests.

- [ ] **Step 6: Commit stats**

Run:

```bash
git add crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/src/source/mod.rs \
        crates/fdc-ingestion/src/lib.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
git commit -m "feat: add source batch processor stats"
```

## Task 5: Formatting, Crate Verification, and No-Dependency Guard

**Files:**
- Modify only files touched in Tasks 1-4 if `cargo fmt` requires formatting.

- [ ] **Step 1: Format only touched Rust files**

Run:

```bash
rustfmt crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/src/source/mod.rs \
        crates/fdc-ingestion/src/lib.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
```

Expected: command exits successfully. Prefer this over workspace-wide `cargo fmt` to avoid unrelated formatting churn.

- [ ] **Step 2: Run source batch tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
```

Expected: PASS with 9 tests.

- [ ] **Step 3: Run all fdc-ingestion tests**

Run:

```bash
cargo test -p fdc-ingestion
```

Expected: PASS. Existing warnings are acceptable if unrelated to B1b.

- [ ] **Step 4: Run fdc-ingestion all-targets check**

Run:

```bash
cargo check -p fdc-ingestion --all-targets
```

Expected: PASS. Existing warnings are acceptable if unrelated to B1b.

- [ ] **Step 5: Run no `fdc-barter` dependency scan**

Run:

```bash
grep -RInE "fdc-barter|fdc_barter" crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: no output and exit code `1`. If the shell treats exit code `1` as failure, run:

```bash
if grep -RInE "fdc-barter|fdc_barter" crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml; then
  echo "unexpected fdc-barter reference in fdc-ingestion"
  exit 1
else
  echo "no fdc-barter reference in fdc-ingestion"
fi
```

Expected output:

```text
no fdc-barter reference in fdc-ingestion
```

- [ ] **Step 6: Inspect git diff for unrelated churn**

Run:

```bash
git diff --stat
git diff -- crates/fdc-ingestion/src/source/batch.rs \
           crates/fdc-ingestion/src/source/mod.rs \
           crates/fdc-ingestion/src/lib.rs \
           crates/fdc-ingestion/tests/source_batch_contract.rs
```

Expected: only B1b files changed. If unrelated files changed due to formatting, revert those unrelated files before committing.

- [ ] **Step 7: Commit final formatting or verification-only adjustments**

If Step 1 changed formatting after the previous commits, run:

```bash
git add crates/fdc-ingestion/src/source/batch.rs \
        crates/fdc-ingestion/src/source/mod.rs \
        crates/fdc-ingestion/src/lib.rs \
        crates/fdc-ingestion/tests/source_batch_contract.rs
git commit -m "style: format source batch implementation"
```

If Step 1 produced no diff, do not create an empty commit.

## Final Verification Checklist

Before reporting completion, collect this evidence:

```bash
cargo test -p fdc-ingestion --test source_batch_contract
cargo test -p fdc-ingestion
cargo check -p fdc-ingestion --all-targets
if grep -RInE "fdc-barter|fdc_barter" crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml; then
  echo "unexpected fdc-barter reference in fdc-ingestion"
  exit 1
else
  echo "no fdc-barter reference in fdc-ingestion"
fi
git status --short --branch
```

Expected final state:

- Source batch contract tests pass.
- All `fdc-ingestion` tests pass.
- `fdc-ingestion` all-targets check passes.
- No `fdc-barter` / `fdc_barter` reference in `fdc-ingestion`.
- Working tree clean after commits.

## Notes for Implementers

- Keep B1b in `source::batch`; do not modify the existing network byte path implementation in `crates/fdc-ingestion/src/batch.rs`.
- Use `Arc<RwLock<Vec<SourceBatchItem<T>>>>` and `Arc<RwLock<Option<Instant>>>` to match existing processor style.
- Drop batch locks before awaiting `sink.write_batch(...)`.
- Do not test timeout behavior with sleeps in this phase; size trigger and explicit `flush()` are deterministic.
- `SourceBatchSink<T>` receives owned `SourceBatchItem<T>` values, so tests can inspect moved items through a recording sink.
- If the `fdc_core::error::Error::storage` constructor name differs, inspect `crates/fdc-core/src/error.rs` and use the existing constructor that can preserve the text `sink unavailable` in `Display` output.
