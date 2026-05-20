# fdc-ingestion Phase B1c Source Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bounded `fdc-ingestion` source pipeline helper that validates finite `SourceEnvelope<T>` inputs, batches them through `SourceBatchProcessor<T>`, flushes remaining items, and returns aggregate counts.

**Architecture:** Create a focused `source::pipeline` module that only orchestrates existing B1a/B1b primitives. It stays generic over payload `T`, does not depend on `fdc-barter`, and avoids real stream/runtime/network/storage behavior. Contract tests use dummy payloads and in-memory sinks.

**Tech Stack:** Rust 2021, `tokio`, `async-trait`, `fdc_core::error::Result`, existing `fdc-ingestion` source modules, existing `BatchConfig`.

---

## Review Input

Read before implementation:

- `docs/superpowers/specs/2026-05-20-fdc-ingestion-phase-b1c-source-pipeline-design.md`
- `docs/DEVELOPMENT_STATUS.md`
- `docs/architecture/fdc-ingestion-phase-b1-source-path-pseudocode-review.md`, B1c section
- `crates/fdc-ingestion/src/source/batch.rs`
- `crates/fdc-ingestion/src/source/validator.rs`
- `crates/fdc-ingestion/src/source/envelope.rs`
- `crates/fdc-ingestion/src/source/mod.rs`
- `crates/fdc-ingestion/src/lib.rs`
- `crates/fdc-ingestion/tests/source_batch_contract.rs`

## Scope

### In Scope

- Create `crates/fdc-ingestion/src/source/pipeline.rs`.
- Add `SourcePipelineResult`.
- Add aggregate helper methods on `SourcePipelineResult`:
  - `processed_count()`
  - `success_count()`
  - `failure_count()`
  - `batch_count()`
- Add `run_source_pipeline_once<T, I>(...)`.
- Re-export pipeline API from `source::mod` and crate root.
- Add `crates/fdc-ingestion/tests/source_pipeline_contract.rs`.
- Verify `fdc-ingestion` has no `fdc-barter` / `fdc_barter` dependency.
- Update `docs/DEVELOPMENT_STATUS.md` after implementation.

### Out of Scope

- No Barter-specific adapter bridge.
- No real WebSocket or REST client.
- No real historical pagination.
- No transform sink implementation.
- No storage sink implementation.
- No checkpoint persistence.
- No stateful dedupe or gap detection.
- No background timers.
- No infinite stream/runtime loop.
- No warning cleanup in older modules.

## File Structure

### Create

- `crates/fdc-ingestion/src/source/pipeline.rs`
  - Owns bounded source pipeline orchestration and aggregate result type.
- `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
  - Contract tests for empty input, valid input, invalid filtering, batch-size result collection, sink error propagation, and public re-exports.

### Modify

- `crates/fdc-ingestion/src/source/mod.rs`
  - Add `pub mod pipeline;`.
  - Re-export `run_source_pipeline_once` and `SourcePipelineResult`.
- `crates/fdc-ingestion/src/lib.rs`
  - Re-export `run_source_pipeline_once` and `SourcePipelineResult` from crate root.
- `docs/DEVELOPMENT_STATUS.md`
  - After implementation, move B1c from next recommended slice to completed work and update verification evidence.

## Public API Target

After implementation, this should compile:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::{error::Result, types::TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourceEnvelope, SourcePipelineResult, SourceType, SourceValidator,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

struct RecordingSink {
    written: Arc<RwLock<Vec<SourceBatchItem<DummyMarketEvent>>>>,
}

#[async_trait]
impl SourceBatchSink<DummyMarketEvent> for RecordingSink {
    async fn write_batch(&self, items: Vec<SourceBatchItem<DummyMarketEvent>>) -> Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}

#[tokio::test]
async fn pipeline_validates_batches_and_flushes() {
    let sink = Arc::new(RecordingSink { written: Arc::new(RwLock::new(Vec::new())) });
    let processor = SourceBatchProcessor::new(
        BatchConfig { batch_size: 10, ..Default::default() },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent { symbol: "BTCUSDT".to_string(), price: 65000.25 },
    )];

    let result: SourcePipelineResult = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 1);
    assert_eq!(result.validation_success_count, 1);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.processed_count(), 1);
    assert_eq!(result.success_count(), 1);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(result.batch_count(), 1);
    assert_eq!(sink.written.read().await.len(), 1);
}
```

---

### Task 1: Empty Pipeline Contract and Minimal API Shell

**Files:**
- Create: `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
- Create: `crates/fdc-ingestion/src/source/pipeline.rs`
- Modify: `crates/fdc-ingestion/src/source/mod.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`

- [ ] **Step 1: Write the failing empty-input contract test**

Create `crates/fdc-ingestion/tests/source_pipeline_contract.rs` with this initial content:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use fdc_core::{error::Result, types::TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourceEnvelope, SourceType, SourceValidator,
};
use tokio::sync::RwLock;

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

struct RecordingSourcePipelineSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> Default for RecordingSourcePipelineSink<T> {
    fn default() -> Self {
        Self {
            written: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> RecordingSourcePipelineSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourcePipelineSink<T>
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
async fn source_pipeline_empty_input_returns_zero_counts() {
    let sink = Arc::new(RecordingSourcePipelineSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(BatchConfig::default(), sink.clone());
    let validator = SourceValidator::default();

    let result = run_source_pipeline_once(Vec::new(), &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 0);
    assert_eq!(result.validation_success_count, 0);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.processed_count(), 0);
    assert_eq!(result.success_count(), 0);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(result.batch_count(), 0);
    assert_eq!(result.batch_results.len(), 0);
    assert_eq!(sink.written_count().await, 0);
}
```

- [ ] **Step 2: Run test to verify it fails because the API is missing**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract source_pipeline_empty_input_returns_zero_counts
```

Expected: FAIL with unresolved imports for `run_source_pipeline_once` and missing `SourcePipelineResult`/pipeline API.

- [ ] **Step 3: Create the minimal pipeline module**

Create `crates/fdc-ingestion/src/source/pipeline.rs`:

```rust
use fdc_core::error::Result;

use super::{SourceBatchProcessor, SourceBatchResult, SourceEnvelope, SourceValidator};

#[derive(Debug, Clone, Default)]
pub struct SourcePipelineResult {
    pub input_count: usize,
    pub validation_success_count: usize,
    pub validation_failure_count: usize,
    pub batch_results: Vec<SourceBatchResult>,
}

impl SourcePipelineResult {
    pub fn processed_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.processed_count)
            .sum()
    }

    pub fn success_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.success_count)
            .sum()
    }

    pub fn failure_count(&self) -> usize {
        self.batch_results
            .iter()
            .map(|result| result.failure_count)
            .sum()
    }

    pub fn batch_count(&self) -> usize {
        self.batch_results.len()
    }
}

pub async fn run_source_pipeline_once<T, I>(
    envelopes: I,
    _validator: &SourceValidator,
    _processor: &SourceBatchProcessor<T>,
) -> Result<SourcePipelineResult>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = SourceEnvelope<T>>,
{
    let input_count = envelopes.into_iter().count();

    Ok(SourcePipelineResult {
        input_count,
        validation_success_count: 0,
        validation_failure_count: 0,
        batch_results: Vec::new(),
    })
}
```

- [ ] **Step 4: Export the minimal API**

Modify `crates/fdc-ingestion/src/source/mod.rs` to include the module and exports:

```rust
pub mod batch;
pub mod checkpoint;
pub mod envelope;
pub mod pipeline;
pub mod quality;
pub mod validator;

pub use batch::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchProcessorStats, SourceBatchResult,
    SourceBatchSink,
};
pub use checkpoint::{SourceCheckpoint, SourcePartition, SourcePosition};
pub use envelope::{SourceEnvelope, SourceMetadata, SourceType};
pub use pipeline::{run_source_pipeline_once, SourcePipelineResult};
pub use quality::SourceQualityFlags;
pub use validator::{
    SourceValidationError, SourceValidationErrorType, SourceValidationResult,
    SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

Modify the `pub use source::{ ... }` list in `crates/fdc-ingestion/src/lib.rs` to include the new exports:

```rust
pub use source::{
    run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor, SourceBatchProcessorStats,
    SourceBatchResult, SourceBatchSink, SourceCheckpoint, SourceEnvelope, SourceMetadata,
    SourcePartition, SourcePipelineResult, SourcePosition, SourceQualityFlags, SourceType,
    SourceValidationError, SourceValidationErrorType, SourceValidationResult,
    SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

- [ ] **Step 5: Run test to verify the empty input path passes**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract source_pipeline_empty_input_returns_zero_counts
```

Expected: PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add crates/fdc-ingestion/src/source/pipeline.rs \
    crates/fdc-ingestion/src/source/mod.rs \
    crates/fdc-ingestion/src/lib.rs \
    crates/fdc-ingestion/tests/source_pipeline_contract.rs
git commit -m "feat: add source pipeline API shell"
```

---

### Task 2: Validation, Batching, and Final Flush

**Files:**
- Modify: `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
- Modify: `crates/fdc-ingestion/src/source/pipeline.rs`

- [ ] **Step 1: Add failing contract tests for valid and invalid envelopes**

Append these tests to `crates/fdc-ingestion/tests/source_pipeline_contract.rs`:

```rust
#[tokio::test]
async fn source_pipeline_validates_batches_and_flushes_valid_items() {
    let sink = Arc::new(RecordingSourcePipelineSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        dummy_envelope("dummy-source", "BTCUSDT"),
        dummy_envelope("dummy-source", "ETHUSDT"),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 2);
    assert_eq!(result.validation_success_count, 2);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(result.success_count(), 2);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(result.batch_count(), 1);
    assert_eq!(sink.written_count().await, 2);

    let validator_stats = validator.get_stats().await;
    assert_eq!(validator_stats.messages_validated, 2);
    assert_eq!(validator_stats.validation_successes, 2);
    assert_eq!(validator_stats.validation_failures, 0);
}

#[tokio::test]
async fn source_pipeline_counts_invalid_items_without_writing_them_to_sink() {
    let sink = Arc::new(RecordingSourcePipelineSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        dummy_envelope("dummy-source", "BTCUSDT"),
        dummy_envelope("", "ETHUSDT"),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 2);
    assert_eq!(result.validation_success_count, 1);
    assert_eq!(result.validation_failure_count, 1);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(result.success_count(), 1);
    assert_eq!(result.failure_count(), 1);
    assert_eq!(result.batch_count(), 1);
    assert_eq!(sink.written_count().await, 1);
    assert_eq!(result.batch_results[0].errors.len(), 1);

    let validator_stats = validator.get_stats().await;
    assert_eq!(validator_stats.messages_validated, 2);
    assert_eq!(validator_stats.validation_successes, 1);
    assert_eq!(validator_stats.validation_failures, 1);
}
```

- [ ] **Step 2: Run tests and confirm current implementation is insufficient**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract source_pipeline_validates_batches_and_flushes_valid_items source_pipeline_counts_invalid_items_without_writing_them_to_sink
```

Expected: FAIL because `run_source_pipeline_once` currently only counts inputs and does not validate, add items, or flush.

- [ ] **Step 3: Implement validation, item wrapping, add_item, and flush**

Replace `run_source_pipeline_once` in `crates/fdc-ingestion/src/source/pipeline.rs` with:

```rust
pub async fn run_source_pipeline_once<T, I>(
    envelopes: I,
    validator: &SourceValidator,
    processor: &SourceBatchProcessor<T>,
) -> Result<SourcePipelineResult>
where
    T: Send + Sync + 'static,
    I: IntoIterator<Item = SourceEnvelope<T>>,
{
    let mut result = SourcePipelineResult::default();

    for envelope in envelopes {
        result.input_count += 1;

        let validation_result = validator.validate(&envelope).await;
        if validation_result.is_valid {
            result.validation_success_count += 1;
        } else {
            result.validation_failure_count += 1;
        }

        let item = super::SourceBatchItem::new(envelope, validation_result);
        if let Some(batch_result) = processor.add_item(item).await? {
            result.batch_results.push(batch_result);
        }
    }

    if let Some(batch_result) = processor.flush().await? {
        result.batch_results.push(batch_result);
    }

    Ok(result)
}
```

Then update the import list at the top of `pipeline.rs` to include `SourceBatchItem`:

```rust
use super::{
    SourceBatchItem, SourceBatchProcessor, SourceBatchResult, SourceEnvelope, SourceValidator,
};
```

- [ ] **Step 4: Run the new tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract source_pipeline_validates_batches_and_flushes_valid_items source_pipeline_counts_invalid_items_without_writing_them_to_sink
```

Expected: PASS.

- [ ] **Step 5: Run all source pipeline contract tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

```bash
git add crates/fdc-ingestion/src/source/pipeline.rs \
    crates/fdc-ingestion/tests/source_pipeline_contract.rs
git commit -m "feat: run bounded source pipeline"
```

---

### Task 3: Batch Result Collection and Sink Error Propagation

**Files:**
- Modify: `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
- Modify: `crates/fdc-ingestion/src/source/pipeline.rs` only if tests reveal a gap

- [ ] **Step 1: Add failing test for size-triggered batch results plus final flush**

Append this test to `crates/fdc-ingestion/tests/source_pipeline_contract.rs`:

```rust
#[tokio::test]
async fn source_pipeline_collects_size_triggered_and_final_flush_results() {
    let sink = Arc::new(RecordingSourcePipelineSink::<DummyMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 2,
            ..Default::default()
        },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        dummy_envelope("dummy-source", "BTCUSDT"),
        dummy_envelope("dummy-source", "ETHUSDT"),
        dummy_envelope("dummy-source", "SOLUSDT"),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap();

    assert_eq!(result.input_count, 3);
    assert_eq!(result.validation_success_count, 3);
    assert_eq!(result.validation_failure_count, 0);
    assert_eq!(result.batch_count(), 2);
    assert_eq!(result.batch_results[0].processed_count, 2);
    assert_eq!(result.batch_results[1].processed_count, 1);
    assert_eq!(result.processed_count(), 3);
    assert_eq!(result.success_count(), 3);
    assert_eq!(result.failure_count(), 0);
    assert_eq!(sink.written_count().await, 3);
}
```

- [ ] **Step 2: Add failing test for sink error propagation**

Append this sink and test to `crates/fdc-ingestion/tests/source_pipeline_contract.rs`:

```rust
struct FailingSourcePipelineSink;

#[async_trait]
impl<T> SourceBatchSink<T> for FailingSourcePipelineSink
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, _items: Vec<SourceBatchItem<T>>) -> Result<usize> {
        Err(fdc_core::error::Error::internal("pipeline sink failed"))
    }
}

#[tokio::test]
async fn source_pipeline_propagates_sink_errors() {
    let sink = Arc::new(FailingSourcePipelineSink);
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 1,
            ..Default::default()
        },
        sink,
    );
    let validator = SourceValidator::default();
    let envelopes = vec![dummy_envelope("dummy-source", "BTCUSDT")];

    let error = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("pipeline sink failed"));
}
```

- [ ] **Step 3: Run the two tests to verify behavior**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract source_pipeline_collects_size_triggered_and_final_flush_results source_pipeline_propagates_sink_errors
```

Expected after Task 2 implementation: PASS. If either fails, fix only `crates/fdc-ingestion/src/source/pipeline.rs` so batch results from `add_item` and final `flush` are collected and sink errors propagate with `?`.

- [ ] **Step 4: Run all source pipeline contract tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract
```

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add crates/fdc-ingestion/src/source/pipeline.rs \
    crates/fdc-ingestion/tests/source_pipeline_contract.rs
git commit -m "test: cover source pipeline batch results"
```

---

### Task 4: Final Verification and Development Status Update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run formatting**

Run:

```bash
cargo fmt --package fdc-ingestion
```

Expected: command exits 0.

- [ ] **Step 2: Run source pipeline contract tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_pipeline_contract
```

Expected: all source pipeline contract tests pass.

- [ ] **Step 3: Run fdc-ingestion tests**

Run:

```bash
cargo test -p fdc-ingestion
```

Expected: all `fdc-ingestion` tests pass. Existing warnings may remain.

- [ ] **Step 4: Run fdc-barter plus fdc-ingestion tests**

Run:

```bash
cargo test -p fdc-barter -p fdc-ingestion
```

Expected: all tests pass. Existing warnings may remain.

- [ ] **Step 5: Run dependency guard**

Run:

```bash
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: command exits 0 with no matches.

- [ ] **Step 6: Update `docs/DEVELOPMENT_STATUS.md`**

Modify `docs/DEVELOPMENT_STATUS.md`:

1. Add a completed section for `fdc-ingestion Phase B1c: Source Pipeline Helper` with:
   - `SourcePipelineResult`
   - `run_source_pipeline_once`
   - validation + batching + final flush behavior
   - tests in `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
2. Update the current verification baseline with the commands run in Steps 2-5.
3. Move the next recommended slice to `B2: fdc-barter to SourceEnvelope bridge`.
4. Keep the boundary that `fdc-ingestion` must not depend on `fdc-barter`.

Use this text as the B1c completed section:

```markdown
### fdc-ingestion Phase B1c: Source Pipeline Helper

Implemented in `crates/fdc-ingestion/src/source/pipeline.rs`.

Completed capabilities:

- Added `SourcePipelineResult` aggregate counts.
- Added `run_source_pipeline_once` for finite source envelope collections.
- The helper validates each `SourceEnvelope<T>` with `SourceValidator`.
- The helper wraps validation results into `SourceBatchItem<T>`.
- The helper sends items through `SourceBatchProcessor<T>`.
- The helper collects size-triggered batch results and final flush results.
- Validation failures are counted and flow through existing batch invalid-item filtering.
- Sink errors propagate to the caller.
- `fdc-ingestion` remains independent from `fdc-barter`.

Contract tests:

- `crates/fdc-ingestion/tests/source_pipeline_contract.rs`
```

- [ ] **Step 7: Inspect git diff for unrelated churn**

Run:

```bash
git diff --stat
git diff -- crates/fdc-ingestion/src/source/pipeline.rs \
    crates/fdc-ingestion/src/source/mod.rs \
    crates/fdc-ingestion/src/lib.rs \
    crates/fdc-ingestion/tests/source_pipeline_contract.rs \
    docs/DEVELOPMENT_STATUS.md | sed -n '1,260p'
```

Expected: only B1c source pipeline files and development status changed.

- [ ] **Step 8: Commit Task 4**

```bash
git add crates/fdc-ingestion/src/source/pipeline.rs \
    crates/fdc-ingestion/src/source/mod.rs \
    crates/fdc-ingestion/src/lib.rs \
    crates/fdc-ingestion/tests/source_pipeline_contract.rs \
    docs/DEVELOPMENT_STATUS.md
git commit -m "docs: update source pipeline status"
```

## Final Acceptance Criteria

- `run_source_pipeline_once` is public from both `fdc_ingestion::source` and crate root.
- `SourcePipelineResult` is public from both `fdc_ingestion::source` and crate root.
- Empty input returns zero counts and no sink writes.
- Valid finite input validates, batches, flushes, and writes to sink.
- Invalid input is counted as validation failure and filtered before sink writes.
- Size-triggered batch results and final flush results are collected.
- Sink errors propagate as `Err`.
- `fdc-ingestion` does not depend on `fdc-barter`.
- `cargo test -p fdc-ingestion --test source_pipeline_contract` passes.
- `cargo test -p fdc-ingestion` passes.
- `cargo test -p fdc-barter -p fdc-ingestion` passes.
- `docs/DEVELOPMENT_STATUS.md` points the next slice at B2 barter-to-source bridge.
