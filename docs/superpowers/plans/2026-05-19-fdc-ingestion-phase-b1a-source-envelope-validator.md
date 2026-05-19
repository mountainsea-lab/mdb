# fdc-ingestion Phase B1a Source Envelope and Validator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first slice of the `fdc-ingestion` structured source path: generic source envelope, source checkpoint, source quality flags, source metadata, and stateless source validation.

**Architecture:** Phase B1a adds a new `source` module under `fdc-ingestion` without changing the existing network byte path. The new module defines generic `SourceEnvelope<T>` plus validation types and a `SourceValidator` that checks envelope metadata and quality flags. It does not depend on `fdc-barter`, does not start network streams, and does not implement batching or transform sinks yet.

**Tech Stack:** Rust 2021, `fdc-core`, `serde`, `serde_json`, `chrono`, `uuid`, `tokio`, existing `fdc-ingestion::buffer::DataBuffer`.

---

## Review Input

Read before implementation:

- `docs/architecture/fdc-ingestion-phase-b1-source-path-pseudocode-review.md`
- `crates/fdc-ingestion/src/buffer.rs`
- `crates/fdc-ingestion/src/validator.rs`
- `crates/fdc-ingestion/src/lib.rs`

## Scope

### In Scope

- Create `crates/fdc-ingestion/src/source/` module tree.
- Add generic `SourceEnvelope<T>`.
- Add `SourceType`.
- Add `SourceCheckpoint`, `SourcePartition`, and `SourcePosition`.
- Add `SourceQualityFlags`.
- Add `SourceMetadata`.
- Add `SourceValidator`, `SourceValidationResult`, `SourceValidationError`, `SourceValidationWarning`, and stats.
- Re-export source path types from `fdc-ingestion` crate root.
- Add tests proving `DataBuffer<SourceEnvelope<T>>` works with structured events.
- Keep existing network byte path unchanged.

### Out of Scope

- No `SourceBatchItem` or `SourceBatchProcessor`. That is B1b.
- No source pipeline runner. That is B1c.
- No `fdc-barter` dependency from `fdc-ingestion`.
- No `fdc-transform` sink.
- No checkpoint persistence.
- No dedupe cache or stateful gap detection.
- No real WebSocket, REST, storage, or network I/O.

## File Structure

### Create

- `crates/fdc-ingestion/src/source/mod.rs`
- `crates/fdc-ingestion/src/source/envelope.rs`
- `crates/fdc-ingestion/src/source/checkpoint.rs`
- `crates/fdc-ingestion/src/source/quality.rs`
- `crates/fdc-ingestion/src/source/validator.rs`
- `crates/fdc-ingestion/tests/source_envelope_contract.rs`
- `crates/fdc-ingestion/tests/source_validator_contract.rs`

### Modify

- `crates/fdc-ingestion/src/lib.rs`

## Public API Target

After B1a, this should compile:

```rust
use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    DataBuffer, SourceEnvelope, SourceMetadata, SourceQualityFlags, SourceType, SourceValidator,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

#[tokio::test]
async fn source_envelope_can_flow_through_data_buffer() {
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent { symbol: "BTCUSDT".to_string(), price: 65000.25 },
    )
    .with_metadata(SourceMetadata {
        adapter: Some("dummy".to_string()),
        exchange: Some("binance_spot".to_string()),
        symbol: Some("BTCUSDT".to_string()),
        kind: Some("Trade".to_string()),
        attributes: std::collections::BTreeMap::new(),
    })
    .with_quality(SourceQualityFlags::default());

    let buffer = DataBuffer::new(fdc_ingestion::config::BufferConfig {
        buffer_size: 2,
        ..Default::default()
    });

    buffer.enqueue(envelope.clone()).await.unwrap();
    let dequeued = buffer.dequeue().await.unwrap();

    assert_eq!(dequeued.source_id, "dummy-source");
    assert_eq!(dequeued.payload.symbol, "BTCUSDT");
}
```

## Task 1: Source Module Shell and Envelope Types

**Files:**
- Create: `crates/fdc-ingestion/src/source/mod.rs`
- Create: `crates/fdc-ingestion/src/source/envelope.rs`
- Create: `crates/fdc-ingestion/src/source/checkpoint.rs`
- Create: `crates/fdc-ingestion/src/source/quality.rs`
- Modify: `crates/fdc-ingestion/src/lib.rs`
- Test: `crates/fdc-ingestion/tests/source_envelope_contract.rs`

- [ ] **Step 1: Write failing source envelope contract test**

Create `crates/fdc-ingestion/tests/source_envelope_contract.rs`:

```rust
use std::collections::BTreeMap;

use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    config::BufferConfig, DataBuffer, SourceCheckpoint, SourceEnvelope, SourceMetadata,
    SourcePartition, SourcePosition, SourceQualityFlags, SourceType,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
    price: f64,
}

#[tokio::test]
async fn source_envelope_can_flow_through_data_buffer() {
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
            price: 65000.25,
        },
    )
    .with_metadata(SourceMetadata {
        adapter: Some("dummy".to_string()),
        exchange: Some("binance_spot".to_string()),
        symbol: Some("BTCUSDT".to_string()),
        kind: Some("Trade".to_string()),
        attributes: BTreeMap::new(),
    })
    .with_quality(SourceQualityFlags::default())
    .with_checkpoint(SourceCheckpoint {
        checkpoint_id: "cp-1".to_string(),
        source_id: "dummy-source".to_string(),
        partition: SourcePartition {
            exchange: Some("binance_spot".to_string()),
            symbol: Some("BTCUSDT".to_string()),
            kind: Some("Trade".to_string()),
            shard: None,
        },
        position: SourcePosition::Timestamp(TimestampNs::from_nanos(1_000)),
        updated_at: TimestampNs::from_nanos(1_200),
    });

    let buffer = DataBuffer::new(BufferConfig {
        buffer_size: 2,
        ..Default::default()
    });

    buffer.enqueue(envelope.clone()).await.unwrap();
    let dequeued = buffer.dequeue().await.unwrap();

    assert_eq!(dequeued.source_id, "dummy-source");
    assert_eq!(dequeued.source_type, SourceType::MarketData);
    assert_eq!(dequeued.payload.symbol, "BTCUSDT");
    assert_eq!(dequeued.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(dequeued.checkpoint.as_ref().unwrap().checkpoint_id, "cp-1");
}

#[test]
fn source_envelope_can_attach_optional_checkpoint() {
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "ETHUSDT".to_string(),
            price: 3500.0,
        },
    )
    .with_optional_checkpoint(None);

    assert!(envelope.checkpoint.is_none());
    assert!(!envelope.envelope_id.is_empty());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p fdc-ingestion --test source_envelope_contract
```

Expected: FAIL because `SourceEnvelope`, `SourceType`, `SourceCheckpoint`, `SourcePartition`, `SourcePosition`, `SourceMetadata`, and `SourceQualityFlags` are not defined.

- [ ] **Step 3: Implement source module exports**

Create `crates/fdc-ingestion/src/source/mod.rs`:

```rust
pub mod checkpoint;
pub mod envelope;
pub mod quality;
pub mod validator;

pub use checkpoint::{SourceCheckpoint, SourcePartition, SourcePosition};
pub use envelope::{SourceEnvelope, SourceMetadata, SourceType};
pub use quality::SourceQualityFlags;
pub use validator::{
    SourceValidationError, SourceValidationErrorType, SourceValidationResult,
    SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

Modify `crates/fdc-ingestion/src/lib.rs` by adding the module and re-exports:

```rust
pub mod source;         // 结构化 source path

pub use source::{
    SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition,
    SourceQualityFlags, SourceType, SourceValidationError, SourceValidationErrorType,
    SourceValidationResult, SourceValidationWarning, SourceValidator, SourceValidatorStats,
};
```

Keep all existing exports unchanged.

- [ ] **Step 4: Implement checkpoint types**

Create `crates/fdc-ingestion/src/source/checkpoint.rs`:

```rust
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
```

- [ ] **Step 5: Implement quality flags**

Create `crates/fdc-ingestion/src/source/quality.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SourceQualityFlags {
    pub is_replay: bool,
    pub is_backfill: bool,
    pub is_duplicate_candidate: bool,
    pub has_gap_before: bool,
    pub is_out_of_order: bool,
}
```

- [ ] **Step 6: Implement envelope types and builders**

Create `crates/fdc-ingestion/src/source/envelope.rs`:

```rust
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
```

- [ ] **Step 7: Add validator placeholder to satisfy module exports**

Create `crates/fdc-ingestion/src/source/validator.rs` with minimal compile-only types. Full behavior comes in Task 2.

```rust
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
```

- [ ] **Step 8: Run tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_envelope_contract
cargo test -p fdc-ingestion
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/fdc-ingestion
git commit -m "feat: add fdc-ingestion source envelope model"
```

## Task 2: Stateless Source Validator

**Files:**
- Modify: `crates/fdc-ingestion/src/source/validator.rs`
- Test: `crates/fdc-ingestion/tests/source_validator_contract.rs`

- [ ] **Step 1: Write failing validator contract test**

Create `crates/fdc-ingestion/tests/source_validator_contract.rs`:

```rust
use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    SourceEnvelope, SourceQualityFlags, SourceValidationErrorType, SourceType, SourceValidator,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
}

fn valid_envelope() -> SourceEnvelope<DummyMarketEvent> {
    SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    )
}

#[tokio::test]
async fn validator_accepts_valid_source_envelope() {
    let validator = SourceValidator::default();
    let result = validator.validate(&valid_envelope()).await;

    assert!(result.is_valid);
    assert!(result.errors.is_empty());
    assert!(result.warnings.is_empty());
}

#[tokio::test]
async fn validator_rejects_empty_source_id() {
    let validator = SourceValidator::default();
    let envelope = SourceEnvelope::new(
        "   ",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    let result = validator.validate(&envelope).await;

    assert!(!result.is_valid);
    assert_eq!(result.errors[0].error_type, SourceValidationErrorType::EmptySourceId);
}

#[tokio::test]
async fn validator_rejects_invalid_event_time() {
    let validator = SourceValidator::default();
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(0),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    let result = validator.validate(&envelope).await;

    assert!(!result.is_valid);
    assert_eq!(result.errors[0].error_type, SourceValidationErrorType::InvalidEventTime);
}

#[tokio::test]
async fn validator_warns_for_quality_flags() {
    let validator = SourceValidator::default();
    let mut quality = SourceQualityFlags::default();
    quality.is_duplicate_candidate = true;
    quality.has_gap_before = true;
    quality.is_out_of_order = true;

    let envelope = valid_envelope().with_quality(quality);
    let result = validator.validate(&envelope).await;

    assert!(result.is_valid);
    assert_eq!(result.warnings.len(), 3);
    assert!(result.warnings.iter().any(|warning| warning.warning_type == "duplicate_candidate"));
    assert!(result.warnings.iter().any(|warning| warning.warning_type == "gap_before"));
    assert!(result.warnings.iter().any(|warning| warning.warning_type == "out_of_order"));
}

#[tokio::test]
async fn validator_tracks_stats() {
    let validator = SourceValidator::default();

    let valid = valid_envelope();
    let invalid = SourceEnvelope::new(
        "",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    validator.validate(&valid).await;
    validator.validate(&invalid).await;
    let stats = validator.get_stats().await;

    assert_eq!(stats.messages_validated, 2);
    assert_eq!(stats.validation_successes, 1);
    assert_eq!(stats.validation_failures, 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p fdc-ingestion --test source_validator_contract
```

Expected: FAIL because `SourceValidator::validate`, `SourceValidator::get_stats`, and full validation behavior are not implemented.

- [ ] **Step 3: Implement validator behavior**

Replace `crates/fdc-ingestion/src/source/validator.rs` with:

```rust
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
```

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test -p fdc-ingestion --test source_validator_contract
cargo test -p fdc-ingestion
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-ingestion
git commit -m "feat: add fdc-ingestion source validator"
```

## Task 3: B1a Final Verification

- [ ] **Step 1: Run fdc-ingestion tests**

```bash
cargo test -p fdc-ingestion
```

Expected: PASS.

- [ ] **Step 2: Run fdc-ingestion check**

```bash
cargo check -p fdc-ingestion --all-targets
```

Expected: exits 0. Existing warnings outside changed source module may remain.

- [ ] **Step 3: Confirm no fdc-barter dependency was added**

```bash
grep -n "fdc-barter\|fdc_barter" crates/fdc-ingestion/Cargo.toml crates/fdc-ingestion/src/source/*.rs crates/fdc-ingestion/tests/*.rs || true
```

Expected: no matches.

- [ ] **Step 4: Commit cleanup only if needed**

```bash
git status --short
```

Expected: clean after Task 1 and Task 2 commits.

## Review Checklist

- [ ] `fdc-ingestion` has a new generic source envelope model.
- [ ] Existing network byte path remains unchanged.
- [ ] `DataBuffer<SourceEnvelope<T>>` works in a test.
- [ ] Validator rejects empty `source_id`.
- [ ] Validator rejects non-positive `event_time`.
- [ ] Validator turns quality flags into warnings.
- [ ] Validator stats count success/failure.
- [ ] `fdc-ingestion` does not depend on `fdc-barter`.
- [ ] No batching, pipeline runner, transform sink, storage, or network code is introduced in B1a.

## Follow-up Plans

After B1a is implemented and verified, write a separate plan for:

- Phase B1b: `SourceBatchItem<T>`, `SourceBatchSink<T>`, `SourceBatchProcessor<T>`, `SourceBatchResult`.
- Phase B1c: bounded source pipeline helper and demo fixture.
