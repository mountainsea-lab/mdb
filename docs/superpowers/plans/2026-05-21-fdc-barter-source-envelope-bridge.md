# fdc-barter SourceEnvelope Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the B2 bridge that converts `fdc-barter` ingestion envelopes into `fdc-ingestion` `SourceEnvelope<BarterMarketEvent>` values and proves they flow through the bounded source pipeline.

**Architecture:** Add a focused bridge module inside `fdc-barter`'s ingestion boundary. `fdc-barter` depends on `fdc-ingestion`; `fdc-ingestion` remains generic and must not reference `fdc-barter`.

**Tech Stack:** Rust 1.95, Cargo workspace, `fdc-barter`, `fdc-ingestion`, `tokio`, `async-trait`, contract tests.

---

## Approved Spec

Read first:

- `docs/superpowers/specs/2026-05-21-fdc-barter-source-envelope-bridge-design.md`
- `docs/DEVELOPMENT_STATUS.md`

## File Structure

Create or modify these files only unless a test failure proves another file is required:

- Modify: `crates/fdc-adapter/barter/Cargo.toml`
  - Add `fdc-ingestion = { path = "../../fdc-ingestion" }` to `[dependencies]`.
- Create: `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`
  - Owns conversion from `BarterIngestionEnvelope` / `BarterCheckpoint` / `DataQualityFlags` to `SourceEnvelope<BarterMarketEvent>`.
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Exports the bridge module and public bridge trait.
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
  - Re-exports the public bridge trait at crate root.
- Create: `crates/fdc-adapter/barter/tests/source_bridge_contract.rs`
  - Contract tests for field mapping, checkpoint mapping, bounded pipeline flow, and dependency direction guard.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Update only after implementation and verification, with B2 completion evidence and next recommended slice.

Do not modify `crates/fdc-ingestion` implementation files for this plan.

---

### Task 1: Add failing bridge mapping contract tests

**Files:**
- Create: `crates/fdc-adapter/barter/tests/source_bridge_contract.rs`
- Modify: `crates/fdc-adapter/barter/Cargo.toml`

- [ ] **Step 1: Add required test/runtime dependencies**

Edit `crates/fdc-adapter/barter/Cargo.toml` so it contains these dependencies:

```toml
[dependencies]
fdc-core = { path = "../../fdc-core" }
fdc-ingestion = { path = "../../fdc-ingestion" }
barter-data = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-data" }
barter-instrument = { path = "/Volumes/wdata/mountainsea-lab/barter-rs/barter-instrument" }
chrono = { workspace = true, features = ["serde"] }
serde = { workspace = true, features = ["derive"] }
thiserror = { workspace = true }
rust_decimal = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
async-trait = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt", "sync"] }
```

If `[dev-dependencies]` already exists but is empty, fill it with the two entries shown in this step.

- [ ] **Step 2: Write failing contract tests**

Create `crates/fdc-adapter/barter/tests/source_bridge_contract.rs` with this complete content:

```rust
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use fdc_barter::{
    BarterCheckpoint, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode,
    BarterMarketEvent, BarterMarketPayload, DataQualityFlags, HistoricalCursor,
    IntoSourceEnvelope, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourcePosition, SourceType, SourceValidator,
};
use rust_decimal::Decimal;
use tokio::sync::RwLock;

fn trade_event(mode: BarterMarketDataMode) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter-rs".to_string(),
        mode,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_000),
        received_at: TimestampNs::from_nanos(1_100),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("trade-1".to_string()),
            price: Price::from_f64(65_000.25).unwrap(),
            quantity: Decimal::new(25, 1),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("seq-42".to_string()),
        checkpoint: None,
    }
}

fn live_envelope() -> BarterIngestionEnvelope {
    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-1".to_string(),
        source_id: "barter-live-binance-btcusdt".to_string(),
        emitted_at: TimestampNs::from_nanos(1_200),
        event: trade_event(BarterMarketDataMode::Live),
        checkpoint: None,
        quality: DataQualityFlags {
            is_replay: false,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: true,
            is_out_of_order: false,
        },
    }
}

fn checkpoint_with_cursor(cursor: HistoricalCursor) -> BarterCheckpoint {
    BarterCheckpoint {
        source_id: "barter-historical-binance-btcusdt".to_string(),
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        mode: BarterMarketDataMode::Historical,
        last_event_time: TimestampNs::from_nanos(2_000),
        cursor: Some(cursor),
        updated_at: TimestampNs::from_nanos(2_100),
    }
}

fn historical_envelope_with_checkpoint(checkpoint: BarterCheckpoint) -> BarterIngestionEnvelope {
    let mut event = trade_event(BarterMarketDataMode::Historical);
    event.checkpoint = Some(checkpoint.clone());

    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-historical-1".to_string(),
        source_id: "barter-historical-binance-btcusdt".to_string(),
        emitted_at: TimestampNs::from_nanos(2_200),
        event,
        checkpoint: Some(checkpoint),
        quality: DataQualityFlags::default(),
    }
}

#[test]
fn live_trade_envelope_maps_identity_timing_payload_quality_and_metadata() {
    let source = live_envelope();
    let converted = source.into_source_envelope();

    assert_eq!(converted.envelope_id, "barter-envelope-1");
    assert_eq!(converted.source_id, "barter-live-binance-btcusdt");
    assert_eq!(converted.source_type, SourceType::MarketData);
    assert_eq!(converted.sequence.as_deref(), Some("seq-42"));
    assert_eq!(converted.event_time, TimestampNs::from_nanos(1_000));
    assert_eq!(converted.received_at, TimestampNs::from_nanos(1_100));
    assert_eq!(converted.emitted_at, TimestampNs::from_nanos(1_200));

    assert_eq!(converted.payload.exchange, "binance_spot");
    assert_eq!(converted.payload.symbol.to_string(), "BTCUSDT");
    assert_eq!(converted.payload.kind, BarterMarketDataKind::Trade);

    assert!(converted.quality.is_duplicate_candidate);
    assert!(converted.quality.has_gap_before);
    assert!(!converted.quality.is_backfill);
    assert!(!converted.quality.is_replay);
    assert!(!converted.quality.is_out_of_order);

    assert_eq!(converted.metadata.adapter.as_deref(), Some("barter-rs"));
    assert_eq!(converted.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(converted.metadata.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(converted.metadata.kind.as_deref(), Some("Trade"));
    assert_eq!(converted.metadata.attributes.get("mode").map(String::as_str), Some("Live"));
    assert_eq!(converted.metadata.attributes.get("payload_kind").map(String::as_str), Some("Trade"));
}

#[test]
fn historical_envelope_maps_to_replay_backfill_source_semantics() {
    let cursor = HistoricalCursor {
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        next_start: Some(TimestampNs::from_nanos(3_000)),
        page_token: None,
        last_seen_exchange_id: None,
    };
    let source = historical_envelope_with_checkpoint(checkpoint_with_cursor(cursor));

    let converted = source.into_source_envelope();

    assert_eq!(converted.source_type, SourceType::Replay);
    assert!(converted.quality.is_backfill);
    assert_eq!(converted.metadata.attributes.get("mode").map(String::as_str), Some("Historical"));
}

#[test]
fn barter_checkpoint_maps_to_source_checkpoint_partition_and_page_token_position() {
    let cursor = HistoricalCursor {
        exchange: "binance_spot".to_string(),
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        next_start: Some(TimestampNs::from_nanos(3_000)),
        page_token: Some("page-token-1".to_string()),
        last_seen_exchange_id: Some("trade-99".to_string()),
    };
    let source = historical_envelope_with_checkpoint(checkpoint_with_cursor(cursor));

    let converted = source.into_source_envelope();
    let checkpoint = converted.checkpoint.expect("checkpoint should map");

    assert_eq!(checkpoint.checkpoint_id, "barter-historical-binance-btcusdt:binance_spot:BTCUSDT:Trade:Historical:2000");
    assert_eq!(checkpoint.source_id, "barter-historical-binance-btcusdt");
    assert_eq!(checkpoint.partition.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(checkpoint.partition.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(checkpoint.partition.kind.as_deref(), Some("Trade"));
    assert_eq!(checkpoint.partition.shard, None);
    assert_eq!(checkpoint.position, SourcePosition::PageToken("page-token-1".to_string()));
    assert_eq!(checkpoint.updated_at, TimestampNs::from_nanos(2_100));
}

struct RecordingSourceBridgeSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

impl<T> Default for RecordingSourceBridgeSink<T> {
    fn default() -> Self {
        Self {
            written: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> RecordingSourceBridgeSink<T> {
    async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourceBridgeSink<T>
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> fdc_core::error::Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}

#[tokio::test]
async fn bridged_barter_envelopes_run_through_bounded_source_pipeline() {
    let sink = Arc::new(RecordingSourceBridgeSink::<BarterMarketEvent>::default());
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        sink.clone(),
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        live_envelope().into_source_envelope(),
        live_envelope().into_source_envelope(),
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
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter() {
    let output = Command::new("grep")
        .args([
            "-R",
            "fdc-barter\\|fdc_barter",
            "-n",
            "crates/fdc-ingestion",
            "Cargo.toml",
            "crates/fdc-ingestion/Cargo.toml",
        ])
        .current_dir(format!("{}/../../..", env!("CARGO_MANIFEST_DIR")))
        .output()
        .expect("grep should run");

    assert!(!output.status.success(), "fdc-ingestion must not reference fdc-barter");
}
```

- [ ] **Step 3: Run the contract test to verify it fails for missing API**

Run:

```bash
rtk cargo test -p fdc-barter --test source_bridge_contract
```

Expected: FAIL with an unresolved import or missing item for `IntoSourceEnvelope` because the bridge has not been implemented yet.

- [ ] **Step 4: Commit the failing tests**

```bash
git add crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/tests/source_bridge_contract.rs
git commit -m "test: cover fdc-barter source bridge contracts"
```

---

### Task 2: Implement the bridge API and field mapping

**Files:**
- Create: `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Add the bridge implementation**

Create `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs` with this content:

```rust
use std::collections::BTreeMap;

use fdc_ingestion::{
    SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition,
    SourceQualityFlags, SourceType,
};

use crate::ingestion::{BarterIngestionEnvelope, DataQualityFlags};
use crate::model::{
    BarterCheckpoint, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    HistoricalCursor,
};

/// Converts Barter-specific ingestion envelopes into generic fdc-ingestion source envelopes.
pub trait IntoSourceEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent>;
}

impl IntoSourceEnvelope for BarterIngestionEnvelope {
    fn into_source_envelope(self) -> SourceEnvelope<BarterMarketEvent> {
        let BarterIngestionEnvelope {
            envelope_id,
            source_id,
            emitted_at,
            event,
            checkpoint,
            quality,
        } = self;

        let source_type = match event.mode {
            BarterMarketDataMode::Live => SourceType::MarketData,
            BarterMarketDataMode::Historical => SourceType::Replay,
        };
        let sequence = event.sequence.clone();
        let event_time = event.timestamp;
        let received_at = event.received_at;
        let metadata = source_metadata(&event);
        let quality = source_quality(quality, event.mode);
        let checkpoint = checkpoint.map(source_checkpoint);

        SourceEnvelope {
            envelope_id,
            source_id,
            source_type,
            sequence,
            event_time,
            received_at,
            emitted_at,
            payload: event,
            checkpoint,
            quality,
            metadata,
        }
    }
}

fn source_metadata(event: &BarterMarketEvent) -> SourceMetadata {
    let mut attributes = BTreeMap::new();
    attributes.insert("mode".to_string(), format!("{:?}", event.mode));
    attributes.insert("payload_kind".to_string(), format!("{:?}", payload_kind(&event.payload)));

    SourceMetadata {
        adapter: Some(event.source.clone()),
        exchange: Some(event.exchange.clone()),
        symbol: Some(event.symbol.to_string()),
        kind: Some(format!("{:?}", event.kind)),
        attributes,
    }
}

fn payload_kind(payload: &BarterMarketPayload) -> crate::model::BarterMarketDataKind {
    payload.kind()
}

fn source_quality(quality: DataQualityFlags, mode: BarterMarketDataMode) -> SourceQualityFlags {
    SourceQualityFlags {
        is_replay: quality.is_replay,
        is_backfill: quality.is_backfill || mode == BarterMarketDataMode::Historical,
        is_duplicate_candidate: quality.is_duplicate_candidate,
        has_gap_before: quality.has_gap_before,
        is_out_of_order: quality.is_out_of_order,
    }
}

fn source_checkpoint(checkpoint: BarterCheckpoint) -> SourceCheckpoint {
    let checkpoint_id = format!(
        "{}:{}:{}:{:?}:{:?}:{}",
        checkpoint.source_id,
        checkpoint.exchange,
        checkpoint.symbol,
        checkpoint.kind,
        checkpoint.mode,
        checkpoint.last_event_time.as_nanos()
    );

    SourceCheckpoint {
        checkpoint_id,
        source_id: checkpoint.source_id,
        partition: SourcePartition {
            exchange: Some(checkpoint.exchange),
            symbol: Some(checkpoint.symbol),
            kind: Some(format!("{:?}", checkpoint.kind)),
            shard: None,
        },
        position: source_position(checkpoint.cursor.as_ref(), checkpoint.last_event_time),
        updated_at: checkpoint.updated_at,
    }
}

fn source_position(
    cursor: Option<&HistoricalCursor>,
    last_event_time: fdc_core::types::TimestampNs,
) -> SourcePosition {
    if let Some(cursor) = cursor {
        if let Some(page_token) = &cursor.page_token {
            return SourcePosition::PageToken(page_token.clone());
        }

        if let Some(next_start) = cursor.next_start {
            return SourcePosition::Timestamp(next_start);
        }

        if let Some(last_seen_exchange_id) = &cursor.last_seen_exchange_id {
            return SourcePosition::Sequence(last_seen_exchange_id.clone());
        }
    }

    SourcePosition::Timestamp(last_event_time)
}
```

- [ ] **Step 2: Export the bridge module and trait from ingestion**

Edit `crates/fdc-adapter/barter/src/ingestion/mod.rs` to this content:

```rust
pub mod envelope;
pub mod source_bridge;

pub use envelope::{BarterIngestionEnvelope, DataQualityFlags};
pub use source_bridge::IntoSourceEnvelope;
```

- [ ] **Step 3: Re-export the bridge trait from the crate root**

Edit `crates/fdc-adapter/barter/src/lib.rs` so the ingestion export line becomes:

```rust
pub use ingestion::{BarterIngestionEnvelope, DataQualityFlags, IntoSourceEnvelope};
```

Keep all other existing exports unchanged.

- [ ] **Step 4: Run the bridge contract test**

Run:

```bash
rtk cargo test -p fdc-barter --test source_bridge_contract
```

Expected: PASS for all tests in `source_bridge_contract`.

- [ ] **Step 5: Run focused package tests**

Run:

```bash
rtk cargo test -p fdc-barter
```

Expected: PASS for existing `fdc-barter` tests plus the new source bridge contract test.

- [ ] **Step 6: Commit the bridge implementation**

```bash
git add crates/fdc-adapter/barter/src/ingestion/source_bridge.rs crates/fdc-adapter/barter/src/ingestion/mod.rs crates/fdc-adapter/barter/src/lib.rs
git commit -m "feat: bridge barter envelopes to source envelopes"
```

---

### Task 3: Verify package integration and dependency boundary

**Files:**
- No source files should be modified in this task unless verification exposes a compile error from Task 2.

- [ ] **Step 1: Run formatting**

Run:

```bash
rtk cargo fmt --package fdc-barter
```

Expected: exit code 0.

If formatting changes files, inspect them with:

```bash
rtk git diff -- crates/fdc-adapter/barter
```

- [ ] **Step 2: Run combined package tests**

Run:

```bash
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Expected: PASS for both packages. Existing warnings may remain.

- [ ] **Step 3: Run dependency guard**

Run:

```bash
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: exit code 0 and no matches from `crates/fdc-ingestion`.

- [ ] **Step 4: Commit formatting changes if any**

If `rtk git status --short` shows formatting changes under `crates/fdc-adapter/barter`, run:

```bash
git add crates/fdc-adapter/barter
git commit -m "style: format fdc-barter source bridge"
```

If there are no changes, do not create an empty commit.

---

### Task 4: Update development status checkpoint

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update the status document**

Edit `docs/DEVELOPMENT_STATUS.md`:

1. Change `Last updated:` to `2026-05-21`.
2. Change `Latest checkpoint commit when this file was written:` to the latest B2 commit hash after Task 3.
3. Add a new completed-work subsection after B1c:

```markdown
### fdc-barter Phase B2: SourceEnvelope Bridge

Implemented in `crates/fdc-adapter/barter/src/ingestion/source_bridge.rs`.

Completed capabilities:

- Added `IntoSourceEnvelope` for converting `BarterIngestionEnvelope` into `SourceEnvelope<BarterMarketEvent>`.
- Preserved Barter envelope identity, source id, sequence, timing, payload, quality flags, metadata, and checkpoint hints.
- Mapped live events to `SourceType::MarketData`.
- Mapped historical events to `SourceType::Replay` and ensured historical events carry backfill semantics.
- Mapped `BarterCheckpoint` into `SourceCheckpoint` without persistence.
- Demonstrated bounded Barter fixture flow through `run_source_pipeline_once` with a recording sink.
- Preserved the dependency boundary: `fdc-ingestion` still has no `fdc-barter` dependency.

Contract tests:

- `crates/fdc-adapter/barter/tests/source_bridge_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-21-fdc-barter-source-envelope-bridge-design.md`
- `docs/superpowers/plans/2026-05-21-fdc-barter-source-envelope-bridge.md`
```

4. Replace the B2 next-slice section with a later-work recommendation:

```markdown
## Next Recommended Development Slice

### Phase B3: Transform Sink Boundary

Goal: connect validated source batch output to a bounded transform-facing handoff without introducing real storage I/O.

Recommended scope:

- Define a small transform sink boundary that accepts validated `SourceBatchItem<BarterMarketEvent>` or a neutral market-data DTO.
- Keep the sink bounded and test-only/demo-friendly at first.
- Do not add database writes, real exchange networking, or checkpoint persistence in this slice.
- Preserve dependency direction and avoid making `fdc-ingestion` depend on adapter crates.
```

5. Update the current verification baseline with the exact commands and observed pass results from Task 3.

- [ ] **Step 2: Review the status diff**

Run:

```bash
rtk git diff -- docs/DEVELOPMENT_STATUS.md
```

Expected: diff only documents B2 completion, verification evidence, and the next recommended slice.

- [ ] **Step 3: Commit the status update**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: update source bridge status"
```

---

### Task 5: Final verification and push readiness

**Files:**
- No files should be modified in this task.

- [ ] **Step 1: Run final verification chain**

Run:

```bash
rtk cargo fmt --package fdc-barter && \
rtk cargo test -p fdc-barter --test source_bridge_contract && \
rtk cargo test -p fdc-barter -p fdc-ingestion && \
! grep -R "fdc-barter\|fdc_barter" -n crates/fdc-ingestion Cargo.toml crates/fdc-ingestion/Cargo.toml
```

Expected: all commands exit 0. Existing warnings may remain if they are outside the B2 bridge scope.

- [ ] **Step 2: Confirm repository status**

Run:

```bash
rtk git status --short
rtk git log --oneline --decorate -8
```

Expected: working tree clean. Recent commits include the B2 plan, tests, bridge implementation, and status update.

- [ ] **Step 3: Prepare final report**

Report these items:

- B2 completed capabilities.
- Files changed.
- Verification commands run and results.
- Whether `fdc-ingestion` dependency boundary stayed clean.
- Next recommended slice from `docs/DEVELOPMENT_STATUS.md`.

Do not claim completion unless Step 1 and Step 2 succeeded.
