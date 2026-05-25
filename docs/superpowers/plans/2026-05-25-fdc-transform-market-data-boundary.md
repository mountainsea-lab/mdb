# FDC Transform Market Data Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a neutral market-data DTO and transform sink boundary, then map `fdc-barter` trade envelopes into that boundary without making `fdc-ingestion` depend on adapters.

**Architecture:** `fdc-transform` owns neutral DTOs and sink traits. `fdc-barter` depends on `fdc-transform` only for adapter-to-neutral mapping. `fdc-ingestion` remains generic and has no dependency on `fdc-barter` or `fdc-transform`.

**Tech Stack:** Rust 1.95 workspace, `serde`, `async-trait`, existing `fdc-core` timestamp/price/symbol types, existing `fdc-ingestion` source batch traits, existing `fdc-barter` market event models.

---

## File Structure

- Create `crates/fdc-transform/src/market_data.rs`: neutral market-data DTOs and quality flags.
- Create `crates/fdc-transform/src/sink.rs`: transform sink trait, result struct, in-memory recording sink.
- Modify `crates/fdc-transform/src/lib.rs`: remove template code, expose modules.
- Modify `crates/fdc-transform/Cargo.toml`: add `fdc-core`, `serde`, `async-trait`, `tokio` dev dependency.
- Create `crates/fdc-transform/tests/market_data_boundary_contract.rs`: contract tests for DTOs and recording sink.
- Create `crates/fdc-adapter/barter/src/mapper/transform.rs`: mapping from Barter envelope/event payloads to `MarketDataDto`.
- Modify `crates/fdc-adapter/barter/src/mapper/mod.rs`: expose transform mapper.
- Modify `crates/fdc-adapter/barter/src/error.rs`: add unsupported DTO mapping error if needed.
- Modify `crates/fdc-adapter/barter/src/lib.rs`: export DTO mapper APIs.
- Modify `crates/fdc-adapter/barter/Cargo.toml`: add dependency on `fdc-transform`.
- Create `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`: contract tests for Barter trade mapping and dependency guard.
- Modify `docs/DEVELOPMENT_STATUS.md`: record B4 completion and verification evidence.

## Task 1: Define `fdc-transform` neutral DTOs and sink boundary

**Files:**
- Modify: `crates/fdc-transform/Cargo.toml`
- Replace: `crates/fdc-transform/src/lib.rs`
- Create: `crates/fdc-transform/src/market_data.rs`
- Create: `crates/fdc-transform/src/sink.rs`
- Create: `crates/fdc-transform/tests/market_data_boundary_contract.rs`

- [ ] **Step 1: Write failing contract tests**

Create `crates/fdc-transform/tests/market_data_boundary_contract.rs`:

```rust
use std::sync::Arc;

use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_transform::{
    MarketDataDto, MarketDataKind, MarketDataPayload, MarketDataTransformSink,
    RecordingMarketDataSink, TradeDto, TradeSide, TransformQualityFlags,
};
use rust_decimal::Decimal;

fn trade_dto() -> MarketDataDto {
    MarketDataDto {
        event_id: "barter-envelope-1".to_string(),
        source_id: "barter-binance-spot-live-trades".to_string(),
        adapter: "barter-rs".to_string(),
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: MarketDataKind::Trade,
        event_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000),
        emitted_at: TimestampNs::from_nanos(1_700_000_000_000_002_000),
        source_sequence: Some("seq-42".to_string()),
        ingestion_sequence: Some("ingest-7".to_string()),
        quality: TransformQualityFlags {
            is_replay: false,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: false,
            is_out_of_order: false,
        },
        payload: MarketDataPayload::Trade(TradeDto {
            trade_id: Some("trade-1".to_string()),
            price: Price::from_f64(65_000.25).unwrap(),
            quantity: Decimal::new(5, 1),
            side: TradeSide::Buy,
        }),
    }
}

#[test]
fn trade_market_data_dto_preserves_identity_timing_payload_and_quality() {
    let dto = trade_dto();

    assert_eq!(dto.event_id, "barter-envelope-1");
    assert_eq!(dto.source_id, "barter-binance-spot-live-trades");
    assert_eq!(dto.adapter, "barter-rs");
    assert_eq!(dto.exchange, "binance_spot");
    assert_eq!(dto.symbol.to_string(), "BTCUSDT");
    assert_eq!(dto.kind, MarketDataKind::Trade);
    assert_eq!(dto.event_time.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(dto.received_at.as_nanos(), 1_700_000_000_000_001_000);
    assert_eq!(dto.emitted_at.as_nanos(), 1_700_000_000_000_002_000);
    assert_eq!(dto.source_sequence.as_deref(), Some("seq-42"));
    assert_eq!(dto.ingestion_sequence.as_deref(), Some("ingest-7"));
    assert!(dto.quality.is_duplicate_candidate);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price.to_f64(), 65_000.25);
            assert_eq!(trade.quantity.to_string(), "0.5");
            assert_eq!(trade.side, TradeSide::Buy);
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
}

#[tokio::test]
async fn recording_market_data_sink_accepts_and_records_bounded_batches() {
    let sink = Arc::new(RecordingMarketDataSink::default());
    let result = sink
        .write_market_data_batch(vec![trade_dto(), trade_dto()])
        .await
        .expect("recording sink should accept valid DTOs");

    assert_eq!(result.accepted_count, 2);
    assert_eq!(result.rejected_count, 0);
    assert_eq!(sink.written_count().await, 2);
    assert_eq!(sink.snapshot().await[0].symbol.to_string(), "BTCUSDT");
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
rtk cargo test -p fdc-transform --test market_data_boundary_contract
```

Expected: compile failure because DTO and sink APIs do not exist yet.

- [ ] **Step 3: Implement minimal DTO and sink APIs**

Update `crates/fdc-transform/Cargo.toml`:

```toml
[package]
name = "fdc-transform"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
homepage.workspace = true
documentation.workspace = true
keywords.workspace = true
categories.workspace = true
rust-version.workspace = true

[dependencies]
fdc-core = { path = "../fdc-core" }
async-trait = { workspace = true }
serde = { workspace = true, features = ["derive"] }
rust_decimal = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt", "sync"] }
```

Replace `crates/fdc-transform/src/lib.rs`:

```rust
pub mod market_data;
pub mod sink;

pub use market_data::{
    CandleDto, MarketDataDto, MarketDataKind, MarketDataPayload, OrderBookL1Dto,
    RawMarketDataDto, TradeDto, TradeSide, TransformQualityFlags,
};
pub use sink::{MarketDataTransformSink, MarketDataTransformSinkResult, RecordingMarketDataSink};
```

Create `crates/fdc-transform/src/market_data.rs`:

```rust
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketDataKind {
    Trade,
    OrderBookL1,
    OrderBook,
    Candle,
    Liquidation,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeDto {
    pub trade_id: Option<String>,
    pub price: Price,
    pub quantity: Decimal,
    pub side: TradeSide,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookL1Dto {
    pub bid_price: Option<Price>,
    pub bid_quantity: Option<Decimal>,
    pub ask_price: Option<Price>,
    pub ask_quantity: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandleDto {
    pub open_time: TimestampNs,
    pub close_time: TimestampNs,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawMarketDataDto {
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketDataPayload {
    Trade(TradeDto),
    OrderBookL1(OrderBookL1Dto),
    OrderBookDelta(RawMarketDataDto),
    Candle(CandleDto),
    Liquidation(RawMarketDataDto),
    Raw(RawMarketDataDto),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TransformQualityFlags {
    pub is_replay: bool,
    pub is_backfill: bool,
    pub is_duplicate_candidate: bool,
    pub has_gap_before: bool,
    pub is_out_of_order: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataDto {
    pub event_id: String,
    pub source_id: String,
    pub adapter: String,
    pub exchange: String,
    pub symbol: Symbol,
    pub kind: MarketDataKind,
    pub event_time: TimestampNs,
    pub received_at: TimestampNs,
    pub emitted_at: TimestampNs,
    pub source_sequence: Option<String>,
    pub ingestion_sequence: Option<String>,
    pub quality: TransformQualityFlags,
    pub payload: MarketDataPayload,
}
```

Create `crates/fdc-transform/src/sink.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::market_data::MarketDataDto;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketDataTransformSinkResult {
    pub accepted_count: usize,
    pub rejected_count: usize,
}

#[async_trait]
pub trait MarketDataTransformSink: Send + Sync {
    async fn write_market_data_batch(
        &self,
        items: Vec<MarketDataDto>,
    ) -> fdc_core::error::Result<MarketDataTransformSinkResult>;
}

#[derive(Debug, Clone, Default)]
pub struct RecordingMarketDataSink {
    written: Arc<RwLock<Vec<MarketDataDto>>>,
}

impl RecordingMarketDataSink {
    pub async fn written_count(&self) -> usize {
        self.written.read().await.len()
    }

    pub async fn snapshot(&self) -> Vec<MarketDataDto> {
        self.written.read().await.clone()
    }
}

#[async_trait]
impl MarketDataTransformSink for RecordingMarketDataSink {
    async fn write_market_data_batch(
        &self,
        items: Vec<MarketDataDto>,
    ) -> fdc_core::error::Result<MarketDataTransformSinkResult> {
        let accepted_count = items.len();
        self.written.write().await.extend(items);
        Ok(MarketDataTransformSinkResult {
            accepted_count,
            rejected_count: 0,
        })
    }
}
```

- [ ] **Step 4: Run passing test**

Run:

```bash
rtk cargo test -p fdc-transform --test market_data_boundary_contract
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit Task 1**

```bash
git add crates/fdc-transform/Cargo.toml crates/fdc-transform/src/lib.rs crates/fdc-transform/src/market_data.rs crates/fdc-transform/src/sink.rs crates/fdc-transform/tests/market_data_boundary_contract.rs
git commit -m "feat: add transform market data boundary"
```

## Task 2: Map `fdc-barter` envelopes into neutral `MarketDataDto`

**Files:**
- Modify: `crates/fdc-adapter/barter/Cargo.toml`
- Modify: `crates/fdc-adapter/barter/src/error.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/src/mapper/mod.rs`
- Create: `crates/fdc-adapter/barter/src/mapper/transform.rs`
- Create: `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`

- [ ] **Step 1: Write failing contract tests**

Create `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`:

```rust
use std::{fs, path::PathBuf};

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, IntoMarketDataDto, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_transform::{MarketDataKind, MarketDataPayload, TradeSide as TransformTradeSide};
use rust_decimal::Decimal;

fn live_trade_envelope() -> BarterIngestionEnvelope {
    BarterIngestionEnvelope {
        envelope_id: "barter-envelope-1".to_string(),
        source_id: "barter-binance-spot-live-trades".to_string(),
        emitted_at: TimestampNs::from_nanos(1_700_000_000_000_002_000),
        event: BarterMarketEvent {
            source: "barter-rs".to_string(),
            mode: BarterMarketDataMode::Live,
            exchange: "binance_spot".to_string(),
            symbol: Symbol::new("BTCUSDT"),
            kind: BarterMarketDataKind::Trade,
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            received_at: TimestampNs::from_nanos(1_700_000_000_000_001_000),
            payload: BarterMarketPayload::Trade(TradePayload {
                trade_id: Some("trade-1".to_string()),
                price: Price::from_f64(65_000.25).unwrap(),
                quantity: Decimal::new(5, 1),
                side: Some(TradeSide::Buy),
            }),
            sequence: Some("seq-42".to_string()),
            checkpoint: None,
        },
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

#[test]
fn barter_live_trade_envelope_maps_to_neutral_market_data_dto() {
    let dto = live_trade_envelope()
        .into_market_data_dto()
        .expect("live trade should map to neutral DTO");

    assert_eq!(dto.event_id, "barter-envelope-1");
    assert_eq!(dto.source_id, "barter-binance-spot-live-trades");
    assert_eq!(dto.adapter, "barter-rs");
    assert_eq!(dto.exchange, "binance_spot");
    assert_eq!(dto.symbol.to_string(), "BTCUSDT");
    assert_eq!(dto.kind, MarketDataKind::Trade);
    assert_eq!(dto.event_time.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(dto.received_at.as_nanos(), 1_700_000_000_000_001_000);
    assert_eq!(dto.emitted_at.as_nanos(), 1_700_000_000_000_002_000);
    assert_eq!(dto.source_sequence.as_deref(), Some("seq-42"));
    assert_eq!(dto.ingestion_sequence.as_deref(), Some("barter-envelope-1"));
    assert!(dto.quality.is_duplicate_candidate);
    assert!(dto.quality.has_gap_before);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price.to_f64(), 65_000.25);
            assert_eq!(trade.quantity.to_string(), "0.5");
            assert_eq!(trade.side, TransformTradeSide::Buy);
        }
        payload => panic!("expected trade payload, got {payload:?}"),
    }
}

#[test]
fn fdc_ingestion_does_not_reference_fdc_barter_or_fdc_transform_after_b4() {
    let adapter_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = adapter_manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .and_then(|path| path.parent())
        .expect("fdc-barter should live under crates/fdc-adapter/barter");

    let files_to_scan = [
        workspace_root.join("Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
    ];
    let ingestion_src_dir = workspace_root.join("crates/fdc-ingestion/src");

    let mut haystack = String::new();
    for file in files_to_scan {
        haystack.push_str(&fs::read_to_string(&file).expect("manifest should be readable"));
    }
    for entry in fs::read_dir(ingestion_src_dir).expect("fdc-ingestion src should be readable") {
        let entry = entry.expect("dir entry should be readable");
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            haystack.push_str(&fs::read_to_string(path).expect("source file should be readable"));
        }
    }

    assert!(!haystack.contains("fdc-barter"));
    assert!(!haystack.contains("fdc_barter"));
    assert!(!haystack.contains("fdc-transform"));
    assert!(!haystack.contains("fdc_transform"));
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
rtk cargo test -p fdc-barter --test transform_boundary_contract
```

Expected: compile failure because `fdc-transform` dependency and `IntoMarketDataDto` do not exist yet.

- [ ] **Step 3: Implement mapper**

Add dependency in `crates/fdc-adapter/barter/Cargo.toml`:

```toml
fdc-transform = { path = "../../fdc-transform" }
```

Ensure it is under `[dependencies]` next to `fdc-ingestion`.

Modify `crates/fdc-adapter/barter/src/error.rs` to add an unsupported mapping error variant while preserving existing variants:

```rust
#[derive(Debug, thiserror::Error)]
pub enum BarterAdapterError {
    #[error("unsupported market data payload for transform DTO mapping: {0}")]
    UnsupportedTransformPayload(String),
    // keep existing variants unchanged
}
```

If the enum already exists, add only the new variant.

Modify `crates/fdc-adapter/barter/src/mapper/mod.rs`:

```rust
pub mod event;
pub mod exchange;
pub mod instrument;
pub mod transform;

pub use transform::IntoMarketDataDto;
```

Create `crates/fdc-adapter/barter/src/mapper/transform.rs`:

```rust
use fdc_transform::{
    CandleDto, MarketDataDto, MarketDataKind, MarketDataPayload, OrderBookL1Dto,
    RawMarketDataDto, TradeDto, TradeSide as TransformTradeSide, TransformQualityFlags,
};

use crate::{
    BarterAdapterError, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketPayload,
    TradeSide,
};

pub trait IntoMarketDataDto {
    fn into_market_data_dto(self) -> Result<MarketDataDto, BarterAdapterError>;
}

impl IntoMarketDataDto for BarterIngestionEnvelope {
    fn into_market_data_dto(self) -> Result<MarketDataDto, BarterAdapterError> {
        let kind = map_kind(self.event.kind);
        let payload = map_payload(self.event.payload)?;
        Ok(MarketDataDto {
            event_id: self.envelope_id.clone(),
            source_id: self.source_id,
            adapter: self.event.source,
            exchange: self.event.exchange,
            symbol: self.event.symbol,
            kind,
            event_time: self.event.timestamp,
            received_at: self.event.received_at,
            emitted_at: self.emitted_at,
            source_sequence: self.event.sequence,
            ingestion_sequence: Some(self.envelope_id),
            quality: TransformQualityFlags {
                is_replay: self.quality.is_replay,
                is_backfill: self.quality.is_backfill,
                is_duplicate_candidate: self.quality.is_duplicate_candidate,
                has_gap_before: self.quality.has_gap_before,
                is_out_of_order: self.quality.is_out_of_order,
            },
            payload,
        })
    }
}

fn map_kind(kind: BarterMarketDataKind) -> MarketDataKind {
    match kind {
        BarterMarketDataKind::Trade => MarketDataKind::Trade,
        BarterMarketDataKind::OrderBookL1 => MarketDataKind::OrderBookL1,
        BarterMarketDataKind::OrderBook => MarketDataKind::OrderBook,
        BarterMarketDataKind::Candle => MarketDataKind::Candle,
        BarterMarketDataKind::Liquidation => MarketDataKind::Liquidation,
    }
}

fn map_trade_side(side: Option<TradeSide>) -> TransformTradeSide {
    match side {
        Some(TradeSide::Buy) => TransformTradeSide::Buy,
        Some(TradeSide::Sell) => TransformTradeSide::Sell,
        None => TransformTradeSide::Unknown,
    }
}

fn map_payload(payload: BarterMarketPayload) -> Result<MarketDataPayload, BarterAdapterError> {
    Ok(match payload {
        BarterMarketPayload::Trade(trade) => MarketDataPayload::Trade(TradeDto {
            trade_id: trade.trade_id,
            price: trade.price,
            quantity: trade.quantity,
            side: map_trade_side(trade.side),
        }),
        BarterMarketPayload::OrderBookL1(book) => MarketDataPayload::OrderBookL1(OrderBookL1Dto {
            bid_price: book.bid_price,
            bid_quantity: book.bid_quantity,
            ask_price: book.ask_price,
            ask_quantity: book.ask_quantity,
        }),
        BarterMarketPayload::OrderBookDelta(raw) => {
            MarketDataPayload::OrderBookDelta(RawMarketDataDto {
                description: raw.description,
            })
        }
        BarterMarketPayload::Candle(candle) => MarketDataPayload::Candle(CandleDto {
            open_time: candle.open_time,
            close_time: candle.close_time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
        }),
        BarterMarketPayload::Liquidation(raw) => MarketDataPayload::Liquidation(RawMarketDataDto {
            description: raw.description,
        }),
        BarterMarketPayload::Raw(raw) => MarketDataPayload::Raw(RawMarketDataDto {
            description: raw.description,
        }),
    })
}
```

Modify `crates/fdc-adapter/barter/src/lib.rs` to export the mapper:

```rust
pub use mapper::IntoMarketDataDto;
```

- [ ] **Step 4: Run passing test**

Run:

```bash
rtk cargo test -p fdc-barter --test transform_boundary_contract
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/src/error.rs crates/fdc-adapter/barter/src/lib.rs crates/fdc-adapter/barter/src/mapper/mod.rs crates/fdc-adapter/barter/src/mapper/transform.rs crates/fdc-adapter/barter/tests/transform_boundary_contract.rs
git commit -m "feat: map barter events to transform DTOs"
```

## Task 3: Integrate through source batch to transform sink and update status docs

**Files:**
- Modify: `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add integration contract test**

Append to `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`:

```rust
use std::sync::Arc;

use async_trait::async_trait;
use fdc_barter::IntoSourceEnvelope;
use fdc_ingestion::{
    config::BatchConfig, run_source_pipeline_once, SourceBatchItem, SourceBatchProcessor,
    SourceBatchSink, SourceValidator,
};
use fdc_transform::{MarketDataTransformSink, RecordingMarketDataSink};

struct TransformForwardingSink {
    transform_sink: Arc<RecordingMarketDataSink>,
}

#[async_trait]
impl SourceBatchSink<BarterMarketEvent> for TransformForwardingSink {
    async fn write_batch(
        &self,
        items: Vec<SourceBatchItem<BarterMarketEvent>>,
    ) -> fdc_core::error::Result<usize> {
        let mut dtos = Vec::with_capacity(items.len());
        for item in items {
            let envelope = BarterIngestionEnvelope {
                envelope_id: item.envelope_id,
                source_id: item.source_id,
                emitted_at: item.emitted_at,
                event: item.payload,
                checkpoint: None,
                quality: DataQualityFlags {
                    is_replay: item.quality.is_replay,
                    is_backfill: item.quality.is_backfill,
                    is_duplicate_candidate: item.quality.is_duplicate_candidate,
                    has_gap_before: item.quality.has_gap_before,
                    is_out_of_order: item.quality.is_out_of_order,
                },
            };
            dtos.push(
                envelope
                    .into_market_data_dto()
                    .expect("source batch item should map to DTO"),
            );
        }
        let result = self
            .transform_sink
            .write_market_data_batch(dtos)
            .await
            .expect("recording transform sink should accept DTO batch");
        Ok(result.accepted_count)
    }
}

#[tokio::test]
async fn validated_barter_source_batch_can_forward_to_transform_sink() {
    let transform_sink = Arc::new(RecordingMarketDataSink::default());
    let forwarding_sink = Arc::new(TransformForwardingSink {
        transform_sink: transform_sink.clone(),
    });
    let processor = SourceBatchProcessor::new(
        BatchConfig {
            batch_size: 10,
            ..Default::default()
        },
        forwarding_sink,
    );
    let validator = SourceValidator::default();
    let envelopes = vec![
        live_trade_envelope().into_source_envelope(),
        live_trade_envelope().into_source_envelope(),
    ];

    let result = run_source_pipeline_once(envelopes, &validator, &processor)
        .await
        .expect("validated source pipeline should forward to transform sink");

    assert_eq!(result.input_count, 2);
    assert_eq!(result.validation_success_count, 2);
    assert_eq!(result.processed_count(), 2);
    assert_eq!(transform_sink.written_count().await, 2);
    assert_eq!(transform_sink.snapshot().await[0].exchange, "binance_spot");
}
```

- [ ] **Step 2: Run integration contract**

Run:

```bash
rtk cargo test -p fdc-barter --test transform_boundary_contract
```

Expected: all tests in the contract pass.

- [ ] **Step 3: Run full verification baseline**

Run:

```bash
rtk cargo fmt --package fdc-transform --package fdc-barter --check
rtk cargo test -p fdc-transform
rtk cargo test -p fdc-barter --test transform_boundary_contract
rtk cargo test -p fdc-barter --test live_acquisition_contract
rtk cargo test -p fdc-barter -p fdc-ingestion
```

Expected:

- Formatting check passes.
- `fdc-transform` tests pass.
- New `transform_boundary_contract` passes.
- Existing live acquisition contract still passes with ignored live smoke tests ignored by default.
- Existing `fdc-barter` and `fdc-ingestion` baseline passes.

- [ ] **Step 4: Update `docs/DEVELOPMENT_STATUS.md`**

Add a new section after B3:

```markdown
### fdc-transform Phase B4: Market Data Transform Boundary

Implemented in `crates/fdc-transform` and `crates/fdc-adapter/barter/src/mapper/transform.rs`.

Completed capabilities:

- Added neutral `MarketDataDto` and market-data payload DTOs in `fdc-transform`.
- Added `MarketDataTransformSink` and in-memory `RecordingMarketDataSink` for bounded test/demo handoff.
- Added `fdc-barter` mapping from `BarterIngestionEnvelope` into neutral `MarketDataDto`.
- Demonstrated validated `SourceEnvelope<BarterMarketEvent>` batches can forward into the transform sink.
- Preserved dependency boundary: `fdc-ingestion` has no dependency on `fdc-barter` or `fdc-transform`.

Contract tests:

- `crates/fdc-transform/tests/market_data_boundary_contract.rs`
- `crates/fdc-adapter/barter/tests/transform_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-25-fdc-transform-market-data-boundary-design.md`
- `docs/superpowers/plans/2026-05-25-fdc-transform-market-data-boundary.md`
```

Update Current Verification Baseline with the commands from Step 3 and their actual results.

- [ ] **Step 5: Commit Task 3**

```bash
git add crates/fdc-adapter/barter/tests/transform_boundary_contract.rs docs/DEVELOPMENT_STATUS.md
git commit -m "test: verify barter transform handoff"
```

## Self-Review

Spec coverage:

- Neutral DTOs in `fdc-transform`: Task 1.
- Sink trait and recording sink: Task 1.
- Barter envelope to DTO mapping: Task 2.
- Source batch to transform sink integration: Task 3.
- Dependency boundary preservation: Task 2 guard and Task 3 verification.
- Docs/status update: Task 3.

Placeholder scan: no TBD/TODO placeholders remain in implementation steps.

Type consistency: `MarketDataDto`, `MarketDataPayload`, `TradeDto`, `MarketDataTransformSink`, `RecordingMarketDataSink`, and `IntoMarketDataDto` are defined before use and exported from their crates.
