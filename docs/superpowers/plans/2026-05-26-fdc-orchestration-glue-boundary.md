# B6 Orchestration Glue Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `fdc-orchestrator` crate that owns bounded cross-layer glue from Barter adapter envelopes through ingestion validation, transform DTOs, and storage write records.

**Architecture:** `fdc-orchestrator` is the only crate allowed to depend on `fdc-barter`, `fdc-ingestion`, `fdc-transform`, and `fdc-storage` together. Core crates remain decoupled and never depend back on the orchestrator. B6 implements only finite in-memory mapping and recording-sink handoff, not live stream lifecycle, checkpoint persistence, API integration, or real DB writes.

**Tech Stack:** Rust 1.95 workspace, `fdc-core::Result`, `fdc-barter`, `fdc-ingestion`, `fdc-transform`, `fdc-storage`, `serde_json`, `async-trait`, `tokio`, `rust_decimal`.

---

## File Structure

- Modify: `Cargo.toml`  
  Add `crates/fdc-orchestrator` as a workspace member.

- Create: `crates/fdc-orchestrator/Cargo.toml`  
  Manifest for the integration crate and its dependencies.

- Create: `crates/fdc-orchestrator/src/lib.rs`  
  Public module declarations and re-exports.

- Create: `crates/fdc-orchestrator/src/barter.rs`  
  Maps `BarterIngestionEnvelope` into `SourceEnvelope<BarterMarketEvent>` and translates quality/checkpoint metadata.

- Create: `crates/fdc-orchestrator/src/market_data.rs`  
  Maps `BarterMarketEvent` into neutral `MarketDataDto`.

- Create: `crates/fdc-orchestrator/src/storage.rs`  
  Maps `MarketDataDto` into deterministic `StorageWriteRecord`.

- Create: `crates/fdc-orchestrator/src/pipeline.rs`  
  Runs a finite in-memory Barter envelope fixture through ingestion validation, DTO mapping, storage record mapping, and a `StorageWriteSink`.

- Create: `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`  
  Contract tests for mapping behavior, bounded pipeline, and dependency guards.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Add B6 completion status, verification evidence, and next recommended slice after implementation.

---

## Task 1: Add failing orchestrator boundary contract tests

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/fdc-orchestrator/Cargo.toml`
- Create: `crates/fdc-orchestrator/src/lib.rs`
- Create: `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

- [ ] **Step 1: Register the new crate shell**

Modify root `Cargo.toml` workspace members by adding `crates/fdc-orchestrator` after `crates/fdc-server`:

```toml
    "crates/fdc-server",
    "crates/fdc-orchestrator",
```

Create `crates/fdc-orchestrator/Cargo.toml`:

```toml
[package]
name = "fdc-orchestrator"
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
description = "Cross-layer orchestration glue for Financial Data Center"

[dependencies]
fdc-core = { path = "../fdc-core" }
fdc-barter = { path = "../fdc-adapter/barter" }
fdc-ingestion = { path = "../fdc-ingestion" }
fdc-transform = { path = "../fdc-transform" }
fdc-storage = { path = "../fdc-storage" }
async-trait = { workspace = true }
chrono = { workspace = true, features = ["serde"] }
rust_decimal = { workspace = true }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt", "rt-multi-thread", "sync"] }
```

Create `crates/fdc-orchestrator/src/lib.rs`:

```rust
//! Cross-layer orchestration glue for Financial Data Center.
//!
//! This crate is the approved home for concrete adapter -> ingestion -> transform
//! -> storage mapping. Core crates must not depend on this crate.

pub mod barter;
pub mod market_data;
pub mod pipeline;
pub mod storage;
```

- [ ] **Step 2: Write the failing contract tests**

Create `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`:

```rust
use std::path::{Path, PathBuf};

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_ingestion::{SourceEnvelope, SourceType};
use fdc_orchestrator::{
    barter::barter_envelope_to_source_envelope,
    market_data::barter_event_to_market_data_dto,
    pipeline::run_barter_envelopes_to_storage_once,
    storage::market_data_dto_to_storage_record,
};
use fdc_storage::{RecordingStorageSink, StorageAccessPatternHint, StorageDurabilityHint};
use fdc_transform::{MarketDataKind, MarketDataPayload, TradeSide as DtoTradeSide};
use rust_decimal::Decimal;

fn sample_trade_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some("trade-1".to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some("seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_envelope() -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", sample_trade_event());
    envelope.envelope_id = "env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: true,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[test]
fn barter_envelope_maps_to_source_envelope() {
    let envelope = sample_envelope();

    let source: SourceEnvelope<BarterMarketEvent> = barter_envelope_to_source_envelope(envelope.clone());

    assert_eq!(source.envelope_id, "env-1");
    assert_eq!(source.source_id, "barter:binance_spot");
    assert_eq!(source.source_type, SourceType::MarketData);
    assert_eq!(source.sequence.as_deref(), Some("seq-1"));
    assert_eq!(source.event_time, envelope.event.timestamp);
    assert_eq!(source.received_at, envelope.event.received_at);
    assert_eq!(source.emitted_at, envelope.emitted_at);
    assert_eq!(source.payload, envelope.event);
    assert!(source.quality.is_duplicate_candidate);
    assert_eq!(source.metadata.adapter.as_deref(), Some("barter"));
    assert_eq!(source.metadata.exchange.as_deref(), Some("binance_spot"));
    assert_eq!(source.metadata.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(source.metadata.kind.as_deref(), Some("trade"));
}

#[test]
fn barter_trade_event_maps_to_market_data_dto() {
    let source = barter_envelope_to_source_envelope(sample_envelope());

    let dto = barter_event_to_market_data_dto(&source).expect("trade should map to DTO");

    assert_eq!(dto.event_id, "env-1");
    assert_eq!(dto.source_id, "barter:binance_spot");
    assert_eq!(dto.adapter, "barter");
    assert_eq!(dto.exchange, "binance_spot");
    assert_eq!(dto.symbol.as_str(), "BTCUSDT");
    assert_eq!(dto.kind, MarketDataKind::Trade);
    assert_eq!(dto.source_sequence.as_deref(), Some("seq-1"));
    assert!(dto.quality.is_duplicate_candidate);

    match dto.payload {
        MarketDataPayload::Trade(trade) => {
            assert_eq!(trade.trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trade.price, Price::new(Decimal::new(42_000_00, 2)));
            assert_eq!(trade.quantity, Decimal::new(125, 3));
            assert_eq!(trade.side, DtoTradeSide::Buy);
        }
        other => panic!("expected trade payload, got {other:?}"),
    }
}

#[test]
fn market_data_dto_maps_to_storage_write_record() {
    let source = barter_envelope_to_source_envelope(sample_envelope());
    let dto = barter_event_to_market_data_dto(&source).expect("trade should map to DTO");

    let record = market_data_dto_to_storage_record(&dto).expect("DTO should map to storage record");

    assert_eq!(record.namespace, "market_data");
    assert_eq!(record.collection, "trades");
    assert_eq!(record.key, b"barter:binance_spot:BTCUSDT:trade:1700000000000000001".to_vec());
    assert_eq!(record.metadata.content_type.as_deref(), Some("application/json"));
    assert_eq!(record.metadata.schema.as_deref(), Some("market_data.trade"));
    assert_eq!(record.metadata.source.as_deref(), Some("barter:binance_spot"));
    assert_eq!(record.placement.access_pattern, StorageAccessPatternHint::Hot);
    assert_eq!(record.placement.durability, StorageDurabilityHint::Persistent);
    assert_eq!(record.placement.shard_key.as_deref(), Some(&b"barter:binance_spot:BTCUSDT"[..]));

    let json: serde_json::Value = serde_json::from_slice(&record.value).expect("record value should be JSON");
    assert_eq!(json["kind"], "Trade");
    assert_eq!(json["symbol"], "BTCUSDT");
}

#[tokio::test]
async fn finite_barter_fixture_flows_into_recording_storage_sink() {
    let sink = RecordingStorageSink::new();

    let result = run_barter_envelopes_to_storage_once(vec![sample_envelope()], &sink)
        .await
        .expect("finite fixture should write");

    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.source_valid, 1);
    assert_eq!(result.source_invalid, 0);
    assert_eq!(result.dto_mapped, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[test]
fn dependency_guard_core_crates_do_not_reference_orchestrator() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-adapter/barter/Cargo.toml"),
        workspace_root.join("crates/fdc-adapter/barter/src"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/src"),
        workspace_root.join("crates/fdc-transform/Cargo.toml"),
        workspace_root.join("crates/fdc-transform/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-orchestrator", "fdc_orchestrator"], &mut violations);
    }

    assert!(violations.is_empty(), "core crates must not depend on orchestrator: {violations:#?}");
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-orchestrator should live two levels under workspace root")
        .to_path_buf()
}

fn collect_forbidden_references(path: &Path, forbidden: &[&str], violations: &mut Vec<String>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("failed to read directory") {
            collect_forbidden_references(&entry.expect("failed to read entry").path(), forbidden, violations);
        }
        return;
    }

    if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
        && path.file_name().and_then(|file_name| file_name.to_str()) != Some("Cargo.toml")
    {
        return;
    }

    let content = std::fs::read_to_string(path).expect("failed to read dependency guard file");
    for needle in forbidden {
        if content.contains(needle) {
            violations.push(format!("{} contains {needle}", path.display()));
        }
    }
}
```

- [ ] **Step 3: Run the contract test to verify it fails before implementation**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
```

Expected: FAIL with unresolved modules/imports such as `could not find barter in fdc_orchestrator` or missing functions.

- [ ] **Step 4: Commit the failing test and crate shell**

Run:

```bash
git add Cargo.toml crates/fdc-orchestrator
git commit -m "test: define orchestrator boundary contract"
```

---

## Task 2: Implement Barter envelope to SourceEnvelope mapping

**Files:**
- Modify: `crates/fdc-orchestrator/src/lib.rs`
- Create: `crates/fdc-orchestrator/src/barter.rs`

- [ ] **Step 1: Implement `barter.rs`**

Create `crates/fdc-orchestrator/src/barter.rs`:

```rust
use fdc_barter::{
    BarterCheckpoint, BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketEvent,
    DataQualityFlags,
};
use fdc_ingestion::{
    SourceCheckpoint, SourceEnvelope, SourceMetadata, SourcePartition, SourcePosition,
    SourceQualityFlags, SourceType,
};

pub fn barter_envelope_to_source_envelope(
    envelope: BarterIngestionEnvelope,
) -> SourceEnvelope<BarterMarketEvent> {
    let event = envelope.event.clone();
    let sequence = event.sequence.clone();
    let checkpoint = envelope.checkpoint.as_ref().map(barter_checkpoint_to_source_checkpoint);

    let source = SourceEnvelope::new(
        envelope.source_id.clone(),
        SourceType::MarketData,
        event.timestamp,
        event.received_at,
        envelope.event,
    )
    .with_optional_checkpoint(checkpoint)
    .with_quality(data_quality_to_source_quality(envelope.quality))
    .with_metadata(SourceMetadata {
        adapter: Some("barter".to_string()),
        exchange: Some(event.exchange.clone()),
        symbol: Some(event.symbol.as_str().to_string()),
        kind: Some(barter_kind_label(event.kind).to_string()),
        attributes: Default::default(),
    });

    let mut source = if let Some(sequence) = sequence {
        source.with_sequence(sequence)
    } else {
        source
    };

    source.envelope_id = envelope.envelope_id;
    source.emitted_at = envelope.emitted_at;
    source
}

pub fn data_quality_to_source_quality(quality: DataQualityFlags) -> SourceQualityFlags {
    SourceQualityFlags {
        is_replay: quality.is_replay,
        is_backfill: quality.is_backfill,
        is_duplicate_candidate: quality.is_duplicate_candidate,
        has_gap_before: quality.has_gap_before,
        is_out_of_order: quality.is_out_of_order,
    }
}

pub fn barter_kind_label(kind: BarterMarketDataKind) -> &'static str {
    match kind {
        BarterMarketDataKind::Trade => "trade",
        BarterMarketDataKind::OrderBookL1 => "order_book_l1",
        BarterMarketDataKind::OrderBook => "order_book",
        BarterMarketDataKind::Candle => "candle",
        BarterMarketDataKind::Liquidation => "liquidation",
    }
}

fn barter_checkpoint_to_source_checkpoint(checkpoint: &BarterCheckpoint) -> SourceCheckpoint {
    SourceCheckpoint {
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        source_id: checkpoint.source_id.clone(),
        partition: SourcePartition {
            exchange: Some(checkpoint.partition.exchange.clone()),
            symbol: Some(checkpoint.partition.symbol.as_str().to_string()),
            kind: Some(barter_kind_label(checkpoint.partition.kind).to_string()),
            shard: checkpoint.partition.shard.clone(),
        },
        position: match checkpoint.position.sequence.clone() {
            Some(sequence) => SourcePosition::Sequence(sequence),
            None => SourcePosition::Timestamp(checkpoint.position.timestamp),
        },
        updated_at: checkpoint.updated_at,
    }
}
```

- [ ] **Step 2: Run focused test and verify remaining failures are for later modules**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract barter_envelope_maps_to_source_envelope
```

Expected: PASS for `barter_envelope_maps_to_source_envelope`, or compile failures only from still-missing `market_data`, `storage`, or `pipeline` functions imported by the full test file.

- [ ] **Step 3: Commit Barter source mapping**

Run:

```bash
git add crates/fdc-orchestrator/src/barter.rs crates/fdc-orchestrator/src/lib.rs
git commit -m "feat: map barter envelopes into source envelopes"
```

---

## Task 3: Implement Barter event to MarketDataDto mapping

**Files:**
- Create: `crates/fdc-orchestrator/src/market_data.rs`

- [ ] **Step 1: Implement `market_data.rs`**

Create `crates/fdc-orchestrator/src/market_data.rs`:

```rust
use fdc_barter::{
    BarterMarketDataKind, BarterMarketEvent, BarterMarketPayload,
    TradeSide as BarterTradeSide,
};
use fdc_core::{error::Error, Result};
use fdc_ingestion::SourceEnvelope;
use fdc_transform::{
    CandleDto, MarketDataDto, MarketDataKind, MarketDataPayload, OrderBookL1Dto,
    RawMarketDataDto, TradeDto, TradeSide, TransformQualityFlags,
};

use crate::barter::barter_kind_label;

pub fn barter_event_to_market_data_dto(
    source: &SourceEnvelope<BarterMarketEvent>,
) -> Result<MarketDataDto> {
    let event = &source.payload;
    let payload = barter_payload_to_market_data_payload(&event.payload)?;
    let kind = barter_kind_to_market_data_kind(event.kind, &event.payload)?;

    Ok(MarketDataDto {
        event_id: source.envelope_id.clone(),
        source_id: source.source_id.clone(),
        adapter: source
            .metadata
            .adapter
            .clone()
            .unwrap_or_else(|| "barter".to_string()),
        exchange: event.exchange.clone(),
        symbol: event.symbol.clone(),
        kind,
        event_time: event.timestamp,
        received_at: event.received_at,
        emitted_at: source.emitted_at,
        source_sequence: event.sequence.clone(),
        ingestion_sequence: source.sequence.clone(),
        quality: TransformQualityFlags {
            is_replay: source.quality.is_replay,
            is_backfill: source.quality.is_backfill,
            is_duplicate_candidate: source.quality.is_duplicate_candidate,
            has_gap_before: source.quality.has_gap_before,
            is_out_of_order: source.quality.is_out_of_order,
        },
        payload,
    })
}

fn barter_kind_to_market_data_kind(
    kind: BarterMarketDataKind,
    payload: &BarterMarketPayload,
) -> Result<MarketDataKind> {
    match (kind, payload) {
        (BarterMarketDataKind::Trade, BarterMarketPayload::Trade(_)) => Ok(MarketDataKind::Trade),
        (BarterMarketDataKind::OrderBookL1, BarterMarketPayload::OrderBookL1(_)) => {
            Ok(MarketDataKind::OrderBookL1)
        }
        (BarterMarketDataKind::OrderBook, BarterMarketPayload::OrderBookDelta(_)) => {
            Ok(MarketDataKind::OrderBook)
        }
        (BarterMarketDataKind::Candle, BarterMarketPayload::Candle(_)) => Ok(MarketDataKind::Candle),
        (BarterMarketDataKind::Liquidation, BarterMarketPayload::Liquidation(_)) => {
            Ok(MarketDataKind::Liquidation)
        }
        (_, BarterMarketPayload::Raw(_)) => Ok(MarketDataKind::Raw),
        (kind, _) => Err(Error::validation(format!(
            "barter event kind {} does not match payload",
            barter_kind_label(kind)
        ))),
    }
}

fn barter_payload_to_market_data_payload(payload: &BarterMarketPayload) -> Result<MarketDataPayload> {
    Ok(match payload {
        BarterMarketPayload::Trade(trade) => MarketDataPayload::Trade(TradeDto {
            trade_id: trade.trade_id.clone(),
            price: trade.price,
            quantity: trade.quantity,
            side: match trade.side {
                Some(BarterTradeSide::Buy) => TradeSide::Buy,
                Some(BarterTradeSide::Sell) => TradeSide::Sell,
                None => TradeSide::Unknown,
            },
        }),
        BarterMarketPayload::OrderBookL1(book) => MarketDataPayload::OrderBookL1(OrderBookL1Dto {
            bid_price: book.bid_price,
            bid_quantity: book.bid_quantity,
            ask_price: book.ask_price,
            ask_quantity: book.ask_quantity,
        }),
        BarterMarketPayload::OrderBookDelta(raw) => {
            MarketDataPayload::OrderBookDelta(RawMarketDataDto {
                description: raw.description.clone(),
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
            description: raw.description.clone(),
        }),
        BarterMarketPayload::Raw(raw) => MarketDataPayload::Raw(RawMarketDataDto {
            description: raw.description.clone(),
        }),
    })
}
```

- [ ] **Step 2: Run focused DTO test**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract barter_trade_event_maps_to_market_data_dto
```

Expected: PASS for `barter_trade_event_maps_to_market_data_dto`, or compile failures only from still-missing storage/pipeline functions imported by the test file.

- [ ] **Step 3: Commit DTO mapping**

Run:

```bash
git add crates/fdc-orchestrator/src/market_data.rs crates/fdc-orchestrator/src/lib.rs
git commit -m "feat: map barter events into market data DTOs"
```

---

## Task 4: Implement MarketDataDto to StorageWriteRecord mapping

**Files:**
- Create: `crates/fdc-orchestrator/src/storage.rs`

- [ ] **Step 1: Implement `storage.rs`**

Create `crates/fdc-orchestrator/src/storage.rs`:

```rust
use std::collections::BTreeMap;

use fdc_core::{error::Error, Result};
use fdc_storage::{
    StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageWriteMetadata,
    StorageWriteRecord,
};
use fdc_transform::{MarketDataDto, MarketDataKind};

pub fn market_data_dto_to_storage_record(dto: &MarketDataDto) -> Result<StorageWriteRecord> {
    let value = serde_json::to_vec(dto).map_err(Error::from)?;
    let collection = collection_for_kind(dto.kind);
    let schema = format!("market_data.{}", schema_kind_for_kind(dto.kind));
    let key = storage_key(dto);
    let shard_key = format!("{}:{}", dto.source_id, dto.symbol.as_str()).into_bytes();

    let mut tags = BTreeMap::new();
    tags.insert("adapter".to_string(), dto.adapter.clone());
    tags.insert("exchange".to_string(), dto.exchange.clone());
    tags.insert("symbol".to_string(), dto.symbol.as_str().to_string());
    tags.insert("kind".to_string(), schema_kind_for_kind(dto.kind).to_string());

    Ok(StorageWriteRecord::new("market_data", collection, key.into_bytes(), value)
        .with_metadata(StorageWriteMetadata {
            content_type: Some("application/json".to_string()),
            schema: Some(schema),
            schema_version: Some("1".to_string()),
            source: Some(dto.source_id.clone()),
            tags,
        })
        .with_placement(StoragePlacementHint {
            target_tier: None,
            access_pattern: access_pattern_for_kind(dto.kind),
            durability: durability_for_kind(dto.kind),
            shard_key: Some(shard_key),
            ttl: None,
        }))
}

fn storage_key(dto: &MarketDataDto) -> String {
    format!(
        "{}:{}:{}:{}",
        dto.source_id,
        dto.symbol.as_str(),
        schema_kind_for_kind(dto.kind),
        dto.event_time.as_nanos()
    )
}

fn collection_for_kind(kind: MarketDataKind) -> &'static str {
    match kind {
        MarketDataKind::Trade => "trades",
        MarketDataKind::OrderBookL1 => "order_book_l1",
        MarketDataKind::OrderBook => "order_book",
        MarketDataKind::Candle => "candles",
        MarketDataKind::Liquidation => "liquidations",
        MarketDataKind::Raw => "raw",
    }
}

fn schema_kind_for_kind(kind: MarketDataKind) -> &'static str {
    match kind {
        MarketDataKind::Trade => "trade",
        MarketDataKind::OrderBookL1 => "order_book_l1",
        MarketDataKind::OrderBook => "order_book",
        MarketDataKind::Candle => "candle",
        MarketDataKind::Liquidation => "liquidation",
        MarketDataKind::Raw => "raw",
    }
}

fn access_pattern_for_kind(kind: MarketDataKind) -> StorageAccessPatternHint {
    match kind {
        MarketDataKind::Trade | MarketDataKind::OrderBookL1 | MarketDataKind::OrderBook => {
            StorageAccessPatternHint::Hot
        }
        MarketDataKind::Candle => StorageAccessPatternHint::Warm,
        MarketDataKind::Liquidation | MarketDataKind::Raw => StorageAccessPatternHint::Unspecified,
    }
}

fn durability_for_kind(kind: MarketDataKind) -> StorageDurabilityHint {
    match kind {
        MarketDataKind::Raw => StorageDurabilityHint::Unspecified,
        _ => StorageDurabilityHint::Persistent,
    }
}
```

- [ ] **Step 2: Run focused storage mapping test**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_dto_maps_to_storage_write_record
```

Expected: PASS for `market_data_dto_maps_to_storage_write_record`, or compile failures only from still-missing pipeline function imported by the test file.

- [ ] **Step 3: Commit storage record mapping**

Run:

```bash
git add crates/fdc-orchestrator/src/storage.rs crates/fdc-orchestrator/src/lib.rs
git commit -m "feat: map market data DTOs into storage records"
```

---

## Task 5: Implement finite orchestrator pipeline helper

**Files:**
- Create: `crates/fdc-orchestrator/src/pipeline.rs`

- [ ] **Step 1: Implement `pipeline.rs`**

Create `crates/fdc-orchestrator/src/pipeline.rs`:

```rust
use fdc_barter::BarterIngestionEnvelope;
use fdc_core::Result;
use fdc_ingestion::SourceValidator;
use fdc_storage::{StorageWriteBatch, StorageWriteSink};

use crate::{
    barter::barter_envelope_to_source_envelope,
    market_data::barter_event_to_market_data_dto,
    storage::market_data_dto_to_storage_record,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrchestratorPipelineResult {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
}

pub async fn run_barter_envelopes_to_storage_once(
    envelopes: Vec<BarterIngestionEnvelope>,
    storage_sink: &dyn StorageWriteSink,
) -> Result<OrchestratorPipelineResult> {
    let validator = SourceValidator::default();
    let mut result = OrchestratorPipelineResult {
        envelopes_received: envelopes.len(),
        ..OrchestratorPipelineResult::default()
    };
    let mut records = Vec::new();

    for envelope in envelopes {
        let source = barter_envelope_to_source_envelope(envelope);
        let validation = validator.validate(&source).await;
        if !validation.is_valid {
            result.source_invalid += 1;
            continue;
        }

        result.source_valid += 1;
        let dto = barter_event_to_market_data_dto(&source)?;
        result.dto_mapped += 1;
        records.push(market_data_dto_to_storage_record(&dto)?);
    }

    if records.is_empty() {
        return Ok(result);
    }

    let outcome = storage_sink.write_batch(StorageWriteBatch::new(records)).await?;
    result.storage_records_written = outcome.accepted_records;
    Ok(result)
}
```

- [ ] **Step 2: Run full orchestrator contract test**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
```

Expected: PASS, 5 tests passed.

- [ ] **Step 3: Commit finite pipeline helper**

Run:

```bash
git add crates/fdc-orchestrator/src/pipeline.rs crates/fdc-orchestrator/src/lib.rs
git commit -m "feat: add finite orchestrator storage pipeline"
```

---

## Task 6: Run verification and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run B6 verification commands**

Run:

```bash
rtk cargo fmt --package fdc-orchestrator --check
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
rtk cargo test -p fdc-orchestrator
rtk cargo test -p fdc-barter -p fdc-ingestion -p fdc-transform -p fdc-storage
```

Expected: all commands exit 0. If the combined multi-package test exposes unrelated legacy failures, record exact failing command/output and still keep the focused orchestrator contract passing.

- [ ] **Step 2: Update `docs/DEVELOPMENT_STATUS.md`**

Add a new completed section after B5:

```markdown
### Phase B6: Orchestration Glue Boundary

Implemented in `crates/fdc-orchestrator`.

Completed capabilities:

- Added a dedicated integration/orchestration crate for bounded cross-layer glue.
- Mapped `BarterIngestionEnvelope` to `SourceEnvelope<BarterMarketEvent>` without adding downstream dependencies to `fdc-barter`.
- Mapped Barter market events to neutral `MarketDataDto` values without adding adapter dependencies to `fdc-transform`.
- Mapped `MarketDataDto` to generic `StorageWriteRecord` values without adding transform dependencies to `fdc-storage`.
- Added a finite in-memory helper that validates source envelopes and writes storage records to any `StorageWriteSink`.
- Verified the bounded fixture path with `RecordingStorageSink` and dependency guard tests.

Contract tests:

- `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-26-fdc-orchestration-glue-boundary-design.md`
- `docs/superpowers/plans/2026-05-26-fdc-orchestration-glue-boundary.md`

Verification:

- `rtk cargo fmt --package fdc-orchestrator --check`
- `rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract`
- `rtk cargo test -p fdc-orchestrator`
- `rtk cargo test -p fdc-barter -p fdc-ingestion -p fdc-transform -p fdc-storage`
```

Update `Current Verification Baseline` to reference the final B6 commit hash and command results.

Update `Next Recommended Development Slice` to:

```markdown
### Phase B7: Server Assembly Boundary

Goal: make `fdc-server` consume `fdc-orchestrator` APIs as the application assembly layer without moving mapping logic into server lifecycle code.

Recommended scope:

- Add a real `fdc-server` application module or main entry shell.
- Wire configuration/tracing placeholders and an orchestrator service handle.
- Keep API integration, production live runners, checkpoint persistence, and real DB writes as later slices.
```

- [ ] **Step 3: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record orchestration glue boundary status"
```

---

## Task 7: Final branch validation and push

**Files:**
- No source changes expected.

- [ ] **Step 1: Inspect final branch state**

Run:

```bash
rtk git status --short --branch
rtk git log --oneline -8
```

Expected: branch `mdb-mqdev` is ahead of `origin/mdb-mqdev`, working tree clean.

- [ ] **Step 2: Push to remote**

Run:

```bash
git push origin mdb-mqdev
```

Expected: push succeeds.

- [ ] **Step 3: Confirm remote sync**

Run:

```bash
rtk git status --short --branch
```

Expected: `mdb-mqdev...origin/mdb-mqdev` with no ahead/behind marker and clean working tree.

---

## Self-Review Notes

- Spec coverage: The plan covers the new crate, all four modules, finite pipeline helper, dependency guards, docs, verification, and push.
- Placeholder scan: This plan contains no unresolved placeholder markers, no open-ended placeholders, and no deferred implementation steps inside B6 scope.
- Type consistency: Function names used by tests are defined in the matching modules: `barter_envelope_to_source_envelope`, `barter_event_to_market_data_dto`, `market_data_dto_to_storage_record`, and `run_barter_envelopes_to_storage_once`.
- Scope: B6 remains bounded and excludes live streams, checkpoint persistence, API/server integration, and real storage writes.
