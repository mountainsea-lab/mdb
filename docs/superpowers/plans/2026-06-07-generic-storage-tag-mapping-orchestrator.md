# P17 Generic Storage Tag Mapping in Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make orchestrator-created storage records emit generic policy tags so `tiered + generic_realtime` can route real market-data writes based on storage metadata.

**Architecture:** Keep all concrete DTO knowledge in `fdc-orchestrator`. Add a small tag-building helper in `crates/fdc-orchestrator/src/storage.rs`, extend existing orchestrator contract tests, and verify through `QueryableMarketDataStore::memory_tiered_with_policy(StorageTieringPolicy::generic_realtime())` using generic tier-scope queries.

**Tech Stack:** Rust, Tokio, `fdc-orchestrator`, `fdc-transform`, `fdc-storage`, TDD with `rtk cargo test`.

---

## Reference design

- `docs/superpowers/specs/2026-06-07-generic-storage-tag-mapping-orchestrator-design.md`
- `crates/fdc-orchestrator/src/storage.rs`
- `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`
- `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`
- `crates/fdc-storage/src/policy.rs`

## File structure

- Modify `crates/fdc-orchestrator/src/storage.rs`
  - Owns the translation from `MarketDataDto` facts into generic `StorageWriteMetadata.tags`.
  - Add `storage_tags_for_dto(dto: &MarketDataDto) -> BTreeMap<String, String>`.
  - Add `data_kind_tags_for_kind(kind: MarketDataKind) -> (&'static str, &'static str)`.
- Modify `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`
  - Lock exact tags for live, backfill, replay, and candle DTO mappings.
- Modify `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`
  - Verify real orchestrator writes route differently under `StorageTieringPolicy::generic_realtime()`.

---

## Task 1: Add failing boundary tests for generic storage tags

**Files:**
- Modify: `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

- [ ] **Step 1: Add helper functions for quality variants and candle events**

Append these helpers near the existing `sample_envelope()` helper:

```rust
fn sample_envelope_with_quality(quality: DataQualityFlags) -> BarterIngestionEnvelope {
    let mut envelope = sample_envelope();
    envelope.quality = quality;
    envelope
}

fn sample_candle_event() -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Historical,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new("BTCUSDT"),
        market_type: BarterMarketType::Spot,
        kind: BarterMarketDataKind::Candle,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Candle(fdc_barter::CandlePayload {
            interval: Some("1m".to_string()),
            open_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            close_time: TimestampNs::from_nanos(1_700_000_060_000_000_000),
            open: Price::new(Decimal::new(42_000_00, 2)),
            high: Price::new(Decimal::new(42_100_00, 2)),
            low: Price::new(Decimal::new(41_900_00, 2)),
            close: Price::new(Decimal::new(42_050_00, 2)),
            volume: Decimal::new(25, 1),
            trade_count: Some(100),
            quote_volume: Some(Decimal::new(1_000_000, 2)),
        }),
        sequence: Some("candle-seq-1".to_string()),
        checkpoint: None,
    }
}

fn sample_candle_envelope() -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(
        "barter:binance_spot",
        sample_candle_event(),
    );
    envelope.envelope_id = "candle-env-1".to_string();
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}
```

If `fdc_barter::CandlePayload` is not imported by the test module, add it to the existing `use fdc_barter::{ ... }` list.

- [ ] **Step 2: Add failing live tag assertions to existing storage mapping test**

In `market_data_dto_maps_to_storage_write_record()`, after the existing shard-key assertion, add:

```rust
assert_eq!(
    record.metadata.tags.get("mode").map(String::as_str),
    Some("live")
);
assert_eq!(
    record.metadata.tags.get("data.kind").map(String::as_str),
    Some("event")
);
assert_eq!(
    record.metadata.tags.get("record.kind").map(String::as_str),
    Some("trade")
);
assert_eq!(
    record.metadata.tags.get("quality.is_replay"),
    None
);
```

- [ ] **Step 3: Add failing backfill and replay tag test**

Append this test:

```rust
#[test]
fn market_data_dto_quality_maps_to_generic_storage_tags() {
    let backfill_source = barter_envelope_to_source_envelope(sample_envelope_with_quality(
        DataQualityFlags {
            is_replay: false,
            is_backfill: true,
            is_duplicate_candidate: false,
            has_gap_before: false,
            is_out_of_order: false,
        },
    ));
    let backfill_dto = barter_event_to_market_data_dto(&backfill_source)
        .expect("backfill trade should map to DTO");
    let backfill_record = market_data_dto_to_storage_record(&backfill_dto)
        .expect("backfill DTO should map to storage record");

    assert_eq!(
        backfill_record.metadata.tags.get("mode").map(String::as_str),
        Some("backfill")
    );

    let replay_source = barter_envelope_to_source_envelope(sample_envelope_with_quality(
        DataQualityFlags {
            is_replay: true,
            is_backfill: false,
            is_duplicate_candidate: true,
            has_gap_before: true,
            is_out_of_order: true,
        },
    ));
    let replay_dto = barter_event_to_market_data_dto(&replay_source)
        .expect("replay trade should map to DTO");
    let replay_record = market_data_dto_to_storage_record(&replay_dto)
        .expect("replay DTO should map to storage record");

    assert_eq!(
        replay_record.metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_replay")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_duplicate_candidate")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.has_gap_before")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        replay_record
            .metadata
            .tags
            .get("quality.is_out_of_order")
            .map(String::as_str),
        Some("true")
    );
}
```

- [ ] **Step 4: Add failing candle tag test**

Append this test:

```rust
#[test]
fn market_data_candle_maps_to_generic_aggregate_storage_tags() {
    let source = barter_envelope_to_source_envelope(sample_candle_envelope());
    let dto = barter_event_to_market_data_dto(&source).expect("candle should map to DTO");
    let record = market_data_dto_to_storage_record(&dto).expect("candle DTO should map");

    assert_eq!(dto.kind, MarketDataKind::Candle);
    assert_eq!(record.collection, "candles");
    assert_eq!(
        record.metadata.tags.get("mode").map(String::as_str),
        Some("backfill")
    );
    assert_eq!(
        record.metadata.tags.get("data.kind").map(String::as_str),
        Some("aggregate")
    );
    assert_eq!(
        record.metadata.tags.get("record.kind").map(String::as_str),
        Some("candle")
    );
}
```

- [ ] **Step 5: Run failing boundary tests**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_dto_maps_to_storage_write_record
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_dto_quality_maps_to_generic_storage_tags
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_candle_maps_to_generic_aggregate_storage_tags
```

Expected: FAIL because the new generic tags are not emitted yet.

---

## Task 2: Implement generic tag mapping in orchestrator storage

**Files:**
- Modify: `crates/fdc-orchestrator/src/storage.rs`

- [ ] **Step 1: Replace inline tag construction with helper call**

In `market_data_dto_to_storage_record()`, replace:

```rust
let mut tags = BTreeMap::new();
tags.insert("adapter".to_string(), dto.adapter.clone());
tags.insert("exchange".to_string(), dto.exchange.clone());
tags.insert("symbol".to_string(), dto.symbol.as_str().to_string());
tags.insert(
    "kind".to_string(),
    schema_kind_for_kind(dto.kind).to_string(),
);
```

with:

```rust
let tags = storage_tags_for_dto(dto);
```

- [ ] **Step 2: Add generic tag helper functions**

Add these functions below `storage_key()`:

```rust
fn storage_tags_for_dto(dto: &MarketDataDto) -> BTreeMap<String, String> {
    let schema_kind = schema_kind_for_kind(dto.kind);
    let (data_kind, record_kind) = data_kind_tags_for_kind(dto.kind);

    let mut tags = BTreeMap::new();
    tags.insert("adapter".to_string(), dto.adapter.clone());
    tags.insert("exchange".to_string(), dto.exchange.clone());
    tags.insert("symbol".to_string(), dto.symbol.as_str().to_string());
    tags.insert("kind".to_string(), schema_kind.to_string());
    tags.insert("data.kind".to_string(), data_kind.to_string());
    tags.insert("record.kind".to_string(), record_kind.to_string());

    let mode = if dto.quality.is_backfill {
        "backfill"
    } else {
        "live"
    };
    tags.insert("mode".to_string(), mode.to_string());

    if dto.quality.is_replay {
        tags.insert("quality.is_replay".to_string(), "true".to_string());
    }
    if dto.quality.is_duplicate_candidate {
        tags.insert(
            "quality.is_duplicate_candidate".to_string(),
            "true".to_string(),
        );
    }
    if dto.quality.has_gap_before {
        tags.insert("quality.has_gap_before".to_string(), "true".to_string());
    }
    if dto.quality.is_out_of_order {
        tags.insert("quality.is_out_of_order".to_string(), "true".to_string());
    }

    tags
}

fn data_kind_tags_for_kind(kind: MarketDataKind) -> (&'static str, &'static str) {
    match kind {
        MarketDataKind::Trade => ("event", "trade"),
        MarketDataKind::OrderBookL1 => ("state", "order_book_l1"),
        MarketDataKind::OrderBook => ("state", "order_book"),
        MarketDataKind::Candle => ("aggregate", "candle"),
        MarketDataKind::Liquidation => ("event", "liquidation"),
        MarketDataKind::Raw => ("raw", "raw"),
    }
}
```

- [ ] **Step 3: Run boundary tests to verify green**

Run:

```bash
rtk cargo fmt --package fdc-orchestrator
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_dto_maps_to_storage_write_record
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_dto_quality_maps_to_generic_storage_tags
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract market_data_candle_maps_to_generic_aggregate_storage_tags
```

Expected: all targeted tests PASS.

- [ ] **Step 4: Commit Task 1-2**

Run:

```bash
git add crates/fdc-orchestrator/src/storage.rs crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs
git commit -m "feat(orchestrator): map market data facts to storage tags"
```

---

## Task 3: Add integration test for generic_realtime tier routing through orchestrator

**Files:**
- Modify: `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs`

- [ ] **Step 1: Add imports for generic storage query and policy APIs**

Extend the existing `use fdc_storage::{ ... }` import from:

```rust
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
```

to:

```rust
use fdc_storage::{
    MarketDataQuery, QueryableMarketDataStore, QueryableStorage, StorageQuery, StorageTier,
    StorageTierScope, StorageTieringPolicy,
};
```

- [ ] **Step 2: Add helper for backfill envelope**

Append near `sample_envelope()`:

```rust
fn sample_backfill_envelope() -> BarterIngestionEnvelope {
    let mut envelope = sample_envelope();
    envelope.envelope_id = "backfill-env-1".to_string();
    envelope.event.sequence = Some("backfill-seq-1".to_string());
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: true,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}
```

- [ ] **Step 3: Add failing tier routing integration test**

Append this test:

```rust
#[tokio::test]
async fn orchestrator_tags_drive_generic_realtime_tier_routing() {
    let store = QueryableMarketDataStore::memory_tiered_with_policy(
        StorageTieringPolicy::generic_realtime(),
    )
    .await
    .expect("tiered store should initialize");

    let result = run_barter_envelopes_to_storage_once(
        vec![sample_envelope(), sample_backfill_envelope()],
        &store,
    )
    .await
    .expect("finite fixtures should write to tiered store");

    assert_eq!(result.storage_records_written, 2);

    let hot = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("hot tier query should succeed");
    let warm = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
        )
        .await
        .expect("warm tier query should succeed");

    assert_eq!(hot.len(), 1);
    assert_eq!(
        hot[0].metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(warm.len(), 1);
    assert_eq!(
        warm[0].metadata.tags.get("mode").map(String::as_str),
        Some("backfill")
    );
}
```

- [ ] **Step 4: Run the routing test**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract orchestrator_tags_drive_generic_realtime_tier_routing
```

Expected after Task 2: PASS. If run before Task 2, expected FAIL because live/backfill tags are missing.

- [ ] **Step 5: Commit integration test**

Run:

```bash
git add crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs
git commit -m "test(orchestrator): verify tag-driven tier routing"
```

---

## Task 4: Final verification

**Files:**
- Modify only if verification exposes issues.

- [ ] **Step 1: Run final verification**

Run:

```bash
rtk cargo fmt --package fdc-orchestrator --package fdc-storage --check
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo test -p fdc-storage --test tiering_policy_contract
```

Expected:

- All commands exit 0.
- `fdc-storage` dependency guard remains green.

- [ ] **Step 2: Update status document**

Prepend to `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-07 P17 Generic Storage Tag Mapping in Orchestrator

Completed:

- Added orchestrator-owned mapping from `MarketDataDto` facts into generic storage metadata tags.
- Preserved existing compatibility tags: `adapter`, `exchange`, `symbol`, and `kind`.
- Added generic policy tags for live/backfill/replay quality and record/data kind.
- Verified `tiered + generic_realtime` routes real orchestrator writes differently through storage tier-scope queries.
- Preserved the `fdc-storage` boundary.

Verification:

- `rtk cargo fmt --package fdc-orchestrator --package fdc-storage --check`
- `rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract`
- `rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract`
- `rtk cargo test -p fdc-storage --test dependency_guard`
- `rtk cargo test -p fdc-storage --test tiering_policy_contract`
```

- [ ] **Step 3: Commit final status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record generic storage tag mapping status"
```

- [ ] **Step 4: Confirm clean working tree**

Run:

```bash
rtk git status --short
```

Expected: clean status.

---

## Self-review

- Spec coverage: tasks cover exact tag mapping, orchestrator boundary tests, routing integration, dependency guard, and status docs.
- Placeholder scan: no `TBD`, `TODO`, or vague implementation steps remain.
- Type consistency: test code uses existing `BarterIngestionEnvelope`, `DataQualityFlags`, `MarketDataDto`, `QueryableMarketDataStore`, `StorageQuery`, and `StorageTierScope` APIs.
- Boundary check: only `fdc-orchestrator` learns concrete DTO semantics; `fdc-storage` remains generic.
