# P17 Generic Storage Tag Mapping in Orchestrator Design

Date: 2026-06-07
Module: `crates/fdc-orchestrator` storage mapping with storage/runtime verification
Status: approved design for implementation

## Goal

Map existing market-data DTO facts into generic `StorageWriteMetadata.tags` so the storage-owned `generic_realtime` policy profile can make useful runtime placement decisions for real orchestrator writes.

## Boundary rule

`fdc-storage` remains generic. It must not depend on market-data DTOs, Barter models, ingestion envelopes, transform DTOs, orchestrator, server, or API crates.

`fdc-orchestrator` owns the cross-module mapping from concrete DTO facts into generic storage metadata. Storage consumes only generic tags and storage facts.

## Current state

P13-P16 completed the storage-side policy engine, runtime backend/profile config, generic tag-aware policy profile, and runtime policy injection.

The remaining gap is in the real write path:

```text
BarterIngestionEnvelope
  -> SourceEnvelope<BarterMarketEvent>
  -> MarketDataDto
  -> market_data_dto_to_storage_record
  -> StorageWriteRecord
```

`market_data_dto_to_storage_record()` currently writes basic tags:

- `adapter`
- `exchange`
- `symbol`
- `kind`

The `StorageTieringPolicy::generic_realtime()` profile can also use tags such as:

- `mode=live`
- `mode=backfill`
- `quality.is_replay=true`
- `data.kind=aggregate`
- `record.kind=candle`

Those tags are not yet emitted by the orchestrator mapping.

## Design

Add a focused helper inside `crates/fdc-orchestrator/src/storage.rs` that builds generic storage tags from `MarketDataDto`.

The helper remains orchestrator-owned because it translates business DTO fields into generic storage facts.

### Tag mapping

Base tags remain unchanged:

| Tag | Value |
| --- | --- |
| `adapter` | `dto.adapter` |
| `exchange` | `dto.exchange` |
| `symbol` | `dto.symbol.as_str()` |
| `kind` | existing schema kind, kept for compatibility |

Add generic policy tags:

| DTO fact | Storage tag |
| --- | --- |
| `dto.quality.is_backfill == true` | `mode=backfill` |
| `dto.quality.is_backfill == false` and `dto.quality.is_replay == false` | `mode=live` |
| `dto.quality.is_replay == true` | `quality.is_replay=true` |
| `dto.quality.is_duplicate_candidate == true` | `quality.is_duplicate_candidate=true` |
| `dto.quality.has_gap_before == true` | `quality.has_gap_before=true` |
| `dto.quality.is_out_of_order == true` | `quality.is_out_of_order=true` |
| `MarketDataKind::Trade` | `data.kind=event`, `record.kind=trade` |
| `MarketDataKind::OrderBookL1` | `data.kind=state`, `record.kind=order_book_l1` |
| `MarketDataKind::OrderBook` | `data.kind=state`, `record.kind=order_book` |
| `MarketDataKind::Candle` | `data.kind=aggregate`, `record.kind=candle` |
| `MarketDataKind::Liquidation` | `data.kind=event`, `record.kind=liquidation` |
| `MarketDataKind::Raw` | `data.kind=raw`, `record.kind=raw` |

### Replay/backfill precedence

Backfill mode is primary for `mode` because backfill/replay data should avoid realtime hot tiers:

1. If `is_backfill`, emit `mode=backfill`.
2. Else emit `mode=live`.
3. Independently, if `is_replay`, emit `quality.is_replay=true`.

This means replay live streams retain `mode=live` plus `quality.is_replay=true`; the storage policy still routes replay to analytical storage because `quality.is_replay=true` is a generic policy signal.

### Timestamp mapping

Set `StorageWriteRecord.timestamp` to `dto.event_time` converted to `DateTime<Utc>` if the existing core timestamp type exposes a safe conversion. If no conversion exists, keep the current `StorageWriteRecord::new()` timestamp default and defer timestamp mapping to a separate slice.

This slice does not require timestamp conversion because P16 already passes record timestamp age when available, and generic tags are the primary missing signal.

## Data flow after P17

```text
Barter quality + transform DTO kind
  -> fdc-orchestrator::storage generic tag mapping
  -> StorageWriteRecord.metadata.tags
  -> TieredStorageStore::write_batch
  -> TierManager::put_with_policy_context
  -> StorageTieringPolicy::generic_realtime
  -> initial tier decision
```

## Testing strategy

Use TDD.

### Orchestrator boundary tests

Extend `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`:

- Live trade maps to `mode=live`, `data.kind=event`, `record.kind=trade`.
- Backfill trade maps to `mode=backfill`.
- Replay trade maps to `quality.is_replay=true`.
- Candle maps to `data.kind=aggregate`, `record.kind=candle`.
- Existing compatibility tags and placement hints remain unchanged.

### Orchestrator to tiered storage integration test

Extend `crates/fdc-orchestrator/tests/orchestrator_queryable_storage_contract.rs` or add a focused storage routing contract test:

- Build `QueryableMarketDataStore::memory_tiered_with_policy(StorageTieringPolicy::generic_realtime())`.
- Run `run_barter_envelopes_to_storage_once()` with a live trade envelope and a backfill/replay or candle envelope.
- Query `StorageTierScope::Only(StorageTier::L2)` and `StorageTierScope::Only(StorageTier::L3)` through generic `QueryableStorage`.
- Assert live trade is in L2 and backfill/replay/candle is in L3.

## Out of scope

- Adding any market-data DTO knowledge to `fdc-storage`.
- Changing the `StorageTieringPolicy::generic_realtime()` rules unless tests expose a real mismatch.
- Runtime physical path configuration for durable L2/L3/L4 engines.
- Server/API route changes.
- Historical REST network behavior.
- Adaptive promotion/demotion scheduler changes.

## Risks and mitigations

### Risk: tag vocabulary drift

Mitigation: contract tests lock exact generic tag keys and values. P17 uses tag names already recognized by P15 policy.

### Risk: older queries rely on `kind`

Mitigation: keep the existing `kind` tag unchanged and add `record.kind` rather than replacing it.

### Risk: replay/live ambiguity

Mitigation: `quality.is_replay=true` is independent of `mode`, and policy treats replay as analytical even if mode is live.

## Verification commands

```bash
rtk cargo fmt --package fdc-orchestrator --package fdc-storage
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract
rtk cargo test -p fdc-orchestrator --test orchestrator_queryable_storage_contract
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo test -p fdc-storage --test tiering_policy_contract
```

## Success criteria

- Real orchestrator-created storage records include generic policy tags.
- `tiered + generic_realtime` routes live and backfill/replay/aggregate records differently through the actual orchestrator write path.
- Existing query and storage compatibility tags continue to work.
- `fdc-storage` dependency guard remains green.

## Self-review

- Placeholder scan: no placeholder requirements remain.
- Scope check: this is one cohesive orchestrator mapping slice, not a storage policy redesign.
- Boundary check: storage remains generic and orchestrator owns concrete DTO translation.
- Compatibility check: existing tags remain additive and unchanged.
