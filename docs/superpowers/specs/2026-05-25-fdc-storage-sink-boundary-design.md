# B5 Tier-aware Storage Sink Boundary Design

Date: 2026-05-25
Branch: `mdb-mqdev`

## Intent

Define a storage-facing write handoff for Financial Data Center that fits the existing layered storage architecture without coupling storage to adapters or transform DTOs.

The B5 slice should let upstream orchestration hand storage-owned generic records to `fdc-storage`, while preserving future routing into L1/L2/L3/L4 tiers, shards, and storage engines.

## Architecture Context

Current authoritative runtime data flow remains:

```text
adapter(s) -> fdc-ingestion -> fdc-transform -> fdc-storage
```

Crate dependencies do not mirror that full flow directly:

- `fdc-adapter/*` owns concrete acquisition and adapter-owned events/envelopes.
- `fdc-ingestion` owns generic source validation, buffering, batching, and pipeline primitives.
- `fdc-transform` owns neutral DTOs and transform sink contracts.
- `fdc-storage` owns storage records, placement hints, sink contracts, tiering, sharding, engines, and storage metrics.
- Concrete cross-layer mapping and wiring belongs in a later orchestration/integration slice.

Existing `fdc-storage` already has a layered architecture:

```text
StorageWriteSink boundary
        ↓
placement/routing policy, future work
        ↓
TierManager / ShardManager, future integration
        ↓
L1 Memory / L2 Redb / L3 DuckDB / L4 RocksDB engines
```

B5 defines only the first boundary and enough placement metadata for later routing. It does not implement production tier routing.

## Scope

B5 will add storage-owned write boundary types and a bounded in-memory recording sink.

In scope:

1. Define `StorageWriteRecord` as a generic storage-owned write unit.
2. Define `StorageWriteBatch` as a bounded collection of write records plus batch metadata.
3. Define `StorageWriteSink` as the async sink trait for accepting write batches.
4. Define tier-aware placement hints that align with existing `StorageTier` and sharding architecture.
5. Add `RecordingStorageSink` for contract tests and future demos.
6. Export the new boundary from `fdc-storage` without changing existing engine APIs.
7. Add contract tests proving batching, placement hints, and dependency boundaries.
8. Update development status docs with B5 completion evidence after implementation.

Out of scope:

- No real database writes.
- No production `TierManager` routing.
- No migration, compaction, retention, replication, backup, or checkpoint persistence.
- No `MarketDataDto -> StorageWriteRecord` mapper in `fdc-storage`.
- No dependency from `fdc-storage` to `fdc-transform`, `fdc-ingestion`, or adapter crates.
- No adapter-specific schemas or market-data-specific storage layout.

## Proposed Modules

```text
crates/fdc-storage/src/
  write.rs         # StorageWriteRecord, StorageWriteBatch, metadata, placement hints
  sink.rs          # StorageWriteSink, RecordingStorageSink, write outcome
```

`lib.rs` will export the new modules and key public types.

## StorageWriteRecord

A record represents one generic write request owned by `fdc-storage`.

Recommended fields:

- `namespace: String`  
  Logical storage domain, for example `market_data`, `analytics`, or `replay`.
- `collection: String`  
  Logical table/bucket/series name inside the namespace.
- `key: Vec<u8>`  
  Storage key chosen by upstream mapping or orchestration.
- `value: Vec<u8>`  
  Serialized payload. The boundary does not require a specific serialization format.
- `timestamp: DateTime<Utc>`  
  Storage-facing event/write timestamp used for ordering and future retention decisions.
- `metadata: StorageWriteMetadata`  
  Generic metadata such as content type, schema version, source label, and user-defined tags.
- `placement: StoragePlacementHint`  
  Optional tier/shard/durability/access hints for later routing.

The record intentionally does not contain `MarketDataDto`, adapter events, `SourceEnvelope`, or transform-specific types.

## StorageWriteMetadata

Metadata stays generic and should be safe for all future data domains.

Recommended fields:

- `content_type: Option<String>` such as `application/json`, `application/octet-stream`, or `application/x-fdc-bincode`.
- `schema: Option<String>` logical schema name.
- `schema_version: Option<String>` schema version string.
- `source: Option<String>` source or orchestrator label, not adapter type coupling.
- `tags: BTreeMap<String, String>` for small routing/search labels.

Metadata is descriptive. Storage routing must not require market-data-specific keys.

## Tier-aware Placement Hints

`StoragePlacementHint` aligns the sink boundary with existing storage tiers while remaining advisory.

Recommended fields:

- `target_tier: Option<StorageTier>`  
  Explicit preferred tier, if known. Uses the existing `StorageTier::{L1,L2,L3,L4}` type.
- `access_pattern: StorageAccessPatternHint`  
  Advisory heat category: `UltraHot`, `Hot`, `Warm`, `Cold`, or `Unspecified`.
- `durability: StorageDurabilityHint`  
  Advisory durability category: `Ephemeral`, `Cached`, `Persistent`, `Archival`, or `Unspecified`.
- `shard_key: Option<Vec<u8>>`  
  Optional routing key for future `ShardManager` integration. If absent, future sinks may use `record.key`.
- `ttl: Option<chrono::Duration>`  
  Optional time-to-live for future retention/migration policy.

Placement hints are not guarantees in B5. `RecordingStorageSink` records them; future production sinks may translate them into tier routing decisions based on `StorageConfig`, enabled tiers, capacity, retention, and migration policy.

## StorageWriteBatch

A batch groups records for a single bounded write operation.

Recommended fields:

- `batch_id: Uuid`
- `created_at: DateTime<Utc>`
- `records: Vec<StorageWriteRecord>`
- `metadata: StorageBatchMetadata`

`StorageWriteBatch::new(records)` should assign a batch id and timestamp. B5 should provide simple helpers such as `len`, `is_empty`, and `validate`.

Validation rules:

- Batch must not be empty when written.
- Each record must have non-empty `namespace`, `collection`, `key`, and `value`.
- Metadata tag keys should be non-empty.
- B5 does not enforce maximum batch size unless an explicit limit is added to the sink implementation.

## StorageWriteSink

The sink trait should be async and object-safe enough for orchestration code to hold behind trait objects.

Conceptual API:

```rust
#[async_trait]
pub trait StorageWriteSink: Send + Sync {
    async fn write_batch(&self, batch: StorageWriteBatch) -> fdc_core::Result<StorageWriteOutcome>;
}
```

`StorageWriteOutcome` should include:

- `batch_id: Uuid`
- `accepted_records: usize`
- `rejected_records: usize`
- `accepted_at: DateTime<Utc>`

For B5, validation failures should reject the whole batch and return an error. Partial success semantics can be designed later when production sinks exist.

## RecordingStorageSink

`RecordingStorageSink` is a file-free, database-free sink for contract tests and future examples.

Behavior:

- Validates each incoming batch.
- Stores accepted batches in memory.
- Exposes read-only snapshots of recorded batches and flattened records.
- Preserves placement hints exactly, so tests can verify tier-aware intent.
- Does not call `StorageEngine`, `TierManager`, `ShardManager`, or real DB code.

This keeps B5 fast, deterministic, and independent from current engine completeness.

## Future Production Routing

B5 should make later production work straightforward without implementing it now.

Future `TierAwareStorageSink` can:

1. Validate `StorageWriteBatch`.
2. Resolve `StoragePlacementHint` against `StorageConfig.enabled_tiers()`.
3. Compute shard id using `ShardManager` and `shard_key` or `record.key`.
4. Route to `TierManager` or specific `StorageEngine`.
5. Record metrics, replication, backup, and write failures.

Possible future default mapping:

| Access hint | Durability hint | Likely tier |
| --- | --- | --- |
| UltraHot | Ephemeral/Cached | L1 |
| Hot | Persistent | L2 |
| Warm | Persistent | L3 |
| Cold | Archival | L4 |

This table is guidance only and should not be hard-coded in B5.

## Error Handling

B5 should use `fdc_core::Result` and existing error helpers where practical.

Expected validation errors:

- Empty batch.
- Empty namespace.
- Empty collection.
- Empty key.
- Empty value.
- Empty metadata tag key.

Recording sink write operations should be atomic from the contract perspective: either the entire batch is recorded, or no records are recorded.

## Testing Strategy

Add contract tests under `crates/fdc-storage/tests/`.

Required tests:

1. A valid batch with tier placement is accepted by `RecordingStorageSink`.
2. Recording sink preserves namespace, collection, key, value, metadata, and placement hints.
3. Empty batches are rejected and do not mutate sink state.
4. Invalid records are rejected atomically.
5. Storage placement hints can reference existing `StorageTier` values without invoking real engines.
6. Dependency guard proves `fdc-storage` does not reference `fdc-transform`, `fdc-ingestion`, or adapter crates in `Cargo.toml`, `src`, or tests except for guard test string allowlists if needed.

Verification commands:

```bash
rtk cargo fmt --package fdc-storage --check
rtk cargo test -p fdc-storage --test storage_sink_boundary_contract
rtk cargo test -p fdc-storage
! grep -RInE "fdc-transform|fdc_transform|fdc-ingestion|fdc_ingestion|fdc-barter|fdc_barter" crates/fdc-storage/Cargo.toml crates/fdc-storage/src
```

If full `fdc-storage` tests expose unrelated pre-existing failures in older engines, the implementation plan must document them and still run the new contract test directly.

## Acceptance Criteria

- `fdc-storage` exports a generic storage write boundary.
- The boundary includes tier-aware placement hints aligned with existing `StorageTier`.
- The boundary includes optional shard routing hints aligned with future `ShardManager` integration.
- `RecordingStorageSink` validates and records batches without using real storage engines.
- No storage code depends on `fdc-transform`, `fdc-ingestion`, or adapter crates.
- No transform DTO, adapter event, or source envelope type appears in the storage write boundary.
- Contract tests cover valid writes, placement preservation, validation failures, and atomic rejection.
- Development status docs describe B5 and the next recommended slice after implementation.

## Non-goals Reaffirmed

B5 is not the full storage runtime. It is a stable boundary that lets later orchestration code hand storage-owned records into `fdc-storage` without violating layer boundaries. Production tier routing, physical layout, schema mapping, and market-data-specific record construction remain separate future design slices.
