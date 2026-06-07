# fdc-storage Intelligent Tiering Policy Engine Design

Date: 2026-06-07
Module: `crates/fdc-storage`
Status: design record for next implementation slice

## 1. Purpose

`fdc-storage` already has a generic tiered store, placement hints, lifecycle reports, and explicit maintenance passes. The next step should not make business callers manually choose L1/L2/L3/L4 for every write. Instead, storage should expose an intelligent tiering policy layer that converts generic record facts and advisory hints into storage-owned placement decisions.

The key distinction is:

- Runtime/server config chooses whether the storage runtime uses a tiered backend and which policy profile is active.
- The tiering policy chooses the initial tier, TTL, retention class, and later promotion/demotion actions for each record.

Business modules may provide hints, but storage remains responsible for the final tier decision.

## 2. Current implementation inventory

### Existing assets

- `src/write.rs`
  - `StorageWriteRecord` already carries generic storage facts: namespace, collection, key, value, timestamp, metadata, and placement.
  - `StoragePlacementHint` already contains advisory fields: `target_tier`, `access_pattern`, `durability`, `shard_key`, and `ttl`.
  - `StorageAccessPatternHint` and `StorageDurabilityHint` are the current caller-facing hint vocabulary.

- `src/tier.rs`
  - `StorageTier` defines L1-L4.
  - `TierConfig` carries tier retention and migration thresholds.
  - `AccessPattern` tracks last access, count, frequency, size, and heat score.
  - `TierManager::put_with_placement()` routes writes through `determine_tier_for_placement()`.
  - `TierManager::get()` records access and schedules promotion when cold-tier records are read.
  - `TierManager::process_migrations()` can execute queued promotion tasks.

- `src/tiered_store.rs`
  - `TieredStorageStore` implements `StorageWriteSink` and calls `put_with_placement()` for every `StorageWriteRecord`.
  - Cross-tier query already supports tier scope, merge, dedupe, ordering, and metrics.
  - `run_lifecycle_once()` handles TTL hard-delete, retention demotion to the next colder initialized tier, and retention delete.
  - `run_maintenance_once_with_options()` wraps lifecycle, compaction, health, timeout guard, and optional audit sink.

- `src/maintenance.rs` and `src/lifecycle.rs`
  - Maintenance and lifecycle reports already expose counts required by future policy observability.

- `src/queryable.rs`
  - `QueryableMarketDataStore` can use either in-memory storage or `TieredStorageStore`.
  - S12 proved storage-backed market-data write/query contracts but did not add automatic policy selection.

### Existing docs to preserve

- `generic-tiered-storage-query-design-analysis.md`: establishes storage as generic, not market-data-specific.
- `storage-boundary-acceptance-report.md`: records S12 readiness and known limitations.
- `production-hardening-followups.md`: lists P2/P3 production hardening work.
- `public-api-stability.md`: protects public storage-facing APIs.

## 3. Design principles

1. **Storage owns tier decisions.** Callers provide facts and hints. The policy engine produces final storage decisions.
2. **Hints are advisory, not commands.** `target_tier` remains useful for tests, migrations, and operator overrides, but normal business writes should not depend on it.
3. **Keep `fdc-storage` generic.** The policy cannot depend on barter, ingestion, transform, orchestrator, server, or API crates.
4. **Policy must be explainable.** Every decision should be inspectable in tests and later metrics/audit records.
5. **Start deterministic, then adapt.** The first implementation should be deterministic and testable. Adaptive hotness-based promotion/demotion should build on access metrics later.
6. **Do not break S12.** Existing `StorageWriteRecord`, `StorageWriteSink`, `QueryableStorage`, `TieredStorageStore`, and `QueryableMarketDataStore` consumers should continue to compile.

## 4. Proposed architecture

Add a policy module owned by `fdc-storage`:

```text
crates/fdc-storage/src/policy.rs
  StorageTieringPolicy
  StorageTieringPolicyProfile
  StorageTieringContext
  StorageTieringDecision
  StorageRetentionClass
  StorageTieringReason
```

Then integrate it into tier routing:

```text
StorageWriteRecord
  -> TieredStorageStore::write_batch
  -> TierManager::put_with_policy(record facts + configured policy)
  -> StorageTieringPolicy::decide_initial_placement(context)
  -> TierManager::put_to_specific_tier(decision.initial_tier)
```

The first implementation can preserve `put_with_placement()` and make it call the default policy internally. That avoids widening the public API too aggressively.

## 5. Policy input model

`StorageTieringContext` should be storage-generic and derived from `StorageWriteRecord` plus tier manager facts:

```rust
pub struct StorageTieringContext<'a> {
    pub namespace: &'a str,
    pub collection: &'a str,
    pub key_len: usize,
    pub value_len: usize,
    pub timestamp_age_seconds: i64,
    pub metadata_tags: &'a BTreeMap<String, String>,
    pub placement_hint: &'a StoragePlacementHint,
    pub available_tiers: &'a [StorageTier],
    pub prior_access: Option<AccessPatternSnapshot>,
}
```

The context should not contain business DTOs. Market-data-specific information must arrive only as generic metadata/tags, for example:

- `kind=trade`
- `kind=candle`
- `mode=live`
- `mode=backfill`
- `quality.is_replay=true`
- `symbol=BTCUSDT`

Storage may use those tags as generic rule inputs without depending on market-data types.

## 6. Policy output model

`StorageTieringDecision` should be explicit and testable:

```rust
pub struct StorageTieringDecision {
    pub initial_tier: StorageTier,
    pub ttl: Option<chrono::Duration>,
    pub retention_class: StorageRetentionClass,
    pub reasons: Vec<StorageTieringReason>,
}
```

Suggested retention classes:

```rust
pub enum StorageRetentionClass {
    Ephemeral,
    RealtimeHot,
    RecentDurable,
    AnalyticalWarm,
    ArchiveCold,
}
```

Suggested reason vocabulary:

```rust
pub enum StorageTieringReason {
    ExplicitTargetTierHint,
    AccessPatternHint,
    DurabilityHint,
    ExistingAccessPattern,
    LiveRecentWrite,
    BackfillOrReplay,
    LargePayload,
    OldTimestamp,
    NearestAvailableTierFallback,
    DefaultProfile,
}
```

This makes tests and future audit/metrics precise.

## 7. Initial policy profiles

### 7.1 `Compatibility` profile

Goal: preserve current behavior.

Rules:

1. If `target_tier` is set, use nearest initialized tier to that target.
2. Else map `access_pattern` hints to L1-L4.
3. Else map `durability` hints to L1-L4.
4. Else if a prior access pattern exists, use its recommended tier.
5. Else default to nearest available L2.

This profile should become the default for the first implementation to minimize behavior drift.

### 7.2 `MarketDataRealtime` profile

Goal: align with FDC market-data runtime without making storage market-data-specific.

Rules based on generic tags:

1. `mode=live` and fresh timestamp -> L1 or L2 depending on durability hint.
2. `kind=trade` or `kind=order_book_l1` with live mode -> short hot TTL and `RealtimeHot` retention.
3. `kind=candle` -> L3 if available, because candles are analytical/time-series friendly.
4. `mode=backfill` or `quality.is_replay=true` -> L3/L4 depending on durability and age.
5. Very old event timestamps -> L3/L4.
6. Very large payloads -> avoid L1 unless caller explicitly asks for `UltraHot`.

This profile remains generic because it only interprets metadata tags and hints.

### 7.3 `Adaptive` profile, later

Goal: use access statistics and maintenance feedback.

Rules:

1. Promote records or key ranges with high recent query heat.
2. Demote records with low heat and expired hot retention windows.
3. Respect tier capacity and migration thresholds.
4. Emit decision metrics and audit entries.

This should be a later slice after deterministic policy contracts are stable.

## 8. Integration with lifecycle and maintenance

Current lifecycle demotes by tier retention config and deletes by TTL. The policy engine should not replace lifecycle. It should supply better initial placement and retention metadata.

Near-term integration:

- Policy applies at write time.
- `TieredStorageStore::run_lifecycle_once()` keeps using record TTL and tier retention config.
- `StorageMaintenanceReport` remains the operator-visible result of lifecycle and compaction.

Later integration:

- Add scheduled maintenance to run lifecycle and migration automatically.
- Add policy metrics: decisions by tier, reasons, fallbacks, promotions, demotions.
- Add audit entries for policy-driven moves.

## 9. Public API strategy

Initial implementation should keep public API changes small:

- Add new public types from `policy.rs` only after tests define their shape.
- Keep `StoragePlacementHint` stable.
- Keep `TieredStorageStore::memory_only()` behavior stable.
- Add optional constructors later, for example:

```rust
TieredStorageStore::with_policy(tier_manager, StorageTieringPolicy::compatibility())
TieredStorageStore::memory_only_with_policy(StorageTieringPolicy::market_data_realtime())
```

Avoid exposing market-data-specific names from `fdc-storage` public APIs.

## 10. Recommended implementation slices

### Slice P13a: deterministic policy module

- Add `src/policy.rs`.
- Add unit tests for compatibility decisions.
- Export policy types from `src/lib.rs`.
- Do not change routing behavior yet except through test-only direct policy calls.

### Slice P13b: integrate policy into write routing

- Teach `TierManager` or `TieredStorageStore` to use `StorageTieringPolicy` for `put_with_placement()` decisions.
- Keep current behavior as `Compatibility` profile.
- Add regression tests proving existing placement hint semantics still hold.

### Slice P13c: market-data tag-aware profile

- Add generic tag-driven rules for live/backfill/replay/candle/trade shapes.
- Add tests using generic `StorageWriteRecord` tags only.
- Do not depend on `fdc-transform` or market-data DTOs.

### Slice P13d: scheduled maintenance foundation

- Add caller-owned scheduler wrapper or runtime hook to periodically run maintenance and migration.
- Keep cancellation/shutdown explicit.
- Wire metrics snapshot export in a separate slice if needed.

### Slice P13e: adaptive heat-based routing

- Add access heat snapshots and policy feedback.
- Promote/demote based on query heat and age.
- Add metrics and audit evidence.

## 11. Acceptance criteria for the next coding plan

A good first implementation plan should prove:

1. `StorageTieringPolicy::compatibility()` returns the same tier decisions as current hint routing.
2. Explicit target tier remains an override but is documented as an operator/test escape hatch.
3. Unhinted writes still default to nearest available L2 under compatibility mode.
4. Generic tag-driven market-data-like records can be routed without any dependency on market-data crates.
5. All tests are in `fdc-storage` and pass with `rtk cargo test -p fdc-storage`.
6. Public API additions are documented in `public-api-stability.md` if exported.

## 12. Recommended next step

Write an implementation plan for Slice P13a and P13b first. Keep P13c as a second plan unless P13a/P13b are small enough after review. The first code change should be TDD: policy decision tests before policy implementation.
