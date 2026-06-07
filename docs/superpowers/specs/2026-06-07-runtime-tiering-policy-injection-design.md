# P16 Runtime Tiering Policy Injection Design

Date: 2026-06-07
Module: `crates/fdc-storage` tiered store and `crates/fdc-server` runtime assembly
Status: approved design for implementation

## Goal

Make the runtime-selected storage policy profile actually affect tiered writes. After this slice, `FDC_MARKET_DATA_STORAGE_BACKEND=tiered` with `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime` builds a tiered market-data store whose write path uses `StorageTieringPolicy::generic_realtime()`.

## Boundary rule

`fdc-storage` remains a generic storage layer. It stores bytes and generic storage metadata, and it accepts generic policy profiles. It does not depend on market-data DTOs, barter, ingestion, transform, orchestrator, server, or API crates.

Callers own concrete data types and decide which generic metadata tags to attach. Storage owns final placement decisions.

## Design

Add policy injection to the storage write path without breaking existing APIs:

- Add `policy: StorageTieringPolicy` to `TierManager`.
- `TierManager::new()` keeps `StorageTieringPolicy::compatibility()` as default.
- Add `TierManager::with_policy(policy)` for configured managers.
- Add `TierManager::policy()` for tests/inspection.
- `TierManager::determine_tier_for_placement()` uses `self.policy` instead of hard-coded compatibility.

Add tiered store constructors:

- `TieredStorageStore::memory_only_with_policy(policy)`
- `QueryableMarketDataStore::memory_tiered_with_policy(policy)`

Keep existing constructors as compatibility wrappers.

Update server runtime builder:

- `Compatibility` maps to `StorageTieringPolicy::compatibility()`.
- `GenericRealtime` maps to `StorageTieringPolicy::generic_realtime()`.
- `memory` backend ignores tier policy because it is not tiered.

## Data flow

```text
FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime
  -> ServerRuntimeConfig.market_data_storage.policy_profile
  -> build_market_data_store_from_runtime_config
  -> QueryableMarketDataStore::memory_tiered_with_policy(StorageTieringPolicy::generic_realtime())
  -> TieredStorageStore::memory_only_with_policy
  -> TierManager::with_policy
  -> write_batch -> put_with_placement -> self.policy.decide_initial_placement
```

## Testing

Storage tests:

- Injected generic realtime policy routes `mode=live` fresh writes to L2.
- Injected generic realtime policy routes `mode=backfill` writes to L3.
- Existing `memory_only()` and `TierManager::new()` default to compatibility.

Server tests:

- Runtime builder with `tiered + generic_realtime` writes/query succeeds.
- If possible, inspect tier hits to confirm generic realtime policy is used through storage-level test, while server remains facade-oriented.

Dependency tests:

- `fdc-storage --test dependency_guard` must pass.

## Out of scope

- Physical path configuration for durable L2/L3/L4 engines.
- Business typed policies inside `fdc-storage`.
- Adaptive promotion/demotion scheduling.
- Metrics/exporter changes.

## Self-review

- Placeholder scan: no placeholder requirements remain.
- Scope check: one cohesive runtime-policy injection slice.
- Boundary check: no business dependency enters `fdc-storage`.
- Compatibility check: existing constructors remain and default to compatibility.
