# P14 Runtime Market-Data Storage Backend Selection Design

Date: 2026-06-07
Module: `crates/fdc-server` runtime assembly, using `crates/fdc-storage`
Status: approved design for implementation

## Goal

Make runtime/server configuration choose the market-data storage backend and storage policy profile, without requiring business writes to manually select physical storage tiers.

## Context

P13 added a storage-owned, deterministic tiering policy API in `fdc-storage`. The current production server state still constructs `QueryableMarketDataStore::new()`, which uses the in-memory facade by default. Tests can inject a storage-backed facade with `ProductionServerState::with_market_data_store`, but normal runtime assembly has no configuration path for selecting storage-backed market data.

## Design

Add server-owned runtime config for market-data storage:

- `MarketDataStorageBackendConfig`
  - `Memory`
  - `Tiered`
- `MarketDataStoragePolicyProfileConfig`
  - `Compatibility`
- `MarketDataStorageRuntimeConfig`
  - `backend`
  - `policy_profile`

The environment variables are:

- `FDC_MARKET_DATA_STORAGE_BACKEND=memory|tiered`
- `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=compatibility`

Defaults stay safe and compatible:

- backend: `memory`
- policy profile: `compatibility`

`fdc-server` owns the runtime builder. It converts config into a `QueryableMarketDataStore`:

- `memory` uses `QueryableMarketDataStore::in_memory()`.
- `tiered` uses `QueryableMarketDataStore::memory_tiered().await` for this slice.

This keeps P14 focused on runtime backend/profile selection. Durable tier path configuration, custom DuckDB/redb/RocksDB locations, and production disk health are separate follow-up work.

## API and crate boundaries

`fdc-storage` remains generic. It does not depend on `fdc-server`, market-data DTO crates, barter, ingestion, transform, API, or orchestrator crates.

`fdc-server` depends on `fdc-storage` and is allowed to assemble `QueryableMarketDataStore` from runtime config. Business writes continue to use the existing market-data facade and generic storage records. Per-write tier selection is not introduced as a runtime feature.

## Runtime flow

```text
ServerRuntimeConfig::from_env_pairs
  -> MarketDataStorageRuntimeConfig
  -> build_market_data_store_from_runtime_config(config.market_data_storage)
  -> ProductionServerState::try_new(config).await
  -> market-data router/service uses QueryableMarketDataStore
  -> TieredStorageStore write path uses P13 compatibility policy
```

`ProductionServerState::new(config)` should remain available for compatibility and can keep the in-memory default. Add async construction for runtime config-driven assembly, for example `ProductionServerState::try_new(config).await`.

## Error handling

Invalid env values return `fdc_core::Error::config` with explicit messages:

- `FDC_MARKET_DATA_STORAGE_BACKEND must be memory or tiered, got <value>`
- `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility, got <value>`

The unsupported `market_data_realtime` profile should not be silently accepted in P14. It can be added in P15 after generic tag-aware policy rules exist.

## Testing

Add contract coverage for:

1. Runtime config defaults to memory backend and compatibility profile.
2. Env overrides accept `tiered` and `compatibility`.
3. Invalid backend/profile values return config errors.
4. Runtime builder creates an in-memory store for memory config and supports write/query through existing service helpers.
5. Runtime builder creates a tiered-backed store for tiered config and supports write/query through the production router.

Use existing server tests and `QueryableMarketDataStore::memory_tiered()` to avoid introducing real disk path configuration in P14.

## Out of scope

- Implementing `MarketDataRealtime` policy profile.
- Configuring individual physical tier paths.
- Scheduled maintenance, metrics exporters, audit persistence, or disk health.
- Changing market-data DTOs or storage record schema.

## Self-review

- Placeholder scan: no placeholder implementation requirements remain.
- Scope check: P14 is limited to server runtime config and assembly.
- Boundary check: storage remains generic; server performs assembly.
- Ambiguity check: unsupported policy profiles fail explicitly until P15.
