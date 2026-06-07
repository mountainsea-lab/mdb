# Durable Tier Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let server runtime configure durable L2/L3/L4 physical tier paths while `fdc-storage` continues to receive only generic tier configs.

**Architecture:** Add optional tier path config to `fdc-server` runtime parsing, translate it in the server storage builder into generic `fdc_storage::TierConfig` values, and add a storage constructor that accepts caller-provided `TierConfig`s. Keep safe memory-backed defaults when paths are absent.

**Tech Stack:** Rust, Tokio async tests, `fdc-storage` generic tier manager, server env parsing tests, temp path based durable engine smoke tests.

---

## File Structure

- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add `MarketDataStorageTierRuntimeConfig` with optional `PathBuf`s.
  - Parse `FDC_MARKET_DATA_STORAGE_L2_REDB_PATH`, `FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH`, and `FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH`.
  - Reject empty path values with `Error::config`.
- Modify `crates/fdc-server/src/runtime/storage.rs`
  - Build generic `TierConfig`s from runtime tier paths.
  - Use durable engines only for tiers with paths.
  - Preserve existing memory-backed tiered default.
  - Add storage builder tests using tempfile under the OS temp directory, without adding new dependencies.
- Modify `crates/fdc-storage/src/tiered_store.rs`
  - Add `TieredStorageStore::with_policy_and_tier_configs(policy, configs)`.
  - Keep `memory_only_with_policy` implemented through the new constructor.
- Modify `crates/fdc-storage/src/queryable.rs` or the file where `QueryableMarketDataStore` constructors live
  - Add `QueryableMarketDataStore::tiered_with_policy_and_configs(policy, configs)`.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P19 status, verification commands, and next slice.

## Task 1: Runtime config parses durable tier paths

**Files:**
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Test: existing config tests in `crates/fdc-server/tests/runtime_config_contract.rs` or module tests if runtime config tests are colocated.

- [ ] **Step 1: Write failing tests for path parsing and empty values**

Add tests that construct env pairs like:

```rust
#[test]
fn parses_market_data_storage_tier_paths() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", "/tmp/fdc/l2.redb"),
        ("FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH", "/tmp/fdc/l3.duckdb"),
        ("FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH", "/tmp/fdc/l4-rocksdb"),
    ])
    .unwrap();

    assert_eq!(
        config.market_data_storage.tiers.l2_redb_path.as_deref(),
        Some(std::path::Path::new("/tmp/fdc/l2.redb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l3_duckdb_path.as_deref(),
        Some(std::path::Path::new("/tmp/fdc/l3.duckdb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l4_rocksdb_path.as_deref(),
        Some(std::path::Path::new("/tmp/fdc/l4-rocksdb"))
    );
}

#[test]
fn rejects_empty_market_data_storage_tier_path() {
    let error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", ""),
    ])
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH must not be empty"),
        "unexpected error: {error}"
    );
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract parses_market_data_storage_tier_paths rejects_empty_market_data_storage_tier_path
```

Expected: compile failure because `tiers` does not exist, or test failure because env vars are ignored.

- [ ] **Step 3: Implement config type and parsing**

In `config.rs`:

```rust
use std::{env, net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MarketDataStorageTierRuntimeConfig {
    pub l2_redb_path: Option<PathBuf>,
    pub l3_duckdb_path: Option<PathBuf>,
    pub l4_rocksdb_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataStorageRuntimeConfig {
    pub backend: MarketDataStorageBackendConfig,
    pub policy_profile: MarketDataStoragePolicyProfileConfig,
    pub tiers: MarketDataStorageTierRuntimeConfig,
}
```

Update `Default` to set `tiers: MarketDataStorageTierRuntimeConfig::default()`.

Add match arms:

```rust
"FDC_MARKET_DATA_STORAGE_L2_REDB_PATH" => {
    market_data_storage.tiers.l2_redb_path = Some(parse_non_empty_path(
        "FDC_MARKET_DATA_STORAGE_L2_REDB_PATH",
        value.as_ref(),
    )?);
}
"FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH" => {
    market_data_storage.tiers.l3_duckdb_path = Some(parse_non_empty_path(
        "FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH",
        value.as_ref(),
    )?);
}
"FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH" => {
    market_data_storage.tiers.l4_rocksdb_path = Some(parse_non_empty_path(
        "FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH",
        value.as_ref(),
    )?);
}
```

Add helper:

```rust
fn parse_non_empty_path(name: &str, value: &str) -> Result<PathBuf> {
    if value.trim().is_empty() {
        return Err(Error::config(format!("{name} must not be empty")));
    }
    Ok(PathBuf::from(value))
}
```

- [ ] **Step 4: Run runtime config tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: all runtime config contract tests pass.

- [ ] **Step 5: Commit config parsing**

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): parse durable storage tier paths"
```

## Task 2: Storage accepts caller-provided generic tier configs

**Files:**
- Modify: `crates/fdc-storage/src/tiered_store.rs`
- Modify: file containing `QueryableMarketDataStore` constructors, likely `crates/fdc-storage/src/queryable.rs`

- [ ] **Step 1: Write failing storage constructor test**

Add a test in `tiered_store.rs` that calls the new constructor with memory configs:

```rust
#[tokio::test]
async fn tiered_store_accepts_caller_provided_tier_configs() {
    let store = TieredStorageStore::with_policy_and_tier_configs(
        StorageTieringPolicy::generic_realtime(),
        vec![
            memory_tier_config(StorageTier::L1),
            memory_tier_config(StorageTier::L2),
            memory_tier_config(StorageTier::L3),
            memory_tier_config(StorageTier::L4),
        ],
    )
    .await
    .unwrap();

    let mut live = tagged_record(b"live-config", "BTCUSDT");
    live.metadata.tags.insert("mode".to_string(), "live".to_string());
    live.placement = StoragePlacementHint::default();

    store
        .write_batch(StorageWriteBatch::new(vec![live.clone()]))
        .await
        .unwrap();

    let live_key = TieredStorageStore::storage_key_for_record(&live);
    assert!(store
        .tier_manager()
        .get_from_tier(&live_key, &StorageTier::L2)
        .await
        .unwrap()
        .is_some());
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store_accepts_caller_provided_tier_configs
```

Expected: compile failure because `with_policy_and_tier_configs` does not exist.

- [ ] **Step 3: Implement `TieredStorageStore` constructor**

Add to `impl TieredStorageStore`:

```rust
pub async fn with_policy_and_tier_configs(
    policy: StorageTieringPolicy,
    configs: Vec<TierConfig>,
) -> Result<Self> {
    let mut manager = TierManager::with_policy(policy);
    for config in configs {
        manager.add_tier(config);
    }
    manager.initialize().await?;
    Ok(Self::new(Arc::new(manager)))
}
```

Update `memory_only_with_policy` to build the existing L1-L4 memory configs and call the new constructor.

- [ ] **Step 4: Add `QueryableMarketDataStore` constructor**

Add a public constructor near existing memory/tiered constructors:

```rust
pub async fn tiered_with_policy_and_configs(
    policy: StorageTieringPolicy,
    configs: Vec<TierConfig>,
) -> Result<Self> {
    Ok(Self::from_tiered_store(
        TieredStorageStore::with_policy_and_tier_configs(policy, configs).await?,
    ))
}
```

Use the existing wrapping pattern used by `memory_tiered_with_policy`.

- [ ] **Step 5: Run storage tests**

Run:

```bash
rtk cargo test -p fdc-storage tiered_store_accepts_caller_provided_tier_configs
rtk cargo test -p fdc-storage tiering_policy_contract
```

Expected: tests pass.

- [ ] **Step 6: Commit storage constructor**

```bash
git add crates/fdc-storage/src/tiered_store.rs crates/fdc-storage/src/queryable.rs
git commit -m "feat(storage): accept generic tier configs for tiered stores"
```

## Task 3: Server storage builder assembles durable tier configs

**Files:**
- Modify: `crates/fdc-server/src/runtime/storage.rs`
- Test: module tests in `crates/fdc-server/src/runtime/storage.rs`

- [ ] **Step 1: Write failing tests for durable assembly**

Add a helper that creates unique temp paths without adding dependencies:

```rust
fn unique_test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fdc-server-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
```

Add test:

```rust
#[tokio::test]
async fn tiered_runtime_config_uses_configured_durable_tier_paths() {
    let root = unique_test_path("durable-tier-paths");
    let l2_path = root.join("l2.redb");
    let l3_path = root.join("l3.duckdb");
    let l4_path = root.join("l4-rocksdb");

    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Tiered,
        policy_profile: MarketDataStoragePolicyProfileConfig::GenericRealtime,
        tiers: crate::MarketDataStorageTierRuntimeConfig {
            l2_redb_path: Some(l2_path.clone()),
            l3_duckdb_path: Some(l3_path.clone()),
            l4_rocksdb_path: Some(l4_path.clone()),
        },
    })
    .await
    .unwrap();

    store
        .write_batch(StorageWriteBatch::new(vec![
            tagged_record(b"durable-live", "live"),
            tagged_record(b"durable-backfill", "backfill"),
        ]))
        .await
        .unwrap();

    let hot = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .unwrap();
    let warm = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L3)),
        )
        .await
        .unwrap();

    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].key, b"durable-live".to_vec());
    assert_eq!(warm.len(), 1);
    assert_eq!(warm[0].key, b"durable-backfill".to_vec());
    assert!(l2_path.exists(), "redb file should exist at configured path");
    assert!(l3_path.exists(), "duckdb file should exist at configured path");
    assert!(l4_path.exists(), "rocksdb directory should exist at configured path");

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
```

Expected: compile failure because `tiers` was not used in storage builder yet, or assertion failure because durable paths are not created.

- [ ] **Step 3: Implement tier config assembly**

In `storage.rs`, import:

```rust
use std::path::Path;
use fdc_storage::{QueryableMarketDataStore, StorageEngineType, StorageTier, StorageTieringPolicy, TierConfig};
```

Add helpers:

```rust
fn tier_configs_from_runtime_config(config: &MarketDataStorageRuntimeConfig) -> Vec<TierConfig> {
    let mut configs = vec![memory_tier_config(StorageTier::L1)];

    configs.push(match config.tiers.l2_redb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L2, StorageEngineType::Redb, path),
        None => memory_tier_config(StorageTier::L2),
    });
    configs.push(match config.tiers.l3_duckdb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L3, StorageEngineType::DuckDB, path),
        None => memory_tier_config(StorageTier::L3),
    });
    configs.push(match config.tiers.l4_rocksdb_path.as_deref() {
        Some(path) => durable_tier_config(StorageTier::L4, StorageEngineType::RocksDB, path),
        None => memory_tier_config(StorageTier::L4),
    });

    configs
}

fn memory_tier_config(tier: StorageTier) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = StorageEngineType::Memory;
    config
}

fn durable_tier_config(tier: StorageTier, engine_type: StorageEngineType, path: &Path) -> TierConfig {
    let mut config = TierConfig::new(tier);
    config.engine_type = engine_type;
    config
        .engine_config
        .insert("db_path".to_string(), path.display().to_string());
    config
}
```

Update tiered branch:

```rust
MarketDataStorageBackendConfig::Tiered => {
    let policy = policy_from_runtime_config(config.policy_profile);
    let tier_configs = tier_configs_from_runtime_config(&config);
    QueryableMarketDataStore::tiered_with_policy_and_configs(policy, tier_configs).await
}
```

- [ ] **Step 4: Run server storage tests**

Run:

```bash
rtk cargo test -p fdc-server tiered_runtime_config_injects_generic_realtime_policy
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
```

Expected: both tests pass.

- [ ] **Step 5: Commit durable assembly**

```bash
git add crates/fdc-server/src/runtime/storage.rs
git commit -m "feat(server): assemble durable tier configs"
```

## Task 4: Verification and status docs

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server tiered_runtime_config_injects_generic_realtime_policy
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
rtk cargo test -p fdc-storage tiered_store_accepts_caller_provided_tier_configs
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt --package fdc-server --package fdc-storage --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Update development status**

Add a P19 entry that records:

- Runtime env vars for L2/L3/L4 paths.
- Server translates them into generic `TierConfig` with `db_path`.
- Defaults remain memory-backed when paths are absent.
- Verification commands and pass results.
- Next recommended slice.

- [ ] **Step 3: Commit status docs**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record durable tier configuration status"
```

- [ ] **Step 4: Final cleanliness check**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
