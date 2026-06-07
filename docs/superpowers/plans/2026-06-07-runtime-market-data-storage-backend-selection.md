# Runtime Market-Data Storage Backend Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `fdc-server` runtime config select the market-data storage backend and policy profile while keeping per-write tier placement storage-owned.

**Architecture:** Add config enums and a nested `MarketDataStorageRuntimeConfig` to `ServerRuntimeConfig`. Add a small server-owned async builder that creates `QueryableMarketDataStore` from the config, and add `ProductionServerState::try_new(config).await` for config-driven runtime assembly. Existing constructors remain compatible.

**Tech Stack:** Rust, Tokio, Axum server tests, `fdc-core::Error`, `fdc-storage::QueryableMarketDataStore`, TDD with `rtk cargo test -p fdc-server`.

---

## Reference design

Read first:

- `docs/superpowers/specs/2026-06-07-runtime-market-data-storage-backend-selection-design.md`
- `crates/fdc-server/src/runtime/config.rs`
- `crates/fdc-server/src/runtime/app.rs`
- `crates/fdc-server/tests/runtime_config_contract.rs`
- `crates/fdc-server/tests/production_server_router_contract.rs`

## File structure

- Modify `crates/fdc-server/src/runtime/config.rs`
  - Add `MarketDataStorageBackendConfig`, `MarketDataStoragePolicyProfileConfig`, and `MarketDataStorageRuntimeConfig`.
  - Parse `FDC_MARKET_DATA_STORAGE_BACKEND` and `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE`.
- Create `crates/fdc-server/src/runtime/storage.rs`
  - Owns `build_market_data_store_from_runtime_config`.
- Modify `crates/fdc-server/src/runtime/app.rs`
  - Add `ProductionServerState::try_new(config).await` using the storage builder.
  - Keep `new` and `with_market_data_store` unchanged for compatibility.
- Modify `crates/fdc-server/src/runtime/mod.rs`
  - Export config types and storage builder.
- Modify `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add config default/override/invalid tests.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add runtime-state tests for memory and tiered backend query behavior.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P14 completion and verification evidence after implementation.

---

## Task 1: Add runtime config contract tests

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`
- Modify: `crates/fdc-server/src/runtime/mod.rs`

- [ ] **Step 1: Write failing tests for defaults, env overrides, and invalid values**

Append these tests to `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
use fdc_server::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig,
};

#[test]
fn runtime_config_defaults_to_memory_market_data_storage() {
    let config = ServerRuntimeConfig::from_env_pairs(std::iter::empty::<(&str, &str)>()).unwrap();

    assert_eq!(
        config.market_data_storage,
        MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Memory,
            policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
        }
    );
}

#[test]
fn runtime_config_accepts_market_data_storage_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "compatibility"),
    ])
    .unwrap();

    assert_eq!(config.market_data_storage.backend, MarketDataStorageBackendConfig::Tiered);
    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::Compatibility
    );
}

#[test]
fn runtime_config_rejects_invalid_market_data_storage_env_values() {
    let backend_error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "l3"),
    ])
    .unwrap_err();
    assert!(backend_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_BACKEND must be memory or tiered"));

    let profile_error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "market_data_realtime"),
    ])
    .unwrap_err();
    assert!(profile_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility"));
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract runtime_config_defaults_to_memory_market_data_storage runtime_config_accepts_market_data_storage_env_overrides runtime_config_rejects_invalid_market_data_storage_env_values
```

Expected: FAIL because the new config types and field do not exist.

- [ ] **Step 3: Implement config types and parsing**

In `crates/fdc-server/src/runtime/config.rs`, add the config enums and struct above `ServerRuntimeConfig`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataStorageBackendConfig {
    Memory,
    Tiered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataStoragePolicyProfileConfig {
    Compatibility,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketDataStorageRuntimeConfig {
    pub backend: MarketDataStorageBackendConfig,
    pub policy_profile: MarketDataStoragePolicyProfileConfig,
}

impl Default for MarketDataStorageRuntimeConfig {
    fn default() -> Self {
        Self {
            backend: MarketDataStorageBackendConfig::Memory,
            policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
        }
    }
}
```

Add `pub market_data_storage: MarketDataStorageRuntimeConfig,` to `ServerRuntimeConfig`.

Inside `from_env_pairs`, initialize:

```rust
let mut market_data_storage = MarketDataStorageRuntimeConfig::default();
```

Add match arms:

```rust
"FDC_MARKET_DATA_STORAGE_BACKEND" => {
    market_data_storage.backend = match value.as_ref() {
        "memory" => MarketDataStorageBackendConfig::Memory,
        "tiered" => MarketDataStorageBackendConfig::Tiered,
        other => {
            return Err(Error::config(format!(
                "FDC_MARKET_DATA_STORAGE_BACKEND must be memory or tiered, got {other}"
            )));
        }
    };
}
"FDC_MARKET_DATA_STORAGE_POLICY_PROFILE" => {
    market_data_storage.policy_profile = match value.as_ref() {
        "compatibility" => MarketDataStoragePolicyProfileConfig::Compatibility,
        other => {
            return Err(Error::config(format!(
                "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility, got {other}"
            )));
        }
    };
}
```

Include `market_data_storage` in the returned `ServerRuntimeConfig`.

Update `crates/fdc-server/src/runtime/mod.rs` exports:

```rust
pub use config::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig, ServerRuntimeConfig, ServerRuntimeEnvironment,
};
```

- [ ] **Step 4: Run config tests and commit**

Run:

```bash
rtk cargo fmt --package fdc-server
rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: PASS.

Commit:

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/src/runtime/mod.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): add market data storage runtime config"
```

---

## Task 2: Add runtime market-data store builder

**Files:**
- Create: `crates/fdc-server/src/runtime/storage.rs`
- Modify: `crates/fdc-server/src/runtime/mod.rs`
- Test: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing builder tests**

Append imports and tests to `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
use fdc_server::{
    build_market_data_store_from_runtime_config, MarketDataStorageBackendConfig,
    MarketDataStoragePolicyProfileConfig, MarketDataStorageRuntimeConfig,
};
use fdc_storage::{StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord, StorageWriteSink};

fn runtime_storage_record(key: &str) -> StorageWriteRecord {
    let mut metadata = StorageWriteMetadata::default();
    metadata.tags.insert("symbol".to_string(), "BTCUSDT".to_string());
    metadata.tags.insert("kind".to_string(), "trade".to_string());
    StorageWriteRecord::new("market_data", "trades", key.as_bytes().to_vec(), b"value".to_vec())
        .with_metadata(metadata)
}

#[tokio::test]
async fn runtime_builder_creates_memory_market_data_store() {
    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Memory,
        policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
    })
    .await
    .unwrap();

    store
        .write_batch(StorageWriteBatch::new(vec![runtime_storage_record("memory-runtime")]))
        .await
        .unwrap();

    assert_eq!(store.record_count(), 1);
}

#[tokio::test]
async fn runtime_builder_creates_tiered_market_data_store() {
    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Tiered,
        policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
    })
    .await
    .unwrap();

    store
        .write_batch(StorageWriteBatch::new(vec![runtime_storage_record("tiered-runtime")]))
        .await
        .unwrap();

    assert_eq!(store.record_count(), 1);
}
```

- [ ] **Step 2: Run the failing builder tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract runtime_builder_creates_memory_market_data_store runtime_builder_creates_tiered_market_data_store
```

Expected: FAIL because the builder is missing.

- [ ] **Step 3: Implement the builder**

Create `crates/fdc-server/src/runtime/storage.rs`:

```rust
use fdc_core::Result;
use fdc_storage::QueryableMarketDataStore;

use crate::{MarketDataStorageBackendConfig, MarketDataStorageRuntimeConfig};

pub async fn build_market_data_store_from_runtime_config(
    config: MarketDataStorageRuntimeConfig,
) -> Result<QueryableMarketDataStore> {
    match config.backend {
        MarketDataStorageBackendConfig::Memory => Ok(QueryableMarketDataStore::in_memory()),
        MarketDataStorageBackendConfig::Tiered => QueryableMarketDataStore::memory_tiered().await,
    }
}
```

Modify `crates/fdc-server/src/runtime/mod.rs`:

```rust
pub mod storage;
pub use storage::build_market_data_store_from_runtime_config;
```

- [ ] **Step 4: Run builder tests and commit**

Run:

```bash
rtk cargo fmt --package fdc-server
rtk cargo test -p fdc-server --test production_server_router_contract runtime_builder_creates_memory_market_data_store runtime_builder_creates_tiered_market_data_store
```

Expected: PASS.

Commit:

```bash
git add crates/fdc-server/src/runtime/storage.rs crates/fdc-server/src/runtime/mod.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): build market data store from runtime config"
```

---

## Task 3: Wire config-driven ProductionServerState

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing production state/router test**

Append this test to `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn production_state_try_new_uses_tiered_runtime_storage_config() {
    let mut config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "compatibility"),
    ])
    .unwrap();
    config.live_enabled = false;

    let state = ProductionServerState::try_new(config).await.unwrap();
    state.ingest_test_trade("BTCUSDT", "tiered-state").await.unwrap();

    let app = build_production_router(state);
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), axum::http::StatusCode::OK);
}
```

- [ ] **Step 2: Run the failing production state test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_state_try_new_uses_tiered_runtime_storage_config
```

Expected: FAIL because `ProductionServerState::try_new` is missing.

- [ ] **Step 3: Implement async state construction**

Modify `crates/fdc-server/src/runtime/app.rs` imports to include the builder:

```rust
use crate::{
    health::build_health_router,
    market_data::{build_market_data_router, ingest_test_trade, supervisor::MarketDataSupervisor},
    build_market_data_store_from_runtime_config, ServerRuntimeConfig,
};
```

Add this method inside `impl ProductionServerState` after `new`:

```rust
pub async fn try_new(config: ServerRuntimeConfig) -> Result<Self> {
    let market_data_store = build_market_data_store_from_runtime_config(config.market_data_storage).await?;
    Ok(Self {
        config,
        market_data_store: Arc::new(market_data_store),
        market_data_supervisor: Arc::new(MarketDataSupervisor::new()),
    })
}
```

- [ ] **Step 4: Run production router tests and commit**

Run:

```bash
rtk cargo fmt --package fdc-server
rtk cargo test -p fdc-server --test production_server_router_contract production_state_try_new_uses_tiered_runtime_storage_config
rtk cargo test -p fdc-server --test production_server_router_contract production_trade_query_reads_tiered_backed_market_data_store
```

Expected: PASS.

Commit:

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): assemble production storage from runtime config"
```

---

## Task 4: Documentation and verification

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update development status**

Add a new note near the top of `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-07 P14 Runtime Storage Backend Selection

Completed:

- Added `FDC_MARKET_DATA_STORAGE_BACKEND=memory|tiered` runtime config.
- Added `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=compatibility` runtime config.
- Added server-owned market-data store assembly from runtime config.
- Preserved storage crate boundaries: `fdc-storage` remains generic and does not depend on server/business crates.

Verification:

- `rtk cargo test -p fdc-server --test runtime_config_contract`
- `rtk cargo test -p fdc-server --test production_server_router_contract`
- `rtk cargo test -p fdc-storage --test dependency_guard`
```

- [ ] **Step 2: Run final verification**

Run:

```bash
rtk cargo fmt --package fdc-server --check
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server --test production_server_router_contract
rtk cargo test -p fdc-storage --test dependency_guard
rtk git status --short
```

Expected: all tests PASS and no untracked test data remains except unrelated pre-existing files if any.

- [ ] **Step 3: Commit docs**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs(server): record runtime storage backend selection"
```

---

## Self-review

- Spec coverage: config defaults, env overrides, invalid values, memory/tiered builder, production state assembly, and docs are covered.
- Placeholder scan: no TBD/TODO/fill-in steps remain.
- Type consistency: config enum/struct names match across tests, implementation, exports, and builder.
- Scope: P14 does not implement market-data realtime policy or physical tier path config.
