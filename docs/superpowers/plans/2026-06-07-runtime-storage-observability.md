# Runtime Storage Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe server runtime storage status summary endpoint for market-data storage backend, policy profile, tier engines, and durable path presence.

**Architecture:** Keep observability server-owned. Add serializable response structs in `market_data/model.rs`, build summaries from `ProductionServerState::config().market_data_storage` in `market_data/service.rs`, and expose them through `GET /market-data/storage/status` in `market_data/router.rs`.

**Tech Stack:** Rust, Axum, Serde, existing `ServerApiResponse`, existing `ServerRuntimeConfig` and market-data router tests.

---

## File Structure

- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add `MarketDataStorageStatusResponse` and `MarketDataStorageTierStatus`.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Add `storage_status(&ProductionServerState) -> MarketDataStorageStatusResponse`.
  - Add private helpers for backend/profile strings, tier summaries, and safe path hints.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add route `GET /market-data/storage/status`.
  - Add handler returning `ServerApiResponse<MarketDataStorageStatusResponse>`.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route contract tests for default memory and durable tiered config.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P21 status and next slice.

## Task 1: Add failing route contract tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add default memory status route test**

Add:

```rust
#[tokio::test]
async fn production_storage_status_reports_memory_defaults() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["backend"], "memory");
    assert_eq!(json["data"]["policy_profile"], "compatibility");
    assert_eq!(json["data"]["tiers"].as_array().unwrap().len(), 4);
    assert_eq!(json["data"]["tiers"][0]["tier"], "L1");
    assert_eq!(json["data"]["tiers"][0]["engine"], "memory");
    assert_eq!(json["data"]["tiers"][0]["durable_path_configured"], false);
    assert!(json["data"]["tiers"]
        .as_array()
        .unwrap()
        .iter()
        .all(|tier| tier["engine"] == "memory"));
}
```

- [ ] **Step 2: Add durable tiered status route test**

Add:

```rust
#[tokio::test]
async fn production_storage_status_reports_durable_tiered_config_without_full_paths() {
    let root = unique_test_path("storage-status");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["data"]["backend"], "tiered");
    assert_eq!(json["data"]["policy_profile"], "generic_realtime");
    assert_eq!(json["data"]["tiers"][1]["tier"], "L2");
    assert_eq!(json["data"]["tiers"][1]["engine"], "redb");
    assert_eq!(json["data"]["tiers"][1]["durable_path_configured"], true);
    assert_eq!(json["data"]["tiers"][1]["path_hint"], "l2.redb");
    assert_eq!(json["data"]["tiers"][2]["engine"], "duckdb");
    assert_eq!(json["data"]["tiers"][2]["path_hint"], "l3.duckdb");
    assert_eq!(json["data"]["tiers"][3]["engine"], "rocksdb");
    assert_eq!(json["data"]["tiers"][3]["path_hint"], "l4-rocksdb");

    let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
    assert!(
        !body_text.contains(root.to_string_lossy().as_ref()),
        "storage status must not leak full configured paths: {body_text}"
    );
}
```

- [ ] **Step 3: Run tests red**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected: fail because route is not implemented yet.

## Task 2: Implement storage status model and service

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Add response models**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageStatusResponse {
    pub backend: String,
    pub policy_profile: String,
    pub tiers: Vec<MarketDataStorageTierStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageTierStatus {
    pub tier: String,
    pub engine: String,
    pub durable_path_configured: bool,
    pub path_hint: Option<String>,
}
```

- [ ] **Step 2: Implement service helper**

In `service.rs`, import the config enums and model types:

```rust
use std::{path::Path, sync::Arc, time::Duration};

use crate::{
    market_data::model::{
        LiveMarketDataStatusResponse, MarketDataStorageStatusResponse,
        MarketDataStorageTierStatus, MarketDataTradeRecord, MarketDataTradesResponse,
        StartLiveMarketDataRequest, StartLiveMarketDataResponse, StopLiveMarketDataResponse,
    },
    run_realtime_barter_envelope_stream, MarketDataStorageBackendConfig,
    MarketDataStoragePolicyProfileConfig, ProductionServerState, RealtimeMarketDataMvpConfig,
};
```

Add:

```rust
pub fn storage_status(state: &ProductionServerState) -> MarketDataStorageStatusResponse {
    let storage = &state.config().market_data_storage;
    let tiered = storage.backend == MarketDataStorageBackendConfig::Tiered;

    MarketDataStorageStatusResponse {
        backend: backend_label(storage.backend).to_string(),
        policy_profile: policy_profile_label(storage.policy_profile).to_string(),
        tiers: vec![
            memory_tier_status("L1"),
            tier_status("L2", tiered, "redb", storage.tiers.l2_redb_path.as_deref()),
            tier_status("L3", tiered, "duckdb", storage.tiers.l3_duckdb_path.as_deref()),
            tier_status("L4", tiered, "rocksdb", storage.tiers.l4_rocksdb_path.as_deref()),
        ],
    }
}

fn backend_label(backend: MarketDataStorageBackendConfig) -> &'static str {
    match backend {
        MarketDataStorageBackendConfig::Memory => "memory",
        MarketDataStorageBackendConfig::Tiered => "tiered",
    }
}

fn policy_profile_label(profile: MarketDataStoragePolicyProfileConfig) -> &'static str {
    match profile {
        MarketDataStoragePolicyProfileConfig::Compatibility => "compatibility",
        MarketDataStoragePolicyProfileConfig::GenericRealtime => "generic_realtime",
    }
}

fn memory_tier_status(tier: &str) -> MarketDataStorageTierStatus {
    MarketDataStorageTierStatus {
        tier: tier.to_string(),
        engine: "memory".to_string(),
        durable_path_configured: false,
        path_hint: None,
    }
}

fn tier_status(
    tier: &str,
    tiered_backend: bool,
    durable_engine: &str,
    configured_path: Option<&Path>,
) -> MarketDataStorageTierStatus {
    match (tiered_backend, configured_path) {
        (true, Some(path)) => MarketDataStorageTierStatus {
            tier: tier.to_string(),
            engine: durable_engine.to_string(),
            durable_path_configured: true,
            path_hint: Some(safe_path_hint(path)),
        },
        _ => memory_tier_status(tier),
    }
}

fn safe_path_hint(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| "configured".to_string())
}
```

## Task 3: Add storage status route

**Files:**
- Modify: `crates/fdc-server/src/market_data/router.rs`

- [ ] **Step 1: Wire route and handler**

Update imports to include `MarketDataStorageStatusResponse` and `storage_status`.

Add route before trades or live routes:

```rust
.route("/market-data/storage/status", get(storage_status_handler))
```

Add handler:

```rust
async fn storage_status_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataStorageStatusResponse>> {
    Json(ServerApiResponse::success(storage_status(&state)))
}
```

- [ ] **Step 2: Run route tests green**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
```

Expected: both storage status route tests pass.

- [ ] **Step 3: Commit implementation**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose safe storage runtime status"
```

## Task 4: Verification and status docs

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-server tiered_runtime_config_uses_configured_durable_tier_paths
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Update development status**

Add a P21 entry above P20 with:

- New `/market-data/storage/status` route.
- Safe summary fields.
- Path hint behavior and no full path disclosure.
- Verification commands and results.
- Next recommended slice.

- [ ] **Step 3: Commit status docs**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record runtime storage observability status"
```

- [ ] **Step 4: Final cleanliness check**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
