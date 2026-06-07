# Storage Health Runtime Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only `GET /market-data/storage/health` runtime surface backed by existing generic tiered storage health snapshots.

**Architecture:** `fdc-storage` exposes a small facade method on `QueryableMarketDataStore` that returns `None` for in-memory storage and delegates to `TieredStorageStore::storage_health_snapshot()` for tiered storage. `fdc-server` converts the generic snapshot into server-owned response DTOs and routes it separately from P21 config status.

**Tech Stack:** Rust, Tokio, Axum, Serde, `fdc-storage`, `fdc-server`, package-scoped cargo tests.

---

## File map

- Modify `crates/fdc-storage/src/queryable.rs`
  - Add tests for storage health facade.
  - Add `QueryableMarketDataStore::storage_health_snapshot`.
- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add server-owned health response DTOs.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Add `MarketDataService::storage_health` and mapping helpers.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add `GET /market-data/storage/health` handler and route.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add memory and tiered route contract tests.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P22 when implementation is complete.

---

### Task 1: Add failing storage facade tests

**Files:**
- Modify: `crates/fdc-storage/src/queryable.rs`

- [ ] **Step 1: Add tests near existing `QueryableMarketDataStore` tests**

Add imports inside the existing test module if needed:

```rust
use crate::StorageTierHealthStatus;
```

Add tests:

```rust
#[tokio::test]
async fn in_memory_market_data_store_has_no_tier_health_snapshot() {
    let store = QueryableMarketDataStore::in_memory();

    let snapshot = store.storage_health_snapshot().await.unwrap();

    assert!(snapshot.is_none());
}

#[tokio::test]
async fn tiered_market_data_store_exposes_storage_health_snapshot() {
    let store = QueryableMarketDataStore::memory_tiered().await.unwrap();

    let snapshot = store.storage_health_snapshot().await.unwrap().unwrap();

    assert_eq!(snapshot.tiers.len(), 4);
    assert_eq!(snapshot.access_patterns, 0);
    assert_eq!(snapshot.migration_queue_len, 0);
    assert!(snapshot.tiers.values().all(|tier| tier.enabled));
    assert!(snapshot.tiers.values().all(|tier| tier.initialized));
    assert!(snapshot
        .tiers
        .values()
        .all(|tier| tier.status == StorageTierHealthStatus::Healthy));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
rtk cargo test -p fdc-storage queryable::tests::in_memory_market_data_store_has_no_tier_health_snapshot queryable::tests::tiered_market_data_store_exposes_storage_health_snapshot
```

Expected: compile failure because `storage_health_snapshot` does not exist on `QueryableMarketDataStore`.

- [ ] **Step 3: Implement the facade method**

In `crates/fdc-storage/src/queryable.rs`, import `StorageHealthSnapshot` if not already in scope and add this method inside `impl QueryableMarketDataStore`:

```rust
pub async fn storage_health_snapshot(&self) -> Result<Option<StorageHealthSnapshot>> {
    match &self.backend {
        QueryableMarketDataBackend::InMemory(_) => Ok(None),
        QueryableMarketDataBackend::Tiered(store) => Ok(Some(store.storage_health_snapshot().await?)),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
rtk cargo test -p fdc-storage queryable::tests::in_memory_market_data_store_has_no_tier_health_snapshot queryable::tests::tiered_market_data_store_exposes_storage_health_snapshot
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/fdc-storage/src/queryable.rs
rtk git commit -m "feat(storage): expose market data storage health facade"
```

---

### Task 2: Add failing server route contract tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add route tests**

Add tests near the existing storage status tests:

```rust
#[tokio::test]
async fn production_storage_health_memory_backend_reports_healthy_non_tiered() {
    let state = ProductionServerState::build_with_config(ProductionServerRuntimeConfig::default())
        .await
        .unwrap();
    let app = market_data_router(Arc::new(state));

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["backend"], "memory");
    assert_eq!(data["tiered"], false);
    assert_eq!(data["status"], "healthy");
    assert_eq!(data["access_patterns"], 0);
    assert_eq!(data["migration_queue_len"], 0);
    assert!(data["tiers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn production_storage_health_tiered_backend_reports_initialized_tiers() {
    let config = ProductionServerRuntimeConfig {
        market_data_storage: MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Tiered,
            policy_profile: MarketDataStoragePolicyProfile::GenericRealtime,
            tiers: MarketDataStorageTierRuntimeConfig::default(),
        },
        ..ProductionServerRuntimeConfig::default()
    };
    let state = ProductionServerState::build_with_config(config).await.unwrap();
    let app = market_data_router(Arc::new(state));

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["backend"], "tiered");
    assert_eq!(data["tiered"], true);
    assert_eq!(data["status"], "healthy");
    assert_eq!(data["access_patterns"], 0);
    assert_eq!(data["migration_queue_len"], 0);

    let tiers = data["tiers"].as_array().unwrap();
    assert_eq!(tiers.len(), 4);
    for expected_tier in ["L1", "L2", "L3", "L4"] {
        let tier = tiers
            .iter()
            .find(|tier| tier["tier"] == expected_tier)
            .unwrap_or_else(|| panic!("missing tier {expected_tier}"));
        assert_eq!(tier["enabled"], true);
        assert_eq!(tier["initialized"], true);
        assert_eq!(tier["status"], "healthy");
        assert!(tier.get("key_count").is_some());
        assert!(tier.get("total_size").is_some());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected: route returns 404 or compile failure because health route/model/service does not exist.

---

### Task 3: Implement server health model, service, and route

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`

- [ ] **Step 1: Add response DTOs**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataStorageHealthResponse {
    pub backend: String,
    pub tiered: bool,
    pub status: String,
    pub tiers: Vec<MarketDataStorageTierHealth>,
    pub access_patterns: usize,
    pub migration_queue_len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataStorageTierHealth {
    pub tier: String,
    pub enabled: bool,
    pub initialized: bool,
    pub status: String,
    pub key_count: Option<usize>,
    pub total_size: Option<u64>,
}
```

- [ ] **Step 2: Add service mapping**

In `service.rs`, import the new DTOs and storage status type:

```rust
use fdc_storage::StorageTierHealthStatus;
```

Add a method on `MarketDataService`:

```rust
pub async fn storage_health(&self) -> MarketDataStorageHealthResponse {
    let backend = self.state.config().market_data_storage.backend.as_str().to_string();
    match self.state.storage().storage_health_snapshot().await {
        Ok(Some(snapshot)) => {
            let tiers: Vec<_> = snapshot
                .tiers
                .values()
                .map(|tier| MarketDataStorageTierHealth {
                    tier: tier.tier.as_str().to_string(),
                    enabled: tier.enabled,
                    initialized: tier.initialized,
                    status: storage_tier_health_status_label(&tier.status).to_string(),
                    key_count: tier.stats.as_ref().map(|stats| stats.key_count),
                    total_size: tier.stats.as_ref().map(|stats| stats.total_size),
                })
                .collect();
            let status = if tiers.iter().all(|tier| tier.status == "healthy") {
                "healthy"
            } else {
                "degraded"
            };
            MarketDataStorageHealthResponse {
                backend,
                tiered: true,
                status: status.to_string(),
                tiers,
                access_patterns: snapshot.access_patterns,
                migration_queue_len: snapshot.migration_queue_len,
            }
        }
        Ok(None) => MarketDataStorageHealthResponse {
            backend,
            tiered: false,
            status: "healthy".to_string(),
            tiers: Vec::new(),
            access_patterns: 0,
            migration_queue_len: 0,
        },
        Err(error) => MarketDataStorageHealthResponse {
            backend,
            tiered: matches!(
                self.state.config().market_data_storage.backend,
                crate::runtime::config::MarketDataStorageBackendConfig::Tiered
            ),
            status: format!("unavailable: {error}"),
            tiers: Vec::new(),
            access_patterns: 0,
            migration_queue_len: 0,
        },
    }
}
```

Add helper:

```rust
fn storage_tier_health_status_label(status: &StorageTierHealthStatus) -> &'static str {
    match status {
        StorageTierHealthStatus::Healthy => "healthy",
        StorageTierHealthStatus::MissingEngine => "missing_engine",
        StorageTierHealthStatus::StatsUnavailable => "stats_unavailable",
    }
}
```

- [ ] **Step 3: Add route handler**

In `router.rs`, add route:

```rust
.route("/storage/health", get(storage_health_handler))
```

Add handler:

```rust
async fn storage_health_handler(
    State(state): State<Arc<ProductionServerState>>,
) -> Json<ServerApiResponse<MarketDataStorageHealthResponse>> {
    let service = MarketDataService::new(state);
    Json(ServerApiResponse::success(service.storage_health().await))
}
```

Ensure `MarketDataStorageHealthResponse` is imported.

- [ ] **Step 4: Run route tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
```

Expected: two tests pass.

- [ ] **Step 5: Commit**

```bash
rtk git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat(server): expose storage health runtime route"
```

---

### Task 4: Verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-storage queryable::tests::in_memory_market_data_store_has_no_tier_health_snapshot queryable::tests::tiered_market_data_store_exposes_storage_health_snapshot
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_status
rtk cargo test -p fdc-server --test runtime_config_contract
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all tests pass and package-scoped fmt check exits 0.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, mark P22 complete with:

- Design doc path
- Plan doc path
- Route added: `GET /market-data/storage/health`
- Storage facade added: `QueryableMarketDataStore::storage_health_snapshot`
- Verification commands and pass counts
- Note that maintenance remains explicit future work and was not wired to HTTP in this slice

- [ ] **Step 3: Commit status update**

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record storage health runtime surface status"
```

- [ ] **Step 4: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected: no output or `ok`.
