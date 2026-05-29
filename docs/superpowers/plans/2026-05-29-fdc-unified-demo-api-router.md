# B16 Unified Demo API Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a unified in-memory demo router in `fdc-api` that composes readiness, runner status, runner control, and market-data query routes.

**Architecture:** `fdc-api::demo` owns only route assembly and a typed `/ready` handler. It merges the existing focused route modules so B16 adds glue without duplicating runner or market-data logic. Tests exercise the full bounded demo flow through one shared `ApiAppState` and in-memory Axum router.

**Tech Stack:** Rust 1.95, Axum, Tokio, Tower `ServiceExt`, Serde JSON, existing `fdc-api`, `fdc-server`, and `fdc-storage` crates.

---

## File Structure

- Create: `crates/fdc-api/src/demo.rs`
  - Responsibility: build the unified demo router and serve typed readiness from `ApiAppState`.
- Modify: `crates/fdc-api/src/lib.rs`
  - Responsibility: export the new `demo` module and `build_demo_router` function.
- Create: `crates/fdc-api/tests/demo_router_contract.rs`
  - Responsibility: contract tests for unified router composition and dependency guard.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record completed B16 capabilities and verification evidence.

---

## Task 1: Add failing unified demo router contract tests

**Files:**
- Create: `crates/fdc-api/tests/demo_router_contract.rs`

- [ ] **Step 1: Write the failing test file**

Create `crates/fdc-api/tests/demo_router_contract.rs` with this content:

```rust
use std::{fs, path::PathBuf, sync::Arc};

use axum::{body::Body, http::{Request, StatusCode}};
use fdc_api::{
    build_demo_router, ApiAppState, ApiResponse, ApiRunnerStatusProjection,
    MarketDataTradesResponse, RunnerStartFixtureRequest,
};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use serde_json::json;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn initialized_state_with_control_runner() -> ApiAppState {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("test app should initialize");

    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(&store))));

    ApiAppState::new(app)
        .with_market_data_store(store)
        .with_market_data_runner_control(runner)
}

#[tokio::test]
async fn demo_router_ready_route_returns_typed_readiness() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let response = router
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be json");

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["status"], "ready");
    assert_eq!(json["data"]["server_lifecycle_state"], "initialized");
}

#[tokio::test]
async fn demo_router_runs_fixture_then_status_and_market_data_queries_share_state() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let start_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/start-fixture")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "trades": [
                            {"symbol": "BTCUSDT", "trade_id": "btc-demo-1", "sequence": "seq-demo-1"}
                        ]
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("start route should respond");

    assert_eq!(start_response.status(), StatusCode::OK);
    let start_body = axum::body::to_bytes(start_response.into_body(), usize::MAX)
        .await
        .expect("start body should read");
    let start: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&start_body).expect("start response should decode");
    assert_eq!(start.status, "success");
    assert_eq!(start.data.state, fdc_api::ApiRunnerLifecycleStatus::Completed);
    assert_eq!(start.data.last_result.as_ref().unwrap().storage_records_written, 1);

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runner/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status route should respond");

    assert_eq!(status_response.status(), StatusCode::OK);
    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX)
        .await
        .expect("status body should read");
    let status: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&status_body).expect("status response should decode");
    assert_eq!(status.data.state, fdc_api::ApiRunnerLifecycleStatus::Completed);
    assert_eq!(status.data.last_result.as_ref().unwrap().market_data_store_records, 1);

    let market_data_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("market-data route should respond");

    assert_eq!(market_data_response.status(), StatusCode::OK);
    let market_data_body = axum::body::to_bytes(market_data_response.into_body(), usize::MAX)
        .await
        .expect("market-data body should read");
    let market_data: ApiResponse<MarketDataTradesResponse> =
        serde_json::from_slice(&market_data_body).expect("market-data response should decode");

    assert_eq!(market_data.status, "success");
    assert_eq!(market_data.data.returned_records, 1);
    assert_eq!(market_data.data.records[0].symbol.as_deref(), Some("BTCUSDT"));
}

#[tokio::test]
async fn demo_router_cancel_route_uses_same_control_surface() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/cancel")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("cancel route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let cancel: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&body).expect("cancel response should decode");

    assert_eq!(cancel.status, "success");
    assert_eq!(cancel.data.state, fdc_api::ApiRunnerLifecycleStatus::Cancelled);
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_api() {
    let root = workspace_root();
    let forbidden = collect_forbidden_references(&root);

    assert!(
        forbidden.is_empty(),
        "lower-level crates must not reference fdc-api: {forbidden:?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}

fn collect_forbidden_references(root: &PathBuf) -> Vec<String> {
    let lower_level_crates = [
        "crates/fdc-barter",
        "crates/fdc-ingestion",
        "crates/fdc-transform",
        "crates/fdc-orchestrator",
        "crates/fdc-storage",
        "crates/fdc-server",
    ];

    lower_level_crates
        .iter()
        .flat_map(|crate_path| {
            let crate_root = root.join(crate_path);
            let cargo_toml = crate_root.join("Cargo.toml");
            let src_dir = crate_root.join("src");
            let mut hits = Vec::new();

            if cargo_toml.exists() {
                let content = fs::read_to_string(&cargo_toml).expect("Cargo.toml should read");
                if content.contains("fdc-api") || content.contains("fdc_api") {
                    hits.push(cargo_toml.display().to_string());
                }
            }

            if src_dir.exists() {
                collect_source_hits(&src_dir, &mut hits);
            }

            hits
        })
        .collect()
}

fn collect_source_hits(dir: &PathBuf, hits: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("source dir should read") {
        let entry = entry.expect("source entry should read");
        let path = entry.path();
        if path.is_dir() {
            collect_source_hits(&path, hits);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            let content = fs::read_to_string(&path).expect("source file should read");
            if content.contains("fdc_api") || content.contains("fdc-api") {
                hits.push(path.display().to_string());
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Expected: FAIL because `fdc_api::build_demo_router` does not exist yet. If it fails only because of an unused import, remove that import and rerun until the expected missing-symbol failure is visible.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-api/tests/demo_router_contract.rs
rtk git commit -m "test: add unified demo api router contract"
```

---

## Task 2: Implement the unified demo router

**Files:**
- Create: `crates/fdc-api/src/demo.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Add the demo router module implementation**

Create `crates/fdc-api/src/demo.rs` with this content:

```rust
use axum::{extract::State, routing::get, Json, Router};

use crate::{
    build_market_data_router, build_runner_control_router, build_runner_status_router,
    readiness_response_from_state, ApiAppState, ApiReadinessProjection, ApiResponse,
};

pub fn build_demo_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/ready", get(readiness_handler))
        .merge(build_runner_status_router(state.clone()))
        .merge(build_runner_control_router(state.clone()))
        .merge(build_market_data_router(state.clone()))
        .with_state(state)
}

async fn readiness_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiReadinessProjection>> {
    Json(readiness_response_from_state(&state))
}
```

- [ ] **Step 2: Export the demo module and router function**

Modify `crates/fdc-api/src/lib.rs`:

Add this module declaration after `pub mod config;`:

```rust
pub mod demo; // unified bounded demo API router
```

Add this re-export after the config/error exports:

```rust
pub use demo::build_demo_router;
```

The top section should include these lines:

```rust
pub mod auth; // 认证和授权
pub mod config; // API配置
pub mod demo; // unified bounded demo API router
pub mod errors; // API错误处理
```

And the re-export section should include:

```rust
pub use config::ApiConfig;
pub use demo::build_demo_router;
pub use errors::{ApiError, ApiResult};
```

- [ ] **Step 3: Run the new contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Expected: PASS, all four tests in `demo_router_contract` pass.

- [ ] **Step 4: Run neighboring API route contract tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
```

Expected: all commands PASS.

- [ ] **Step 5: Format and commit implementation**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api
rtk git add crates/fdc-api/src/demo.rs crates/fdc-api/src/lib.rs
rtk git commit -m "feat: add unified demo api router"
```

---

## Task 3: Verify integration and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run full B16 verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: every command PASS.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, add a new completed-work section after the B15 section or near the latest completed phases:

```markdown
### fdc-api Phase B16: Unified Demo API Router

Implemented in `crates/fdc-api/src/demo.rs`.

Completed capabilities:

- Added `build_demo_router(state)` for in-memory bounded MVP demos.
- Combined typed readiness, runner status, runner control, and market-data query routes into one shared-state Axum router.
- Verified `POST /runner/start-fixture` writes records that are immediately readable through `GET /market-data/trades` on the same demo router.
- Verified `POST /runner/cancel` works through the unified router for a fresh created runner.
- Preserved dependency boundaries: lower-level crates do not reference `fdc-api`.

Contract tests:

- `crates/fdc-api/tests/demo_router_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-unified-demo-api-router-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-unified-demo-api-router.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server`
```

Also update the `Last updated` date to `2026-05-29` and the latest checkpoint commit description to `feat: add unified demo api router` after that commit exists.

- [ ] **Step 3: Commit status update**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-29-fdc-unified-demo-api-router.md
rtk git commit -m "docs: record unified demo api router"
```

## Self-Review

- Spec coverage: all B16 design requirements map to Task 1 tests, Task 2 implementation, and Task 3 verification/status documentation.
- Placeholder scan: no TBD/TODO placeholders remain.
- Type consistency: public function is consistently named `build_demo_router(state: ApiAppState) -> Router`; tests import existing exported DTOs and route response types.
