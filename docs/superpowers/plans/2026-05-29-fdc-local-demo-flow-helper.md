# B17 Local Demo Flow Helper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a no-listener local demo flow helper in `fdc-api` that exercises the B16 unified demo router end to end and returns a typed summary.

**Architecture:** `fdc-api::demo_flow` builds an initialized in-memory state, calls the B16 router through Axum/Tower `oneshot`, decodes existing typed API responses, and returns a `DemoFlowSummary`. The helper is route-stack focused, not a production server, and it does not bind sockets.

**Tech Stack:** Rust 1.95, Axum, Tower `ServiceExt`, Tokio, Serde JSON, existing `fdc-api`, `fdc-server`, and `fdc-storage` crates.

---

## File Structure

- Create: `crates/fdc-api/src/demo_flow.rs`
  - Responsibility: demo request/summary DTOs, default fixture request, initialized demo state builder, in-memory route execution helper.
- Modify: `crates/fdc-api/src/lib.rs`
  - Responsibility: export the `demo_flow` module and public demo flow types/functions.
- Create: `crates/fdc-api/tests/demo_flow_contract.rs`
  - Responsibility: contract tests for deterministic no-listener demo execution and dependency boundaries.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record completed B17 work and verification evidence.

---

## Task 1: Add failing B17 demo flow contract tests

**Files:**
- Create: `crates/fdc-api/tests/demo_flow_contract.rs`

- [ ] **Step 1: Write the failing contract tests**

Create `crates/fdc-api/tests/demo_flow_contract.rs` with this content:

```rust
use std::{fs, path::PathBuf};

use fdc_api::{
    default_demo_flow_request, run_demo_flow_once, DemoFixtureTrade, DemoFlowRequest,
};

#[test]
fn default_demo_flow_request_is_deterministic_btcusdt_fixture() {
    let request = default_demo_flow_request();

    assert_eq!(request.query_symbol, "BTCUSDT");
    assert_eq!(request.query_limit, 10);
    assert_eq!(request.trades.len(), 1);
    assert_eq!(request.trades[0].symbol, "BTCUSDT");
    assert_eq!(request.trades[0].trade_id, "btc-demo-1");
    assert_eq!(request.trades[0].sequence.as_deref(), Some("seq-demo-1"));
}

#[tokio::test]
async fn demo_flow_default_request_returns_ready_completed_and_queryable_trade() {
    let summary = run_demo_flow_once(default_demo_flow_request())
        .await
        .expect("default demo flow should run");

    assert_eq!(summary.readiness.status, fdc_api::ApiReadinessStatus::Ready);
    assert_eq!(summary.readiness.server_lifecycle_state, "initialized");
    assert_eq!(
        summary.start_status.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        summary.final_status.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        summary
            .final_status
            .last_result
            .as_ref()
            .expect("final status should include last result")
            .storage_records_written,
        1
    );
    assert_eq!(summary.market_data.returned_records, 1);
    assert_eq!(summary.market_data.records[0].symbol.as_deref(), Some("BTCUSDT"));
}

#[tokio::test]
async fn demo_flow_can_query_one_symbol_from_multiple_fixture_trades() {
    let request = DemoFlowRequest {
        trades: vec![
            DemoFixtureTrade {
                symbol: "BTCUSDT".to_string(),
                trade_id: "btc-demo-1".to_string(),
                sequence: Some("seq-demo-1".to_string()),
            },
            DemoFixtureTrade {
                symbol: "ETHUSDT".to_string(),
                trade_id: "eth-demo-1".to_string(),
                sequence: Some("seq-demo-2".to_string()),
            },
        ],
        query_symbol: "ETHUSDT".to_string(),
        query_limit: 10,
    };

    let summary = run_demo_flow_once(request)
        .await
        .expect("multi-trade demo flow should run");

    assert_eq!(
        summary
            .final_status
            .last_result
            .as_ref()
            .expect("final status should include last result")
            .storage_records_written,
        2
    );
    assert_eq!(summary.market_data.returned_records, 1);
    assert_eq!(summary.market_data.records[0].symbol.as_deref(), Some("ETHUSDT"));
}

#[tokio::test]
async fn demo_flow_rejects_empty_fixture_trade_request() {
    let request = DemoFlowRequest {
        trades: Vec::new(),
        query_symbol: "BTCUSDT".to_string(),
        query_limit: 10,
    };

    let error = run_demo_flow_once(request)
        .await
        .expect_err("empty demo flow request should fail");

    assert!(error.to_string().contains("at least one fixture trade"));
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
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
```

Expected: FAIL because `default_demo_flow_request`, `run_demo_flow_once`, `DemoFixtureTrade`, and `DemoFlowRequest` do not exist yet.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-api/tests/demo_flow_contract.rs
rtk git commit -m "test: add local demo flow contract"
```

---

## Task 2: Implement demo flow public API and in-memory execution

**Files:**
- Create: `crates/fdc-api/src/demo_flow.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Add demo flow implementation**

Create `crates/fdc-api/src/demo_flow.rs` with this content:

```rust
use std::sync::Arc;

use axum::{body::Body, http::{Request, StatusCode}, Router};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Mutex;
use tower::ServiceExt;

use crate::{
    build_demo_router, ApiAppState, ApiError, ApiReadinessProjection, ApiResponse,
    ApiRunnerStatusProjection, MarketDataTradesResponse, RunnerFixtureTradeInput,
    RunnerStartFixtureRequest,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoFixtureTrade {
    pub symbol: String,
    pub trade_id: String,
    pub sequence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoFlowRequest {
    pub trades: Vec<DemoFixtureTrade>,
    pub query_symbol: String,
    pub query_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DemoFlowSummary {
    pub readiness: ApiReadinessProjection,
    pub start_status: ApiRunnerStatusProjection,
    pub final_status: ApiRunnerStatusProjection,
    pub market_data: MarketDataTradesResponse,
}

pub fn default_demo_flow_request() -> DemoFlowRequest {
    DemoFlowRequest {
        trades: vec![DemoFixtureTrade {
            symbol: "BTCUSDT".to_string(),
            trade_id: "btc-demo-1".to_string(),
            sequence: Some("seq-demo-1".to_string()),
        }],
        query_symbol: "BTCUSDT".to_string(),
        query_limit: 10,
    }
}

pub async fn run_demo_flow_once(request: DemoFlowRequest) -> Result<DemoFlowSummary, ApiError> {
    if request.trades.is_empty() {
        return Err(ApiError::validation(
            "demo flow requires at least one fixture trade",
        ));
    }

    let router = build_demo_router(build_demo_state()?);

    let readiness: ApiResponse<ApiReadinessProjection> =
        get_json(router.clone(), "/ready", "ready").await?;
    let start_status: ApiResponse<ApiRunnerStatusProjection> = post_json(
        router.clone(),
        "/runner/start-fixture",
        &RunnerStartFixtureRequest {
            trades: request.trades.into_iter().map(Into::into).collect(),
        },
        "runner start fixture",
    )
    .await?;
    ensure_api_success(&start_status, "runner start fixture")?;

    let final_status: ApiResponse<ApiRunnerStatusProjection> =
        get_json(router.clone(), "/runner/status", "runner status").await?;
    let market_data_path = format!(
        "/market-data/trades?symbol={}&limit={}",
        request.query_symbol, request.query_limit
    );
    let market_data: ApiResponse<MarketDataTradesResponse> =
        get_json(router, &market_data_path, "market data query").await?;

    Ok(DemoFlowSummary {
        readiness: readiness.data,
        start_status: start_status.data,
        final_status: final_status.data,
        market_data: market_data.data,
    })
}

fn build_demo_state() -> Result<ApiAppState, ApiError> {
    let mut app = FdcServerApp::with_defaults();
    app.initialize()
        .map_err(|error| ApiError::internal(error.to_string()))?;

    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));

    Ok(ApiAppState::new(app)
        .with_market_data_store(store)
        .with_market_data_runner_control(runner))
}

async fn get_json<T>(router: Router, path: &str, context: &str) -> Result<T, ApiError>
where
    T: DeserializeOwned,
{
    let response = router
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .map_err(|error| ApiError::internal(format!("{context} request build failed: {error}")))?,
        )
        .await
        .map_err(|error| ApiError::internal(format!("{context} route failed: {error}")))?;

    decode_json_response(response.status(), response.into_body(), context).await
}

async fn post_json<T, B>(
    router: Router,
    path: &str,
    body: &B,
    context: &str,
) -> Result<T, ApiError>
where
    T: DeserializeOwned,
    B: Serialize,
{
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(body).map_err(|error| {
                        ApiError::internal(format!("{context} json encode failed: {error}"))
                    })?,
                ))
                .map_err(|error| ApiError::internal(format!("{context} request build failed: {error}")))?,
        )
        .await
        .map_err(|error| ApiError::internal(format!("{context} route failed: {error}")))?;

    decode_json_response(response.status(), response.into_body(), context).await
}

async fn decode_json_response<T>(
    status: StatusCode,
    body: Body,
    context: &str,
) -> Result<T, ApiError>
where
    T: DeserializeOwned,
{
    if !status.is_success() {
        return Err(ApiError::internal(format!(
            "{context} route returned HTTP {status}"
        )));
    }

    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|error| ApiError::internal(format!("{context} body read failed: {error}")))?;

    serde_json::from_slice(&bytes)
        .map_err(|error| ApiError::internal(format!("{context} json decode failed: {error}")))
}

fn ensure_api_success<T>(response: &ApiResponse<T>, context: &str) -> Result<(), ApiError> {
    if response.status == "success" {
        return Ok(());
    }

    Err(ApiError::validation(
        response
            .message
            .clone()
            .unwrap_or_else(|| format!("{context} returned API error")),
    ))
}

impl From<DemoFixtureTrade> for RunnerFixtureTradeInput {
    fn from(value: DemoFixtureTrade) -> Self {
        Self {
            symbol: value.symbol,
            trade_id: value.trade_id,
            sequence: value.sequence,
        }
    }
}
```

- [ ] **Step 2: Export demo flow API**

Modify `crates/fdc-api/src/lib.rs`.

Add module declaration after `pub mod demo;`:

```rust
pub mod demo_flow; // no-listener local demo flow helper
```

Add re-export after `pub use demo::build_demo_router;`:

```rust
pub use demo_flow::{
    default_demo_flow_request, run_demo_flow_once, DemoFixtureTrade, DemoFlowRequest,
    DemoFlowSummary,
};
```

- [ ] **Step 3: Run B17 contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
```

Expected: PASS, all five tests pass.

- [ ] **Step 4: Run B16 regression contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Expected: PASS, 4 tests pass.

- [ ] **Step 5: Format and commit implementation**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api
rtk git add crates/fdc-api/src/demo_flow.rs crates/fdc-api/src/lib.rs
rtk git commit -m "feat: add local demo flow helper"
```

---

## Task 3: Verify integration and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run full B17 verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: every command PASS.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, add a new completed-work section after B16:

```markdown
### Phase B17: Local Demo Flow Helper

Implemented in `crates/fdc-api/src/demo_flow.rs`.

Completed capabilities:

- Added `default_demo_flow_request` with deterministic BTCUSDT fixture input.
- Added `run_demo_flow_once` for no-listener in-memory demo execution.
- Added typed demo DTOs: `DemoFixtureTrade`, `DemoFlowRequest`, and `DemoFlowSummary`.
- Exercised the B16 unified router through Axum/Tower `oneshot` calls rather than direct helper shortcuts.
- Verified ready -> start-fixture -> status -> market-data query returns one typed summary.
- Verified multiple fixture trades can be filtered by query symbol.
- Preserved dependency boundaries: lower-level crates do not reference `fdc-api`.
- Kept real listener binding, CLI/binary startup, live network acquisition, persistence, SQL integration, and production daemon supervision out of scope.

Contract tests:

- `crates/fdc-api/tests/demo_flow_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-29-fdc-local-demo-flow-helper-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-local-demo-flow-helper.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server`
```

Also update:

- `Last updated: 2026-05-29`
- Latest checkpoint commit description to `feat: add local demo flow helper`
- Next recommended slice to `B18: Demo Documentation or Gated HTTP Demo Entrypoint`

- [ ] **Step 3: Commit status update and plan**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-29-fdc-local-demo-flow-helper.md
rtk git commit -m "docs: record local demo flow helper"
```

## Self-Review

- Spec coverage: all B17 design goals map to Task 1 tests, Task 2 implementation, and Task 3 verification/status documentation.
- Placeholder scan: no TBD/TODO placeholders remain.
- Type consistency: public DTO/function names match the B17 design spec and test imports.
