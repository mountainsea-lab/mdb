# B14 Runner Status API Projection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a stable API projection and in-memory route for observing B13 bounded runner status.

**Architecture:** Extend `ApiAppState` with an optional shared runner handle, add a focused `runner_status.rs` API module for DTOs/projection/route, and keep the route read-only. No production runner mutation, background runtime, persistence, SQL, or lower-level API dependency changes.

**Tech Stack:** Rust 2021, Axum in-memory router, Tokio tests, Serde DTOs, `fdc-api`, `fdc-server` runner types.

---

## File Structure

- Create `crates/fdc-api/src/runner_status.rs`
  - Defines `ApiRunnerLifecycleStatus`, `ApiRunnerLastResultProjection`, `ApiRunnerStatusProjection`.
  - Implements `runner_status_response_from_state` and `build_runner_status_router`.
- Modify `crates/fdc-api/src/state.rs`
  - Adds optional `Arc<BoundedMarketDataRunnerHandle>` to `ApiAppState`.
  - Adds `with_market_data_runner` and `market_data_runner` accessors.
- Modify `crates/fdc-api/src/lib.rs`
  - Exports new runner status module API.
- Create `crates/fdc-api/tests/runner_status_contract.rs`
  - Contract tests for no runner, created, completed, cancelled, and route JSON behavior.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Records B14 completion and next recommended slice.

## Task 1: Add failing runner status API contract tests

**Files:**
- Create: `crates/fdc-api/tests/runner_status_contract.rs`

- [ ] **Step 1: Write failing contract tests**

Create `crates/fdc-api/tests/runner_status_contract.rs` with:

```rust
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_runner_status_router, runner_status_response_from_state, ApiAppState,
    ApiRunnerLifecycleStatus,
};
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use rust_decimal::Decimal;
use tower::ServiceExt;

fn sample_trade_event(symbol: &str, trade_id: &str, sequence: &str) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(sequence.to_string()),
        checkpoint: None,
    }
}

fn sample_envelope(symbol: &str, trade_id: &str, sequence: &str) -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(
        "barter:binance_spot",
        sample_trade_event(symbol, trade_id, sequence),
    );
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[test]
fn default_state_projects_runner_not_configured() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());

    let response = runner_status_response_from_state(&state);

    assert_eq!(response.status, "success");
    assert!(!response.data.configured);
    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::NotConfigured);
    assert_eq!(response.data.last_result, None);
    assert_eq!(response.data.failure_message, None);
}

#[test]
fn created_runner_projects_created_state() {
    let runner = Arc::new(BoundedMarketDataRunnerHandle::new(Arc::new(
        QueryableMarketDataStore::new(),
    )));
    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_runner(runner);

    let response = runner_status_response_from_state(&state);

    assert!(response.data.configured);
    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Created);
    assert_eq!(response.data.last_result, None);
}

#[tokio::test]
async fn completed_runner_projects_last_result_counts() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("fixture run should complete");
    let state = ApiAppState::new(FdcServerApp::with_defaults())
        .with_market_data_runner(Arc::new(runner));

    let response = runner_status_response_from_state(&state);
    let result = response.data.last_result.expect("last result should project");

    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Completed);
    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(response.data.failure_message, None);
}

#[test]
fn cancelled_runner_projects_cancelled_state() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner.cancel().expect("created runner should cancel");
    let state = ApiAppState::new(FdcServerApp::with_defaults())
        .with_market_data_runner(Arc::new(runner));

    let response = runner_status_response_from_state(&state);

    assert_eq!(response.data.state, ApiRunnerLifecycleStatus::Cancelled);
    assert_eq!(response.data.last_result, None);
    assert_eq!(response.data.failure_message, None);
}

#[tokio::test]
async fn in_memory_runner_status_route_returns_completed_json() {
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::new(QueryableMarketDataStore::new()));
    runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("fixture run should complete");
    let state = ApiAppState::new(FdcServerApp::with_defaults())
        .with_market_data_runner(Arc::new(runner));
    let router = build_runner_status_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/runner/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be JSON");

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["configured"], true);
    assert_eq!(json["data"]["state"], "completed");
    assert_eq!(json["data"]["last_result"]["storage_records_written"], 1);
}
```

- [ ] **Step 2: Run test to verify RED failure**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
```

Expected: FAIL because runner status API types/functions do not exist yet.

## Task 2: Implement API state extension and runner status module

**Files:**
- Create: `crates/fdc-api/src/runner_status.rs`
- Modify: `crates/fdc-api/src/state.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Extend ApiAppState**

In `crates/fdc-api/src/state.rs`, import the runner handle:

```rust
use fdc_server::{
    BoundedMarketDataRunnerHandle, FdcServerApp, ServerEnvironment, ServerLifecycleState,
};
```

Add this field to `ApiAppState`:

```rust
market_data_runner: Option<Arc<BoundedMarketDataRunnerHandle>>,
```

Initialize it as `None` in `new` and `from_shared`.

Add methods:

```rust
pub fn with_market_data_runner(
    mut self,
    market_data_runner: Arc<BoundedMarketDataRunnerHandle>,
) -> Self {
    self.market_data_runner = Some(market_data_runner);
    self
}

pub fn market_data_runner(&self) -> Option<Arc<BoundedMarketDataRunnerHandle>> {
    self.market_data_runner.as_ref().map(Arc::clone)
}
```

- [ ] **Step 2: Add runner status module**

Create `crates/fdc-api/src/runner_status.rs` with:

```rust
use axum::{extract::State, routing::get, Json, Router};
use fdc_server::{BoundedMarketDataMvpResult, BoundedRunnerState};
use serde::{Deserialize, Serialize};

use crate::{ApiAppState, ApiResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiRunnerLifecycleStatus {
    NotConfigured,
    Created,
    Running,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRunnerLastResultProjection {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiRunnerStatusProjection {
    pub configured: bool,
    pub state: ApiRunnerLifecycleStatus,
    pub last_result: Option<ApiRunnerLastResultProjection>,
    pub failure_message: Option<String>,
}

pub fn runner_status_response_from_state(
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    ApiResponse::success(runner_status_projection_from_state(state))
}

pub fn build_runner_status_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/runner/status", get(runner_status_handler))
        .with_state(state)
}

async fn runner_status_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(runner_status_response_from_state(&state))
}

fn runner_status_projection_from_state(state: &ApiAppState) -> ApiRunnerStatusProjection {
    let Some(runner) = state.market_data_runner() else {
        return ApiRunnerStatusProjection {
            configured: false,
            state: ApiRunnerLifecycleStatus::NotConfigured,
            last_result: None,
            failure_message: None,
        };
    };

    ApiRunnerStatusProjection {
        configured: true,
        state: runner_lifecycle_status_label(runner.state()),
        last_result: runner.last_result().map(last_result_projection),
        failure_message: runner.failure().map(|failure| failure.message.clone()),
    }
}

fn runner_lifecycle_status_label(state: BoundedRunnerState) -> ApiRunnerLifecycleStatus {
    match state {
        BoundedRunnerState::Created => ApiRunnerLifecycleStatus::Created,
        BoundedRunnerState::Running => ApiRunnerLifecycleStatus::Running,
        BoundedRunnerState::Completed => ApiRunnerLifecycleStatus::Completed,
        BoundedRunnerState::Cancelled => ApiRunnerLifecycleStatus::Cancelled,
        BoundedRunnerState::Failed => ApiRunnerLifecycleStatus::Failed,
    }
}

fn last_result_projection(result: &BoundedMarketDataMvpResult) -> ApiRunnerLastResultProjection {
    ApiRunnerLastResultProjection {
        envelopes_received: result.envelopes_received,
        source_valid: result.source_valid,
        source_invalid: result.source_invalid,
        dto_mapped: result.dto_mapped,
        storage_records_written: result.storage_records_written,
        market_data_store_records: result.market_data_store_records,
    }
}
```

- [ ] **Step 3: Export module and public API**

Modify `crates/fdc-api/src/lib.rs`:

Add module:

```rust
pub mod runner_status;
```

Add exports:

```rust
pub use runner_status::{
    build_runner_status_router, runner_status_response_from_state, ApiRunnerLastResultProjection,
    ApiRunnerLifecycleStatus, ApiRunnerStatusProjection,
};
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
```

Expected: PASS with 5 tests passing.

- [ ] **Step 5: Commit implementation**

Run:

```bash
git add crates/fdc-api/src/lib.rs crates/fdc-api/src/state.rs crates/fdc-api/src/runner_status.rs crates/fdc-api/tests/runner_status_contract.rs
git commit -m "feat: add runner status api projection"
```

## Task 3: Verify integration and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: all commands PASS.

- [ ] **Step 2: Update development status**

Add a B14 completed section after B13 and update next recommended slice to B15. Include verification evidence from Step 1.

- [ ] **Step 3: Commit docs**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record runner status api projection"
```

- [ ] **Step 4: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.

## Self-Review

- Spec coverage: Task 1 tests all requested status states and route JSON. Task 2 implements state extension, projection DTOs, helper, and route. Task 3 verifies and documents completion.
- Placeholder scan: no unfinished markers are present.
- Type consistency: public names and route path match the design spec.
