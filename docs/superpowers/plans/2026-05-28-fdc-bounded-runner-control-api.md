# B15 Bounded Runner Control API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add in-memory API controls to start and cancel a bounded fixture runner for local MVP demos.

**Architecture:** Extend `ApiAppState` with an optional `Arc<tokio::sync::Mutex<BoundedMarketDataRunnerHandle>>` control handle while preserving B14 read-only status compatibility. Add `runner_control.rs` for request DTOs, fixture conversion, pure async control helpers, and Axum routes.

**Tech Stack:** Rust 2021, Tokio `Mutex`, Axum JSON routes, Serde DTOs, `fdc-api`, `fdc-server`, `fdc-barter` fixture model types.

---

## File Structure

- Create `crates/fdc-api/src/runner_control.rs`
  - Defines `RunnerFixtureTradeInput`, `RunnerStartFixtureRequest`, control helpers, and routes.
- Modify `crates/fdc-api/src/state.rs`
  - Adds optional mutable runner control handle.
  - Adds `with_market_data_runner_control` and `market_data_runner_control` accessors.
- Modify `crates/fdc-api/src/runner_status.rs`
  - Status projection prefers mutable control handle when present, otherwise falls back to read-only B14 handle.
  - Adds reusable projection helper for `BoundedMarketDataRunnerHandle`.
- Modify `crates/fdc-api/src/lib.rs`
  - Exports runner control public API.
- Create `crates/fdc-api/tests/runner_control_contract.rs`
  - Contract tests for start fixture, cancel, missing handle, empty input, and market-data query after start.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Records B15 completion and next recommended slice.

## Task 1: Add failing runner control contract tests

**Files:**
- Create: `crates/fdc-api/tests/runner_control_contract.rs`

- [ ] **Step 1: Write failing contract tests**

Create `crates/fdc-api/tests/runner_control_contract.rs` with:

```rust
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_market_data_router, build_runner_control_router, cancel_runner_from_state,
    start_fixture_runner_from_state, ApiAppState, MarketDataTradeQueryParams,
    RunnerFixtureTradeInput, RunnerStartFixtureRequest,
};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn state_with_control_runner() -> (ApiAppState, Arc<QueryableMarketDataStore>) {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));
    let state = ApiAppState::new(FdcServerApp::with_defaults())
        .with_market_data_store(Arc::clone(&store))
        .with_market_data_runner_control(runner);
    (state, store)
}

fn start_request() -> RunnerStartFixtureRequest {
    RunnerStartFixtureRequest {
        trades: vec![
            RunnerFixtureTradeInput {
                symbol: "BTCUSDT".to_string(),
                trade_id: "btc-1".to_string(),
                sequence: Some("seq-1".to_string()),
            },
            RunnerFixtureTradeInput {
                symbol: "ETHUSDT".to_string(),
                trade_id: "eth-1".to_string(),
                sequence: Some("seq-2".to_string()),
            },
        ],
    }
}

#[tokio::test]
async fn pure_start_helper_runs_fixture_and_returns_completed_status() {
    let (state, store) = state_with_control_runner();

    let response = start_fixture_runner_from_state(&state, start_request()).await;

    assert_eq!(response.status, "success");
    assert_eq!(response.data.configured, true);
    assert_eq!(response.data.state, fdc_api::ApiRunnerLifecycleStatus::Completed);
    assert_eq!(
        response
            .data
            .last_result
            .as_ref()
            .expect("last result should project")
            .storage_records_written,
        2
    );
    assert_eq!(
        fdc_api::query_market_data_trades(
            &state,
            MarketDataTradeQueryParams {
                symbol: Some("BTCUSDT".to_string()),
                limit: Some(10),
            },
        )
        .data
        .returned_records,
        1
    );
    assert_eq!(store.query(&fdc_storage::MarketDataQuery::for_trades()).len(), 2);
}

#[tokio::test]
async fn start_fixture_route_writes_records_readable_by_market_data_route() {
    let (state, _store) = state_with_control_runner();
    let control_router = build_runner_control_router(state.clone());

    let response = control_router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/start-fixture")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&start_request()).expect("request should serialize"),
                ))
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
    assert_eq!(json["data"]["state"], "completed");

    let market_data_router = build_market_data_router(state);
    let query_response = market_data_router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("market-data router should respond");
    assert_eq!(query_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn cancel_helper_transitions_created_runner_to_cancelled() {
    let (state, _store) = state_with_control_runner();

    let response = cancel_runner_from_state(&state).await;

    assert_eq!(response.status, "success");
    assert_eq!(response.data.state, fdc_api::ApiRunnerLifecycleStatus::Cancelled);
    assert_eq!(response.data.last_result, None);
}

#[tokio::test]
async fn missing_control_handle_returns_error_response() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());

    let response = start_fixture_runner_from_state(&state, start_request()).await;

    assert_eq!(response.status, "error");
    assert!(
        response
            .message
            .expect("error response should include a message")
            .contains("runner control handle is not configured")
    );
    assert!(!response.data.configured);
}

#[tokio::test]
async fn empty_start_request_returns_error_response() {
    let (state, _store) = state_with_control_runner();

    let response = start_fixture_runner_from_state(
        &state,
        RunnerStartFixtureRequest { trades: Vec::new() },
    )
    .await;

    assert_eq!(response.status, "error");
    assert!(
        response
            .message
            .expect("error response should include a message")
            .contains("at least one fixture trade")
    );
}
```

- [ ] **Step 2: Run test to verify RED failure**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
```

Expected: FAIL because runner control API functions/types do not exist yet.

## Task 2: Implement runner control API

**Files:**
- Create: `crates/fdc-api/src/runner_control.rs`
- Modify: `crates/fdc-api/src/state.rs`
- Modify: `crates/fdc-api/src/runner_status.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Extend `ApiAppState` with mutable runner control**

In `crates/fdc-api/src/state.rs`, add:

```rust
use tokio::sync::Mutex;
```

Add this field to `ApiAppState`:

```rust
market_data_runner_control: Option<Arc<Mutex<BoundedMarketDataRunnerHandle>>>,
```

Initialize it as `None` in `new` and `from_shared`.

Add methods:

```rust
pub fn with_market_data_runner_control(
    mut self,
    market_data_runner_control: Arc<Mutex<BoundedMarketDataRunnerHandle>>,
) -> Self {
    self.market_data_runner_control = Some(market_data_runner_control);
    self
}

pub fn market_data_runner_control(&self) -> Option<Arc<Mutex<BoundedMarketDataRunnerHandle>>> {
    self.market_data_runner_control.as_ref().map(Arc::clone)
}
```

- [ ] **Step 2: Update B14 status projection to prefer control handle**

In `crates/fdc-api/src/runner_status.rs`, change `runner_status_response_from_state` to be async-aware is not desired for B14 compatibility. Instead add a public helper:

```rust
pub fn runner_status_projection_from_runner(
    runner: &fdc_server::BoundedMarketDataRunnerHandle,
) -> ApiRunnerStatusProjection
```

Use that helper for read-only runner status projection. B15 control code can call it while holding the mutex lock. Keep existing `runner_status_response_from_state` synchronous and unchanged for read-only handle fallback.

The helper body should return configured `true`, map lifecycle state, last result, and failure message.

- [ ] **Step 3: Add runner control module**

Create `crates/fdc-api/src/runner_control.rs` with:

```rust
use axum::{extract::State, routing::post, Json, Router};
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    runner_status::runner_status_projection_from_runner, ApiAppState, ApiResponse,
    ApiRunnerStatusProjection,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerFixtureTradeInput {
    pub symbol: String,
    pub trade_id: String,
    pub sequence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerStartFixtureRequest {
    pub trades: Vec<RunnerFixtureTradeInput>,
}

pub async fn start_fixture_runner_from_state(
    state: &ApiAppState,
    request: RunnerStartFixtureRequest,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let Some(runner) = state.market_data_runner_control() else {
        return runner_control_error("runner control handle is not configured", state).await;
    };
    if request.trades.is_empty() {
        return runner_control_error("runner start requires at least one fixture trade", state).await;
    }

    let envelopes = request
        .trades
        .into_iter()
        .enumerate()
        .map(|(index, trade)| fixture_trade_to_envelope(index, trade))
        .collect();

    let mut runner = runner.lock().await;
    match runner.start_once(envelopes).await {
        Ok(_) => ApiResponse::success(runner_status_projection_from_runner(&runner)),
        Err(error) => ApiResponse::error(
            runner_status_projection_from_runner(&runner),
            error.to_string(),
        ),
    }
}

pub async fn cancel_runner_from_state(
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let Some(runner) = state.market_data_runner_control() else {
        return runner_control_error("runner control handle is not configured", state).await;
    };

    let mut runner = runner.lock().await;
    match runner.cancel() {
        Ok(()) => ApiResponse::success(runner_status_projection_from_runner(&runner)),
        Err(error) => ApiResponse::error(
            runner_status_projection_from_runner(&runner),
            error.to_string(),
        ),
    }
}

pub fn build_runner_control_router(state: ApiAppState) -> Router {
    Router::new()
        .route("/runner/start-fixture", post(start_fixture_handler))
        .route("/runner/cancel", post(cancel_handler))
        .with_state(state)
}

async fn start_fixture_handler(
    State(state): State<ApiAppState>,
    Json(request): Json<RunnerStartFixtureRequest>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(start_fixture_runner_from_state(&state, request).await)
}

async fn cancel_handler(
    State(state): State<ApiAppState>,
) -> Json<ApiResponse<ApiRunnerStatusProjection>> {
    Json(cancel_runner_from_state(&state).await)
}

async fn runner_control_error(
    message: impl Into<String>,
    state: &ApiAppState,
) -> ApiResponse<ApiRunnerStatusProjection> {
    let projection = if let Some(runner) = state.market_data_runner_control() {
        let runner = runner.lock().await;
        runner_status_projection_from_runner(&runner)
    } else {
        crate::runner_status_response_from_state(state).data
    };
    ApiResponse::error(projection, message.into())
}

fn fixture_trade_to_envelope(
    index: usize,
    input: RunnerFixtureTradeInput,
) -> BarterIngestionEnvelope {
    let sequence = input.sequence.unwrap_or_else(|| format!("seq-{}", index + 1));
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(input.symbol.clone()),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000 + index as u64),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_100 + index as u64),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(input.trade_id.clone()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(1, 0),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(sequence),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = format!("env-{}", input.trade_id);
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_200 + index as u64);
    envelope.quality = DataQualityFlags::default();
    envelope
}
```

- [ ] **Step 4: Add `ApiResponse::error` helper**

In `crates/fdc-api/src/models.rs`, add to `impl<T> ApiResponse<T>`:

```rust
pub fn error(data: T, message: String) -> Self {
    Self {
        data,
        status: "error".to_string(),
        message: Some(message),
        timestamp: chrono::Utc::now(),
        request_id: Uuid::new_v4().to_string(),
        metadata: None,
    }
}
```

- [ ] **Step 5: Export runner control API**

Modify `crates/fdc-api/src/lib.rs`:

Add module:

```rust
pub mod runner_control;
```

Add exports:

```rust
pub use runner_control::{
    build_runner_control_router, cancel_runner_from_state, start_fixture_runner_from_state,
    RunnerFixtureTradeInput, RunnerStartFixtureRequest,
};
```

- [ ] **Step 6: Run contract tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
```

Expected: PASS with 5 tests passing.

- [ ] **Step 7: Commit implementation**

Run:

```bash
git add crates/fdc-api/src/lib.rs crates/fdc-api/src/state.rs crates/fdc-api/src/runner_status.rs crates/fdc-api/src/runner_control.rs crates/fdc-api/src/models.rs crates/fdc-api/tests/runner_control_contract.rs
git commit -m "feat: add bounded runner control api"
```

## Task 3: Verify integration and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_control_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test runner_status_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: all commands PASS.

- [ ] **Step 2: Update development status**

Add B15 completed section after B14 and update next recommended slice to B16 Unified Demo API Router. Include verification evidence from Step 1.

- [ ] **Step 3: Commit docs**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record bounded runner control api"
```

- [ ] **Step 4: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.

## Self-Review

- Spec coverage: Task 1 tests start, cancel, missing handle, empty input, and market-data readability. Task 2 implements state extension, mutable control helper, route, DTOs, and error responses. Task 3 verifies and records completion.
- Placeholder scan: no unfinished markers are present.
- Type consistency: route paths, DTO names, and helper names match the design spec.
