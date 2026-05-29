# Production Live Supervisor Enabled Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable real Binance Spot live acquisition through the production `fdc-server` `/market-data/live/start` route, with supervisor state tracking and queryable storage readback.

**Architecture:** Keep HTTP handling in `market_data/router.rs`, business orchestration in `market_data/service.rs`, and lifecycle state in `market_data/supervisor.rs`. Migrate the bounded live acquisition/write pattern already proven in `fdc-api::live_runner_control` into `fdc-server`, gated by `ServerRuntimeConfig.live_enabled`. Default tests remain offline; real network validation is an explicit manual smoke.

**Tech Stack:** Rust 1.95, Axum 0.7, Tokio, futures, `fdc-barter`, `fdc-storage`, existing `run_realtime_barter_envelope_stream`.

---

## File Structure

- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
  - Add state transition methods and concurrency guard.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add enabled live acquisition path using `fdc-barter` streams.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Route enabled live start to the service instead of returning disabled response unconditionally.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add tests for supervisor/status behavior with live enabled using fake/test ingestion helpers.
- Create: `crates/fdc-server/tests/production_live_smoke.rs`
  - Ignored/gated real Binance Spot HTTP/service smoke.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record enabled production live support and verification.

---

## Task 1: Supervisor State Transitions

**Files:**
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing supervisor/status test**

Append to `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
#[test]
fn production_live_supervisor_tracks_start_complete_and_rejects_concurrent_start() {
    use fdc_server::market_data::{
        model::{MarketDataLiveState, StartLiveMarketDataResponse},
        supervisor::MarketDataSupervisor,
    };

    let supervisor = MarketDataSupervisor::new();
    assert_eq!(supervisor.status().state, MarketDataLiveState::Idle);

    supervisor.try_start().expect("idle supervisor should start");
    assert_eq!(supervisor.status().state, MarketDataLiveState::Starting);

    let error = supervisor
        .try_start()
        .expect_err("concurrent start should be rejected");
    assert!(error.to_string().contains("already starting"));

    supervisor.complete(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        envelopes_received: 2,
        storage_records_written: 2,
        market_data_store_records: 2,
    });

    let status = supervisor.status();
    assert_eq!(status.state, MarketDataLiveState::Completed);
    assert_eq!(
        status.last_result.as_ref().unwrap().storage_records_written,
        2
    );
    assert!(status.failure_message.is_none());
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract production_live_supervisor_tracks_start_complete_and_rejects_concurrent_start
```

Expected: FAIL because `try_start` and `complete` do not exist.

- [ ] **Step 3: Implement supervisor transitions**

Modify `MarketDataSupervisor` to use interior mutability:

```rust
use parking_lot::Mutex;
```

Define an inner state struct and implement:

```rust
pub fn try_start(&self) -> fdc_core::Result<()>;
pub fn complete(&self, result: StartLiveMarketDataResponse);
pub fn fail(&self, message: impl Into<String>);
pub fn status(&self) -> LiveMarketDataStatusResponse;
```

Rules:

- `try_start` accepts `Idle`, `Completed`, `Failed`, `Stopped`.
- `try_start` rejects `Starting`, `Running`, `Stopping` with validation error.
- `complete` sets state `Completed`, stores result, clears failure.
- `fail` sets state `Failed`, stores message.

- [ ] **Step 4: Run supervisor test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract production_live_supervisor_tracks_start_complete_and_rejects_concurrent_start
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data/supervisor.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production live supervisor transitions"
```

---

## Task 2: Enabled Live Service Path

**Files:**
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing enabled-config offline route test**

Append a test that enables live in config but uses an intentionally tiny timeout to exercise supervisor failure/status without public internet dependency:

```rust
#[tokio::test]
async fn production_live_start_with_enabled_config_updates_status_on_failure() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "1"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/start")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"timeout_secs":1,"max_envelopes":1}"#))
                .expect("request should build"),
        )
        .await
        .expect("start should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

    assert!(json["status"] == "success" || json["status"] == "error");
    assert!(json["data"]["state"].is_string());

    let status_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/live/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status should respond");
    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX)
        .await
        .expect("status body should read");
    let status_json: serde_json::Value = serde_json::from_slice(&status_body).expect("json");

    assert!(matches!(
        status_json["data"]["state"].as_str().unwrap(),
        "completed" | "failed"
    ));
}
```

This test may touch network if enabled. If it is too slow/flaky locally, replace it with a service-level fake in Task 2 before committing.

- [ ] **Step 2: Implement enabled live service**

In `service.rs`, add:

```rust
pub async fn start_live(state: &ProductionServerState, request: StartLiveMarketDataRequest)
    -> Result<StartLiveMarketDataResponse, String>
```

Behavior:

1. If live disabled, return disabled error.
2. `state.market_data_supervisor().try_start()`.
3. Determine `timeout_secs` and `max_envelopes` from request/config.
4. Run live collection and storage using the proven `fdc-api` pattern:
   - `init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())`
   - `streams.select_all().map(public_trade_result_to_data_kind)`
   - `collect_live_trade_envelopes(...)`
   - `run_realtime_barter_envelope_stream(stream::iter(envelopes), state.market_data_store(), config)`
5. On success, call `supervisor.complete(response.clone())` and return `Ok(response)`.
6. On failure, call `supervisor.fail(message.clone())` and return `Err(message)`.

Because the Barter stream type may not be `Send`, use the same `tokio::task::spawn_blocking` + current-thread runtime pattern from `fdc-api::live_runner_control`.

- [ ] **Step 3: Wire router to service**

In `router.rs`, replace the enabled branch with:

```rust
match start_live(&state, request).await {
    Ok(data) => Json(ServerApiResponse::success(data)),
    Err(message) => Json(ServerApiResponse::error(live_status_as_start_response(&state), message)),
}
```

Alternatively, return the failed response from service directly.

- [ ] **Step 4: Run production router tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: PASS. If network behavior makes the enabled-config test flaky, mark that test ignored and rely on Task 3 live smoke for real network.

- [ ] **Step 5: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: enable production live market data start"
```

---

## Task 3: Ignored Production Live Smoke

**Files:**
- Create: `crates/fdc-server/tests/production_live_smoke.rs`

- [ ] **Step 1: Add ignored real live smoke test**

Create `crates/fdc-server/tests/production_live_smoke.rs`:

```rust
use axum::{body::Body, http::{Request, StatusCode}};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

#[ignore = "requires public internet and FDC_LIVE_ENABLED=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_production_live_start_writes_real_trades_and_query_reads_them() {
    if std::env::var("FDC_LIVE_ENABLED").as_deref() != Ok("1") {
        eprintln!("skipping production live smoke because FDC_LIVE_ENABLED=1 is not set");
        return;
    }

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "20"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "20"),
    ])
    .expect("config should parse");
    let router = build_production_router(ProductionServerState::new(config));

    let start = router.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/market-data/live/start")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"timeout_secs":20,"max_envelopes":20}"#))
            .expect("request should build"),
    ).await.expect("start should respond");

    assert_eq!(start.status(), StatusCode::OK);
    let start_body = axum::body::to_bytes(start.into_body(), usize::MAX).await.expect("body");
    let start_json: serde_json::Value = serde_json::from_slice(&start_body).expect("json");
    eprintln!("production live start response: {start_json:#}");
    assert_eq!(start_json["status"], "success");
    assert!(start_json["data"]["storage_records_written"].as_u64().unwrap() >= 1);

    let query = router.oneshot(
        Request::builder()
            .uri("/market-data/trades?limit=5")
            .body(Body::empty())
            .expect("request should build"),
    ).await.expect("query should respond");
    let query_body = axum::body::to_bytes(query.into_body(), usize::MAX).await.expect("body");
    let query_json: serde_json::Value = serde_json::from_slice(&query_body).expect("json");
    eprintln!("production live query response: {query_json:#}");
    assert_eq!(query_json["status"], "success");
    assert!(query_json["data"]["returned_records"].as_u64().unwrap() >= 1);
}
```

- [ ] **Step 2: Run ignored test list/default behavior**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_live_smoke
```

Expected: PASS with 1 ignored.

- [ ] **Step 3: Commit smoke test**

Run:

```bash
rtk git add crates/fdc-server/tests/production_live_smoke.rs
rtk git commit -m "test: add production live market data smoke"
```

---

## Task 4: Real HTTP Verification and Docs

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run final default tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_live_smoke
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected: PASS, live smoke ignored by default.

- [ ] **Step 2: Optional real live verification**

If public internet is available, run:

```bash
FDC_LIVE_ENABLED=1 cargo test -p fdc-server --test production_live_smoke ignored_production_live_start_writes_real_trades_and_query_reads_them -- --ignored --nocapture
```

Expected: PASS with real Binance Spot trades written and queried.

- [ ] **Step 3: Update development status**

Record:

- production `/market-data/live/start` now supports enabled live mode.
- supervisor tracks completed/failed status.
- default tests and optional live smoke evidence.
- next slice: true background indefinite streaming + stop route.

- [ ] **Step 4: Commit docs**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record production live supervisor enabled mode"
```

## Self-Review

- This plan migrates real live acquisition into `fdc-server` while keeping default tests offline.
- It does not implement indefinite streaming or stop/cancel, matching the spec's bounded first enabled mode.
- Supervisor state and route status are covered.
- The existing `fdc-api` demo route remains compatible.
