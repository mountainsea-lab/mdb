# Production Background Live Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production background Binance Spot live runner with optional service autostart, live stop control, and rich live status snapshots.

**Architecture:** Keep the production boundary inside `fdc-server::market_data`. Add `FDC_LIVE_AUTOSTART`, extend live DTOs/status, evolve `MarketDataSupervisor` into the single-runner control snapshot, then wire routes and startup autostart. Default tests use fake/test helpers only; real Binance validation stays ignored/gated.

**Tech Stack:** Rust 1.95, Tokio, Axum 0.7, futures, existing `fdc-barter`, `fdc-orchestrator`, `fdc-storage`, `fdc-server` runtime modules.

---

## File Structure

- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Parse `FDC_LIVE_AUTOSTART` and expose `live_autostart: bool`.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Cover default false and env override true.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add background status fields and `StopLiveMarketDataResponse`.
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
  - Add task reservation, running/stopping/stopped/failure transitions, cancellation signal, counters, and snapshot fields.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Cover status shape, stop route, disabled default, and fake background start/stop behavior.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add background start/stop/autostart service functions and a fake/test helper for offline tests.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add `POST /market-data/live/stop`, route start through background service, return status-rich responses.
- Modify: `crates/fdc-server/src/runtime/app.rs`
  - Add `start_live_autostart_if_enabled` hook on `ProductionServerState`.
- Modify: `crates/fdc-server/src/bin/fdc_server.rs`
  - Call the autostart hook before serving.
- Create: `crates/fdc-server/tests/production_background_live_smoke.rs`
  - Ignored/gated real autostart -> query -> stop smoke.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record completed slice and verification evidence.

---

## Task 1: Runtime Config Autostart Flag

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Add failing config assertions**

Edit `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
#[test]
fn runtime_config_defaults_are_safe_for_local_production_server() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("defaults should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Development);
    assert!(!config.live_enabled);
    assert!(!config.live_autostart);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
}

#[test]
fn runtime_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_SERVER_ADDR", "127.0.0.1:19090"),
        ("FDC_SERVER_ENV", "production"),
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_AUTOSTART", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "12"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "34"),
    ])
    .expect("env overrides should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:19090");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert!(config.live_enabled);
    assert!(config.live_autostart);
    assert_eq!(config.live_default_timeout_secs, 12);
    assert_eq!(config.live_default_max_envelopes, 34);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: FAIL with missing field `live_autostart`.

- [ ] **Step 3: Implement config field**

Edit `crates/fdc-server/src/runtime/config.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    pub bind_addr: SocketAddr,
    pub environment: ServerRuntimeEnvironment,
    pub live_enabled: bool,
    pub live_autostart: bool,
    pub live_default_timeout_secs: u64,
    pub live_default_max_envelopes: usize,
}
```

Inside `from_env_pairs` initialize and parse:

```rust
let mut live_autostart = false;
```

Add match arm:

```rust
"FDC_LIVE_AUTOSTART" => {
    live_autostart = matches!(value.as_ref(), "1" | "true" | "yes" | "on");
}
```

Return it:

```rust
Ok(Self {
    bind_addr,
    environment,
    live_enabled,
    live_autostart,
    live_default_timeout_secs,
    live_default_max_envelopes,
})
```

- [ ] **Step 4: Verify GREEN**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
rtk git commit -m "feat: add production live autostart config"
```

---

## Task 2: Status DTOs and Supervisor Snapshot

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing supervisor snapshot test**

Append to `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
#[test]
fn production_live_supervisor_tracks_background_task_snapshot_and_stop() {
    use fdc_server::market_data::{
        model::MarketDataLiveState,
        supervisor::MarketDataSupervisor,
    };

    let supervisor = MarketDataSupervisor::new();
    let task_id = supervisor
        .start_background(vec!["binance_spot:BTCUSDT:trades".to_string()])
        .expect("background task should reserve");

    let running = supervisor.status();
    assert_eq!(running.state, MarketDataLiveState::Running);
    assert_eq!(running.task_id.as_deref(), Some(task_id.as_str()));
    assert!(running.started_at_ns.is_some());
    assert!(running.stopped_at_ns.is_none());
    assert_eq!(running.subscriptions, vec!["binance_spot:BTCUSDT:trades"]);

    supervisor.record_progress(2, 2, 2, Some(123));
    let progressed = supervisor.status();
    assert_eq!(progressed.envelopes_received, 2);
    assert_eq!(progressed.storage_records_written, 2);
    assert_eq!(progressed.market_data_store_records, 2);
    assert_eq!(progressed.last_record_at_ns, Some(123));

    assert!(supervisor.request_stop("requested"));
    assert_eq!(supervisor.status().state, MarketDataLiveState::Stopping);

    supervisor.stopped("requested");
    let stopped = supervisor.status();
    assert_eq!(stopped.state, MarketDataLiveState::Stopped);
    assert_eq!(stopped.stop_reason.as_deref(), Some("requested"));
    assert!(stopped.stopped_at_ns.is_some());
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract production_live_supervisor_tracks_background_task_snapshot_and_stop
```

Expected: FAIL with missing status fields and methods.

- [ ] **Step 3: Extend models**

Edit `crates/fdc-server/src/market_data/model.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveMarketDataStatusResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub started_at_ns: Option<u64>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
    pub subscriptions: Vec<String>,
    pub last_record_at_ns: Option<u64>,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
    pub last_result: Option<StartLiveMarketDataResponse>,
    pub failure_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub stopped_at_ns: Option<u64>,
    pub stop_reason: Option<String>,
}
```

Update all existing `StartLiveMarketDataResponse` construction sites to include `task_id: None` initially.

- [ ] **Step 4: Implement supervisor snapshot methods**

Edit `crates/fdc-server/src/market_data/supervisor.rs`. Add fields to inner state:

```rust
next_task_sequence: u64,
task_id: Option<String>,
started_at_ns: Option<u64>,
stopped_at_ns: Option<u64>,
stop_reason: Option<String>,
subscriptions: Vec<String>,
last_record_at_ns: Option<u64>,
envelopes_received: usize,
storage_records_written: usize,
market_data_store_records: usize,
stop_requested: bool,
```

Add helper:

```rust
fn now_ns() -> u64 {
    fdc_core::types::TimestampNs::now().as_nanos().max(0) as u64
}
```


Add methods:

```rust
pub fn start_background(&self, subscriptions: Vec<String>) -> Result<String> {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    match inner.state {
        MarketDataLiveState::Idle | MarketDataLiveState::Completed | MarketDataLiveState::Failed | MarketDataLiveState::Stopped => {
            inner.next_task_sequence += 1;
            let task_id = format!("market-data-live-{}", inner.next_task_sequence);
            inner.state = MarketDataLiveState::Running;
            inner.task_id = Some(task_id.clone());
            inner.started_at_ns = Some(now_ns());
            inner.stopped_at_ns = None;
            inner.stop_reason = None;
            inner.subscriptions = subscriptions;
            inner.last_record_at_ns = None;
            inner.envelopes_received = 0;
            inner.storage_records_written = 0;
            inner.market_data_store_records = 0;
            inner.failure_message = None;
            inner.stop_requested = false;
            Ok(task_id)
        }
        MarketDataLiveState::Starting => Err(Error::validation("market-data live runner is already starting")),
        MarketDataLiveState::Running => Err(Error::validation("market-data live runner is already running")),
        MarketDataLiveState::Stopping => Err(Error::validation("market-data live runner is already stopping")),
    }
}

pub fn record_progress(&self, envelopes: usize, storage_records: usize, store_records: usize, last_record_at_ns: Option<u64>) {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    inner.envelopes_received += envelopes;
    inner.storage_records_written += storage_records;
    inner.market_data_store_records = store_records;
    if last_record_at_ns.is_some() {
        inner.last_record_at_ns = last_record_at_ns;
    }
}

pub fn request_stop(&self, reason: impl Into<String>) -> bool {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    match inner.state {
        MarketDataLiveState::Running | MarketDataLiveState::Starting => {
            inner.state = MarketDataLiveState::Stopping;
            inner.stop_requested = true;
            inner.stop_reason = Some(reason.into());
            true
        }
        MarketDataLiveState::Stopping => true,
        _ => false,
    }
}

pub fn stop_requested(&self) -> bool {
    self.inner.lock().expect("market-data supervisor mutex should not be poisoned").stop_requested
}

pub fn stopped(&self, reason: impl Into<String>) {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    inner.state = MarketDataLiveState::Stopped;
    inner.stopped_at_ns = Some(now_ns());
    inner.stop_reason = Some(reason.into());
}
```

Update `status()` to fill all new fields.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract production_live_supervisor_tracks_background_task_snapshot_and_stop
```

Expected: PASS.

- [ ] **Step 6: Run package tests and fix construction sites**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected after fixes: PASS. Update any missing `task_id: None` response construction.

- [ ] **Step 7: Commit**

```bash
rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/supervisor.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production live supervisor snapshots"
```

---

## Task 3: Background Start/Stop Service with Offline Fake Runner

**Files:**
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing route tests for stop and fake background lifecycle**

Append to `production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn production_live_stop_is_idempotent_when_no_runner_is_active() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("stop should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["state"], "idle");
}
```

Add fake lifecycle test using a service helper:

```rust
#[tokio::test]
async fn production_live_fake_background_start_stop_updates_status() {
    use fdc_server::market_data::service::start_fake_background_live_for_test;

    let config = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_ENABLED", "1")])
        .expect("config should parse");
    let state = ProductionServerState::new(config);

    let started = start_fake_background_live_for_test(&state, 3, std::time::Duration::from_millis(10))
        .await
        .expect("fake runner should start");
    assert_eq!(started.state, fdc_server::market_data::model::MarketDataLiveState::Running);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let status = state.market_data_supervisor().status();
    assert_eq!(status.state, fdc_server::market_data::model::MarketDataLiveState::Running);
    assert!(status.envelopes_received >= 1);

    let router = build_production_router(state.clone());
    let stop = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("stop should respond");
    let stop_body = axum::body::to_bytes(stop.into_body(), usize::MAX).await.expect("body");
    let stop_json: serde_json::Value = serde_json::from_slice(&stop_body).expect("json");
    assert_eq!(stop_json["status"], "success");
    assert!(matches!(
        stop_json["data"]["state"].as_str().unwrap(),
        "stopping" | "stopped"
    ));
}
```


- [ ] **Step 2: Verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: FAIL because stop route and fake helper do not exist.

- [ ] **Step 3: Add stop service and route**

In `service.rs` add:

```rust
pub fn stop_live(state: &ProductionServerState) -> StopLiveMarketDataResponse {
    let supervisor = state.market_data_supervisor();
    supervisor.request_stop("requested");
    let status = supervisor.status();
    StopLiveMarketDataResponse {
        state: status.state,
        task_id: status.task_id,
        stopped_at_ns: status.stopped_at_ns,
        stop_reason: status.stop_reason,
    }
}
```

In `router.rs` import `StopLiveMarketDataResponse` and `stop_live`, then add route:

```rust
.route("/market-data/live/stop", post(stop_live_handler))
```

Add handler:

```rust
async fn stop_live_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<StopLiveMarketDataResponse>> {
    Json(ServerApiResponse::success(stop_live(&state)))
}
```

- [ ] **Step 4: Add fake background helper**

In `service.rs` add public test-oriented helper guarded by normal code because integration tests cannot access `#[cfg(test)]` items:

```rust
pub async fn start_fake_background_live_for_test(
    state: &ProductionServerState,
    ticks: usize,
    interval: Duration,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(vec!["test:fake:trades".to_string()])
        .map_err(|error| error.to_string())?;
    let supervisor_for_task = state.market_data_supervisor();
    let store = state.market_data_store();
    tokio::spawn(async move {
        for index in 0..ticks {
            if supervisor_for_task.stop_requested() {
                supervisor_for_task.stopped("requested");
                return;
            }
            tokio::time::sleep(interval).await;
            supervisor_for_task.record_progress(1, 1, store.record_count() + index + 1, Some(fdc_core::types::TimestampNs::now().as_nanos().max(0) as u64));
        }
        while !supervisor_for_task.stop_requested() {
            tokio::time::sleep(interval).await;
        }
        supervisor_for_task.stopped("requested");
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}
```


- [ ] **Step 5: Verify GREEN**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: PASS with existing ignored network test still ignored.

- [ ] **Step 6: Commit**

```bash
rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production live stop control"
```

---

## Task 4: Production Background Live Start and Autostart Hook

**Files:**
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/bin/fdc_server.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing autostart-disabled test**

Append:

```rust
#[tokio::test]
async fn production_live_autostart_does_not_run_when_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );

    state.start_live_autostart_if_enabled().await.expect("disabled autostart should be ok");
    assert_eq!(
        state.market_data_supervisor().status().state,
        fdc_server::market_data::model::MarketDataLiveState::Idle
    );
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract production_live_autostart_does_not_run_when_disabled_by_default
```

Expected: FAIL missing method.

- [ ] **Step 3: Add autostart hook**

In `runtime/app.rs` add:

```rust
impl ProductionServerState {
    pub async fn start_live_autostart_if_enabled(&self) -> Result<()> {
        if !(self.config.live_enabled && self.config.live_autostart) {
            return Ok(());
        }
        crate::market_data::service::start_background_live(
            self,
            crate::market_data::model::StartLiveMarketDataRequest {
                timeout_secs: None,
                max_envelopes: None,
            },
        )
        .await
        .map(|_| ())
        .map_err(fdc_core::error::Error::internal)
    }
}
```


- [ ] **Step 4: Implement background start service**

In `service.rs`, change `start_live` so it delegates to new `start_background_live` for enabled requests:

```rust
pub async fn start_background_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    if !state.config().live_enabled {
        let (_, message) = start_live_disabled(state);
        return Err(message);
    }

    let subscriptions = vec![
        "binance_spot:BTCUSDT:public_trades".to_string(),
        "binance_spot:ETHUSDT:public_trades".to_string(),
    ];
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(subscriptions)
        .map_err(|error| error.to_string())?;
    let timeout_secs = request
        .timeout_secs
        .unwrap_or(state.config().live_default_timeout_secs)
        .max(1);
    let max_envelopes = request
        .max_envelopes
        .unwrap_or(state.config().live_default_max_envelopes)
        .max(1);
    let store = state.market_data_store();
    let supervisor_for_task = state.market_data_supervisor();

    tokio::spawn(async move {
        if let Err(message) = run_background_live_collection(supervisor_for_task.clone(), store, timeout_secs, max_envelopes).await {
            supervisor_for_task.fail(message);
        }
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}

pub async fn start_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    start_background_live(state, request).await
}
```

Add `run_background_live_collection` as an async wrapper using the existing proven `run_live_collection_and_storage` as an initial bounded loop. For this task, each background cycle collects up to `max_envelopes`, records progress, then repeats until stop is requested:

```rust
async fn run_background_live_collection(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<(), String> {
    while !supervisor.stop_requested() {
        let before = store.record_count();
        let result = run_live_collection_and_storage(store.clone(), timeout_secs, max_envelopes).await?;
        let after = store.record_count();
        supervisor.record_progress(
            result.envelopes_received,
            result.storage_records_written,
            after,
            Some(fdc_core::types::TimestampNs::now().as_nanos().max(0) as u64),
        );
        if after == before && supervisor.stop_requested() {
            break;
        }
    }
    supervisor.stopped("requested");
    Ok(())
}
```

This keeps the first background slice simple and testable. The later actor/event-loop slice can replace the repeated bounded cycles with direct continuous stream ownership.

- [ ] **Step 5: Wire binary autostart**

In `crates/fdc-server/src/bin/fdc_server.rs`:

```rust
let state = ProductionServerState::new(config);
state.start_live_autostart_if_enabled().await?;
let router = build_production_router(state);
```

- [ ] **Step 6: Verify default tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server
```

Expected: PASS; no network touched by default.

- [ ] **Step 7: Commit**

```bash
rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/bin/fdc_server.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production background live autostart"
```

---

## Task 5: Ignored Real Background Smoke and Status Docs

**Files:**
- Create: `crates/fdc-server/tests/production_background_live_smoke.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add ignored real smoke test**

Create `crates/fdc-server/tests/production_background_live_smoke.rs`:

```rust
use axum::{body::Body, http::Request};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

#[ignore = "requires public internet, FDC_LIVE_ENABLED=1, and FDC_LIVE_AUTOSTART=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_background_live_autostart_writes_trades_and_stop_finishes() {
    if std::env::var("FDC_LIVE_ENABLED").as_deref() != Ok("1")
        || std::env::var("FDC_LIVE_AUTOSTART").as_deref() != Ok("1")
    {
        eprintln!("skipping background live smoke because live flags are not set");
        return;
    }

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_AUTOSTART", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "10"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "5"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    state
        .start_live_autostart_if_enabled()
        .await
        .expect("autostart should spawn");
    let router = build_production_router(state.clone());

    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    let status = router
        .clone()
        .oneshot(Request::builder().uri("/market-data/live/status").body(Body::empty()).unwrap())
        .await
        .expect("status should respond");
    let status_body = axum::body::to_bytes(status.into_body(), usize::MAX).await.unwrap();
    let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
    eprintln!("background status: {status_json:#}");
    assert_eq!(status_json["status"], "success");
    assert!(status_json["data"]["envelopes_received"].as_u64().unwrap() >= 1);

    let query = router
        .clone()
        .oneshot(Request::builder().uri("/market-data/trades?limit=5").body(Body::empty()).unwrap())
        .await
        .expect("query should respond");
    let query_body = axum::body::to_bytes(query.into_body(), usize::MAX).await.unwrap();
    let query_json: serde_json::Value = serde_json::from_slice(&query_body).unwrap();
    eprintln!("background query: {query_json:#}");
    assert_eq!(query_json["status"], "success");
    assert!(query_json["data"]["returned_records"].as_u64().unwrap() >= 1);

    let stop = router
        .oneshot(Request::builder().method("POST").uri("/market-data/live/stop").body(Body::empty()).unwrap())
        .await
        .expect("stop should respond");
    let stop_body = axum::body::to_bytes(stop.into_body(), usize::MAX).await.unwrap();
    let stop_json: serde_json::Value = serde_json::from_slice(&stop_body).unwrap();
    eprintln!("background stop: {stop_json:#}");
    assert_eq!(stop_json["status"], "success");
}
```

- [ ] **Step 2: Verify default ignored behavior**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_background_live_smoke
```

Expected: PASS with 1 ignored.

- [ ] **Step 3: Run final default verification**

Run:

```bash
rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_background_live_smoke
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected: PASS; smoke ignored by default.

- [ ] **Step 4: Optional real smoke**

Run when public internet is available:

```bash
FDC_LIVE_ENABLED=1 FDC_LIVE_AUTOSTART=1 cargo test -p fdc-server --test production_background_live_smoke ignored_background_live_autostart_writes_trades_and_stop_finishes -- --ignored --nocapture
```

Expected: PASS with real Binance Spot records queried and stop response success.

- [ ] **Step 5: Update `docs/DEVELOPMENT_STATUS.md`**

Add a new checkpoint section summarizing:

- `FDC_LIVE_AUTOSTART` added.
- Background live start returns promptly with `running`.
- Stop route added and idempotent.
- Status exposes task/counter/subscription/timestamp metadata.
- Default verification results.
- Optional real smoke result if run.
- Next recommended slice: replace repeated bounded background cycles with actor/event-loop direct stream supervisor.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/fdc-server/tests/production_background_live_smoke.rs docs/DEVELOPMENT_STATUS.md
rtk git commit -m "test: add production background live smoke"
```

---

## Final Verification

Before reporting completion, run:

```bash
rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_live_smoke
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_background_live_smoke
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

If public internet is available, also run:

```bash
FDC_LIVE_ENABLED=1 FDC_LIVE_AUTOSTART=1 cargo test -p fdc-server --test production_background_live_smoke ignored_background_live_autostart_writes_trades_and_stop_finishes -- --ignored --nocapture
```

Expected default result: all default tests pass; live smoke tests remain ignored unless explicitly selected.
