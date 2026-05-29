# Production Server Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first production-oriented `fdc-server` runtime with business-module layering, health/readiness routes, market-data live control/status/query routes, and a runnable `fdc_server` binary.

**Architecture:** Add new `runtime`, `health`, and `market_data` modules under `fdc-server`, split into `model/router/service/supervisor` where applicable. Keep lower-level acquisition/storage logic in `fdc-barter`, `fdc-orchestrator`, and `fdc-storage`; preserve the existing `fdc-api` demo path. Use TDD contracts for config, router behavior, and the production binary.

**Tech Stack:** Rust 1.95, Axum 0.7, Tokio, futures, existing `fdc-server`, `fdc-barter`, `fdc-storage`, and `fdc-orchestrator` crates.

---

## File Structure

- Create: `crates/fdc-server/src/runtime/mod.rs`
  - Re-export runtime config/app/shutdown modules.
- Create: `crates/fdc-server/src/runtime/config.rs`
  - Parse server runtime env/defaults.
- Create: `crates/fdc-server/src/runtime/app.rs`
  - Build shared production app state and router.
- Create: `crates/fdc-server/src/runtime/shutdown.rs`
  - Provide shutdown signal helper.
- Create: `crates/fdc-server/src/health/mod.rs`
  - Re-export health module files.
- Create: `crates/fdc-server/src/health/model.rs`
  - Health and readiness response DTOs.
- Create: `crates/fdc-server/src/health/service.rs`
  - Compute health/readiness from runtime state.
- Create: `crates/fdc-server/src/health/router.rs`
  - Expose `/health` and `/ready`.
- Create: `crates/fdc-server/src/market_data/mod.rs`
  - Re-export market-data module files.
- Create: `crates/fdc-server/src/market_data/model.rs`
  - Start live request/response, status, trade query DTOs.
- Create: `crates/fdc-server/src/market_data/supervisor.rs`
  - Track live runner state, last result, failure, and concurrency guard.
- Create: `crates/fdc-server/src/market_data/service.rs`
  - Use-case methods for live start/status/query.
- Create: `crates/fdc-server/src/market_data/router.rs`
  - Expose `/market-data/live/start`, `/market-data/live/status`, `/market-data/trades`.
- Create: `crates/fdc-server/src/bin/fdc_server.rs`
  - Production binary entrypoint.
- Modify: `crates/fdc-server/src/lib.rs`
  - Export new modules.
- Modify: `crates/fdc-server/Cargo.toml`
  - Add Axum/serde/serde_json dependencies if not already present.
- Create: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Contract tests for config parsing/defaults.
- Create: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Router/service tests for health, readiness, live disabled, fixture/fake ingest, and query.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record implementation and verification.

---

## Task 1: Runtime Config

**Files:**
- Create: `crates/fdc-server/tests/runtime_config_contract.rs`
- Create: `crates/fdc-server/src/runtime/mod.rs`
- Create: `crates/fdc-server/src/runtime/config.rs`
- Modify: `crates/fdc-server/src/lib.rs`

- [ ] **Step 1: Write failing config contract tests**

Create `crates/fdc-server/tests/runtime_config_contract.rs`:

```rust
use fdc_server::{ServerRuntimeConfig, ServerRuntimeEnvironment};

#[test]
fn runtime_config_defaults_are_safe_for_local_production_server() {
    let config = ServerRuntimeConfig::from_env_pairs([]).expect("defaults should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Development);
    assert!(!config.live_enabled);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
}

#[test]
fn runtime_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_SERVER_ADDR", "127.0.0.1:19090"),
        ("FDC_SERVER_ENV", "production"),
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "12"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "34"),
    ])
    .expect("env overrides should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:19090");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert!(config.live_enabled);
    assert_eq!(config.live_default_timeout_secs, 12);
    assert_eq!(config.live_default_max_envelopes, 34);
}

#[test]
fn runtime_config_rejects_invalid_values() {
    let error = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "0")])
        .expect_err("zero timeout should be rejected");

    assert!(error.to_string().contains("FDC_LIVE_DEFAULT_TIMEOUT_SECS"));
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: FAIL because `ServerRuntimeConfig` does not exist.

- [ ] **Step 3: Implement runtime config**

Create `crates/fdc-server/src/runtime/mod.rs`:

```rust
pub mod app;
pub mod config;
pub mod shutdown;

pub use config::{ServerRuntimeConfig, ServerRuntimeEnvironment};
```

Create `crates/fdc-server/src/runtime/config.rs`:

```rust
use std::{env, net::SocketAddr};

use fdc_core::{error::Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRuntimeEnvironment {
    Development,
    Test,
    Production,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    pub bind_addr: SocketAddr,
    pub environment: ServerRuntimeEnvironment,
    pub live_enabled: bool,
    pub live_default_timeout_secs: u64,
    pub live_default_max_envelopes: usize,
}

impl ServerRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_env_pairs(env::vars())
    }

    pub fn from_env_pairs<I, K, V>(pairs: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut bind_addr = "127.0.0.1:18080".to_string();
        let mut environment = ServerRuntimeEnvironment::Development;
        let mut live_enabled = false;
        let mut live_default_timeout_secs = 30_u64;
        let mut live_default_max_envelopes = 100_usize;

        for (key, value) in pairs {
            match key.as_ref() {
                "FDC_SERVER_ADDR" => bind_addr = value.as_ref().to_string(),
                "FDC_SERVER_ENV" => {
                    environment = match value.as_ref() {
                        "development" | "dev" => ServerRuntimeEnvironment::Development,
                        "test" => ServerRuntimeEnvironment::Test,
                        "production" | "prod" => ServerRuntimeEnvironment::Production,
                        other => {
                            return Err(Error::config(format!(
                                "FDC_SERVER_ENV must be development, test, or production, got {other}"
                            )));
                        }
                    };
                }
                "FDC_LIVE_ENABLED" => {
                    live_enabled = matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
                "FDC_LIVE_DEFAULT_TIMEOUT_SECS" => {
                    live_default_timeout_secs = parse_positive_u64(
                        "FDC_LIVE_DEFAULT_TIMEOUT_SECS",
                        value.as_ref(),
                    )?;
                }
                "FDC_LIVE_DEFAULT_MAX_ENVELOPES" => {
                    live_default_max_envelopes = parse_positive_usize(
                        "FDC_LIVE_DEFAULT_MAX_ENVELOPES",
                        value.as_ref(),
                    )?;
                }
                _ => {}
            }
        }

        let bind_addr = bind_addr.parse().map_err(|error| {
            Error::config(format!("FDC_SERVER_ADDR must be a socket address: {error}"))
        })?;

        Ok(Self {
            bind_addr,
            environment,
            live_enabled,
            live_default_timeout_secs,
            live_default_max_envelopes,
        })
    }
}

fn parse_positive_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| Error::config(format!("{name} must be a positive integer: {error}")))?;
    if parsed == 0 {
        return Err(Error::config(format!("{name} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| Error::config(format!("{name} must be a positive integer: {error}")))?;
    if parsed == 0 {
        return Err(Error::config(format!("{name} must be greater than zero")));
    }
    Ok(parsed)
}
```

Modify `crates/fdc-server/src/lib.rs`:

```rust
pub mod runtime;

pub use runtime::{ServerRuntimeConfig, ServerRuntimeEnvironment};
```

- [ ] **Step 4: Run config tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
```

Expected: PASS, 3 tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/runtime crates/fdc-server/src/lib.rs crates/fdc-server/tests/runtime_config_contract.rs
rtk git commit -m "feat: add server runtime config"
```

---

## Task 2: Health Module and Production App State

**Files:**
- Create: `crates/fdc-server/tests/production_server_router_contract.rs`
- Create: `crates/fdc-server/src/runtime/app.rs`
- Create: `crates/fdc-server/src/health/mod.rs`
- Create: `crates/fdc-server/src/health/model.rs`
- Create: `crates/fdc-server/src/health/service.rs`
- Create: `crates/fdc-server/src/health/router.rs`
- Modify: `crates/fdc-server/src/lib.rs`
- Modify: `crates/fdc-server/Cargo.toml`

- [ ] **Step 1: Write failing health/router tests**

Create `crates/fdc-server/tests/production_server_router_contract.rs`:

```rust
use axum::{body::Body, http::{Request, StatusCode}};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

#[tokio::test]
async fn production_router_exposes_health_and_readiness() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let health = router.clone().oneshot(
        Request::builder().uri("/health").body(Body::empty()).expect("request should build"),
    ).await.expect("health should respond");
    assert_eq!(health.status(), StatusCode::OK);
    let health_body = axum::body::to_bytes(health.into_body(), usize::MAX).await.expect("body should read");
    let health_json: serde_json::Value = serde_json::from_slice(&health_body).expect("json");
    assert_eq!(health_json["status"], "healthy");

    let ready = router.oneshot(
        Request::builder().uri("/ready").body(Body::empty()).expect("request should build"),
    ).await.expect("ready should respond");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready_body = axum::body::to_bytes(ready.into_body(), usize::MAX).await.expect("body should read");
    let ready_json: serde_json::Value = serde_json::from_slice(&ready_body).expect("json");
    assert_eq!(ready_json["status"], "ready");
    assert_eq!(ready_json["live_enabled"], false);
    assert_eq!(ready_json["market_data_store_available"], true);
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: FAIL because production router/state do not exist.

- [ ] **Step 3: Add dependencies**

Modify `crates/fdc-server/Cargo.toml` dependencies:

```toml
axum = "0.7"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tower = { version = "0.4", features = ["util"] }
```

- [ ] **Step 4: Implement production state and health module**

Create focused module files matching the file structure. Implement:

- `ProductionServerState` with fields:
  - `config: ServerRuntimeConfig`
  - `market_data_store: Arc<QueryableMarketDataStore>`
- `build_production_router(state) -> axum::Router`
- health DTOs:
  - `HealthResponse { status: String }`
  - `ReadinessResponse { status: String, live_enabled: bool, market_data_store_available: bool }`
- routes:
  - `GET /health`
  - `GET /ready`

- [ ] **Step 5: Run health/router test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: PASS for the health/readiness test.

- [ ] **Step 6: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/Cargo.toml crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/health crates/fdc-server/src/lib.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production health router"
```

---

## Task 3: Market Data Module with Disabled Live Gate and Query

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
- Create: `crates/fdc-server/src/market_data/mod.rs`
- Create: `crates/fdc-server/src/market_data/model.rs`
- Create: `crates/fdc-server/src/market_data/supervisor.rs`
- Create: `crates/fdc-server/src/market_data/service.rs`
- Create: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/lib.rs`

- [ ] **Step 1: Add failing market-data route tests**

Append tests to `production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn production_live_start_is_explicitly_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router.oneshot(
        Request::builder()
            .method("POST")
            .uri("/market-data/live/start")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"timeout_secs":1,"max_envelopes":1}"#))
            .expect("request should build"),
    ).await.expect("start should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["state"], "idle");
    assert!(json["message"].as_str().unwrap().contains("FDC_LIVE_ENABLED=1"));
}

#[tokio::test]
async fn production_trade_query_reads_shared_store_after_fixture_ingest_helper() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([]).expect("config should parse"),
    );
    state.ingest_test_trade("BTCUSDT", "prod-btc-1").await.expect("fixture ingest should write");
    state.ingest_test_trade("ETHUSDT", "prod-eth-1").await.expect("fixture ingest should write");
    state.ingest_test_trade("BTCUSDT", "prod-btc-2").await.expect("fixture ingest should write");

    let router = build_production_router(state);
    let response = router.oneshot(
        Request::builder()
            .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
            .body(Body::empty())
            .expect("request should build"),
    ).await.expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 2);
}
```

- [ ] **Step 2: Run test to verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: FAIL because market-data routes and `ingest_test_trade` do not exist.

- [ ] **Step 3: Implement market-data module**

Implement:

- `MarketDataLiveState` enum serialized snake_case.
- `StartLiveMarketDataRequest { timeout_secs: Option<u64>, max_envelopes: Option<usize> }`.
- `StartLiveMarketDataResponse { state, envelopes_received, storage_records_written, market_data_store_records }`.
- `LiveMarketDataStatusResponse`.
- `MarketDataTradesResponse { returned_records, records }` using existing storage records or JSON values.
- `MarketDataSupervisor` with idle status and disabled gate response.
- `ProductionServerState::ingest_test_trade(symbol, trade_id)` test helper using existing Barter envelope + `run_realtime_barter_envelope_stream`.
- Routes:
  - `POST /market-data/live/start`
  - `GET /market-data/live/status`
  - `GET /market-data/trades`

For this task, live start only needs to enforce disabled-by-default behavior. Real enabled live acquisition can reuse the already verified `fdc-api` route pattern in Task 4.

- [ ] **Step 4: Run production router tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/market_data crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/lib.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat: add production market data routes"
```

---

## Task 4: Production Binary and Shutdown

**Files:**
- Create: `crates/fdc-server/src/bin/fdc_server.rs`
- Modify: `crates/fdc-server/src/runtime/shutdown.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add binary build verification test command**

No new Rust test is necessary for a process that blocks forever. The verification command is:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server
```

Expected before implementation: FAIL because the bin target does not exist.

- [ ] **Step 2: Implement shutdown helper**

Create `crates/fdc-server/src/runtime/shutdown.rs`:

```rust
pub async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("fdc server: failed to listen for shutdown signal: {error}");
    }
    eprintln!("fdc server: shutdown signal received");
}
```

- [ ] **Step 3: Implement production binary**

Create `crates/fdc-server/src/bin/fdc_server.rs`:

```rust
use fdc_server::{build_production_router, shutdown_signal, ProductionServerState, ServerRuntimeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerRuntimeConfig::from_env()?;
    let bind_addr = config.bind_addr;
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    eprintln!("fdc server listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
```

Export `shutdown_signal` from `lib.rs` through `runtime`.

- [ ] **Step 4: Run binary build verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server
```

Expected: PASS, 0 tests.

- [ ] **Step 5: Run all server tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/bin/fdc_server.rs crates/fdc-server/src/runtime/shutdown.rs crates/fdc-server/src/lib.rs
rtk git commit -m "feat: add production server binary"
```

---

## Task 5: Documentation and Final Verification

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update development status**

Add a section recording:

- production server runtime design path.
- new binary command: `cargo run -p fdc-server --bin fdc_server`.
- module layout.
- test evidence.
- next slice: enabled live acquisition in production supervisor as background task if not completed in this slice.

- [ ] **Step 2: Run final verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test runtime_config_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test production_server_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --bin fdc_server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test live_runner_route_contract
```

Expected: all PASS, live network tests ignored or gated by default.

- [ ] **Step 3: Commit docs**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record production server runtime slice"
```

## Self-Review

- Spec coverage: config, binary, module layering, health/readiness, live gate, query, compatibility, and docs are covered.
- The first slice intentionally implements disabled live gate and production module shape; true long-running background live streaming can follow if not included during Task 3.
- No default test depends on public internet.
- Existing `fdc-api` demo path remains untouched except final compatibility verification.
