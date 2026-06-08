# P36 Live Collection Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden production live market-data collection with bounded retry, suppression, observable failure state, and a default-disabled confirmation-protected resume route.

**Architecture:** Keep live lifecycle and operator recovery semantics inside `fdc-server`. Extend runtime config, live DTOs/status, `MarketDataSupervisor`, live service loop, and Axum router contracts without changing `fdc-storage` or storage maintenance behavior.

**Tech Stack:** Rust, Tokio, Axum, serde DTOs, existing `fdc-server` runtime config/service/router patterns, Cargo package tests, TDD.

---

## File Structure

- Modify: `crates/fdc-server/src/runtime/config.rs`
  - Add live retry/resume config fields and validation.
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
  - Add config default, override, and invalid-value tests.
- Modify: `crates/fdc-server/src/market_data/model.rs`
  - Add `suppressed` live state, retry/status fields, and live resume DTOs.
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
  - Track consecutive failures, retry count, sanitized errors, next retry, suppression, and resume preparation.
- Modify: `crates/fdc-server/src/market_data/service.rs`
  - Add live retry loop policy, test seam, resume result type/service, and status mapping with `resume_enabled`.
- Modify: `crates/fdc-server/src/market_data/router.rs`
  - Add `POST /market-data/live/resume` and HTTP status mapping.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route/status/resume contracts.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P36 completion after implementation.

No `fdc-storage` source files should be modified.

---

## Task 1: Live retry and resume runtime config TDD

**Files:**
- Modify: `crates/fdc-server/tests/runtime_config_contract.rs`
- Modify: `crates/fdc-server/src/runtime/config.rs`

- [ ] **Step 1: Write failing config tests**

In `runtime_config_defaults_are_safe_for_local_production_server()`, add assertions after the existing live defaults:

```rust
    assert!(config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 1000);
    assert_eq!(config.live_retry_max_delay_ms, 30000);
    assert_eq!(config.live_max_consecutive_failures, 3);
    assert!(!config.market_data_live_resume_enabled);
```

Add these tests after `runtime_config_rejects_invalid_values()`:

```rust
#[test]
fn live_retry_config_accepts_valid_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_RETRY_ENABLED", "0"),
        ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "250"),
        ("FDC_LIVE_RETRY_MAX_DELAY_MS", "5000"),
        ("FDC_LIVE_MAX_CONSECUTIVE_FAILURES", "5"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("live retry config should parse");

    assert!(!config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 250);
    assert_eq!(config.live_retry_max_delay_ms, 5000);
    assert_eq!(config.live_max_consecutive_failures, 5);
    assert!(config.market_data_live_resume_enabled);
}

#[test]
fn live_retry_config_rejects_invalid_values() {
    let initial_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_LIVE_RETRY_INITIAL_DELAY_MS",
        "99",
    )])
    .expect_err("short initial retry delay should be rejected");
    assert!(initial_error
        .to_string()
        .contains("FDC_LIVE_RETRY_INITIAL_DELAY_MS"));

    let max_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_LIVE_RETRY_MAX_DELAY_MS",
        "99",
    )])
    .expect_err("short max retry delay should be rejected");
    assert!(max_error
        .to_string()
        .contains("FDC_LIVE_RETRY_MAX_DELAY_MS"));

    let ordering_error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "5000"),
        ("FDC_LIVE_RETRY_MAX_DELAY_MS", "1000"),
    ])
    .expect_err("max retry delay below initial delay should be rejected");
    assert!(ordering_error
        .to_string()
        .contains("FDC_LIVE_RETRY_MAX_DELAY_MS must be >= FDC_LIVE_RETRY_INITIAL_DELAY_MS"));

    let failures_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_LIVE_MAX_CONSECUTIVE_FAILURES",
        "0",
    )])
    .expect_err("zero max failures should be rejected");
    assert!(failures_error
        .to_string()
        .contains("FDC_LIVE_MAX_CONSECUTIVE_FAILURES"));
}
```

- [ ] **Step 2: Run RED config tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract live_retry_config
```

Expected: FAIL to compile because the new config fields do not exist.

- [ ] **Step 3: Implement config fields and parser**

In `ServerRuntimeConfig`, add after `live_default_max_envelopes`:

```rust
    pub live_retry_enabled: bool,
    pub live_retry_initial_delay_ms: u64,
    pub live_retry_max_delay_ms: u64,
    pub live_max_consecutive_failures: u32,
    pub market_data_live_resume_enabled: bool,
```

In `from_env_pairs`, initialize after live defaults:

```rust
        let mut live_retry_enabled = true;
        let mut live_retry_initial_delay_ms = 1000_u64;
        let mut live_retry_max_delay_ms = 30000_u64;
        let mut live_max_consecutive_failures = 3_u32;
        let mut market_data_live_resume_enabled = false;
```

Add match arms after `FDC_LIVE_DEFAULT_MAX_ENVELOPES`:

```rust
                "FDC_LIVE_RETRY_ENABLED" => {
                    live_retry_enabled = matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
                "FDC_LIVE_RETRY_INITIAL_DELAY_MS" => {
                    live_retry_initial_delay_ms = parse_u64_range(
                        "FDC_LIVE_RETRY_INITIAL_DELAY_MS",
                        value.as_ref(),
                        100,
                        600000,
                    )?;
                }
                "FDC_LIVE_RETRY_MAX_DELAY_MS" => {
                    live_retry_max_delay_ms = parse_u64_range(
                        "FDC_LIVE_RETRY_MAX_DELAY_MS",
                        value.as_ref(),
                        100,
                        3600000,
                    )?;
                }
                "FDC_LIVE_MAX_CONSECUTIVE_FAILURES" => {
                    live_max_consecutive_failures = parse_u32_range(
                        "FDC_LIVE_MAX_CONSECUTIVE_FAILURES",
                        value.as_ref(),
                        1,
                        100,
                    )?;
                }
                "FDC_MARKET_DATA_LIVE_RESUME_ENABLED" => {
                    market_data_live_resume_enabled =
                        matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
```

After scheduler jitter validation, add:

```rust
        if live_retry_max_delay_ms < live_retry_initial_delay_ms {
            return Err(Error::config(
                "FDC_LIVE_RETRY_MAX_DELAY_MS must be >= FDC_LIVE_RETRY_INITIAL_DELAY_MS",
            ));
        }
```

Add the new fields to the `Ok(Self { ... })` assignment.

- [ ] **Step 4: Run GREEN config tests**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract live_retry_config
rtk cargo test -p fdc-server --test runtime_config_contract runtime_config_defaults_are_safe_for_local_production_server
```

Expected: PASS.

- [ ] **Step 5: Commit config slice**

```bash
git add crates/fdc-server/src/runtime/config.rs crates/fdc-server/tests/runtime_config_contract.rs
git commit -m "feat(server): add live retry config"
```

---

## Task 2: Live DTO/status contract TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing route status test**

In `production_server_router_contract.rs`, add after the existing live status tests:

```rust
#[tokio::test]
async fn production_live_status_exposes_retry_and_resume_fields() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/live/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["state"], "idle");
    assert_eq!(json["data"]["consecutive_failures"], 0);
    assert_eq!(json["data"]["retry_count"], 0);
    assert!(json["data"]["last_error"].is_null());
    assert!(json["data"]["last_error_at_ns"].is_null());
    assert!(json["data"]["next_retry_at_ns"].is_null());
    assert!(json["data"]["suppressed_reason"].is_null());
    assert_eq!(json["data"]["resume_enabled"], false);
}
```

- [ ] **Step 2: Run RED status test**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_status_exposes_retry_and_resume_fields
```

Expected: FAIL because response fields are missing.

- [ ] **Step 3: Implement DTO fields**

In `MarketDataLiveState`, add:

```rust
    Suppressed,
```

Extend `LiveMarketDataStatusResponse`:

```rust
    pub consecutive_failures: u32,
    pub retry_count: u64,
    pub last_error: Option<String>,
    pub last_error_at_ns: Option<u64>,
    pub next_retry_at_ns: Option<u64>,
    pub suppressed_reason: Option<String>,
    pub resume_enabled: bool,
```

Add resume DTOs after `StartLiveMarketDataResponse`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeLiveMarketDataRequest {
    pub confirm: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeLiveMarketDataResponse {
    pub state: MarketDataLiveState,
    pub task_id: Option<String>,
    pub resumed: bool,
    pub reason: Option<String>,
    pub consecutive_failures: u32,
    pub retry_count: u64,
    pub next_retry_at_ns: Option<u64>,
}
```

- [ ] **Step 4: Populate default supervisor status fields**

In `MarketDataSupervisorInner`, add fields matching the DTO except `resume_enabled`.
Initialize them to zero/`None` in `new()`.
In `status()`, populate them and set `resume_enabled: false`; Task 2 Step 5 will make service-level status responses expose the runtime gate value.

- [ ] **Step 5: Update service status mapping**

Change `live_status(state)` to:

```rust
pub fn live_status(
    state: &ProductionServerState,
) -> crate::market_data::model::LiveMarketDataStatusResponse {
    let mut status = state.market_data_supervisor().status();
    status.resume_enabled = state.config().market_data_live_resume_enabled;
    status
}
```

- [ ] **Step 6: Run GREEN status test**

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_status_exposes_retry_and_resume_fields
```

Expected: PASS.

- [ ] **Step 7: Commit DTO/status slice**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/supervisor.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose live retry status fields"
```

---

## Task 3: Supervisor failure, suppression, and resume primitives TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`

- [ ] **Step 1: Write failing supervisor tests**

Append inside `#[cfg(test)] mod tests` in `supervisor.rs`. If the file has no test module, create one at the end:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_supervisor_records_failure_and_suppresses_at_threshold() {
        let supervisor = MarketDataSupervisor::new();
        supervisor.start_background(vec!["test:BTCUSDT:trade".to_string()]).unwrap();

        supervisor.record_failure_for_retry("first\nerror", 2, Some(123));
        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Running);
        assert_eq!(status.consecutive_failures, 1);
        assert_eq!(status.retry_count, 1);
        assert_eq!(status.last_error.as_deref(), Some("first error"));
        assert_eq!(status.next_retry_at_ns, Some(123));

        supervisor.record_failure_for_retry("second error", 2, None);
        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Suppressed);
        assert_eq!(status.consecutive_failures, 2);
        assert_eq!(status.suppressed_reason.as_deref(), Some("suppressed_after_failures"));
        assert!(status.next_retry_at_ns.is_none());
    }

    #[test]
    fn live_supervisor_success_resets_failure_state() {
        let supervisor = MarketDataSupervisor::new();
        supervisor.start_background(vec!["test:BTCUSDT:trade".to_string()]).unwrap();
        supervisor.record_failure_for_retry("temporary", 3, Some(456));
        supervisor.record_progress(1, 1, 1, Some(789));

        let status = supervisor.status();
        assert_eq!(status.consecutive_failures, 0);
        assert!(status.last_error.is_none());
        assert!(status.last_error_at_ns.is_none());
        assert!(status.next_retry_at_ns.is_none());
        assert!(status.suppressed_reason.is_none());
    }

    #[test]
    fn live_supervisor_prepare_resume_clears_suppression_when_safe() {
        let supervisor = MarketDataSupervisor::new();
        supervisor.start_background(vec!["test:BTCUSDT:trade".to_string()]).unwrap();
        supervisor.record_failure_for_retry("boom", 1, None);

        let outcome = supervisor.prepare_resume("operator".to_string());
        assert!(outcome.resumed);
        assert!(!outcome.running);
        assert_eq!(outcome.consecutive_failures, 0);

        let status = supervisor.status();
        assert_eq!(status.state, MarketDataLiveState::Idle);
        assert_eq!(status.stop_reason.as_deref(), Some("operator"));
        assert!(status.last_error.is_none());
    }

    #[test]
    fn live_supervisor_prepare_resume_rejects_running_state() {
        let supervisor = MarketDataSupervisor::new();
        supervisor.start_background(vec!["test:BTCUSDT:trade".to_string()]).unwrap();

        let outcome = supervisor.prepare_resume("operator".to_string());
        assert!(!outcome.resumed);
        assert!(outcome.running);
        assert_eq!(supervisor.status().state, MarketDataLiveState::Running);
    }
}
```

- [ ] **Step 2: Run RED supervisor tests**

```bash
rtk cargo test -p fdc-server live_supervisor_
```

Expected: FAIL because `record_failure_for_retry()` and `prepare_resume()` do not exist.

- [ ] **Step 3: Implement primitives**

Add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveResumeOutcome {
    pub resumed: bool,
    pub running: bool,
    pub previous_consecutive_failures: u32,
    pub consecutive_failures: u32,
    pub retry_count: u64,
}
```

Implement methods:

```rust
pub fn record_failure_for_retry(
    &self,
    message: impl Into<String>,
    max_consecutive_failures: u32,
    next_retry_at_ns: Option<u64>,
) {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
    inner.retry_count = inner.retry_count.saturating_add(1);
    inner.last_error = Some(sanitize_live_error(message.into()));
    inner.failure_message = inner.last_error.clone();
    inner.last_error_at_ns = Some(now_ns());
    if inner.consecutive_failures >= max_consecutive_failures {
        inner.state = MarketDataLiveState::Suppressed;
        inner.suppressed_reason = Some("suppressed_after_failures".to_string());
        inner.next_retry_at_ns = None;
        inner.stop_requested = false;
    } else {
        inner.next_retry_at_ns = next_retry_at_ns;
    }
}

pub fn prepare_resume(&self, reason: String) -> LiveResumeOutcome {
    let mut inner = self.inner.lock().expect("market-data supervisor mutex should not be poisoned");
    let previous_consecutive_failures = inner.consecutive_failures;
    match inner.state {
        MarketDataLiveState::Starting | MarketDataLiveState::Running | MarketDataLiveState::Stopping => {
            LiveResumeOutcome {
                resumed: false,
                running: true,
                previous_consecutive_failures,
                consecutive_failures: inner.consecutive_failures,
                retry_count: inner.retry_count,
            }
        }
        _ => {
            inner.state = MarketDataLiveState::Idle;
            inner.consecutive_failures = 0;
            inner.retry_count = 0;
            inner.last_error = None;
            inner.failure_message = None;
            inner.last_error_at_ns = None;
            inner.next_retry_at_ns = None;
            inner.suppressed_reason = None;
            inner.stop_requested = false;
            inner.stop_reason = Some(reason);
            LiveResumeOutcome {
                resumed: true,
                running: false,
                previous_consecutive_failures,
                consecutive_failures: 0,
                retry_count: 0,
            }
        }
    }
}
```

Update `record_progress()` to reset failure fields on successful progress. Add helper:

```rust
fn sanitize_live_error(message: String) -> String {
    let single_line = message.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX_LEN: usize = 256;
    if single_line.len() > MAX_LEN {
        format!("{}...", &single_line[..MAX_LEN])
    } else {
        single_line
    }
}
```

- [ ] **Step 4: Run GREEN supervisor tests**

```bash
rtk cargo test -p fdc-server live_supervisor_
```

Expected: PASS.

- [ ] **Step 5: Commit supervisor slice**

```bash
git add crates/fdc-server/src/market_data/supervisor.rs
git commit -m "feat(server): add live suppression state primitives"
```

---

## Task 4: Live retry loop service TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/supervisor.rs`

- [ ] **Step 1: Write failing service tests**

Append tests near existing service tests in `service.rs`:

```rust
#[cfg(test)]
mod live_retry_tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[tokio::test]
    async fn live_retry_loop_suppresses_after_configured_failures() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_LIVE_ENABLED", "1"),
            ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "100"),
            ("FDC_LIVE_RETRY_MAX_DELAY_MS", "100"),
            ("FDC_LIVE_MAX_CONSECUTIVE_FAILURES", "2"),
        ])
        .expect("config should parse");
        let state = ProductionServerState::new(config);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_runner = attempts.clone();

        let result = run_background_live_collection_with_runner_for_test(
            state.market_data_supervisor(),
            state.market_data_store(),
            LiveRetryPolicy::from_config(state.config()),
            move || {
                attempts_for_runner.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err("network down".to_string()) })
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let status = state.market_data_supervisor().status();
        assert_eq!(status.state, MarketDataLiveState::Suppressed);
        assert_eq!(status.consecutive_failures, 2);
    }
}
```

- [ ] **Step 2: Run RED retry service test**

```bash
rtk cargo test -p fdc-server live_retry_loop_suppresses_after_configured_failures
```

Expected: FAIL because test seam and `LiveRetryPolicy` do not exist.

- [ ] **Step 3: Implement retry policy and loop seam**

Add:

```rust
#[derive(Debug, Clone, Copy)]
struct LiveRetryPolicy {
    enabled: bool,
    initial_delay: Duration,
    max_delay: Duration,
    max_consecutive_failures: u32,
}

impl LiveRetryPolicy {
    fn from_config(config: &ServerRuntimeConfig) -> Self {
        Self {
            enabled: config.live_retry_enabled,
            initial_delay: Duration::from_millis(config.live_retry_initial_delay_ms),
            max_delay: Duration::from_millis(config.live_retry_max_delay_ms),
            max_consecutive_failures: config.live_max_consecutive_failures,
        }
    }

    fn delay_for_failure(&self, consecutive_failures: u32) -> Duration {
        let multiplier = 1_u32.checked_shl(consecutive_failures.saturating_sub(1).min(16)).unwrap_or(u32::MAX);
        self.initial_delay.saturating_mul(multiplier).min(self.max_delay)
    }
}
```

Refactor `run_background_live_collection()` to call an internal generic loop that accepts a runner closure. On failure:

- if retry disabled, call `supervisor.fail(message)` and return `Err(message)`;
- if retry enabled, call `record_failure_for_retry()`;
- if supervisor is suppressed, return `Err("live collection suppressed after consecutive failures".to_string())`;
- otherwise sleep for bounded delay and retry.

The test seam name must be:

```rust
async fn run_background_live_collection_with_runner_for_test<F, Fut>(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    retry_policy: LiveRetryPolicy,
    mut runner: F,
) -> std::result::Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<StartLiveMarketDataResponse, String>>,
```

- [ ] **Step 4: Run GREEN retry service test**

```bash
rtk cargo test -p fdc-server live_retry_loop_suppresses_after_configured_failures
```

Expected: PASS.

- [ ] **Step 5: Run live regression tests**

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_fake_background_start_stop_updates_status
rtk cargo test -p fdc-server --test production_server_router_contract production_live_stop_when_idle_returns_idle_state
```

Expected: PASS.

- [ ] **Step 6: Commit retry loop slice**

```bash
git add crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/supervisor.rs
git commit -m "feat(server): retry live collection before suppression"
```

---

## Task 5: Live resume service and route TDD

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add route request helper**

In `production_server_router_contract.rs`, add near other request helpers:

```rust
fn live_resume_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}
```

- [ ] **Step 2: Write failing resume route tests**

Add tests:

```rust
#[tokio::test]
async fn production_live_resume_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("resume_live_collection"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["resumed"], false);
}

#[tokio::test]
async fn production_live_resume_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["resumed"], false);
}

#[tokio::test]
async fn production_live_resume_conflicts_when_running() {
    use fdc_server::market_data::service::start_fake_background_live_for_test;

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    start_fake_background_live_for_test(&state, 10, std::time::Duration::from_millis(25))
        .await
        .expect("fake live should start");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("resume_live_collection"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::CONFLICT);
}
```

- [ ] **Step 3: Run RED resume route tests**

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
```

Expected: FAIL with 404 and/or missing service symbols.

- [ ] **Step 4: Implement service result and resume logic**

In `service.rs`, import resume DTOs. Add:

```rust
pub const LIVE_RESUME_CONFIRMATION: &str = "resume_live_collection";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveResumeResult {
    pub http_status: StorageMaintenanceHttpStatus,
    pub response: ResumeLiveMarketDataResponse,
    pub message: Option<String>,
}
```

Implement `resume_live(state, request)`:

- forbidden if `!state.config().live_enabled`;
- forbidden if `!state.config().market_data_live_resume_enabled`;
- bad request if confirm mismatch;
- call `prepare_resume(reason.clone().unwrap_or_else(|| "resume".to_string()))`;
- conflict if `outcome.running`;
- call `start_background_live(state, StartLiveMarketDataRequest { timeout_secs: None, max_envelopes: None }).await`;
- return success DTO with current status and `resumed: true` on start acceptance;
- if start fails, return conflict with `resumed: false` and current status.

- [ ] **Step 5: Implement router wiring**

In `router.rs`:

- import `ResumeLiveMarketDataRequest` and `ResumeLiveMarketDataResponse`;
- import `resume_live`;
- add route `.route("/market-data/live/resume", post(resume_live_handler))` after stop route;
- add handler mirroring scheduler resume handler and using `storage_maintenance_status_code()` for status mapping.

- [ ] **Step 6: Run GREEN resume route tests**

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
```

Expected: PASS.

- [ ] **Step 7: Commit resume route slice**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): add gated live resume route"
```

---

## Task 6: Documentation and final verification

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update development status**

Add a new section above P35:

```markdown
## 2026-06-08 P36 Live Collection Hardening

Completed:

- Added bounded live retry configuration:
  - `FDC_LIVE_RETRY_ENABLED`
  - `FDC_LIVE_RETRY_INITIAL_DELAY_MS`
  - `FDC_LIVE_RETRY_MAX_DELAY_MS`
  - `FDC_LIVE_MAX_CONSECUTIVE_FAILURES`
- Added default-disabled live resume gate:
  - `FDC_MARKET_DATA_LIVE_RESUME_ENABLED`
- Added `suppressed` live state after repeated live collection failures.
- Extended live status with consecutive failure count, retry count, sanitized last error, retry schedule, suppression reason, and resume gate visibility.
- Added explicit confirmation-protected live resume route:
  - `POST /market-data/live/resume`
  - confirmation string: `resume_live_collection`
- Resume clears live failure/suppression state and starts a normal live background task when safe.
- Resume does not clear market-data records, reset storage/audit data, run storage maintenance, or alter storage tier paths.
- Preserved the `fdc-storage` boundary.

Design and plan:

- `docs/superpowers/specs/2026-06-08-live-collection-hardening-design.md`
- `docs/superpowers/plans/2026-06-08-live-collection-hardening.md`

Verification:

- `rtk cargo test -p fdc-server --test runtime_config_contract live_retry_config` - record pass count from command output.
- `rtk cargo test -p fdc-server --test runtime_config_contract runtime_config_defaults_are_safe_for_local_production_server` - record pass count from command output.
- `rtk cargo test -p fdc-server live_supervisor_` - record pass count from command output.
- `rtk cargo test -p fdc-server live_retry_loop_suppresses_after_configured_failures` - record pass count from command output.
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_status_exposes_retry_and_resume_fields` - record pass count from command output.
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume` - record pass count from command output.
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_fake_background_start_stop_updates_status` - record pass count from command output.
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_stop_when_idle_returns_idle_state` - record pass count from command output.
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume` - record pass count from command output.
- `rtk cargo test -p fdc-storage --test dependency_guard` - record pass count from command output.
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - record exit 0.

Recommended next slice:

- P37 acquisition to four-tier storage to query end-to-end acceptance.
```

After running Step 2, replace each `record pass count from command output` note above with the actual pass count or `exit 0` before committing.

- [ ] **Step 2: Run focused verification**

Run:

```bash
rtk cargo test -p fdc-server --test runtime_config_contract live_retry_config
rtk cargo test -p fdc-server --test runtime_config_contract runtime_config_defaults_are_safe_for_local_production_server
rtk cargo test -p fdc-server live_supervisor_
rtk cargo test -p fdc-server live_retry_loop_suppresses_after_configured_failures
rtk cargo test -p fdc-server --test production_server_router_contract production_live_status_exposes_retry_and_resume_fields
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
rtk cargo test -p fdc-server --test production_server_router_contract production_live_fake_background_start_stop_updates_status
rtk cargo test -p fdc-server --test production_server_router_contract production_live_stop_when_idle_returns_idle_state
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all pass. The public-internet ignored smoke tests remain ignored unless explicitly requested.

- [ ] **Step 3: Commit status update**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record live collection hardening status"
```

---

## Plan Self-Review

Spec coverage:

- Bounded retry: Task 1 config, Task 4 loop.
- Failure suppression: Task 2 DTO, Task 3 supervisor, Task 4 loop.
- Rich status: Task 2.
- Gated resume: Task 1 config, Task 5 service/route.
- Stop-over-retry safety: Task 4 regression and loop requirements.
- No storage mutation: Task 5 service constraints and final regression with storage boundary guard.
- P37/P38/P39 deferred: documented as future slices.

Placeholder scan: no `TBD`, `TODO`, or deferred implementation placeholders remain. Task 6 explicitly instructs workers to replace verification recording notes with actual command output before the status commit.

Type consistency:

- Config names match the design spec.
- DTO names are `ResumeLiveMarketDataRequest` and `ResumeLiveMarketDataResponse`.
- Confirmation string is `resume_live_collection`.
- Live state variant is `Suppressed`, serialized as `suppressed`.
