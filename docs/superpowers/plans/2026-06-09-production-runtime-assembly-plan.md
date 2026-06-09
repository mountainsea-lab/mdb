# P40 Production Runtime Assembly Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the production `fdc_server` binary assemble the runtime-configured tiered market-data store and expose the runbook-required `/version` readiness route.

**Architecture:** The binary must use the existing async production constructor, `ProductionServerState::try_new(config).await?`, so environment storage config drives the actual running store. `/version` belongs with the existing health/readiness module as a read-only service metadata route. Regression coverage uses router contracts plus one process-level binary smoke to prove the binary no longer routes tiered config through a memory store.

**Tech Stack:** Rust, Tokio, Axum, Tower `ServiceExt`, serde/serde_json, `std::process::Command` for binary smoke tests, existing `fdc-server` test contract style.

---

## File Structure

- Modify `crates/fdc-server/src/bin/fdc_server.rs`
  - Replace memory-store startup with `ProductionServerState::try_new(config).await?`.
- Modify `crates/fdc-server/src/health/model.rs`
  - Add `VersionResponse` with `status`, `data`, and `message` fields.
  - Add `VersionData` with `service` and `version` fields.
- Modify `crates/fdc-server/src/health/service.rs`
  - Add `version_response()` returning `fdc-server` and `env!("CARGO_PKG_VERSION")`.
- Modify `crates/fdc-server/src/health/router.rs`
  - Add `GET /version` route and handler.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add router contract for `/version`.
  - Add router-level production runtime assembly regression using `ProductionServerState::try_new` and durable tier env.
- Create `crates/fdc-server/tests/production_binary_runtime_contract.rs`
  - Start the compiled `fdc_server` binary on a unique localhost port with safe tiered env.
  - Verify `/market-data/storage/status`, `/market-data/storage/health`, `/market-data/storage/maintenance/run-once`, and `/version` over HTTP.
- Modify `docs/runbooks/market-data-production-runbook.md`
  - Add `curl --noproxy '*'` note for localhost smoke checks.
  - Keep `/version` as a required readiness check because P40 implements it.

---

## Task 1: RED router contracts for `/version` and runtime-assembled tiered storage

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add `/version` router contract test**

Insert after `production_router_exposes_health_and_readiness`:

```rust
#[tokio::test]
async fn production_router_exposes_version_metadata() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/version")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("version should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["message"], serde_json::Value::Null);
    assert_eq!(json["data"]["service"], "fdc-server");
    assert_eq!(json["data"]["version"], env!("CARGO_PKG_VERSION"));
}
```

- [ ] **Step 2: Run `/version` contract and verify it fails RED**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_router_exposes_version_metadata -- --nocapture
```

Expected: FAIL with HTTP 404 or route not found for `/version`.

- [ ] **Step 3: Add runtime assembly router regression**

Insert near the storage status/health tests, after `production_storage_status_reports_durable_tiered_config_without_full_paths`:

```rust
#[tokio::test]
async fn production_runtime_assembly_uses_tiered_store_for_health_and_maintenance() {
    let root = unique_test_path("runtime-assembly-tiered");
    std::fs::create_dir_all(&root).expect("durable root should be created");
    let mut env = durable_tier_env(&root).to_vec();
    env.push((
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED".to_string(),
        "1".to_string(),
    ));
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build from runtime config");
    let router = build_production_router(state);

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["status"], "success");
    assert_eq!(status_json["data"]["backend"], "tiered");
    assert_eq!(status_json["data"]["tiered"], true);
    assert_eq!(status_json["data"]["durable_tiers_configured"], 3);
    assert_eq!(status_json["data"]["maintenance_enabled"], true);

    let health_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage health should respond");
    assert_eq!(health_response.status(), StatusCode::OK);
    let health_json = response_body_json(health_response).await;
    assert_eq!(health_json["status"], "success");
    assert_eq!(health_json["data"]["backend"], "tiered");
    assert_eq!(health_json["data"]["tiered"], true);
    assert_eq!(health_json["data"]["status"], "healthy");
    assert_eq!(health_json["data"]["tiers"].as_array().unwrap().len(), 4);

    let maintenance_response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");
    assert_eq!(maintenance_response.status(), StatusCode::OK);
    let maintenance_json = response_body_json(maintenance_response).await;
    assert_eq!(maintenance_json["status"], "success");
    assert_eq!(maintenance_json["data"]["accepted"], true);
    assert_eq!(maintenance_json["data"]["status"], "completed");
}
```

- [ ] **Step 4: Run runtime assembly router regression**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_runtime_assembly_uses_tiered_store_for_health_and_maintenance -- --nocapture
```

Expected: PASS already, because this test documents the correct constructor behavior. It protects the library assembly path but does not yet prove the binary uses it.

- [ ] **Step 5: Commit RED router tests**

Only commit after Step 2 has failed as expected and Step 4 has passed:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git add crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "test(server): cover p40 runtime assembly contracts"
```

---

## Task 2: GREEN `/version` route in health module

**Files:**
- Modify: `crates/fdc-server/src/health/model.rs`
- Modify: `crates/fdc-server/src/health/service.rs`
- Modify: `crates/fdc-server/src/health/router.rs`

- [ ] **Step 1: Add version response models**

Edit `crates/fdc-server/src/health/model.rs` to include these structs after `ReadinessResponse`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionResponse {
    pub status: String,
    pub data: VersionData,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionData {
    pub service: String,
    pub version: String,
}
```

- [ ] **Step 2: Add version service function**

Edit `crates/fdc-server/src/health/service.rs` imports and add `version_response()`:

```rust
use crate::{
    health::model::{HealthResponse, ReadinessResponse, VersionData, VersionResponse},
    ProductionServerState,
};
```

```rust
pub fn version_response() -> VersionResponse {
    VersionResponse {
        status: "success".to_string(),
        data: VersionData {
            service: "fdc-server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        message: None,
    }
}
```

- [ ] **Step 3: Add `/version` route and handler**

Edit `crates/fdc-server/src/health/router.rs` imports and router:

```rust
use crate::{
    health::{
        model::{HealthResponse, ReadinessResponse, VersionResponse},
        service::{health_response, readiness_response, version_response},
    },
    ProductionServerState,
};
```

Update `build_health_router`:

```rust
pub fn build_health_router(state: ProductionServerState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(readiness_handler))
        .route("/version", get(version_handler))
        .with_state(state)
}
```

Add handler after `readiness_handler`:

```rust
async fn version_handler() -> Json<VersionResponse> {
    Json(version_response())
}
```

- [ ] **Step 4: Run focused `/version` test and verify GREEN**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_router_exposes_version_metadata -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run existing health/readiness test**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_router_exposes_health_and_readiness -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit `/version` implementation**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git add crates/fdc-server/src/health/model.rs crates/fdc-server/src/health/service.rs crates/fdc-server/src/health/router.rs
rtk git commit -m "feat(server): expose version readiness endpoint"
```

---

## Task 3: RED process-level binary contract for production runtime assembly

**Files:**
- Create: `crates/fdc-server/tests/production_binary_runtime_contract.rs`

- [ ] **Step 1: Create binary runtime contract test file**

Create `crates/fdc-server/tests/production_binary_runtime_contract.rs` with this content:

```rust
use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

fn unique_test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fdc-server-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
    listener.local_addr().expect("listener should expose addr")
}

fn wait_for_server(child: &mut Child, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("child status should be readable") {
            panic!("fdc_server exited before readiness check: {status}");
        }

        match http_get_json(addr, "/health") {
            Ok(json) if json["status"] == "healthy" => return,
            Ok(_) | Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }

    panic!("fdc_server did not become healthy before timeout");
}

fn http_get_json(addr: SocketAddr, path: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--noproxy")
        .arg("*")
        .arg(format!("http://{addr}{path}"))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "curl failed for {path}: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

fn http_post_json(
    addr: SocketAddr,
    path: &str,
    body: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--fail")
        .arg("--noproxy")
        .arg("*")
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-d")
        .arg(body)
        .arg(format!("http://{addr}{path}"))
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "curl failed for {path}: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(serde_json::from_slice(&output.stdout)?)
}

fn spawn_stderr_reader(child: &mut Child) -> mpsc::Receiver<String> {
    let stderr = child.stderr.take().expect("stderr should be piped");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = sender.send(line);
        }
    });
    receiver
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn production_binary_assembles_tiered_runtime_store() {
    let binary = env!("CARGO_BIN_EXE_fdc_server");
    let addr = free_loopback_addr();
    let root = unique_test_path("binary-runtime-assembly");
    std::fs::create_dir_all(&root).expect("durable root should be created");

    let mut child = Command::new(binary)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("FDC_SERVER_ADDR", addr.to_string())
        .env("FDC_SERVER_ENV", "production")
        .env("FDC_LIVE_ENABLED", "0")
        .env("FDC_LIVE_AUTOSTART", "0")
        .env("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered")
        .env("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime")
        .env("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", root.join("l2.redb"))
        .env("FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH", root.join("l3.duckdb"))
        .env("FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH", root.join("l4-rocksdb"))
        .env("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")
        .env("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("fdc_server binary should start");
    let stderr = spawn_stderr_reader(&mut child);
    let mut child = ChildGuard(child);

    wait_for_server(&mut child.0, addr);

    let version = http_get_json(addr, "/version").expect("version should respond");
    assert_eq!(version["status"], "success");
    assert_eq!(version["data"]["service"], "fdc-server");
    assert_eq!(version["data"]["version"], env!("CARGO_PKG_VERSION"));

    let status = http_get_json(addr, "/market-data/storage/status").expect("status should respond");
    assert_eq!(status["status"], "success");
    assert_eq!(status["data"]["backend"], "tiered");
    assert_eq!(status["data"]["tiered"], true);
    assert_eq!(status["data"]["durable_tiers_configured"], 3);

    let health = http_get_json(addr, "/market-data/storage/health").expect("health should respond");
    assert_eq!(health["status"], "success");
    assert_eq!(health["data"]["backend"], "tiered");
    assert_eq!(health["data"]["tiered"], true);
    assert_eq!(health["data"]["status"], "healthy");
    assert_eq!(health["data"]["tiers"].as_array().unwrap().len(), 4);

    let maintenance = http_post_json(
        addr,
        "/market-data/storage/maintenance/run-once",
        r#"{"confirm":"run_maintenance_once","reason":"binary-contract"}"#,
    )
    .expect("maintenance should respond");
    assert_eq!(maintenance["status"], "success");
    assert_eq!(maintenance["data"]["accepted"], true);
    assert_eq!(maintenance["data"]["status"], "completed");

    let stderr_lines: Vec<String> = stderr.try_iter().collect();
    assert!(
        stderr_lines
            .iter()
            .any(|line| line.contains("fdc server listening on")),
        "server did not print listening line; stderr={stderr_lines:?}"
    );
}
```

- [ ] **Step 2: Run binary contract and verify it fails RED before binary fix**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_binary_runtime_contract production_binary_assembles_tiered_runtime_store -- --nocapture
```

Expected before Task 2 is implemented: may fail on `/version` 404. Expected after Task 2 but before Task 4 binary fix: FAIL because `/market-data/storage/health` reports `tiered=false` or maintenance reports `unsupported_backend`.

- [ ] **Step 3: Commit RED binary contract**

Commit after observing the failure mode that proves the binary assembly bug:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git add crates/fdc-server/tests/production_binary_runtime_contract.rs
rtk git commit -m "test(server): reproduce binary runtime assembly mismatch"
```

---

## Task 4: GREEN binary production runtime assembly

**Files:**
- Modify: `crates/fdc-server/src/bin/fdc_server.rs`

- [ ] **Step 1: Change binary to use async production constructor**

Replace this line:

```rust
let state = ProductionServerState::new(config);
```

with:

```rust
let state = ProductionServerState::try_new(config).await?;
```

- [ ] **Step 2: Run binary contract and verify GREEN**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_binary_runtime_contract production_binary_assembles_tiered_runtime_store -- --nocapture
```

Expected: PASS. The test must show status and health both report `tiered=true`, and maintenance run-once returns `status=success` with `data.status=completed`.

- [ ] **Step 3: Run router runtime assembly regression**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_runtime_assembly_uses_tiered_store_for_health_and_maintenance -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit binary fix**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git add crates/fdc-server/src/bin/fdc_server.rs
rtk git commit -m "fix(server): assemble production runtime store from config"
```

---

## Task 5: Runbook proxy-safe smoke wording

**Files:**
- Modify: `docs/runbooks/market-data-production-runbook.md`

- [ ] **Step 1: Update local curl guidance**

Find the smoke/readiness command section. Add this note before localhost curl examples:

```markdown
> Local proxy note: for localhost smoke checks, use `curl --noproxy '*' ...` so `HTTP_PROXY`/`HTTPS_PROXY` environment variables cannot route checks through a corporate proxy and produce false 503 responses.
```

For each localhost readiness command in the smoke block, prefer this form:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/health
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready
curl --noproxy '*' -fsS http://127.0.0.1:18080/version
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/status
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health
```

If the runbook already uses a variable such as `$FDC_SERVER_ADDR`, keep the variable but add `--noproxy '*'`.

- [ ] **Step 2: Verify runbook still references `/version` as required readiness**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
grep -nE "noproxy|/version|/market-data/storage/health|/market-data/storage/maintenance/run-once" docs/runbooks/market-data-production-runbook.md
```

Expected: output includes `--noproxy '*'`, `/version`, storage health, and maintenance run-once references.

- [ ] **Step 3: Commit runbook update**

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git add docs/runbooks/market-data-production-runbook.md
rtk git commit -m "docs(server): make production smoke checks proxy safe"
```

---

## Task 6: Verification and final commit hygiene

**Files:**
- No new files expected beyond the task files above.

- [ ] **Step 1: Format check focused crates**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: exit 0. If formatting fails, run `rtk cargo fmt -p fdc-server -p fdc-storage`, inspect diff, and commit formatting with the relevant code commit if possible.

- [ ] **Step 2: Run focused P40 tests**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract production_router_exposes_version_metadata -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract production_runtime_assembly_uses_tiered_store_for_health_and_maintenance -- --nocapture
rtk cargo test -p fdc-server --test production_binary_runtime_contract production_binary_assembles_tiered_runtime_store -- --nocapture
```

Expected: all PASS.

- [ ] **Step 3: Run P39/P38 regression set**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test runtime_config_contract production_local_example_env -- --nocapture
rtk cargo test -p fdc-server --test runtime_config_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract p38_ -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume -- --nocapture
rtk cargo test -p fdc-storage dependency_guard -- --nocapture
```

Expected: all PASS, matching P39 post-merge baseline plus P40 additions.

- [ ] **Step 4: Run full router contract if focused tests are green**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk cargo test -p fdc-server --test production_server_router_contract -- --nocapture
```

Expected: all router contract tests PASS.

- [ ] **Step 5: Check git status and log**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/p40-production-runtime-assembly
rtk git status --short
rtk git log --oneline -n 8
```

Expected: working tree clean. Log includes design spec commit plus P40 implementation commits.

- [ ] **Step 6: Optional local MVP smoke after tests pass**

Run only after all tests are green. Start the server with a copied local env, use `--noproxy '*'`, then kill the process and delete the copied env file. Verify:

```bash
curl --noproxy '*' -fsS http://127.0.0.1:18080/health
curl --noproxy '*' -fsS http://127.0.0.1:18080/ready
curl --noproxy '*' -fsS http://127.0.0.1:18080/version
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/status
curl --noproxy '*' -fsS http://127.0.0.1:18080/market-data/storage/health
curl --noproxy '*' -fsS -X POST -H 'Content-Type: application/json' -d '{"confirm":"run_maintenance_once","reason":"p40-smoke"}' http://127.0.0.1:18080/market-data/storage/maintenance/run-once
```

Expected: `/version` is 200, storage status and health both report tiered, maintenance run-once is 200 success.

---

## Self-Review Checklist

- [ ] Spec coverage: binary uses `try_new`; tiered status/health/maintenance agree; `/version` route exists; runbook proxy note added; no query/live/scheduler expansion.
- [ ] Placeholder scan: the plan contains no `TBD`, `TODO`, `implement later`, or unspecified test commands.
- [ ] Type consistency: `VersionResponse`, `VersionData`, and `version_response()` names match across model/service/router/test snippets.
- [ ] Scope control: no new market-data query routes or operator controls are introduced.
- [ ] Verification discipline: every code change has focused tests and commit steps.
