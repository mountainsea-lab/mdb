# B8 API State Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a testable API state boundary so `fdc-api` can receive and project `fdc-server` application state without owning lifecycle, network startup, or orchestrator mapping logic.

**Architecture:** `fdc-api` depends on `fdc-server` and adds a focused `state` module. `ApiAppState` wraps an `Arc<FdcServerApp>` and returns serializable readiness projections through pure functions. Existing network-serving code remains untouched except for public exports.

**Tech Stack:** Rust 1.95 workspace, `fdc-api`, `fdc-server`, `serde`, `Arc`, `ApiResponse`, cargo contract tests.

---

## File Structure

- Modify: `crates/fdc-api/Cargo.toml`  
  Add dependency on `fdc-server`.

- Modify: `crates/fdc-api/src/lib.rs`  
  Export the new `state` module and public API state types.

- Create: `crates/fdc-api/src/state.rs`  
  Own `ApiAppState`, `ApiReadinessStatus`, `ApiReadinessProjection`, and `readiness_response_from_state`.

- Create: `crates/fdc-api/tests/api_state_boundary_contract.rs`  
  Contract tests for readiness projection, response wrapping, and reverse dependency guards.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Record B8 completion, verification evidence, and next recommended slice.

---

## Task 1: Add failing API state boundary contract tests

**Files:**
- Modify: `crates/fdc-api/Cargo.toml`
- Create: `crates/fdc-api/tests/api_state_boundary_contract.rs`

- [ ] **Step 1: Add `fdc-server` dependency for the API boundary test**

Add this line under `[dependencies]` in `crates/fdc-api/Cargo.toml`:

```toml
fdc-server = { path = "../fdc-server" }
```

- [ ] **Step 2: Write the failing contract test**

Create `crates/fdc-api/tests/api_state_boundary_contract.rs` with:

```rust
use std::path::{Path, PathBuf};

use fdc_api::{
    readiness_response_from_state, ApiAppState, ApiReadinessStatus,
};
use fdc_server::{FdcServerApp, ServerEnvironment, ServerLifecycleState};

#[test]
fn default_uninitialized_server_projects_not_ready() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());
    let projection = state.readiness_projection();

    assert_eq!(projection.status, ApiReadinessStatus::NotReady);
    assert_eq!(projection.service_name, "fdc-server");
    assert_eq!(projection.environment, "development");
    assert_eq!(projection.server_lifecycle_state, "created");
    assert!(!projection.components_ready);
    assert!(!projection.api_enabled);
    assert!(projection.market_data_orchestrator_enabled);
    assert_eq!(state.server_app().state(), ServerLifecycleState::Created);
}

#[test]
fn initialized_server_projects_ready() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("default server app should initialize");
    let state = ApiAppState::new(app);
    let projection = state.readiness_projection();

    assert_eq!(projection.status, ApiReadinessStatus::Ready);
    assert_eq!(projection.environment, "development");
    assert_eq!(projection.server_lifecycle_state, "initialized");
    assert!(projection.components_ready);
}

#[test]
fn readiness_response_wraps_projection_in_successful_api_response() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("default server app should initialize");
    let state = ApiAppState::new(app);

    let response = readiness_response_from_state(&state);

    assert_eq!(response.status, "success");
    assert!(response.message.is_none());
    assert_eq!(response.data.status, ApiReadinessStatus::Ready);
    assert_eq!(response.data.service_name, "fdc-server");
}

#[test]
fn server_environment_and_lifecycle_labels_are_stable_api_strings() {
    assert_eq!(fdc_api::server_environment_label(ServerEnvironment::Development), "development");
    assert_eq!(fdc_api::server_environment_label(ServerEnvironment::Test), "test");
    assert_eq!(fdc_api::server_environment_label(ServerEnvironment::Production), "production");
    assert_eq!(fdc_api::server_lifecycle_state_label(ServerLifecycleState::Created), "created");
    assert_eq!(fdc_api::server_lifecycle_state_label(ServerLifecycleState::Initialized), "initialized");
    assert_eq!(fdc_api::server_lifecycle_state_label(ServerLifecycleState::Stopped), "stopped");
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_api() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-server/Cargo.toml"),
        workspace_root.join("crates/fdc-server/src"),
        workspace_root.join("crates/fdc-orchestrator/Cargo.toml"),
        workspace_root.join("crates/fdc-orchestrator/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
        workspace_root.join("crates/fdc-transform/Cargo.toml"),
        workspace_root.join("crates/fdc-transform/src"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/src"),
        workspace_root.join("crates/fdc-adapter/barter/Cargo.toml"),
        workspace_root.join("crates/fdc-adapter/barter/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-api", "fdc_api"], &mut violations);
    }

    assert!(
        violations.is_empty(),
        "lower-level/application assembly crates must not depend on fdc-api: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-api should live two levels under workspace root")
        .to_path_buf()
}

fn collect_forbidden_references(path: &Path, forbidden: &[&str], violations: &mut Vec<String>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("failed to read dependency guard directory") {
            let entry = entry.expect("failed to read dependency guard entry");
            collect_forbidden_references(&entry.path(), forbidden, violations);
        }
        return;
    }

    if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
        && path.file_name().and_then(|file_name| file_name.to_str()) != Some("Cargo.toml")
    {
        return;
    }

    let content = std::fs::read_to_string(path).expect("failed to read dependency guard file");
    for needle in forbidden {
        if content.contains(needle) {
            violations.push(format!("{} contains {needle}", path.display()));
        }
    }
}
```

- [ ] **Step 3: Run the contract test and verify RED**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test api_state_boundary_contract
```

Expected: FAIL because `ApiAppState`, readiness projection helpers, and exports do not exist yet.

---

## Task 2: Implement the API state boundary

**Files:**
- Create: `crates/fdc-api/src/state.rs`
- Modify: `crates/fdc-api/src/lib.rs`

- [ ] **Step 1: Add `state` module implementation**

Create `crates/fdc-api/src/state.rs`:

```rust
use std::sync::Arc;

use fdc_server::{FdcServerApp, ServerEnvironment, ServerLifecycleState};
use serde::{Deserialize, Serialize};

use crate::models::ApiResponse;

#[derive(Debug, Clone)]
pub struct ApiAppState {
    server_app: Arc<FdcServerApp>,
}

impl ApiAppState {
    pub fn new(server_app: FdcServerApp) -> Self {
        Self {
            server_app: Arc::new(server_app),
        }
    }

    pub fn from_shared(server_app: Arc<FdcServerApp>) -> Self {
        Self { server_app }
    }

    pub fn server_app(&self) -> &FdcServerApp {
        &self.server_app
    }

    pub fn shared_server_app(&self) -> Arc<FdcServerApp> {
        Arc::clone(&self.server_app)
    }

    pub fn readiness_projection(&self) -> ApiReadinessProjection {
        let app = self.server_app();
        ApiReadinessProjection {
            status: if app.is_ready() {
                ApiReadinessStatus::Ready
            } else {
                ApiReadinessStatus::NotReady
            },
            service_name: app.config().service_name.clone(),
            environment: server_environment_label(app.config().environment).to_string(),
            server_lifecycle_state: server_lifecycle_state_label(app.state()).to_string(),
            components_ready: app.is_ready(),
            api_enabled: app.config().enable_api,
            market_data_orchestrator_enabled: app.config().enable_market_data_orchestrator,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiReadinessStatus {
    Ready,
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiReadinessProjection {
    pub status: ApiReadinessStatus,
    pub service_name: String,
    pub environment: String,
    pub server_lifecycle_state: String,
    pub components_ready: bool,
    pub api_enabled: bool,
    pub market_data_orchestrator_enabled: bool,
}

pub fn readiness_response_from_state(
    state: &ApiAppState,
) -> ApiResponse<ApiReadinessProjection> {
    ApiResponse::success(state.readiness_projection())
}

pub fn server_environment_label(environment: ServerEnvironment) -> &'static str {
    match environment {
        ServerEnvironment::Development => "development",
        ServerEnvironment::Test => "test",
        ServerEnvironment::Production => "production",
    }
}

pub fn server_lifecycle_state_label(state: ServerLifecycleState) -> &'static str {
    match state {
        ServerLifecycleState::Created => "created",
        ServerLifecycleState::Initialized => "initialized",
        ServerLifecycleState::Stopped => "stopped",
    }
}
```

- [ ] **Step 2: Export API state types**

Modify `crates/fdc-api/src/lib.rs` to include:

```rust
pub mod state;
pub use state::{
    readiness_response_from_state, server_environment_label, server_lifecycle_state_label,
    ApiAppState, ApiReadinessProjection, ApiReadinessStatus,
};
```

- [ ] **Step 3: Run contract test and verify GREEN**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test api_state_boundary_contract
```

Expected: PASS, 5 tests.

---

## Task 3: Verify package and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test api_state_boundary_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: all pass. Existing warnings outside B8 may remain if tests pass.

- [ ] **Step 2: Update development status**

Add a B8 completed section to `docs/DEVELOPMENT_STATUS.md` recording:

```markdown
### Phase B8: API State Boundary

Implemented in `crates/fdc-api`.

Completed capabilities:

- Added `ApiAppState` as a shared API-facing handle around `FdcServerApp`.
- Added serializable readiness projection types for API responses.
- Added a pure readiness response helper that does not start network services.
- Preserved dependency direction: `fdc-api` consumes `fdc-server`; lower-level/application assembly crates do not reference `fdc-api`.

Contract tests:

- `crates/fdc-api/tests/api_state_boundary_contract.rs`
```

- [ ] **Step 3: Commit B8**

Run:

```bash
git add crates/fdc-api docs/DEVELOPMENT_STATUS.md docs/superpowers/specs/2026-05-27-fdc-api-state-boundary-design.md docs/superpowers/plans/2026-05-27-fdc-api-state-boundary.md
git commit -m "feat: add api state boundary"
```
