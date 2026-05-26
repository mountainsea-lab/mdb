# B7 Server Assembly Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the template `fdc-server` crate with a lightweight, testable application assembly boundary that can hold orchestrator/storage components without starting real services.

**Architecture:** `fdc-server` gets focused modules for configuration, component handles, and lifecycle assembly. It depends on `fdc-core`, `fdc-storage`, and `fdc-orchestrator` as an application consumer, while lower-level crates do not depend back on `fdc-server`. B7 performs no network serving, no API handler integration, no live runners, and no real database writes.

**Tech Stack:** Rust 1.95 workspace, `fdc-core::Result`, `fdc-storage::StorageWriteSink`, `fdc-storage::RecordingStorageSink`, `fdc-orchestrator::pipeline::OrchestratorPipelineResult`, `Arc<dyn Trait>`.

---

## File Structure

- Modify: `crates/fdc-server/Cargo.toml`  
  Add dependencies on `fdc-core`, `fdc-storage`, and `fdc-orchestrator`.

- Replace: `crates/fdc-server/src/lib.rs`  
  Remove the template add function and export focused server assembly modules and public types.

- Create: `crates/fdc-server/src/config.rs`  
  Owns `FdcServerConfig` and `ServerEnvironment`.

- Create: `crates/fdc-server/src/components.rs`  
  Owns `ServerComponents`, default recording storage sink construction, readiness checks, and a public orchestrator result alias.

- Create: `crates/fdc-server/src/app.rs`  
  Owns `FdcServerApp` and `ServerLifecycleState`.

- Create: `crates/fdc-server/tests/server_assembly_contract.rs`  
  Contract tests for default assembly, readiness, lifecycle transitions, custom component injection, orchestrator dependency consumption, and reverse dependency guards.

- Modify: `docs/DEVELOPMENT_STATUS.md`  
  Add B7 completion status, verification evidence, and next recommended slice after implementation.

---

## Task 1: Add failing server assembly contract tests

**Files:**
- Modify: `crates/fdc-server/Cargo.toml`
- Create: `crates/fdc-server/tests/server_assembly_contract.rs`

- [ ] **Step 1: Add B7 dependencies to the server manifest**

Update `crates/fdc-server/Cargo.toml` to include:

```toml
[dependencies]
fdc-core = { path = "../fdc-core" }
fdc-storage = { path = "../fdc-storage" }
fdc-orchestrator = { path = "../fdc-orchestrator" }
```

- [ ] **Step 2: Write the failing contract test**

Create `crates/fdc-server/tests/server_assembly_contract.rs` with this complete content:

```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fdc_orchestrator::pipeline::OrchestratorPipelineResult;
use fdc_server::{
    FdcServerApp, FdcServerConfig, ServerComponents, ServerEnvironment, ServerLifecycleState,
};
use fdc_storage::{RecordingStorageSink, StorageWriteSink};

#[test]
fn default_app_uses_development_config_without_api_startup() {
    let app = FdcServerApp::with_defaults();

    assert_eq!(app.config().service_name, "fdc-server");
    assert_eq!(app.config().environment, ServerEnvironment::Development);
    assert!(!app.config().enable_api);
    assert!(app.config().enable_market_data_orchestrator);
    assert_eq!(app.state(), ServerLifecycleState::Created);
    assert!(!app.is_ready(), "created app should not be ready until initialized");
}

#[test]
fn default_components_are_ready_and_database_free() {
    let components = ServerComponents::with_recording_storage_sink();

    assert!(components.is_ready());
    assert_eq!(components.market_data_storage_sink().recorded_record_count(), 0);
}

#[test]
fn app_initialization_marks_ready_without_starting_network_services() {
    let mut app = FdcServerApp::with_defaults();

    app.initialize().expect("default app should initialize");

    assert_eq!(app.state(), ServerLifecycleState::Initialized);
    assert!(app.is_ready());
}

#[test]
fn app_stop_transitions_to_stopped() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("default app should initialize");

    app.stop().expect("initialized app should stop");

    assert_eq!(app.state(), ServerLifecycleState::Stopped);
    assert!(!app.is_ready());
}

#[test]
fn custom_storage_sink_can_be_injected() {
    let sink = Arc::new(RecordingStorageSink::new());
    let dyn_sink: Arc<dyn StorageWriteSink> = sink.clone();
    let components = ServerComponents::new(dyn_sink);
    let config = FdcServerConfig::for_tests();

    let app = FdcServerApp::new(config, components);

    assert!(app.components().is_ready());
    assert_eq!(sink.recorded_record_count(), 0);
}

#[test]
fn server_can_reference_orchestrator_public_types() {
    let result = OrchestratorPipelineResult::default();

    assert_eq!(result.envelopes_received, 0);
    assert_eq!(result.storage_records_written, 0);
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_server() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-core/Cargo.toml"),
        workspace_root.join("crates/fdc-core/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
        workspace_root.join("crates/fdc-orchestrator/Cargo.toml"),
        workspace_root.join("crates/fdc-orchestrator/src"),
        workspace_root.join("crates/fdc-api/Cargo.toml"),
        workspace_root.join("crates/fdc-api/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-server", "fdc_server"], &mut violations);
    }

    assert!(
        violations.is_empty(),
        "lower-level or protocol crates must not depend on fdc-server: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-server should live two levels under workspace root")
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

- [ ] **Step 3: Run the contract test to verify it fails before implementation**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract
```

Expected: FAIL with unresolved imports such as `no FdcServerApp in the root`, `no ServerComponents in the root`, or similar missing type errors.

- [ ] **Step 4: Commit the failing test**

Run:

```bash
git add crates/fdc-server/Cargo.toml crates/fdc-server/tests/server_assembly_contract.rs
git commit -m "test: define server assembly boundary contract"
```

---

## Task 2: Add server configuration types

**Files:**
- Replace: `crates/fdc-server/src/lib.rs`
- Create: `crates/fdc-server/src/config.rs`

- [ ] **Step 1: Replace `lib.rs` exports**

Replace `crates/fdc-server/src/lib.rs` with:

```rust
//! Application assembly boundary for Financial Data Center.
//!
//! `fdc-server` composes application-level components. It does not own adapter,
//! ingestion, transform, or storage mapping logic; that remains in
//! `fdc-orchestrator` and lower-level crates.

pub mod app;
pub mod components;
pub mod config;

pub use app::{FdcServerApp, ServerLifecycleState};
pub use components::{MarketDataOrchestratorResult, ServerComponents};
pub use config::{FdcServerConfig, ServerEnvironment};
```

- [ ] **Step 2: Implement `config.rs`**

Create `crates/fdc-server/src/config.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FdcServerConfig {
    pub service_name: String,
    pub environment: ServerEnvironment,
    pub enable_api: bool,
    pub enable_market_data_orchestrator: bool,
}

impl FdcServerConfig {
    pub fn for_tests() -> Self {
        Self {
            service_name: "fdc-server-test".to_string(),
            environment: ServerEnvironment::Test,
            enable_api: false,
            enable_market_data_orchestrator: true,
        }
    }
}

impl Default for FdcServerConfig {
    fn default() -> Self {
        Self {
            service_name: "fdc-server".to_string(),
            environment: ServerEnvironment::Development,
            enable_api: false,
            enable_market_data_orchestrator: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerEnvironment {
    Development,
    Test,
    Production,
}
```

- [ ] **Step 3: Add temporary empty modules for compilation**

Create `crates/fdc-server/src/components.rs`:

```rust
pub type MarketDataOrchestratorResult = fdc_orchestrator::pipeline::OrchestratorPipelineResult;
```

Create `crates/fdc-server/src/app.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerLifecycleState {
    Created,
    Initialized,
    Stopped,
}
```

These temporary modules let the config-focused build proceed; later tasks replace them with full implementations.

- [ ] **Step 4: Run focused config test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract default_app_uses_development_config_without_api_startup
```

Expected: compile errors about missing `FdcServerApp` or `ServerComponents`, confirming config types exist but app/components are not implemented yet.

- [ ] **Step 5: Commit configuration types**

Run:

```bash
git add crates/fdc-server/src/lib.rs crates/fdc-server/src/config.rs crates/fdc-server/src/components.rs crates/fdc-server/src/app.rs
git commit -m "feat: add server assembly configuration types"
```

---

## Task 3: Implement server components

**Files:**
- Modify: `crates/fdc-server/src/components.rs`

- [ ] **Step 1: Implement `components.rs`**

Replace `crates/fdc-server/src/components.rs` with:

```rust
use std::sync::Arc;

use fdc_storage::{RecordingStorageSink, StorageWriteSink};

pub type MarketDataOrchestratorResult = fdc_orchestrator::pipeline::OrchestratorPipelineResult;

#[derive(Clone)]
pub struct ServerComponents {
    market_data_storage_sink: Arc<dyn StorageWriteSink>,
}

impl ServerComponents {
    pub fn new(market_data_storage_sink: Arc<dyn StorageWriteSink>) -> Self {
        Self {
            market_data_storage_sink,
        }
    }

    pub fn with_recording_storage_sink() -> Self {
        Self::new(Arc::new(RecordingStorageSink::new()))
    }

    pub fn market_data_storage_sink(&self) -> Arc<dyn StorageWriteSink> {
        Arc::clone(&self.market_data_storage_sink)
    }

    pub fn is_ready(&self) -> bool {
        true
    }
}

impl Default for ServerComponents {
    fn default() -> Self {
        Self::with_recording_storage_sink()
    }
}
```

- [ ] **Step 2: Run focused components tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract default_components_are_ready_and_database_free custom_storage_sink_can_be_injected server_can_reference_orchestrator_public_types
```

Expected: the command may fail because `FdcServerApp` is still missing, but errors should not mention missing `ServerComponents` methods or missing `MarketDataOrchestratorResult`.

- [ ] **Step 3: Commit server components**

Run:

```bash
git add crates/fdc-server/src/components.rs
git commit -m "feat: add server assembly components"
```

---

## Task 4: Implement server application lifecycle

**Files:**
- Modify: `crates/fdc-server/src/app.rs`

- [ ] **Step 1: Implement `app.rs`**

Replace `crates/fdc-server/src/app.rs` with:

```rust
use fdc_core::{error::Error, Result};

use crate::{FdcServerConfig, ServerComponents};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerLifecycleState {
    Created,
    Initialized,
    Stopped,
}

pub struct FdcServerApp {
    config: FdcServerConfig,
    components: ServerComponents,
    state: ServerLifecycleState,
}

impl FdcServerApp {
    pub fn new(config: FdcServerConfig, components: ServerComponents) -> Self {
        Self {
            config,
            components,
            state: ServerLifecycleState::Created,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(FdcServerConfig::default(), ServerComponents::default())
    }

    pub fn initialize(&mut self) -> Result<()> {
        if self.config.service_name.trim().is_empty() {
            return Err(Error::validation("server service_name must not be empty"));
        }
        if !self.components.is_ready() {
            return Err(Error::validation("server components are not ready"));
        }

        self.state = ServerLifecycleState::Initialized;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        self.state = ServerLifecycleState::Stopped;
        Ok(())
    }

    pub fn config(&self) -> &FdcServerConfig {
        &self.config
    }

    pub fn components(&self) -> &ServerComponents {
        &self.components
    }

    pub fn state(&self) -> ServerLifecycleState {
        self.state
    }

    pub fn is_ready(&self) -> bool {
        self.state == ServerLifecycleState::Initialized && self.components.is_ready()
    }
}
```

- [ ] **Step 2: Run full server assembly contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract
```

Expected: PASS, 7 tests passed.

- [ ] **Step 3: Commit server app lifecycle**

Run:

```bash
git add crates/fdc-server/src/app.rs crates/fdc-server/src/lib.rs
git commit -m "feat: add server assembly lifecycle"
```

---

## Task 5: Run verification and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run B7 verification commands**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-orchestrator -p fdc-storage
```

Expected: all commands exit 0.

- [ ] **Step 2: Update `docs/DEVELOPMENT_STATUS.md`**

Add a new completed section after B6:

```markdown
### Phase B7: Server Assembly Boundary

Implemented in `crates/fdc-server`.

Completed capabilities:

- Replaced the template `fdc-server` crate with a real application assembly boundary.
- Added `FdcServerConfig` and `ServerEnvironment` for server-level assembly configuration.
- Added `ServerComponents` with an injectable `Arc<dyn StorageWriteSink>` market-data storage sink.
- Added default database-free assembly using `RecordingStorageSink`.
- Added `FdcServerApp` and `ServerLifecycleState` for deterministic lifecycle transitions.
- Verified that `fdc-server` can consume `fdc-orchestrator` public types without moving orchestration glue into server code.
- Preserved dependency direction: lower-level crates and `fdc-api` do not reference `fdc-server`.

Contract tests:

- `crates/fdc-server/tests/server_assembly_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-26-fdc-server-assembly-boundary-design.md`
- `docs/superpowers/plans/2026-05-26-fdc-server-assembly-boundary.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test server_assembly_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-orchestrator -p fdc-storage`
```

Update `Current Verification Baseline` to reference the final B7 commit hash and command results.

Update `Next Recommended Development Slice` to:

```markdown
### Phase B8: API State Boundary

Goal: define how `fdc-api` receives application state assembled by `fdc-server` without owning orchestrator mapping logic.

Recommended scope:

- Define API-facing state handles and readiness projection.
- Keep HTTP handlers simulated unless a separate B8 implementation plan explicitly wires one bounded endpoint.
- Do not start real network services or production storage writes yet.
```

- [ ] **Step 3: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record server assembly boundary status"
```

---

## Task 6: Final branch validation and push

**Files:**
- No source changes expected.

- [ ] **Step 1: Inspect final branch state**

Run:

```bash
rtk git status --short --branch
rtk git log --oneline -10
```

Expected: branch `mdb-mqdev` is ahead of `origin/mdb-mqdev`, working tree clean.

- [ ] **Step 2: Push to remote**

Run:

```bash
git push origin mdb-mqdev
```

Expected: push succeeds.

- [ ] **Step 3: Confirm remote sync**

Run:

```bash
rtk git status --short --branch
```

Expected: `mdb-mqdev...origin/mdb-mqdev` with no ahead/behind marker and clean working tree.

---

## Self-Review Notes

- Spec coverage: The plan covers config, components, lifecycle app, dependency guards, docs, verification, and push.
- Placeholder scan: This plan contains no unresolved placeholder markers, no open-ended implementation gaps, and no deferred implementation steps inside B7 scope.
- Type consistency: Types used by tests are defined in the planned modules: `FdcServerApp`, `FdcServerConfig`, `ServerComponents`, `ServerEnvironment`, `ServerLifecycleState`, and `MarketDataOrchestratorResult`.
- Scope: B7 remains an assembly shell only and excludes `main.rs`, socket binding, `fdc-api` integration, live streams, checkpoint persistence, and real DB writes.
