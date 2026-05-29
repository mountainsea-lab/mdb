# B19 MVP Acceptance Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a verifiable acceptance report that declares the first internal MVP accepted and freezes its scope.

**Architecture:** B19 is documentation-only. It creates `docs/mvp/first-mvp-acceptance-report.md` and a documentation contract test in `fdc-api` that anchors the report to real MVP APIs, verification commands, and non-goals. Runtime code remains unchanged.

**Tech Stack:** Markdown, Rust integration test, existing `fdc-api` test harness, offline Cargo verification.

---

## File Structure

- Create: `docs/mvp/first-mvp-acceptance-report.md`
  - Responsibility: MVP acceptance decision, scope, verification evidence, non-goals, caveats, next steps.
- Create: `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`
  - Responsibility: verify the report mentions required artifacts, APIs, commands, scope boundaries, and has no placeholders.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record B19 completion and next recommended development track.

---

## Task 1: Add failing MVP acceptance report contract test

**Files:**
- Create: `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`

- [ ] **Step 1: Write the failing report contract test**

Create `crates/fdc-api/tests/mvp_acceptance_report_contract.rs` with this content:

```rust
use std::{fs, path::PathBuf};

#[test]
fn first_mvp_acceptance_report_freezes_scope_and_verification() {
    let report = read_report();

    for required in [
        "First internal MVP: accepted",
        "docs/mvp/first-mvp-demo.md",
        "run_demo_flow_once",
        "build_demo_router",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract",
        "no-listener",
        "in-memory",
        "no persistence",
        "SQL integration is out of scope",
        "live network acquisition is not part of default verification",
        "gated local HTTP demo entrypoint",
    ] {
        assert!(
            report.contains(required),
            "MVP acceptance report must mention `{required}`"
        );
    }
}

#[test]
fn first_mvp_acceptance_report_has_no_placeholders() {
    let report = read_report();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !report.contains(forbidden),
            "MVP acceptance report must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_report() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/first-mvp-acceptance-report.md"))
        .expect("docs/mvp/first-mvp-acceptance-report.md should exist and be readable")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
```

Expected: FAIL because `docs/mvp/first-mvp-acceptance-report.md` does not exist yet.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-api/tests/mvp_acceptance_report_contract.rs
rtk git commit -m "test: add mvp acceptance report contract"
```

---

## Task 2: Write the MVP acceptance report

**Files:**
- Create: `docs/mvp/first-mvp-acceptance-report.md`

- [ ] **Step 1: Create the acceptance report**

Create `docs/mvp/first-mvp-acceptance-report.md` with this content:

```markdown
# First MVP Acceptance Report

## Acceptance decision

First internal MVP: accepted.

This acceptance covers an internal, no-listener, in-memory market-data demo. It proves the bounded path from deterministic fixture input to API-readable market-data records through the current crate boundaries.

This is not an external production release and not an interactive HTTP demo.

## Accepted user journey

A developer or reviewer can verify the first MVP by reading `docs/mvp/first-mvp-demo.md` and running the no-listener demo flow tests.

The accepted journey is:

1. Build an initialized in-memory API state.
2. Build the B16 unified demo router with `build_demo_router`.
3. Run `POST /runner/start-fixture` with deterministic fixture trades.
4. Read final runner status through `GET /runner/status`.
5. Query written records through `GET /market-data/trades`.
6. Exercise the whole flow programmatically with `run_demo_flow_once`.

## Included capabilities

The accepted MVP includes these capabilities:

- Barter-owned market-data event and ingestion-envelope models.
- Generic source envelope, validation, batch, and finite pipeline helpers.
- Neutral market-data DTO and storage write boundaries.
- Orchestration glue from Barter fixture envelopes to queryable storage records.
- Server assembly boundary with deterministic lifecycle state.
- API app state, readiness projection, market-data query route, runner status route, and runner control route.
- Unified in-memory demo router via `build_demo_router`.
- No-listener demo flow helper via `run_demo_flow_once`.
- Human-facing demo guide at `docs/mvp/first-mvp-demo.md`.

## Verification evidence

Run these commands from the repository root:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

Expected results at acceptance time:

```text
demo_documentation_contract: 2 passed
demo_flow_contract: 5 passed
demo_router_contract: 4 passed
fdc-api: 54 passed, 1 ignored
```

The ignored test is not part of default MVP verification.

## Explicit non-goals frozen for this MVP

The first internal MVP deliberately excludes these capabilities:

- no-listener: default verification does not bind an HTTP port.
- in-memory only: storage is created per demo flow.
- no persistence: no durable database, file sink, or recovery contract is included.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and long-running lifecycle management are out of scope.
- live network acquisition is not part of default verification. Live smoke remains optional, ignored, and environment-gated.
- Performance claims are out of scope. Current verification proves behavior and boundaries, not latency or throughput.

## Key artifacts

- Demo guide: `docs/mvp/first-mvp-demo.md`
- Acceptance report: `docs/mvp/first-mvp-acceptance-report.md`
- Demo router: `fdc_api::build_demo_router`
- Demo flow helper: `fdc_api::run_demo_flow_once`
- Default fixture helper: `fdc_api::default_demo_flow_request`
- Development status: `docs/DEVELOPMENT_STATUS.md`

## Residual risks and caveats

- The MVP uses deterministic fixture data by default, so it does not prove live exchange reliability.
- The queryable store is in-memory and per-run, so it does not prove persistence or recovery.
- API tests use in-memory Axum/Tower requests, so they do not prove socket binding, deployment, TLS, or middleware configuration.
- Existing warning cleanup remains a separate stabilization task.
- The workspace currently has a `Cargo.lock` policy caveat documented in `docs/DEVELOPMENT_STATUS.md`.

## Recommended next slices

Choose the next track based on audience:

1. For external interactive demos: add a gated local HTTP demo entrypoint that reuses `build_demo_router` without changing route behavior.
2. For stabilization: clean warnings, decide Cargo.lock policy, audit dependency boundaries, and polish docs.
3. For product capability expansion: design persistence, SQL query integration, or production runner lifecycle as separate milestones.
```

- [ ] **Step 2: Run report contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
```

Expected: PASS, 2 tests pass.

- [ ] **Step 3: Run adjacent documentation/demo tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Expected: all commands PASS.

- [ ] **Step 4: Commit acceptance report**

Run:

```bash
rtk git add docs/mvp/first-mvp-acceptance-report.md
rtk git commit -m "docs: add first mvp acceptance report"
```

---

## Task 3: Verify and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run B19 verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

Expected: every command PASS.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, add a new completed-work section after B18:

```markdown
### Phase B19: MVP Acceptance Report

Implemented in `docs/mvp/first-mvp-acceptance-report.md`, with a documentation contract in `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`.

Completed capabilities:

- Declared the first internal MVP accepted.
- Froze the accepted MVP as a no-listener, in-memory market-data demo.
- Summarized included capabilities from Barter fixture models through API-readable market-data records.
- Listed exact verification commands and expected pass counts.
- Linked the first MVP demo guide.
- Froze non-goals: persistence, SQL integration, auth, production daemon supervision, default live network acquisition, and performance claims.
- Listed residual risks and recommended post-MVP tracks.
- Added a documentation contract test to keep the acceptance report anchored to real APIs, commands, and scope boundaries.

Contract tests:

- `crates/fdc-api/tests/mvp_acceptance_report_contract.rs`

Important docs:

- `docs/mvp/first-mvp-acceptance-report.md`
- `docs/mvp/first-mvp-demo.md`
- `docs/superpowers/specs/2026-05-29-fdc-mvp-acceptance-report-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-mvp-acceptance-report.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api`
```

Update latest checkpoint commit description to `docs: add first mvp acceptance report` after that commit exists.

Update next recommended slice to choose between:

- `B20: Gated Local HTTP Demo Entrypoint`
- `B20: Stabilization Cleanup`
- `B20: Persistence Boundary`

- [ ] **Step 3: Commit status update and plan**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-29-fdc-mvp-acceptance-report.md
rtk git commit -m "docs: record mvp acceptance report"
```

## Self-Review

- Spec coverage: all B19 design requirements map to Task 1 report contract, Task 2 report, and Task 3 status update.
- Placeholder scan: report contract forbids TODO/TBD/FIXME and plan contains no placeholders.
- Type consistency: required API names match current public exports: `build_demo_router` and `run_demo_flow_once`.
