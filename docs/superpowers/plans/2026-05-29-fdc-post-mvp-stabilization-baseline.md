# B20a Post-MVP Stabilization Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a verifiable post-MVP stabilization baseline report for warnings, Cargo.lock policy, and dependency boundaries.

**Architecture:** B20a is documentation and contract-test only. It creates `docs/mvp/post-mvp-stabilization-baseline.md` and a small `fdc-api` documentation contract that anchors the report to the current warning count, affected crates, Cargo.lock policy, and follow-up tracks. Runtime code and `.gitignore` remain unchanged.

**Tech Stack:** Markdown, Rust integration test, existing `fdc-api` test harness, offline Cargo verification.

---

## File Structure

- Create: `docs/mvp/post-mvp-stabilization-baseline.md`
  - Responsibility: record current warning baseline, Cargo.lock policy state, dependency-boundary expectations, and follow-up stabilization tracks.
- Create: `crates/fdc-api/tests/stabilization_baseline_contract.rs`
  - Responsibility: verify the report mentions required anchors and has no placeholders.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record B20a completion and next recommended narrow stabilization slice.

---

## Task 1: Add failing stabilization baseline contract test

**Files:**
- Create: `crates/fdc-api/tests/stabilization_baseline_contract.rs`

- [ ] **Step 1: Write the failing contract test**

Create `crates/fdc-api/tests/stabilization_baseline_contract.rs` with this content:

```rust
use std::{fs, path::PathBuf};

#[test]
fn post_mvp_stabilization_baseline_records_current_technical_debt() {
    let report = read_report();

    for required in [
        "post-MVP stabilization baseline",
        "CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api",
        "28 warnings",
        "fdc-wasm",
        "fdc-types",
        "fdc-storage",
        "fdc-query",
        "fdc-ingestion",
        "Cargo.lock",
        ".gitignore",
        "dependency boundary",
        "B20b warning cleanup by crate",
        "B20c Cargo.lock policy decision",
    ] {
        assert!(
            report.contains(required),
            "stabilization baseline must mention `{required}`"
        );
    }
}

#[test]
fn post_mvp_stabilization_baseline_has_no_placeholders() {
    let report = read_report();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !report.contains(forbidden),
            "stabilization baseline must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_report() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/post-mvp-stabilization-baseline.md"))
        .expect("docs/mvp/post-mvp-stabilization-baseline.md should exist and be readable")
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
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
```

Expected: FAIL because `docs/mvp/post-mvp-stabilization-baseline.md` does not exist yet.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-api/tests/stabilization_baseline_contract.rs
rtk git commit -m "test: add stabilization baseline contract"
```

---

## Task 2: Write the stabilization baseline report

**Files:**
- Create: `docs/mvp/post-mvp-stabilization-baseline.md`

- [ ] **Step 1: Create the report**

Create `docs/mvp/post-mvp-stabilization-baseline.md` with this content:

```markdown
# Post-MVP Stabilization Baseline

This document is the post-MVP stabilization baseline after acceptance of the first internal MVP. It records current technical debt and follow-up tracks without changing runtime behavior.

## Accepted MVP reference

The accepted MVP is documented here:

- Acceptance report: `docs/mvp/first-mvp-acceptance-report.md`
- Demo guide: `docs/mvp/first-mvp-demo.md`
- Development status: `docs/DEVELOPMENT_STATUS.md`

The accepted MVP remains a no-listener, in-memory, deterministic market-data demo.

## Warning baseline

Baseline command:

```bash
CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api
```

Baseline result at B20a start:

```text
0 errors, 28 warnings
```

The 28 warnings are in the `fdc-api` dependency graph, not necessarily in `fdc-api` itself.

Affected crates observed in the baseline:

- `fdc-wasm`
- `fdc-types`
- `fdc-storage`
- `fdc-query`
- `fdc-ingestion`

Warning classes observed:

- unused imports
- unused variables
- unnecessary `mut`
- dead fields in older manager/runtime structs

B20a records this baseline only. It does not perform broad warning cleanup because warnings cross multiple older crates and should be handled in narrow, reviewable slices.

## Cargo.lock policy baseline

`Cargo.lock` exists locally at the workspace root, but `.gitignore` currently ignores `Cargo.lock`.

B20a does not change this policy. The decision should be made in a dedicated follow-up because the repository currently has an application-like workspace with reproducibility benefits from tracking lockfiles, but it also has existing history and ignore behavior that should be changed deliberately.

Recommended follow-up: B20c Cargo.lock policy decision.

## Dependency boundary baseline

The post-MVP dependency boundary remains:

- lower-level crates must not depend on `fdc-api`.
- adapter, ingestion, transform, storage, orchestrator, server, and API boundaries should remain explicit.
- cross-layer market-data glue belongs in orchestration or API/demo assembly, not in lower-level core crates.

Existing MVP contract tests include dependency boundary checks for relevant API-facing slices. B20a does not add a broad workspace dependency graph tool yet.

Recommended follow-up: B20d dependency boundary audit report.

## Stabilization follow-up tracks

Recommended next stabilization slices:

1. B20b warning cleanup by crate: pick one crate at a time, remove unused imports/variables where behavior is unaffected, and run that crate's tests.
2. B20c Cargo.lock policy decision: decide whether to track `Cargo.lock`, update `.gitignore`, and document the rationale.
3. B20d dependency boundary audit report: centralize dependency-boundary checks and document allowed edges.

## Non-goals for B20a

B20a does not:

- edit runtime code.
- fix all 28 warnings.
- change `.gitignore`.
- start tracking or deleting `Cargo.lock`.
- add production persistence, SQL integration, auth, or HTTP demo startup.
- turn warnings into hard errors.

## Verification

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```
```

- [ ] **Step 2: Run stabilization contract and adjacent MVP tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
```

Expected: all commands PASS.

- [ ] **Step 3: Commit stabilization report**

Run:

```bash
rtk git add docs/mvp/post-mvp-stabilization-baseline.md
rtk git commit -m "docs: add post-mvp stabilization baseline"
```

---

## Task 3: Verify and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run B20a verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

Expected: every command PASS.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, add a new completed-work section after B19:

```markdown
### Phase B20a: Post-MVP Stabilization Baseline

Implemented in `docs/mvp/post-mvp-stabilization-baseline.md`, with a documentation contract in `crates/fdc-api/tests/stabilization_baseline_contract.rs`.

Completed capabilities:

- Recorded the post-MVP stabilization baseline.
- Documented `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api` warning baseline: 0 errors, 28 warnings.
- Listed affected crates: `fdc-wasm`, `fdc-types`, `fdc-storage`, `fdc-query`, and `fdc-ingestion`.
- Documented current `Cargo.lock` and `.gitignore` policy without changing it.
- Re-stated dependency boundary expectations after MVP acceptance.
- Defined follow-up stabilization tracks: B20b warning cleanup by crate, B20c Cargo.lock policy decision, and B20d dependency boundary audit report.
- Added a documentation contract test to keep the stabilization baseline anchored.

Contract tests:

- `crates/fdc-api/tests/stabilization_baseline_contract.rs`

Important docs:

- `docs/mvp/post-mvp-stabilization-baseline.md`
- `docs/superpowers/specs/2026-05-29-fdc-post-mvp-stabilization-baseline-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-post-mvp-stabilization-baseline.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api`
```

Update latest checkpoint commit description to `docs: add post-mvp stabilization baseline` after that commit exists.

Update next recommended slice to `B20b: Warning Cleanup by Crate`.

- [ ] **Step 3: Commit status update and plan**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-29-fdc-post-mvp-stabilization-baseline.md
rtk git commit -m "docs: record post-mvp stabilization baseline"
```

## Self-Review

- Spec coverage: all B20a requirements map to Task 1 contract, Task 2 report, and Task 3 status update.
- Placeholder scan: report contract forbids TODO/TBD/FIXME and plan contains no placeholders.
- Scope consistency: B20a does not change runtime code, `.gitignore`, or `Cargo.lock` tracking policy.
