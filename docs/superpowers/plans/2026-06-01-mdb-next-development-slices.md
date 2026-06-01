# MDB Next Development Slices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the post-market-data-expansion backlog into narrow, testable development slices that can be implemented without violating existing crate boundaries.

**Architecture:** Keep Barter-rs integration inside `fdc-barter`; keep cross-layer mapping in `fdc-orchestrator`; keep persistence and query execution behind storage/query crate boundaries; keep production runtime concerns in `fdc-server` and API projection concerns in `fdc-api`. Each slice starts with a design/spec and contract tests before runtime behavior changes.

**Tech Stack:** Rust, Cargo, Barter-rs local path crates, Axum/Tower tests, `fdc-barter`, `fdc-orchestrator`, `fdc-storage`, `fdc-query`, `fdc-server`, `fdc-api`, markdown contract docs.

---

## File Structure

### Existing files to update as slices are completed

- `docs/DEVELOPMENT_STATUS.md`
  - Append a completion checkpoint after each slice.
- `docs/superpowers/specs/YYYY-MM-DD-<slice>-design.md`
  - One design document per slice before implementation.
- `docs/superpowers/plans/YYYY-MM-DD-<slice>.md`
  - One implementation plan per slice.

### Candidate files by slice

- Historical REST backfill:
  - `crates/fdc-adapter/barter/src/ingestion/historical.rs`
  - `crates/fdc-adapter/barter/src/capability/exchange.rs`
  - `crates/fdc-adapter/barter/tests/historical_backfill_contract.rs`
- Persistence and tier-aware storage routing:
  - `crates/fdc-storage/src/persistence.rs`
  - `crates/fdc-storage/src/tiered.rs`
  - `crates/fdc-storage/tests/persistence_boundary_contract.rs`
- SQL/query integration:
  - `crates/fdc-query/src/planner.rs`
  - `crates/fdc-query/src/executor.rs`
  - `crates/fdc-query/tests/market_data_query_contract.rs`
- Production live supervisor evolution:
  - `crates/fdc-server/src/market_data/supervisor.rs`
  - `crates/fdc-server/src/market_data/service.rs`
  - `crates/fdc-server/tests/production_server_router_contract.rs`
  - `crates/fdc-server/tests/production_background_live_smoke.rs`
- Stabilization:
  - `.gitignore`
  - `Cargo.lock`
  - crate-specific warning cleanup files named by `cargo check` output
  - `docs/mvp/post-mvp-stabilization-baseline.md`

---

## Task 1: B20b Warning Cleanup by Crate

**Files:**
- Modify only files identified by `CARGO_NET_OFFLINE=true rtk cargo check -p <crate>` warning output.
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Capture current warning baseline**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api
```

Expected: command exits successfully. Record the warning count and affected crates before editing.

- [ ] **Step 2: Pick one crate and remove behavior-free warnings**

Start with one crate from the warning output, for example `fdc-storage` or `fdc-ingestion`. Remove only unused imports, unused variables, unnecessary `mut`, or dead private fields that do not alter public APIs.

- [ ] **Step 3: Verify the selected crate**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p <selected-crate>
CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api
```

Expected: selected crate tests pass and the workspace warning count decreases or remains explainable.

- [ ] **Step 4: Commit one crate cleanup**

Run:

```bash
rtk git add <changed-files> docs/DEVELOPMENT_STATUS.md
rtk git commit -m "chore: clean <selected-crate> warnings"
```

---

## Task 2: B20c Cargo.lock Policy Decision

**Files:**
- Modify: `.gitignore`
- Modify or add: `Cargo.lock`
- Modify: `docs/DEVELOPMENT_STATUS.md`
- Create: `docs/superpowers/specs/YYYY-MM-DD-cargo-lock-policy-design.md`
- Create: `docs/superpowers/plans/YYYY-MM-DD-cargo-lock-policy.md`

- [ ] **Step 1: Write the policy design**

Decide whether this repository should track `Cargo.lock`. The current recommendation is to track it because the workspace includes application binaries and production-like runners.

- [ ] **Step 2: Add a contract check for policy text**

Add or update a documentation contract test if the project has one for stabilization docs. The test must assert that the selected policy and rationale are documented.

- [ ] **Step 3: Apply the policy**

If tracking the lockfile, remove the `Cargo.lock` ignore rule from `.gitignore`, regenerate or update `Cargo.lock`, and include it in git. If not tracking it, document why reproducible application builds are handled another way.

- [ ] **Step 4: Verify**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api
```

Expected: tests/checks pass with the documented warning baseline.

- [ ] **Step 5: Commit**

Run:

```bash
rtk git add .gitignore Cargo.lock docs/DEVELOPMENT_STATUS.md docs/superpowers/specs docs/superpowers/plans
rtk git commit -m "docs: decide cargo lock policy"
```

---

## Task 3: B20d Dependency Boundary Audit Report

**Files:**
- Create: `docs/architecture/dependency-boundary-audit.md`
- Create: `crates/fdc-api/tests/dependency_boundary_report_contract.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add documentation contract test**

Create a test that reads `docs/architecture/dependency-boundary-audit.md` and asserts that it names the allowed dependency direction for `fdc-barter`, `fdc-ingestion`, `fdc-transform`, `fdc-storage`, `fdc-orchestrator`, `fdc-server`, and `fdc-api`.

- [ ] **Step 2: Write the audit report**

Document allowed edges and forbidden edges. Explicitly state that Barter-rs crates may only appear under `crates/fdc-adapter/barter`.

- [ ] **Step 3: Run dependency grep guard**

Run:

```bash
grep -R "barter_data\|barter-instrument\|barter_instrument" -n crates \
  | grep -v "crates/fdc-adapter/barter" \
  | grep -v "target" || true
```

Expected: no output.

- [ ] **Step 4: Verify and commit**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test dependency_boundary_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
rtk git add docs/architecture/dependency-boundary-audit.md crates/fdc-api/tests/dependency_boundary_report_contract.rs docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: add dependency boundary audit"
```

---

## Task 4: Historical REST Backfill Design Slice

**Files:**
- Create: `docs/superpowers/specs/YYYY-MM-DD-fdc-barter-historical-backfill-design.md`
- Create: `docs/superpowers/plans/YYYY-MM-DD-fdc-barter-historical-backfill.md`
- Later implementation candidate: `crates/fdc-adapter/barter/src/ingestion/historical.rs`

- [ ] **Step 1: Write design before code**

Define exchange scope, supported data kinds, checkpoint semantics, pagination, rate limits, and error semantics. Start with one exchange and one data kind unless the design justifies more.

- [ ] **Step 2: Plan contract tests**

Contract tests must prove request validation, checkpoint progression, rate-limit metadata, and replay/backfill quality flags without requiring network by default.

- [ ] **Step 3: Defer runtime code until design approval**

Do not add live REST calls or durable writes in this task. End with a reviewed design and implementation plan.

---

## Task 5: Persistence / SQL / Production Supervisor Roadmap Split

**Files:**
- Create one design per selected direction:
  - `docs/superpowers/specs/YYYY-MM-DD-fdc-storage-persistence-boundary-design.md`
  - `docs/superpowers/specs/YYYY-MM-DD-fdc-query-market-data-sql-design.md`
  - `docs/superpowers/specs/YYYY-MM-DD-fdc-production-live-supervisor-v2-design.md`

- [ ] **Step 1: Choose one next product capability**

Choose exactly one of these tracks for the next implementation cycle:

1. Durable persistence boundary.
2. SQL/query integration for market data.
3. Production live supervisor v2 with actor/event-loop semantics.

- [ ] **Step 2: Write the selected design**

Keep it narrow enough for one implementation plan. Do not mix persistence, SQL, and supervisor runtime changes into one implementation slice.

- [ ] **Step 3: Create the implementation plan**

Use `superpowers:writing-plans` and commit the plan before coding.

---

## Current Recommendation

Execute the next slices in this order:

1. B20b warning cleanup by crate.
2. B20c Cargo.lock policy decision.
3. B20d dependency boundary audit report.
4. Production live supervisor v2 design, because repeated bounded background cycles are now the most important runtime architecture decision.
5. Persistence boundary design.
6. SQL/query integration design.
7. Historical REST backfill design and implementation.

## Self-Review

- Spec coverage: this plan covers the requested follow-up directions without bundling independent subsystems into a single coding slice.
- Placeholder scan: no unfinished placeholder markers are present.
- Boundary check: Barter-rs remains isolated to `fdc-barter`; persistence, SQL, and server runtime work stay in their owning crates.
