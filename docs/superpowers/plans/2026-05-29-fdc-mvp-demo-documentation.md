# B18 MVP Demo Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add verifiable documentation for the first internally runnable MVP demo flow.

**Architecture:** B18 is documentation-first. It creates `docs/mvp/first-mvp-demo.md` and a small `fdc-api` documentation contract test that anchors the guide to real B16/B17 public APIs and verification commands. No runtime code changes.

**Tech Stack:** Markdown, Rust integration test, existing `fdc-api` test harness, offline Cargo verification.

---

## File Structure

- Create: `docs/mvp/first-mvp-demo.md`
  - Responsibility: human-facing first MVP demo guide, usage instructions, scope boundaries, expected outputs.
- Create: `crates/fdc-api/tests/demo_documentation_contract.rs`
  - Responsibility: verify the guide names real APIs, routes, commands, and no-listener/no-persistence scope.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record completed B18 work and next recommended slice.

---

## Task 1: Add failing documentation contract test

**Files:**
- Create: `crates/fdc-api/tests/demo_documentation_contract.rs`

- [ ] **Step 1: Write the failing documentation contract test**

Create `crates/fdc-api/tests/demo_documentation_contract.rs` with this content:

```rust
use std::{fs, path::PathBuf};

#[test]
fn first_mvp_demo_guide_mentions_real_demo_flow_api_and_routes() {
    let guide = read_guide();

    for required in [
        "run_demo_flow_once",
        "default_demo_flow_request",
        "DemoFlowSummary",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract",
        "GET /ready",
        "POST /runner/start-fixture",
        "GET /runner/status",
        "GET /market-data/trades",
        "no-listener",
        "in-memory",
        "no persistence",
        "SQL integration is out of scope",
        "live network acquisition is not part of the default MVP",
    ] {
        assert!(
            guide.contains(required),
            "MVP demo guide must mention `{required}`"
        );
    }
}

#[test]
fn first_mvp_demo_guide_has_no_placeholders() {
    let guide = read_guide();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !guide.contains(forbidden),
            "MVP demo guide must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_guide() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/first-mvp-demo.md"))
        .expect("docs/mvp/first-mvp-demo.md should exist and be readable")
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
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
```

Expected: FAIL because `docs/mvp/first-mvp-demo.md` does not exist yet.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-api/tests/demo_documentation_contract.rs
rtk git commit -m "test: add mvp demo documentation contract"
```

---

## Task 2: Write the MVP demo guide

**Files:**
- Create: `docs/mvp/first-mvp-demo.md`

- [ ] **Step 1: Create the guide directory and document**

Create `docs/mvp/first-mvp-demo.md` with this content:

````markdown
# First MVP Demo Guide

This guide explains the first internally verifiable Financial Data Center MVP. It is intentionally a no-listener, in-memory demo flow. It does not start a production service, bind a port, write persistent storage, or require live network access.

## What this MVP demonstrates

The first MVP demonstrates a bounded market-data path from deterministic fixture input to API-readable market-data records:

1. API readiness can be projected from an initialized server app.
2. A fixture trade can be submitted through the unified demo router with `POST /runner/start-fixture`.
3. Runner lifecycle can be read through `GET /runner/status`.
4. Written market-data records can be queried through `GET /market-data/trades`.
5. The whole flow can be run in memory through `run_demo_flow_once` without binding a socket.

## What this MVP does not demonstrate

The MVP deliberately excludes production concerns:

- no-listener: no HTTP listener is started by the default demo flow.
- in-memory only: the queryable market-data store is created for one demo run.
- no persistence: records are not written to disk or an external database.
- SQL integration is out of scope.
- Authentication and authorization are out of scope.
- Production middleware, CORS policy, daemon supervision, and lifecycle management are out of scope.
- live network acquisition is not part of the default MVP. Optional live smoke validation remains ignored and environment-gated.

## Quick verification

Run the core demo-flow contract:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
```

Expected result:

```text
cargo test: 5 passed
```

Useful adjacent checks:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

These verify the B16 unified router and the broader API/server boundary.

## Programmatic no-listener usage

Use `default_demo_flow_request` for the deterministic BTCUSDT fixture flow:

```rust
use fdc_api::{default_demo_flow_request, run_demo_flow_once};

#[tokio::main]
async fn main() -> Result<(), fdc_api::ApiError> {
    let summary = run_demo_flow_once(default_demo_flow_request()).await?;

    println!("readiness: {:?}", summary.readiness.status);
    println!("runner: {:?}", summary.final_status.state);
    println!("records: {}", summary.market_data.returned_records);

    Ok(())
}
```

`run_demo_flow_once` builds an initialized in-memory `ApiAppState`, creates the B16 unified router, sends in-memory Axum/Tower requests, and returns a typed `DemoFlowSummary`.

## Default fixture request shape

`default_demo_flow_request` is equivalent to this logical request:

```json
{
  "trades": [
    {
      "symbol": "BTCUSDT",
      "trade_id": "btc-demo-1",
      "sequence": "seq-demo-1"
    }
  ],
  "query_symbol": "BTCUSDT",
  "query_limit": 10
}
```

The request is deterministic so tests and documentation can rely on stable values.

## Expected DemoFlowSummary highlights

A successful `DemoFlowSummary` includes:

```text
readiness.status = ready
readiness.server_lifecycle_state = initialized
start_status.state = completed
final_status.state = completed
final_status.last_result.storage_records_written = 1
market_data.returned_records = 1
market_data.records[0].symbol = BTCUSDT
```

The full `DemoFlowSummary` type contains:

- `readiness`: typed readiness projection from `GET /ready`.
- `start_status`: runner status returned by `POST /runner/start-fixture`.
- `final_status`: runner status returned by `GET /runner/status` after fixture ingestion.
- `market_data`: typed trade query response returned by `GET /market-data/trades`.

## Route-to-summary mapping

| B16 route | B17 summary field | Purpose |
| --- | --- | --- |
| `GET /ready` | `DemoFlowSummary.readiness` | Confirms the demo server app is initialized and API-enabled. |
| `POST /runner/start-fixture` | `DemoFlowSummary.start_status` | Runs finite fixture trades through the bounded runner and writes market data. |
| `GET /runner/status` | `DemoFlowSummary.final_status` | Confirms final runner lifecycle and last-result counts. |
| `GET /market-data/trades` | `DemoFlowSummary.market_data` | Reads records written by the fixture flow from the shared in-memory store. |

## Optional live smoke validation

Optional Binance Spot live smoke validation exists in ignored tests from earlier slices. It is not part of the default MVP because it requires live network access and environment opt-in.

Use it only when validating external connectivity and Barter-rs integration manually. The default MVP remains deterministic and offline.

## Current MVP status

For an internal developer/reviewer, this is enough to verify the first MVP without starting a service:

1. Run the demo-flow contract command.
2. Read the deterministic fixture request.
3. Compare the expected `DemoFlowSummary` highlights.
4. Confirm explicit non-goals are acceptable for the first MVP.

If an interactive external demo is needed next, the next slice should add a gated local HTTP demo entrypoint that reuses `build_demo_router` instead of creating new route behavior.
````

- [ ] **Step 2: Run documentation contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
```

Expected: PASS, 2 tests pass.

- [ ] **Step 3: Run behavior regression tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
```

Expected: both commands PASS.

- [ ] **Step 4: Commit guide**

Run:

```bash
rtk git add docs/mvp/first-mvp-demo.md
rtk git commit -m "docs: add first mvp demo guide"
```

---

## Task 3: Verify and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run B18 verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

Expected: every command PASS.

- [ ] **Step 2: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, add a new completed-work section after B17:

```markdown
### Phase B18: MVP Demo Documentation

Implemented in `docs/mvp/first-mvp-demo.md`, with a documentation contract in `crates/fdc-api/tests/demo_documentation_contract.rs`.

Completed capabilities:

- Added a human-facing first MVP demo guide.
- Documented the no-listener, in-memory, deterministic MVP scope.
- Documented the exact demo-flow verification command.
- Documented programmatic usage of `default_demo_flow_request` and `run_demo_flow_once`.
- Documented default fixture request shape and expected `DemoFlowSummary` highlights.
- Mapped B16 routes to B17 summary fields.
- Documented optional live smoke validation as outside the default MVP.
- Added a documentation contract test to keep the guide anchored to real APIs, routes, and scope boundaries.

Contract tests:

- `crates/fdc-api/tests/demo_documentation_contract.rs`

Important docs:

- `docs/mvp/first-mvp-demo.md`
- `docs/superpowers/specs/2026-05-29-fdc-mvp-demo-documentation-design.md`
- `docs/superpowers/plans/2026-05-29-fdc-mvp-demo-documentation.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api`
```

Update latest checkpoint commit description to `docs: add first mvp demo guide` after that commit exists.

Update next recommended slice to `B19: MVP Acceptance Report or Gated HTTP Demo Entrypoint`.

- [ ] **Step 3: Commit status update and plan**

Run:

```bash
rtk git add docs/DEVELOPMENT_STATUS.md docs/superpowers/plans/2026-05-29-fdc-mvp-demo-documentation.md
rtk git commit -m "docs: record mvp demo documentation"
```

## Self-Review

- Spec coverage: all B18 design requirements map to Task 1 documentation contract, Task 2 guide, and Task 3 verification/status update.
- Placeholder scan: guide contract forbids TODO/TBD/FIXME and plan contains no placeholders.
- Type consistency: public API names match B17 exports: `default_demo_flow_request`, `run_demo_flow_once`, and `DemoFlowSummary`.
