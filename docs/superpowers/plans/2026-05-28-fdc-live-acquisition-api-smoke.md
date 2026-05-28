# B12 Ignored Live Acquisition API Smoke Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an ignored, network-gated live smoke contract proving one Binance Spot trade can flow through `fdc-barter` live acquisition, the B11 MVP store helper, and the B10 API route.

**Architecture:** Keep production code unchanged. Add one opt-in integration test in `fdc-api` because that test crate can legally compose `fdc-barter`, `fdc-server`, `fdc-storage`, and the API router while lower-level crates remain decoupled.

**Tech Stack:** Rust 2021, Tokio multi-thread ignored test, Axum in-memory router, Barter-rs live Binance Spot streams, `fdc-barter`, `fdc-server`, `fdc-storage`, `fdc-api`.

---

## File Structure

- Modify `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`
  - Add imports for `fdc-barter` live helpers and `futures::StreamExt`.
  - Add one ignored live smoke test: `ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api`.
- Modify `crates/fdc-api/Cargo.toml`
  - Add dev dependency `futures = { workspace = true }` if needed by the test import.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record B12 completion, verification evidence, and next recommended slice.

## Task 1: Add the ignored live smoke contract test

**Files:**
- Modify: `crates/fdc-api/Cargo.toml`
- Modify: `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

- [ ] **Step 1: Add the futures dev dependency**

Modify `crates/fdc-api/Cargo.toml` `[dev-dependencies]` to include:

```toml
futures = { workspace = true }
```

Keep existing dev dependencies:

```toml
tempfile = "3.8"
criterion = "0.5"
fdc-barter = { path = "../fdc-adapter/barter" }
fdc-core = { path = "../fdc-core" }
fdc-server = { path = "../fdc-server" }
rust_decimal = { workspace = true }
```

- [ ] **Step 2: Extend imports in the contract test**

In `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`, replace the existing `fdc_barter` import block:

```rust
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
```

with:

```rust
use fdc_barter::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, public_trade_result_to_data_kind, BarterIngestionEnvelope,
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
```

Then add this import after the `fdc_storage` import:

```rust
use futures::StreamExt;
```

- [ ] **Step 3: Add the ignored live smoke test**

Append this test to `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`:

```rust
#[ignore = "requires public internet and FDC_BARTER_LIVE_SMOKE=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api() {
    if std::env::var("FDC_BARTER_LIVE_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping live smoke test because FDC_BARTER_LIVE_SMOKE=1 is not set");
        return;
    }

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .expect("live Binance Spot stream should initialize");
    let stream = streams.select_all().map(public_trade_result_to_data_kind);
    let envelopes = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        collect_live_trade_envelopes("barter-binance-spot-live-trades", stream, 1),
    )
    .await
    .expect("should receive one live trade within timeout")
    .expect("live collection should succeed");

    assert!(!envelopes.is_empty(), "expected at least one live envelope");

    let store = Arc::new(QueryableMarketDataStore::new());
    let result = run_barter_fixture_mvp_once(envelopes, Arc::clone(&store))
        .await
        .expect("live envelopes should write through the bounded MVP runner");

    assert!(
        result.storage_records_written >= 1,
        "expected the MVP runner to write at least one live trade record"
    );

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let router = build_market_data_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be JSON");

    eprintln!("live acquisition API smoke response: {json:#}");

    let returned_records = json["data"]["returned_records"]
        .as_u64()
        .expect("returned_records should be numeric");
    assert_eq!(json["status"], "success");
    assert!(returned_records >= 1, "expected at least one API trade record");
    assert_eq!(json["data"]["records"][0]["kind"], "trade");
    assert!(
        json["data"]["records"][0]["payload"]["symbol"].is_string(),
        "live trade payload should expose a symbol"
    );
}
```

- [ ] **Step 4: Run the default offline contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Expected: PASS. The ignored live smoke test should compile but not run. The output should show the existing two tests passed and one ignored.

- [ ] **Step 5: Optional explicit live smoke command**

Only run this command when public internet is available and live validation is desired:

```bash
FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture
```

Expected when network is available: PASS and a printed live API response.

- [ ] **Step 6: Commit the test**

Run:

```bash
git add crates/fdc-api/Cargo.toml crates/fdc-api/tests/acquisition_api_mvp_contract.rs
git commit -m "test: add live acquisition api smoke"
```

## Task 2: Verify and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run formatting checks**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check
```

Expected: PASS.

If formatting fails, run:

```bash
rtk cargo fmt --package fdc-api
```

Then rerun the `--check` command.

- [ ] **Step 2: Run focused tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
```

Expected: both commands PASS. The B12 live smoke remains ignored in the default command.

- [ ] **Step 3: Run package integration tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server -p fdc-barter
```

Expected: PASS. Existing ignored live tests remain ignored.

- [ ] **Step 4: Update status documentation**

In `docs/DEVELOPMENT_STATUS.md`, append a new completed section after Phase B11:

```markdown
### Phase B12: Ignored Live Acquisition Smoke to MVP Store

Implemented as an ignored, network-gated contract test in `crates/fdc-api`.

Completed capabilities:

- Added an opt-in live smoke test that initializes Binance Spot public trade streams via `fdc-barter`.
- Collects one live trade envelope with a bounded timeout.
- Writes the live envelope through the B11 `run_barter_fixture_mvp_once` helper into a shared `QueryableMarketDataStore`.
- Queries the shared store through the B10 in-memory API route and asserts a returned trade record.
- Keeps default test runs offline because the live smoke is `#[ignore]` and gated by `FDC_BARTER_LIVE_SMOKE=1`.

Contract tests:

- `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-live-acquisition-api-smoke-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-live-acquisition-api-smoke.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract` exit 0, 2 passed and 1 ignored.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server -p fdc-barter` exit 0.
- Optional live command: `FDC_BARTER_LIVE_SMOKE=1 cargo test -p fdc-api --test acquisition_api_mvp_contract ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api -- --ignored --nocapture`.
```

Then update `## Next Recommended Development Slice` to recommend `Phase B13: Bounded Application Runner Lifecycle`, scoped to deterministic lifecycle/cancellation/readiness without production persistence or SQL.

- [ ] **Step 5: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record live acquisition api smoke status"
```

- [ ] **Step 6: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.

## Self-Review

- Spec coverage: Task 1 implements the ignored live smoke path and keeps it environment-gated. Task 2 verifies default offline behavior and records next work.
- Placeholder scan: no unfinished markers are present.
- Type consistency: the test name, helper names, environment variable, and commands match the design spec.
