# B11 Bounded Acquisition-to-API MVP Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fixture-only bounded MVP runner that writes Barter acquisition envelopes into a shared queryable store and proves the B10 API route can query the resulting trades.

**Architecture:** `fdc-server` owns the new assembly-level runner because it can depend on `fdc-orchestrator` and `fdc-storage` without reversing lower-level dependency direction. The runner reuses `fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once` and accepts an injected `QueryableMarketDataStore`, then tests build `fdc-api` state around the same store and query the in-memory Axum route.

**Tech Stack:** Rust 2021, Tokio tests, Axum in-memory router, `fdc-server`, `fdc-orchestrator`, `fdc-storage`, `fdc-api`, Barter adapter fixture types.

---

## File Structure

- Create `crates/fdc-server/src/mvp.rs`
  - Owns `BoundedMarketDataMvpRunner`, `BoundedMarketDataMvpResult`, and `run_barter_fixture_mvp_once`.
  - Keeps the B11 helper small and separate from lifecycle code in `app.rs`.
- Modify `crates/fdc-server/src/lib.rs`
  - Exports the new `mvp` module and public B11 runner types.
- Modify `crates/fdc-server/Cargo.toml`
  - Adds `fdc-barter` dependency so the server assembly boundary can accept Barter envelopes for this MVP slice.
- Create `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`
  - End-to-end offline contract: fixture envelopes -> B11 runner -> shared `QueryableMarketDataStore` -> B10 route.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Records B11 completion, verification evidence, and next recommended slice.

## Task 1: Add failing B11 acquisition-to-API contract test

**Files:**
- Create: `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`
- Modify: `crates/fdc-server/Cargo.toml`

- [ ] **Step 1: Add dev dependencies needed by the API integration test**

Modify `crates/fdc-api/Cargo.toml` `[dev-dependencies]` to include:

```toml
fdc-barter = { path = "../fdc-adapter/barter" }
fdc-core = { path = "../fdc-core" }
fdc-server = { path = "../fdc-server" }
rust_decimal = { workspace = true }
```

Do not remove the existing `tempfile` or `criterion` entries.

- [ ] **Step 2: Write the failing end-to-end test**

Create `crates/fdc-api/tests/acquisition_api_mvp_contract.rs` with:

```rust
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{build_market_data_router, ApiAppState};
use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{run_barter_fixture_mvp_once, BoundedMarketDataMvpRunner, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use rust_decimal::Decimal;
use tower::ServiceExt;

fn sample_trade_event(symbol: &str, trade_id: &str, sequence: &str) -> BarterMarketEvent {
    BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_001),
        received_at: TimestampNs::from_nanos(1_700_000_000_000_000_010),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: DecimalQuantity::new(125, 3),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(sequence.to_string()),
        checkpoint: None,
    }
}

fn sample_envelope(symbol: &str, trade_id: &str, sequence: &str) -> BarterIngestionEnvelope {
    let mut envelope = BarterIngestionEnvelope::from_event(
        "barter:binance_spot",
        sample_trade_event(symbol, trade_id, sequence),
    );
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags {
        is_replay: false,
        is_backfill: false,
        is_duplicate_candidate: false,
        has_gap_before: false,
        is_out_of_order: false,
    };
    envelope
}

#[tokio::test]
async fn bounded_mvp_runner_populates_store_read_by_api_route() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = BoundedMarketDataMvpRunner::new(Arc::clone(&store));

    let result = runner
        .run_barter_fixtures_once(vec![
            sample_envelope("BTCUSDT", "btc-1", "seq-1"),
            sample_envelope("ETHUSDT", "eth-1", "seq-2"),
            sample_envelope("BTCUSDT", "btc-2", "seq-3"),
        ])
        .await
        .expect("bounded fixture runner should write to the queryable store");

    assert_eq!(result.envelopes_received, 3);
    assert_eq!(result.storage_records_written, 3);
    assert_eq!(result.market_data_store_records, 3);

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let router = build_market_data_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
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

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"], "btc-1");
    assert_eq!(json["data"]["records"][1]["payload"]["payload"]["Trade"]["trade_id"], "btc-2");
}

#[tokio::test]
async fn convenience_helper_uses_the_injected_store() {
    let store = Arc::new(QueryableMarketDataStore::new());

    let result = run_barter_fixture_mvp_once(
        vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")],
        Arc::clone(&store),
    )
    .await
    .expect("convenience helper should write fixture data");

    assert_eq!(result.envelopes_received, 1);
    assert_eq!(result.source_valid, 1);
    assert_eq!(result.storage_records_written, 1);
    assert_eq!(result.market_data_store_records, 1);

    let state = ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store);
    let response = fdc_api::query_market_data_trades(
        &state,
        fdc_api::MarketDataTradeQueryParams {
            symbol: Some("BTCUSDT".to_string()),
            limit: Some(10),
        },
    );

    assert_eq!(response.data.returned_records, 1);
    assert_eq!(response.data.records[0].symbol.as_deref(), Some("BTCUSDT"));
}
```

- [ ] **Step 3: Run the failing test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Expected: FAIL because `fdc_server::BoundedMarketDataMvpRunner` and `fdc_server::run_barter_fixture_mvp_once` do not exist yet.

## Task 2: Implement the B11 bounded MVP runner in fdc-server

**Files:**
- Create: `crates/fdc-server/src/mvp.rs`
- Modify: `crates/fdc-server/src/lib.rs`
- Modify: `crates/fdc-server/Cargo.toml`

- [ ] **Step 1: Add the Barter adapter dependency to fdc-server**

Modify `crates/fdc-server/Cargo.toml` `[dependencies]` to include:

```toml
fdc-barter = { path = "../fdc-adapter/barter" }
```

Keep the existing `fdc-core`, `fdc-storage`, and `fdc-orchestrator` dependencies.

- [ ] **Step 2: Implement the runner module**

Create `crates/fdc-server/src/mvp.rs` with:

```rust
use std::sync::Arc;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::Result;
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BoundedMarketDataMvpResult {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
}

#[derive(Clone)]
pub struct BoundedMarketDataMvpRunner {
    market_data_store: Arc<QueryableMarketDataStore>,
}

impl BoundedMarketDataMvpRunner {
    pub fn new(market_data_store: Arc<QueryableMarketDataStore>) -> Self {
        Self { market_data_store }
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }

    pub async fn run_barter_fixtures_once(
        &self,
        envelopes: Vec<BarterIngestionEnvelope>,
    ) -> Result<BoundedMarketDataMvpResult> {
        let pipeline_result =
            run_barter_envelopes_to_storage_once(envelopes, self.market_data_store.as_ref())
                .await?;
        let market_data_store_records = self
            .market_data_store
            .query(&MarketDataQuery::for_trades())
            .len();

        Ok(BoundedMarketDataMvpResult {
            envelopes_received: pipeline_result.envelopes_received,
            source_valid: pipeline_result.source_valid,
            source_invalid: pipeline_result.source_invalid,
            dto_mapped: pipeline_result.dto_mapped,
            storage_records_written: pipeline_result.storage_records_written,
            market_data_store_records,
        })
    }
}

pub async fn run_barter_fixture_mvp_once(
    envelopes: Vec<BarterIngestionEnvelope>,
    market_data_store: Arc<QueryableMarketDataStore>,
) -> Result<BoundedMarketDataMvpResult> {
    BoundedMarketDataMvpRunner::new(market_data_store)
        .run_barter_fixtures_once(envelopes)
        .await
}
```

- [ ] **Step 3: Export the runner API**

Modify `crates/fdc-server/src/lib.rs` to include the module and exports:

```rust
pub mod app;
pub mod components;
pub mod config;
pub mod mvp;

pub use app::{FdcServerApp, ServerLifecycleState};
pub use components::{MarketDataOrchestratorResult, ServerComponents};
pub use config::{FdcServerConfig, ServerEnvironment};
pub use mvp::{
    run_barter_fixture_mvp_once, BoundedMarketDataMvpResult, BoundedMarketDataMvpRunner,
};
```

- [ ] **Step 4: Run the B11 contract test**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Expected: PASS with 2 tests passing.

- [ ] **Step 5: Commit runner implementation**

Run:

```bash
git add crates/fdc-server/Cargo.toml crates/fdc-server/src/lib.rs crates/fdc-server/src/mvp.rs crates/fdc-api/Cargo.toml crates/fdc-api/tests/acquisition_api_mvp_contract.rs
git commit -m "feat: add bounded acquisition api mvp runner"
```

## Task 3: Verify integration boundary and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run formatting checks**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --package fdc-api --check
```

Expected: PASS.

If formatting fails, run:

```bash
rtk cargo fmt --package fdc-server --package fdc-api
```

Then rerun the `--check` command.

- [ ] **Step 2: Run focused B11 and B10 tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
```

Expected: both commands PASS.

- [ ] **Step 3: Run package integration tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-orchestrator -p fdc-storage
```

Expected: PASS. Existing ignored tests may remain ignored.

- [ ] **Step 4: Update status documentation**

In `docs/DEVELOPMENT_STATUS.md`, append a new completed section after Phase B10:

```markdown
### Phase B11: Bounded Acquisition-to-API MVP Runner

Implemented in `crates/fdc-server`, with end-to-end API contract coverage in `crates/fdc-api`.

Completed capabilities:

- Added `BoundedMarketDataMvpRunner` and `run_barter_fixture_mvp_once` as a fixture-only assembly helper.
- Reused `fdc-orchestrator::run_barter_envelopes_to_storage_once` for Barter envelope mapping and storage writes.
- Populated a shared `QueryableMarketDataStore` and queried it through the B10 in-memory market-data API route.
- Kept production lifecycle, persistence, SQL integration, and ungated network tests out of scope.

Contract tests:

- `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-bounded-acquisition-api-mvp-runner-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-bounded-acquisition-api-mvp-runner.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --package fdc-api --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract` exit 0, 2 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-orchestrator -p fdc-storage` exit 0.
```

Then update `## Next Recommended Development Slice` to recommend an ignored, network-gated live acquisition smoke bridge or application runner lifecycle slice, keeping persistence and SQL separate.

- [ ] **Step 5: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record bounded acquisition api mvp status"
```

- [ ] **Step 6: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.

## Self-Review

- Spec coverage: Tasks implement fixture-only runner, shared queryable store, API route query, offline contract, and docs update. Non-goals are preserved because no lifecycle, persistence, SQL, or default network tests are added.
- Placeholder scan: no unfinished placeholder markers are present.
- Type consistency: public names are consistent across test, implementation, exports, and status docs.
