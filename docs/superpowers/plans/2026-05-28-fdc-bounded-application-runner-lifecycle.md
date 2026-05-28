# B13 Bounded Application Runner Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deterministic, fixture-only bounded runner lifecycle in `fdc-server` for finite market-data MVP demo runs.

**Architecture:** Add a focused `runner.rs` module in `fdc-server` that wraps the existing B11 MVP helper with lifecycle state tracking. Keep all tests offline and avoid API route, persistence, SQL, and production background task changes.

**Tech Stack:** Rust 2021, Tokio tests, `fdc-server`, `fdc-barter` fixture envelopes, `fdc-storage::QueryableMarketDataStore`, B11 `run_barter_fixture_mvp_once`.

---

## File Structure

- Create `crates/fdc-server/src/runner.rs`
  - Defines `BoundedRunnerState`, `BoundedRunnerFailure`, and `BoundedMarketDataRunnerHandle`.
  - Keeps lifecycle logic separate from `FdcServerApp` and the B11 MVP mapping helper.
- Modify `crates/fdc-server/src/lib.rs`
  - Exports the new runner module and public runner types.
- Create `crates/fdc-server/tests/bounded_runner_contract.rs`
  - Offline contract tests for created, completed, cancelled, and rerun-denied lifecycle behavior.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Records B13 completion, verification evidence, and next recommended slice.

## Task 1: Add failing bounded runner lifecycle contract tests

**Files:**
- Create: `crates/fdc-server/tests/bounded_runner_contract.rs`

- [ ] **Step 1: Write the failing contract tests**

Create `crates/fdc-server/tests/bounded_runner_contract.rs` with:

```rust
use std::sync::Arc;

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{BoundedMarketDataRunnerHandle, BoundedRunnerState};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use rust_decimal::Decimal;

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

#[test]
fn new_runner_starts_created_without_result_or_failure() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    assert_eq!(runner.state(), BoundedRunnerState::Created);
    assert_eq!(runner.last_result(), None);
    assert_eq!(runner.failure(), None);
    assert!(Arc::ptr_eq(&runner.market_data_store(), &store));
}

#[tokio::test]
async fn start_once_completes_and_records_queryable_market_data() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    let result = runner
        .start_once(vec![
            sample_envelope("BTCUSDT", "btc-1", "seq-1"),
            sample_envelope("ETHUSDT", "eth-1", "seq-2"),
        ])
        .await
        .expect("finite runner should complete");

    assert_eq!(runner.state(), BoundedRunnerState::Completed);
    assert_eq!(result.envelopes_received, 2);
    assert_eq!(result.storage_records_written, 2);
    assert_eq!(runner.last_result(), Some(&result));
    assert_eq!(runner.failure(), None);

    let btc_records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));
    assert_eq!(btc_records.len(), 1);
}

#[tokio::test]
async fn cancel_before_start_prevents_later_run_without_writes() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(Arc::clone(&store));

    runner.cancel().expect("created runner should cancel");

    assert_eq!(runner.state(), BoundedRunnerState::Cancelled);

    let error = runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect_err("cancelled runner must not start");

    assert!(error.to_string().contains("cancelled"));
    assert_eq!(runner.state(), BoundedRunnerState::Cancelled);
    assert_eq!(store.query(&MarketDataQuery::for_trades()).len(), 0);
}

#[tokio::test]
async fn completed_runner_rejects_second_run_and_preserves_first_result() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let mut runner = BoundedMarketDataRunnerHandle::new(store);

    let first = runner
        .start_once(vec![sample_envelope("BTCUSDT", "btc-1", "seq-1")])
        .await
        .expect("first run should complete");

    let error = runner
        .start_once(vec![sample_envelope("ETHUSDT", "eth-1", "seq-2")])
        .await
        .expect_err("completed runner must not rerun");

    assert!(error.to_string().contains("completed"));
    assert_eq!(runner.state(), BoundedRunnerState::Completed);
    assert_eq!(runner.last_result(), Some(&first));
}
```

- [ ] **Step 2: Run tests to verify RED failure**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract
```

Expected: FAIL because `BoundedMarketDataRunnerHandle` and `BoundedRunnerState` are not exported yet.

## Task 2: Implement bounded runner lifecycle

**Files:**
- Create: `crates/fdc-server/src/runner.rs`
- Modify: `crates/fdc-server/src/lib.rs`

- [ ] **Step 1: Implement the runner module**

Create `crates/fdc-server/src/runner.rs` with:

```rust
use std::sync::Arc;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::{error::Error, Result};
use fdc_storage::QueryableMarketDataStore;

use crate::{run_barter_fixture_mvp_once, BoundedMarketDataMvpResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedRunnerState {
    Created,
    Running,
    Completed,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedRunnerFailure {
    pub message: String,
}

#[derive(Clone)]
pub struct BoundedMarketDataRunnerHandle {
    market_data_store: Arc<QueryableMarketDataStore>,
    state: BoundedRunnerState,
    last_result: Option<BoundedMarketDataMvpResult>,
    failure: Option<BoundedRunnerFailure>,
}

impl BoundedMarketDataRunnerHandle {
    pub fn new(market_data_store: Arc<QueryableMarketDataStore>) -> Self {
        Self {
            market_data_store,
            state: BoundedRunnerState::Created,
            last_result: None,
            failure: None,
        }
    }

    pub fn state(&self) -> BoundedRunnerState {
        self.state
    }

    pub fn last_result(&self) -> Option<&BoundedMarketDataMvpResult> {
        self.last_result.as_ref()
    }

    pub fn failure(&self) -> Option<&BoundedRunnerFailure> {
        self.failure.as_ref()
    }

    pub fn market_data_store(&self) -> Arc<QueryableMarketDataStore> {
        Arc::clone(&self.market_data_store)
    }

    pub fn cancel(&mut self) -> Result<()> {
        match self.state {
            BoundedRunnerState::Created | BoundedRunnerState::Cancelled => {
                self.state = BoundedRunnerState::Cancelled;
                Ok(())
            }
            BoundedRunnerState::Running => Err(Error::validation(
                "bounded market-data runner cannot cancel while running",
            )),
            BoundedRunnerState::Completed => Err(Error::validation(
                "bounded market-data runner cannot cancel after completed",
            )),
            BoundedRunnerState::Failed => Err(Error::validation(
                "bounded market-data runner cannot cancel after failed",
            )),
        }
    }

    pub async fn start_once(
        &mut self,
        envelopes: Vec<BarterIngestionEnvelope>,
    ) -> Result<BoundedMarketDataMvpResult> {
        match self.state {
            BoundedRunnerState::Created => {}
            BoundedRunnerState::Running => {
                return Err(Error::validation(
                    "bounded market-data runner is already running",
                ));
            }
            BoundedRunnerState::Completed => {
                return Err(Error::validation(
                    "bounded market-data runner already completed",
                ));
            }
            BoundedRunnerState::Cancelled => {
                return Err(Error::validation(
                    "bounded market-data runner was cancelled",
                ));
            }
            BoundedRunnerState::Failed => {
                return Err(Error::validation("bounded market-data runner already failed"));
            }
        }

        self.state = BoundedRunnerState::Running;
        self.failure = None;

        match run_barter_fixture_mvp_once(envelopes, Arc::clone(&self.market_data_store)).await {
            Ok(result) => {
                self.state = BoundedRunnerState::Completed;
                self.last_result = Some(result.clone());
                Ok(result)
            }
            Err(error) => {
                self.state = BoundedRunnerState::Failed;
                self.failure = Some(BoundedRunnerFailure {
                    message: error.to_string(),
                });
                Err(error)
            }
        }
    }
}
```

- [ ] **Step 2: Export runner types**

Modify `crates/fdc-server/src/lib.rs` to include:

```rust
pub mod runner;
```

and add exports:

```rust
pub use runner::{BoundedMarketDataRunnerHandle, BoundedRunnerFailure, BoundedRunnerState};
```

The top-level module/export block should include both `mvp` and `runner`.

- [ ] **Step 3: Run the contract tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract
```

Expected: PASS with 4 tests passing.

- [ ] **Step 4: Commit runner implementation**

Run:

```bash
git add crates/fdc-server/src/lib.rs crates/fdc-server/src/runner.rs crates/fdc-server/tests/bounded_runner_contract.rs
git commit -m "feat: add bounded market data runner lifecycle"
```

## Task 3: Verify integration and update status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run formatting checks**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check
```

Expected: PASS.

If formatting fails, run:

```bash
rtk cargo fmt --package fdc-server
```

Then rerun the `--check` command.

- [ ] **Step 2: Run focused and package tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected: both commands PASS.

- [ ] **Step 3: Run related integration packages**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-storage
```

Expected: PASS. Existing ignored tests may remain ignored.

- [ ] **Step 4: Update development status**

In `docs/DEVELOPMENT_STATUS.md`, append a new completed section after Phase B12:

```markdown
### Phase B13: Bounded Application Runner Lifecycle

Implemented in `crates/fdc-server`.

Completed capabilities:

- Added `BoundedMarketDataRunnerHandle` for fixture-only finite market-data MVP runs.
- Added `BoundedRunnerState` with `Created`, `Running`, `Completed`, `Cancelled`, and `Failed` states.
- Added deterministic pre-start cancellation and invalid transition validation.
- Stored successful B11 MVP results and failure messages for later status projection work.
- Kept production background tasks, live stream supervision, persistence, SQL integration, and API status routes out of scope.

Contract tests:

- `crates/fdc-server/tests/bounded_runner_contract.rs`

Important docs:

- `docs/superpowers/specs/2026-05-28-fdc-bounded-application-runner-lifecycle-design.md`
- `docs/superpowers/plans/2026-05-28-fdc-bounded-application-runner-lifecycle.md`

Verification:

- `CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --check` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test bounded_runner_contract` exit 0, 4 passed.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server` exit 0.
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-storage` exit 0.
```

Then update `## Next Recommended Development Slice` to recommend `Phase B14: Runner Status API Projection`, scoped to exposing runner state/readiness through API projection without adding background production runtime.

- [ ] **Step 5: Commit status update**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record bounded runner lifecycle status"
```

- [ ] **Step 6: Final status check**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev`.

## Self-Review

- Spec coverage: Task 1 covers all lifecycle requirements in tests. Task 2 implements the runner states, cancellation, result/failure accessors, and invalid transition validation. Task 3 verifies and records status.
- Placeholder scan: no unfinished markers are present.
- Type consistency: public type names and method signatures match between tests, implementation, exports, and docs.
