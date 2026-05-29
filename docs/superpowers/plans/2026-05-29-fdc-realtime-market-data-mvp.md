# Realtime Market Data MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a realtime market-data MVP helper that continuously processes live-style trade envelopes during a runtime window, writes them to queryable storage, and verifies API query readback.

**Architecture:** Implement the first slice in `fdc-server` as an application assembly helper over existing Barter/orchestrator/storage APIs. Use a generic stream runner for offline multi-event tests, then add an ignored env-gated Binance Spot live smoke wrapper for real data. Keep production daemon supervision and persistence out of scope.

**Tech Stack:** Rust 1.95, Tokio, futures `Stream`, existing `fdc-barter`, `fdc-orchestrator`, `fdc-storage`, `fdc-server`, `fdc-api` crates.

---

## File Structure

- Create: `crates/fdc-server/src/realtime.rs`
  - Responsibility: realtime MVP config, summary, generic envelope stream runner, Binance live wrapper.
- Modify: `crates/fdc-server/src/lib.rs`
  - Responsibility: export realtime MVP public API.
- Create: `crates/fdc-server/tests/realtime_mvp_contract.rs`
  - Responsibility: offline multi-event stream contract and ignored live smoke contract.
- Modify: `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`
  - Responsibility: replace one-record live smoke shape or add API-facing realtime query smoke using new helper.
- Modify: `docs/mvp/first-mvp-acceptance-report.md`
  - Responsibility: update MVP acceptance language from fixture/no-listener only to realtime acquisition -> storage -> query with gated live validation.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: record realtime MVP completion and verification.

---

## Task 1: Add failing offline realtime MVP contract tests

**Files:**
- Create: `crates/fdc-server/tests/realtime_mvp_contract.rs`

- [ ] **Step 1: Write contract tests**

Create `crates/fdc-server/tests/realtime_mvp_contract.rs` with this content:

```rust
use std::sync::Arc;
use std::time::Duration;

use fdc_barter::{
    BarterIngestionEnvelope, BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent,
    BarterMarketPayload, DataQualityFlags, DecimalQuantity, TradePayload, TradeSide,
};
use fdc_core::types::{Price, Symbol, TimestampNs};
use fdc_server::{run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use futures::stream;
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
        "barter:binance_spot:live",
        sample_trade_event(symbol, trade_id, sequence),
    );
    envelope.envelope_id = format!("live-env-{trade_id}");
    envelope.emitted_at = TimestampNs::from_nanos(1_700_000_000_000_000_020);
    envelope.quality = DataQualityFlags::default();
    envelope
}

#[tokio::test]
async fn realtime_runner_writes_all_available_stream_events_and_queries_them() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let stream = stream::iter(vec![
        sample_envelope("BTCUSDT", "btc-live-1", "seq-1"),
        sample_envelope("ETHUSDT", "eth-live-1", "seq-2"),
        sample_envelope("BTCUSDT", "btc-live-2", "seq-3"),
    ]);

    let summary = run_realtime_barter_envelope_stream(
        stream,
        Arc::clone(&store),
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(1),
            idle_timeout: Duration::from_millis(50),
            max_errors: 0,
        },
    )
    .await
    .expect("offline realtime stream should complete");

    assert_eq!(summary.envelopes_received, 3);
    assert_eq!(summary.storage_records_written, 3);
    assert_eq!(summary.market_data_store_records, 3);
    assert!(summary.started_at <= summary.stopped_at);

    let btc_records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));
    assert_eq!(btc_records.len(), 2);
}

#[tokio::test]
async fn realtime_runner_stops_on_idle_without_fixed_record_limit() {
    let store = Arc::new(QueryableMarketDataStore::new());
    let stream = stream::iter(vec![
        sample_envelope("BTCUSDT", "btc-live-1", "seq-1"),
        sample_envelope("BTCUSDT", "btc-live-2", "seq-2"),
    ]);

    let summary = run_realtime_barter_envelope_stream(
        stream,
        Arc::clone(&store),
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(60),
            idle_timeout: Duration::from_millis(10),
            max_errors: 0,
        },
    )
    .await
    .expect("offline realtime stream should stop after source exhaustion/idle");

    assert_eq!(summary.envelopes_received, 2);
    assert_eq!(summary.storage_records_written, 2);
    assert_eq!(store.query(&MarketDataQuery::for_trades()).len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract
```

Expected: FAIL because `realtime` API does not exist.

- [ ] **Step 3: Commit failing contract test**

Run:

```bash
rtk git add crates/fdc-server/tests/realtime_mvp_contract.rs
rtk git commit -m "test: add realtime market data mvp contract"
```

---

## Task 2: Implement offline realtime envelope stream runner

**Files:**
- Create: `crates/fdc-server/src/realtime.rs`
- Modify: `crates/fdc-server/src/lib.rs`

- [ ] **Step 1: Implement realtime module**

Create `crates/fdc-server/src/realtime.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use fdc_barter::BarterIngestionEnvelope;
use fdc_core::{error::Error, types::TimestampNs, Result};
use fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once;
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore};
use futures::{Stream, StreamExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeMarketDataMvpConfig {
    pub runtime_window: Duration,
    pub idle_timeout: Duration,
    pub max_errors: usize,
}

impl Default for RealtimeMarketDataMvpConfig {
    fn default() -> Self {
        Self {
            runtime_window: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(2),
            max_errors: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealtimeMarketDataMvpSummary {
    pub envelopes_received: usize,
    pub source_valid: usize,
    pub source_invalid: usize,
    pub dto_mapped: usize,
    pub storage_records_written: usize,
    pub market_data_store_records: usize,
    pub errors_observed: usize,
    pub started_at: TimestampNs,
    pub stopped_at: TimestampNs,
}

pub async fn run_realtime_barter_envelope_stream<S>(
    mut stream: S,
    market_data_store: Arc<QueryableMarketDataStore>,
    config: RealtimeMarketDataMvpConfig,
) -> Result<RealtimeMarketDataMvpSummary>
where
    S: Stream<Item = BarterIngestionEnvelope> + Unpin,
{
    if config.runtime_window.is_zero() {
        return Err(Error::validation(
            "realtime market-data MVP runtime_window must be greater than zero",
        ));
    }
    if config.idle_timeout.is_zero() {
        return Err(Error::validation(
            "realtime market-data MVP idle_timeout must be greater than zero",
        ));
    }

    let started_at = TimestampNs::now();
    let deadline = tokio::time::Instant::now() + config.runtime_window;
    let mut summary = RealtimeMarketDataMvpSummary {
        envelopes_received: 0,
        source_valid: 0,
        source_invalid: 0,
        dto_mapped: 0,
        storage_records_written: 0,
        market_data_store_records: 0,
        errors_observed: 0,
        started_at,
        stopped_at: started_at,
    };

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        let wait_for = remaining.min(config.idle_timeout);
        match tokio::time::timeout(wait_for, stream.next()).await {
            Ok(Some(envelope)) => {
                summary.envelopes_received += 1;
                let pipeline_result = run_barter_envelopes_to_storage_once(
                    vec![envelope],
                    market_data_store.as_ref(),
                )
                .await?;
                summary.source_valid += pipeline_result.source_valid;
                summary.source_invalid += pipeline_result.source_invalid;
                summary.dto_mapped += pipeline_result.dto_mapped;
                summary.storage_records_written += pipeline_result.storage_records_written;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    summary.market_data_store_records = market_data_store.query(&MarketDataQuery::for_trades()).len();
    summary.stopped_at = TimestampNs::now();
    Ok(summary)
}
```

- [ ] **Step 2: Export realtime API**

Modify `crates/fdc-server/src/lib.rs`:

Add:

```rust
pub mod realtime;
```

Add exports:

```rust
pub use realtime::{
    run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig,
    RealtimeMarketDataMvpSummary,
};
```

- [ ] **Step 3: Run realtime server contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract
```

Expected: PASS, 2 tests pass.

- [ ] **Step 4: Run server tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server
```

Expected: PASS.

- [ ] **Step 5: Commit implementation**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server
rtk git add crates/fdc-server/src/realtime.rs crates/fdc-server/src/lib.rs
rtk git commit -m "feat: add realtime market data mvp runner"
```

---

## Task 3: Add API-facing realtime query contract and ignored live smoke wrapper

**Files:**
- Modify: `crates/fdc-api/tests/acquisition_api_mvp_contract.rs`

- [ ] **Step 1: Add API-facing offline realtime test**

Append a test that builds a fake stream with multiple envelopes, calls `run_realtime_barter_envelope_stream`, then queries through `build_market_data_router` for returned records.

Use imports if missing:

```rust
use fdc_server::{run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig};
use futures::stream;
```

Add test body similar to the server offline contract, but assert the API JSON returns at least two records for BTCUSDT when two BTCUSDT events were streamed.

- [ ] **Step 2: Update ignored live smoke to use realtime runner semantics**

Replace the one-record language in `ignored_live_smoke_writes_binance_trade_to_store_and_reads_it_through_api` with a duration-window collection path if practical. If not practical in this slice, keep collection bounded by timeout but rename expectations to state that live validation is gated and real-data, and do not assert exactly one record.

- [ ] **Step 3: Run API acquisition contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
```

Expected: PASS, ignored live smoke remains ignored by default.

- [ ] **Step 4: Commit API-facing test updates**

Run:

```bash
rtk git add crates/fdc-api/tests/acquisition_api_mvp_contract.rs
rtk git commit -m "test: verify realtime mvp data is queryable through api"
```

---

## Task 4: Update MVP docs and status

**Files:**
- Modify: `docs/mvp/first-mvp-acceptance-report.md`
- Modify: `docs/mvp/first-mvp-demo.md`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Update MVP docs**

Update MVP docs to state the MVP target is realtime acquisition -> storage -> query, with default offline tests using fake streams and real live validation gated by `FDC_BARTER_LIVE_SMOKE=1`.

- [ ] **Step 2: Run full verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test realtime_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test acquisition_api_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api -p fdc-server
```

Expected: all PASS, live smoke ignored unless explicitly enabled.

- [ ] **Step 3: Update development status**

Add a phase section for realtime market-data MVP and record verification.

- [ ] **Step 4: Commit docs/status**

Run:

```bash
rtk git add docs/mvp/first-mvp-acceptance-report.md docs/mvp/first-mvp-demo.md docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record realtime market data mvp"
```

## Self-Review

- This plan implements a continuous realtime stream helper bounded by time/idle/cancel style, not one fixed record.
- Offline tests prove multi-event streaming without network.
- Live validation remains env-gated and ignored by default.
- No persistence, SQL, auth, or production daemon behavior is added.
