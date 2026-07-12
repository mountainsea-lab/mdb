# Candle Downstream Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore downstream compilation after the derivatives adapter expansion, then complete the candle business loop from fetched candle envelopes to DTO/storage/query API, and finally add structured derivatives DTO support.

**Architecture:** Adapter events continue to enter downstream through `fdc-orchestrator`. Candle remains a first-class `MarketDataKind::Candle` stored in the `candles` collection and exposed via a server query endpoint. New derivatives payloads first unblock compilation as raw market-data records, then become structured transform DTO variants with storage/query tags.

**Tech Stack:** Rust workspace, Tokio tests, Axum routes, fdc-barter, fdc-orchestrator, fdc-transform, fdc-storage, fdc-server.

---

## File Map

- `crates/fdc-orchestrator/src/barter.rs`: Add labels for `FundingRate`, `OpenInterest`, `MarkPrice`, `IndexPrice`.
- `crates/fdc-orchestrator/src/market_data.rs`: Map derivatives payloads without breaking candle mapping.
- `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`: Add/extend tests for derivatives fallback and candle storage mapping.
- `crates/fdc-storage/src/queryable.rs`: Add `MarketDataQuery::for_candles()`.
- `crates/fdc-server/src/market_data/model.rs`: Add candle query response/record models.
- `crates/fdc-server/src/market_data/service.rs`: Add candle query service and test candle ingestion helper.
- `crates/fdc-server/src/market_data/router.rs`: Add `GET /market-data/candles`.
- `crates/fdc-server/tests/realtime_mvp_contract.rs`: Verify candle envelopes flow to storage/query.
- `crates/fdc-server/tests/production_server_router_contract.rs`: Verify `/market-data/candles` HTTP behavior.
- Later derivatives structured support will also touch `crates/fdc-transform/src/market_data.rs`, `crates/fdc-transform/src/lib.rs`, and storage/orchestrator mappings.

---

### Task 1: Restore orchestrator compilation after derivatives payload expansion

**Files:**
- Modify: `crates/fdc-orchestrator/src/barter.rs`
- Modify: `crates/fdc-orchestrator/src/market_data.rs`
- Modify: `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

- [ ] **Step 1: Run failing compile/test**

Run: `rtk cargo check -p fdc-orchestrator`

Expected before fix: FAIL with non-exhaustive match errors for `FundingRate`, `OpenInterest`, `MarkPrice`, `IndexPrice`.

- [ ] **Step 2: Add failing behavior test for derivatives fallback**

Add a test in `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs` that creates a `BarterMarketDataKind::FundingRate` event with `BarterMarketPayload::FundingRate`, maps it through `barter_envelope_to_source_envelope` and `barter_event_to_market_data_dto`, and asserts `MarketDataKind::Raw` plus a raw description containing `funding_rate`.

- [ ] **Step 3: Run test to verify it fails**

Run: `rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract derivatives_payload_maps_to_raw_market_data -- --nocapture`

Expected before fix: FAIL to compile with the same non-exhaustive match errors.

- [ ] **Step 4: Implement minimal fallback**

In `barter_kind_label`, add labels:

```rust
BarterMarketDataKind::FundingRate => "funding_rate",
BarterMarketDataKind::OpenInterest => "open_interest",
BarterMarketDataKind::MarkPrice => "mark_price",
BarterMarketDataKind::IndexPrice => "index_price",
```

In `barter_kind_to_market_data_kind`, map matching derivatives payloads to `MarketDataKind::Raw`.

In `barter_payload_to_market_data_payload`, convert derivatives payloads to `MarketDataPayload::Raw(RawMarketDataDto { description: ... })`.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract derivatives_payload_maps_to_raw_market_data -- --nocapture
rtk cargo check -p fdc-orchestrator
```

Expected: PASS / exit 0.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-orchestrator/src/barter.rs crates/fdc-orchestrator/src/market_data.rs crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs
git commit -m "fix: restore orchestrator derivatives mapping"
```

---

### Task 2: Complete candle DTO/storage/query business loop

**Files:**
- Modify: `crates/fdc-storage/src/queryable.rs`
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/realtime_mvp_contract.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing storage/query test for candle envelopes**

In `realtime_mvp_contract.rs`, add sample candle event/envelope helpers and a test that runs `run_realtime_barter_envelope_stream` over one candle, then queries `MarketDataQuery::for_candles().with_symbol("BTCUSDT")` and asserts one record in collection `candles` with tag `kind=candle`.

Run: `rtk cargo test -p fdc-server --test realtime_mvp_contract realtime_runner_writes_candle_stream_events_and_queries_them -- --nocapture`

Expected before implementation: FAIL because `MarketDataQuery::for_candles` does not exist.

- [ ] **Step 2: Implement `MarketDataQuery::for_candles`**

Add:

```rust
pub fn for_candles() -> Self {
    Self::new().with_collection("candles").with_kind("candle")
}
```

Run the same test. Expected: PASS if downstream candle storage already works.

- [ ] **Step 3: Add failing HTTP route test**

In `production_server_router_contract.rs`, add a test that ingests a test candle into state, calls `/market-data/candles?symbol=BTCUSDT&limit=10`, and asserts `status=success`, `data_kind=candle`, `returned_records=1`, and the JSON payload contains `Candle.close`.

Expected before route implementation: FAIL with 404 or compile error for missing helper.

- [ ] **Step 4: Implement candle service models and route**

Add `MarketDataCandlesResponse` and `MarketDataCandleRecord` mirroring the trade response shape.

Add query service using the same limit validation constants as trades:

```rust
let mut query = MarketDataQuery::for_candles().with_limit(applied_limit);
```

Add `GET /market-data/candles` route and handler.

Add `ingest_test_candle` helper for router tests.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p fdc-server --test realtime_mvp_contract realtime_runner_writes_candle_stream_events_and_queries_them -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract p39_market_data_candles_query_returns_candle_records -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-storage/src/queryable.rs crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/realtime_mvp_contract.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat: expose candle market data query"
```

---

### Task 3: Add structured derivatives DTO/storage support

**Files:**
- Modify: `crates/fdc-transform/src/market_data.rs`
- Modify: `crates/fdc-transform/src/lib.rs`
- Modify: `crates/fdc-orchestrator/src/market_data.rs`
- Modify: `crates/fdc-orchestrator/src/storage.rs`
- Modify: `crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs`

- [ ] **Step 1: Add failing derivatives structured mapping tests**

Add tests for `FundingRate`, `OpenInterest`, `MarkPrice`, and `IndexPrice` payloads asserting structured DTO variants and storage collections `funding_rates`, `open_interest`, `mark_prices`, and `index_prices`.

Expected before implementation: FAIL because transform DTO variants do not exist.

- [ ] **Step 2: Add transform DTO types**

Add variants to `MarketDataKind` and `MarketDataPayload`, plus DTO structs preserving all adapter fields.

- [ ] **Step 3: Map derivatives in orchestrator**

Map derivative payloads to structured DTO variants instead of Raw.

- [ ] **Step 4: Map derivatives to storage**

Add collection/schema/tag/access/durability mappings for derivative kinds.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract -- --nocapture
rtk cargo test -p fdc-transform --test market_data_boundary_contract -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-transform/src/market_data.rs crates/fdc-transform/src/lib.rs crates/fdc-orchestrator/src/market_data.rs crates/fdc-orchestrator/src/storage.rs crates/fdc-orchestrator/tests/orchestrator_boundary_contract.rs
git commit -m "feat: add structured derivatives market data dto"
```

---

## Final Verification

Run:

```bash
rtk cargo test -p fdc-barter --test historical_ohlcv_contract -- --nocapture
rtk cargo test -p fdc-barter --test binance_spot_ohlcv_provider_contract -- --nocapture
rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract -- --nocapture
rtk cargo test -p fdc-orchestrator --test orchestrator_boundary_contract -- --nocapture
rtk cargo test -p fdc-server --test realtime_mvp_contract -- --nocapture
rtk cargo test -p fdc-server --test production_server_router_contract market_data_candles -- --nocapture
rtk cargo run -p fdc-barter --example historical_binance_spot_ohlcv
```

Expected: all targeted tests pass and the example prints visible candle records.
