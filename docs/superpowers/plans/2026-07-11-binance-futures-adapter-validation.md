# Binance Futures Adapter Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate Binance Futures USD derivatives data acquisition inside `crates/fdc-adapter/barter`, producing visible example output and stable adapter envelopes for later storage/query stages.

**Architecture:** Extend only the `fdc-barter` adapter boundary. Add derivatives market data kinds and payloads, then add Binance Futures REST descriptor/provider functions following the existing Binance Spot historical REST patterns. Finish with a runnable example that prints fixture data by default and real Binance data only when explicitly enabled.

**Tech Stack:** Rust, Tokio, async-trait, serde/serde_json, rust_decimal, reqwest/barter-integration REST client, existing `fdc-core` timestamp/price types.

---

## File Structure

Modify:

- `docs/roadmaps/factor-data-stage-status.md`
  - Keep Stage 1 as `in_progress` during implementation.
  - Mark Stage 1 `validated` only after all acceptance checks pass.
- `crates/fdc-adapter/barter/src/model/event.rs`
  - Add derivatives market data kinds and payload structs.
  - Extend `BarterMarketPayload::kind()`.
- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
  - Add Binance Futures USD capabilities, REST descriptors, response structs, providers, and executor helpers.
  - Add `BarterIntegrationHistoricalRestExecutor::binance_futures_usd()`.
- `crates/fdc-adapter/barter/src/lib.rs`
  - Export new kinds, payloads, capabilities, descriptors, providers, and execute helpers.
- `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`
  - Update implemented/current state after validation.

Create:

- `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs`
  - Offline descriptor and fake executor tests.
- `crates/fdc-adapter/barter/tests/binance_futures_derivatives_provider_contract.rs`
  - Offline provider/fixture parser tests.
- `crates/fdc-adapter/barter/examples/historical_binance_futures_usd_derivatives.rs`
  - Runnable example with visible logs.
- `crates/fdc-adapter/barter/docs/binance-futures-validation-report.md`
  - Validation report with commands, example output, supported fields, and unsupported items.

Do not modify:

- `crates/fdc-storage`
- `crates/fdc-server`
- `crates/fdc-analytics`
- `crates/fdc-orchestrator`

---

## Task 1: Derivatives Adapter Data Model

**Files:**

- Modify: `crates/fdc-adapter/barter/src/model/event.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Test: existing crate tests plus compile checks

- [ ] **Step 1: Add failing compile references through a small test section**

Add a test module at the bottom of `crates/fdc-adapter/barter/src/model/event.rs`:

```rust
#[cfg(test)]
mod derivatives_payload_tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn derivatives_payloads_report_expected_kinds() {
        let funding = BarterMarketPayload::FundingRate(FundingRatePayload {
            funding_rate: Decimal::new(125, 6),
            funding_time: TimestampNs::from_nanos(1_700_000_000_000_000_000),
            mark_price: None,
        });
        assert_eq!(funding.kind(), BarterMarketDataKind::FundingRate);

        let open_interest = BarterMarketPayload::OpenInterest(OpenInterestPayload {
            open_interest: Decimal::new(12345, 2),
            timestamp: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        });
        assert_eq!(open_interest.kind(), BarterMarketDataKind::OpenInterest);
    }
}
```

- [ ] **Step 2: Run the targeted test and verify it fails**

Run:

```bash
rtk cargo test -p fdc-barter derivatives_payloads_report_expected_kinds
```

Expected: compile failure because `FundingRatePayload`, `OpenInterestPayload`, and new enum variants do not exist.

- [ ] **Step 3: Add derivatives kinds and payload structs**

In `BarterMarketDataKind`, add:

```rust
FundingRate,
OpenInterest,
MarkPrice,
IndexPrice,
```

After `CandlePayload`, add:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRatePayload {
    pub funding_rate: Decimal,
    pub funding_time: TimestampNs,
    pub mark_price: Option<Price>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenInterestPayload {
    pub open_interest: Decimal,
    pub timestamp: TimestampNs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkPricePayload {
    pub mark_price: Price,
    pub index_price: Option<Price>,
    pub estimated_settle_price: Option<Price>,
    pub funding_rate: Option<Decimal>,
    pub next_funding_time: Option<TimestampNs>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexPricePayload {
    pub index_price: Price,
    pub timestamp: TimestampNs,
}
```

Extend `BarterMarketPayload` with:

```rust
FundingRate(FundingRatePayload),
OpenInterest(OpenInterestPayload),
MarkPrice(MarkPricePayload),
IndexPrice(IndexPricePayload),
```

Extend `kind()` match with:

```rust
Self::FundingRate(_) => BarterMarketDataKind::FundingRate,
Self::OpenInterest(_) => BarterMarketDataKind::OpenInterest,
Self::MarkPrice(_) => BarterMarketDataKind::MarkPrice,
Self::IndexPrice(_) => BarterMarketDataKind::IndexPrice,
```

- [ ] **Step 4: Export new payloads**

In `crates/fdc-adapter/barter/src/lib.rs`, add the new payloads to the `pub use model::{ ... }` list:

```rust
FundingRatePayload, OpenInterestPayload, MarkPricePayload, IndexPricePayload,
```

- [ ] **Step 5: Run tests**

Run:

```bash
rtk cargo test -p fdc-barter derivatives_payloads_report_expected_kinds
rtk cargo test -p fdc-barter
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-adapter/barter/src/model/event.rs crates/fdc-adapter/barter/src/lib.rs
git commit -m "feat: add derivatives payloads to barter adapter"
```

---

## Task 2: Binance Futures REST Descriptor Contracts

**Files:**

- Create: `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write descriptor tests**

Create `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs` with tests for:

```rust
use fdc_barter::{
    binance_futures_usd_funding_rate_rest_request_descriptor,
    binance_futures_usd_mark_price_rest_request_descriptor,
    binance_futures_usd_ohlcv_rest_request_descriptor,
    binance_futures_usd_open_interest_rest_request_descriptor,
    BarterMarketDataKind, BarterMarketType, HistoricalBackfillRequest,
};
use fdc_core::types::TimestampNs;

fn futures_request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-futures-usd-history".to_string(),
        exchange: "binance_futures_usd".to_string(),
        market_type: BarterMarketType::Perpetual,
        symbol: "BTCUSDT".to_string(),
        kind,
        interval: (kind == BarterMarketDataKind::Candle).then(|| "1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

#[test]
fn funding_rate_descriptor_matches_binance_futures_shape() {
    let descriptor = binance_futures_usd_funding_rate_rest_request_descriptor(
        &futures_request(BarterMarketDataKind::FundingRate),
    )
    .expect("valid funding request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_futures_usd");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/fapi/v1/fundingRate");
    assert_eq!(descriptor.timeout_ms, 5_000);
    assert_eq!(
        descriptor.query,
        vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("startTime".to_string(), "1700000000000".to_string()),
            ("endTime".to_string(), "1700000060000".to_string()),
            ("limit".to_string(), "2".to_string()),
        ]
    );
}
```

Add analogous assertions:

- open interest path `/fapi/v1/openInterest` with only `symbol`.
- mark price path `/fapi/v1/premiumIndex` with only `symbol`.
- kline path `/fapi/v1/klines` with `symbol`, `interval`, `startTime`, `endTime`, `limit`.
- unsupported spot market type returns error.
- wrong kind for a descriptor returns error.

- [ ] **Step 2: Run descriptor test and verify it fails**

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract
```

Expected: compile failure because descriptor functions do not exist.

- [ ] **Step 3: Implement capabilities and descriptor functions**

In `historical.rs`, add capabilities:

```rust
pub fn binance_futures_usd_funding_rate_capabilities() -> HistoricalProviderCapabilities { ... }
pub fn binance_futures_usd_open_interest_capabilities() -> HistoricalProviderCapabilities { ... }
pub fn binance_futures_usd_mark_price_capabilities() -> HistoricalProviderCapabilities { ... }
pub fn binance_futures_usd_ohlcv_capabilities() -> HistoricalProviderCapabilities { ... }
```

Each should use:

```rust
exchange: "binance_futures_usd".to_string(),
market_types: vec![BarterMarketType::Perpetual],
```

Add descriptors with exact paths from the test. Use existing `validate_historical_backfill_request`, `capabilities.validate_request(request)?`, `nanos_to_millis`, and uppercase symbol pattern from Spot descriptors.

- [ ] **Step 4: Export descriptor functions**

In `lib.rs`, export the new capabilities and descriptors.

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract
rtk cargo test -p fdc-barter
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/historical.rs crates/fdc-adapter/barter/src/lib.rs crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs
git commit -m "feat: add binance futures rest descriptors"
```

---

## Task 3: Binance Futures Provider Fixture Contracts

**Files:**

- Create: `crates/fdc-adapter/barter/tests/binance_futures_derivatives_provider_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write provider fixture tests**

Create tests that assert fixture responses map to envelopes:

```rust
use fdc_barter::{
    binance_futures_usd_funding_rate_provider_from_response,
    binance_futures_usd_mark_price_provider_from_response,
    binance_futures_usd_ohlcv_provider_from_response,
    binance_futures_usd_open_interest_provider_from_response,
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType, HistoricalBackfillRequest,
    HistoricalExchangeProvider,
};
use fdc_core::types::TimestampNs;

fn request(kind: BarterMarketDataKind) -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-futures-usd-history".to_string(),
        exchange: "binance_futures_usd".to_string(),
        market_type: BarterMarketType::Perpetual,
        symbol: "BTCUSDT".to_string(),
        kind,
        interval: (kind == BarterMarketDataKind::Candle).then(|| "1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_120_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

#[tokio::test]
async fn funding_provider_maps_response_to_envelopes() {
    let response = r#"[
        {"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"0.00010000","markPrice":"35000.10"}
    ]"#;
    let provider = binance_futures_usd_funding_rate_provider_from_response(response)
        .expect("funding fixture should parse");
    let page = provider.fetch_page(request(BarterMarketDataKind::FundingRate)).await.unwrap();
    assert_eq!(page.envelopes.len(), 1);
    assert!(page.complete);
    let BarterMarketPayload::FundingRate(payload) = &page.envelopes[0].event.payload else {
        panic!("expected funding payload");
    };
    assert_eq!(payload.funding_rate.to_string(), "0.00010000");
    assert_eq!(payload.funding_time.as_nanos(), 1_700_000_000_000_000_000);
}
```

Add analogous tests for:

- open interest fixture: `{"symbol":"BTCUSDT","openInterest":"12345.678"}`.
- mark price fixture: `{"symbol":"BTCUSDT","markPrice":"35000.10","indexPrice":"34990.00","lastFundingRate":"0.0001","nextFundingTime":1700003600000,"time":1700000000000}`.
- kline fixture using Binance kline array shape.
- invalid numeric payload returns error.

- [ ] **Step 2: Run provider tests and verify they fail**

```bash
rtk cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
```

Expected: compile failure because provider constructors do not exist.

- [ ] **Step 3: Implement response structs and providers**

In `historical.rs`, implement Binance Futures response structs with serde fields matching Binance names. Follow the existing Binance Spot provider shape:

- parse JSON response in `*_provider_from_response`.
- store parsed rows in a provider struct.
- implement `HistoricalExchangeProvider for Provider`.
- create `BarterIngestionEnvelope::from_backfill_event(...)` envelopes.
- set `event.exchange = "binance_futures_usd"`.
- set `event.market_type = BarterMarketType::Perpetual`.
- set `quality.is_backfill = true`.

For current snapshot endpoints, return one envelope and `complete=true`.
For paginated endpoints, use the existing short-page completion rule.

- [ ] **Step 4: Export provider constructors and provider types**

In `lib.rs`, export provider constructors and concrete provider types.

- [ ] **Step 5: Run tests**

```bash
rtk cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
rtk cargo test -p fdc-barter
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/historical.rs crates/fdc-adapter/barter/src/lib.rs crates/fdc-adapter/barter/tests/binance_futures_derivatives_provider_contract.rs
git commit -m "feat: parse binance futures derivatives responses"
```

---

## Task 4: Real REST Executor Helpers and Ignored Smoke

**Files:**

- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs`

- [ ] **Step 1: Add fake executor tests**

In `binance_futures_historical_rest_contract.rs`, add fake executor tests for each execute helper. Example:

```rust
struct FakeFuturesExecutor;

#[async_trait::async_trait]
impl fdc_barter::HistoricalRestExecutor for FakeFuturesExecutor {
    async fn execute(
        &self,
        descriptor: &fdc_barter::HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        match descriptor.path.as_str() {
            "/fapi/v1/fundingRate" => Ok(r#"[{"symbol":"BTCUSDT","fundingTime":1700000000000,"fundingRate":"0.00010000","markPrice":"35000.10"}]"#.to_string()),
            "/fapi/v1/openInterest" => Ok(r#"{"symbol":"BTCUSDT","openInterest":"12345.678"}"#.to_string()),
            "/fapi/v1/premiumIndex" => Ok(r#"{"symbol":"BTCUSDT","markPrice":"35000.10","indexPrice":"34990.00","lastFundingRate":"0.0001","nextFundingTime":1700003600000,"time":1700000000000}"#.to_string()),
            "/fapi/v1/klines" => Ok(r#"[[1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]]"#.to_string()),
            unsupported => panic!("unexpected path {unsupported}"),
        }
    }
}
```

Assert each `execute_binance_futures_usd_*_rest` helper returns envelopes.

- [ ] **Step 2: Run fake executor tests and verify they fail**

```bash
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract fake_executor
```

Expected: compile failure because execute helpers do not exist.

- [ ] **Step 3: Implement execute helpers and futures executor constructor**

Add to `BarterIntegrationHistoricalRestExecutor`:

```rust
pub fn binance_futures_usd() -> Self {
    Self {
        client: RestClient::new(
            "https://fapi.binance.com",
            PublicNoHeaders,
            BinanceSpotHistoricalHttpParser,
        ),
    }
}
```

If the existing executor client type is named with Spot parser, keep it if compatible, or rename the parser to a generic Binance historical parser in the same commit.

Add helpers:

```rust
pub async fn execute_binance_futures_usd_funding_rate_rest(
    executor: &impl HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage> { ... }
```

Add analogous helpers for open interest, mark price, and OHLCV.

- [ ] **Step 4: Add ignored smoke tests**

Add ignored tests gated by `MDB_BARTER_ENABLE_REAL_NETWORK_TESTS=1`, one per endpoint. Each should:

- build a BTCUSDT request,
- call `BarterIntegrationHistoricalRestExecutor::binance_futures_usd()`,
- execute one page,
- assert non-empty envelopes,
- print endpoint/kind/records summary with `eprintln!`.

- [ ] **Step 5: Run default tests**

```bash
rtk cargo test -p fdc-barter
```

Expected: pass, ignored smoke not run.

- [ ] **Step 6: Optionally run real smoke when network is available**

```bash
MDB_BARTER_ENABLE_REAL_NETWORK_TESTS=1 \
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract -- --ignored --nocapture
```

Expected: real Binance Futures public endpoint data prints in test logs.

- [ ] **Step 7: Commit**

```bash
git add crates/fdc-adapter/barter/src/ingestion/historical.rs crates/fdc-adapter/barter/src/lib.rs crates/fdc-adapter/barter/tests/binance_futures_historical_rest_contract.rs
git commit -m "feat: validate binance futures rest execution"
```

---

## Task 5: Runnable Example with Visible Logs

**Files:**

- Create: `crates/fdc-adapter/barter/examples/historical_binance_futures_usd_derivatives.rs`

- [ ] **Step 1: Create example with fixture-first output**

The example must:

- initialize tracing/log output,
- run fixture mode by default,
- run real-network mode only if `MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1`,
- print one block per kind containing endpoint, exchange, market type, symbol, kind, records, first event time, first payload, and complete.

Use this structure:

```rust
fn real_network_enabled() -> bool {
    std::env::var("MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES").as_deref() == Ok("1")
}

#[tokio::main]
async fn main() -> fdc_barter::Result<()> {
    if real_network_enabled() {
        run_real_network().await
    } else {
        run_fixture_mode().await
    }
}
```

- [ ] **Step 2: Run example and verify output**

```bash
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
```

Expected output includes text equivalent to:

```text
endpoint=/fapi/v1/fundingRate
exchange=binance_futures_usd
market_type=perpetual
symbol=BTCUSDT
kind=funding_rate
records=1
first_event_time=1700000000000000000
first_payload=FundingRate
complete=true
```

- [ ] **Step 3: Run default tests**

```bash
rtk cargo test -p fdc-barter
```

Expected: pass.

- [ ] **Step 4: Optionally run real example when network is available**

```bash
MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1 \
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
```

Expected: logs show live Binance Futures public data summaries.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-adapter/barter/examples/historical_binance_futures_usd_derivatives.rs
git commit -m "feat: add binance futures validation example"
```

---

## Task 6: Validation Report and Stage Completion Marking

**Files:**

- Create: `crates/fdc-adapter/barter/docs/binance-futures-validation-report.md`
- Modify: `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [ ] **Step 1: Write validation report**

Create `crates/fdc-adapter/barter/docs/binance-futures-validation-report.md` with:

```markdown
# Binance Futures Adapter Validation Report

**Status:** validated
**Module:** `crates/fdc-adapter/barter`
**Exchange:** `binance_futures_usd`
**Market type:** `perpetual`

## Validated endpoints

| Kind | Endpoint | Default example | Real-network example |
|---|---|---|---|
| FundingRate | `/fapi/v1/fundingRate` | validated | validated when opt-in command succeeds |
| OpenInterest | `/fapi/v1/openInterest` | validated | validated when opt-in command succeeds |
| MarkPrice | `/fapi/v1/premiumIndex` | validated | validated when opt-in command succeeds |
| Candle | `/fapi/v1/klines` | validated | validated when opt-in command succeeds |

## Acceptance commands

```bash
rtk cargo test -p fdc-barter
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
```

## Visible example output summary

```text
endpoint=/fapi/v1/fundingRate
exchange=binance_futures_usd
market_type=perpetual
symbol=BTCUSDT
kind=funding_rate
records=1
complete=true
```

## Downstream adapter envelope contract

```text
exchange=binance_futures_usd
market_type=perpetual
symbol=BTCUSDT
kind=funding_rate | open_interest | mark_price | index_price | candle
mode=historical
source_id=barter-binance-futures-usd-history
event_time=<exchange event time>
received_at=<adapter receive time>
checkpoint=<optional historical checkpoint>
```

## Not supported in this stage

- Storage writes.
- Server query API.
- Analytics factor calculation.
- Other exchanges.
- L2 reconstruction.
- Historical open-interest time series endpoint.
```

Replace the command output summary with actual output captured during implementation.

- [ ] **Step 2: Update adapter requirements doc**

In `market-data-collection-requirements.md`, update current state to mention Binance Futures USD REST validation for funding, open interest, mark/index price, and futures OHLCV.

- [ ] **Step 3: Mark Stage 1 validated**

In `docs/roadmaps/factor-data-stage-status.md`:

- Set Stage 1 status to `validated`.
- Set completion commit to the final implementation commit.
- Set acceptance evidence to include `rtk cargo test -p fdc-barter` and the example command.
- Add a Stage 1 completion record using the template.

- [ ] **Step 4: Run final verification**

```bash
rtk cargo test -p fdc-barter
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
rtk git status --short
```

Expected:

- Tests pass.
- Example prints visible data summary.
- Worktree shows only intended docs before commit, then clean after commit.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-adapter/barter/docs/binance-futures-validation-report.md crates/fdc-adapter/barter/docs/market-data-collection-requirements.md docs/roadmaps/factor-data-stage-status.md
git commit -m "docs: mark binance futures adapter validation complete"
```

---

## Final Acceptance Checklist

- [ ] `rtk cargo test -p fdc-barter` passes.
- [ ] `rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives` prints visible fixture data.
- [ ] Optional real-network example works with `MDB_BARTER_ENABLE_REAL_NETWORK_EXAMPLES=1`.
- [ ] Optional ignored smoke works with `MDB_BARTER_ENABLE_REAL_NETWORK_TESTS=1`.
- [ ] `docs/roadmaps/factor-data-stage-status.md` marks Stage 1 as `validated` only after verification.
- [ ] No changes under `crates/fdc-storage`, `crates/fdc-server`, `crates/fdc-analytics`, or `crates/fdc-orchestrator`.
- [ ] Worktree is clean after final commit.
