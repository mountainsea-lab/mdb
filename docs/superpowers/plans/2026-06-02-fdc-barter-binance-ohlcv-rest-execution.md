# fdc-barter Binance Spot OHLCV REST Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real Binance Spot historical OHLCV REST execution path to `fdc-barter` while keeping default tests offline and network-free.

**Architecture:** Keep all exchange REST transport inside `fdc-barter`, but reuse `barter-rs` network primitives instead of hand-rolling an HTTP client. Convert the existing adapter-owned `HistoricalRestRequestDescriptor` into a private Binance Spot `RestRequest`, execute it with `barter_integration::protocol::http::rest::client::RestClient<PublicNoHeaders, Parser>`, and parse the returned Binance kline rows through the existing `BinanceSpotOhlcvProvider`. Default tests use a fake executor; real HTTP runs only in an ignored smoke gated by `FDC_BARTER_HISTORICAL_SMOKE=1`.

**Tech Stack:** Rust, `async-trait`, `barter-integration` protocol HTTP REST client, `tokio`, existing `serde_json` Binance kline parser, ignored network smoke tests.

---

## File Structure

### Modify

- `crates/fdc-adapter/barter/Cargo.toml`
  - Add direct `barter-integration` dependency with `protocol`, `error`, `serde`, and `metric` features.
- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
  - Add adapter-owned `HistoricalRestExecutor` trait.
  - Add `execute_binance_spot_ohlcv_rest()` helper that fetches through an injected executor and reuses `binance_spot_ohlcv_provider_from_response()`.
  - Add private `BinanceSpotKlinesRestRequest`, `BinanceSpotKlinesQuery`, `BinanceSpotHistoricalHttpParser`, and `BinanceSpotHistoricalRestError` to adapt the descriptor to `barter-integration`.
  - Add public `BarterIntegrationHistoricalRestExecutor` backed by `RestClient<PublicNoHeaders, BinanceSpotHistoricalHttpParser>`.
- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Re-export the new executor trait, barter-integration executor, and execution helper.
- `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export the new public historical REST execution types.
- `docs/DEVELOPMENT_STATUS.md`
  - Append a checkpoint after implementation and verification.

### Create

- `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`
  - Offline fake-executor contract.
  - Ignored + env-gated real Binance Spot OHLCV smoke.

---

## Task 1: Offline Historical REST Executor Boundary

**Files:**
- Create: `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write the failing offline executor contract test**

Create `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`:

```rust
use async_trait::async_trait;
use fdc_barter::{
    execute_binance_spot_ohlcv_rest, BarterMarketDataKind, BarterMarketPayload,
    BarterMarketType, HistoricalBackfillRequest, HistoricalRestExecutor,
    HistoricalRestRequestDescriptor,
};
use fdc_core::types::TimestampNs;

fn ohlcv_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(1),
        cursor: None,
    }
}

struct FakeExecutor;

#[async_trait]
impl HistoricalRestExecutor for FakeExecutor {
    async fn execute(
        &self,
        descriptor: &HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        assert_eq!(descriptor.exchange, "binance_spot");
        assert_eq!(descriptor.method, "GET");
        assert_eq!(descriptor.path, "/api/v3/klines");
        assert!(descriptor
            .query
            .contains(&("symbol".to_string(), "BTCUSDT".to_string())));
        assert!(descriptor
            .query
            .contains(&("interval".to_string(), "1m".to_string())));

        Ok(r#"[[1700000000000,"100.10","110.20","90.30","105.40","123.45000000",1700000059999,"12999.99000000",42,"60.00000000","6300.00000000","0"]]"#.to_string())
    }
}

#[tokio::test]
async fn executor_fetches_binance_spot_ohlcv_page_without_network_in_default_tests() {
    let page = execute_binance_spot_ohlcv_rest(&FakeExecutor, ohlcv_request())
        .await
        .expect("fake executor response should parse into a historical page");

    assert_eq!(page.envelopes.len(), 1);
    assert!(!page.complete);
    assert!(page.envelopes[0].quality.is_backfill);

    let BarterMarketPayload::Candle(candle) = &page.envelopes[0].event.payload else {
        panic!("expected candle payload");
    };

    assert_eq!(candle.interval.as_deref(), Some("1m"));
    assert_eq!(candle.open.to_f64(), 100.10);
    assert_eq!(candle.trade_count, Some(42));
    assert_eq!(candle.quote_volume.unwrap().to_string(), "12999.99000000");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract
```

Expected: compile failure because `HistoricalRestExecutor` and `execute_binance_spot_ohlcv_rest` are not defined or exported.

- [ ] **Step 3: Add the executor trait and execution helper**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `HistoricalRestRequestDescriptor`, add:

```rust
/// Adapter-owned execution boundary for public historical REST descriptors.
#[async_trait]
pub trait HistoricalRestExecutor: Send + Sync {
    async fn execute(&self, descriptor: &HistoricalRestRequestDescriptor) -> Result<String>;
}
```

After `binance_spot_ohlcv_provider_from_response()`, add:

```rust
/// Execute a Binance Spot OHLCV REST descriptor through an injected executor and parse the response.
pub async fn execute_binance_spot_ohlcv_rest(
    executor: &dyn HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage> {
    let descriptor = binance_spot_ohlcv_rest_request_descriptor(&request)?;
    let response_body = executor.execute(&descriptor).await?;
    let provider = binance_spot_ohlcv_provider_from_response(&response_body)?;
    provider.fetch_page(request).await
}
```

- [ ] **Step 4: Re-export the executor API**

Modify `crates/fdc-adapter/barter/src/ingestion/mod.rs` historical re-export block to include:

```rust
execute_binance_spot_ohlcv_rest, HistoricalRestExecutor,
```

Modify `crates/fdc-adapter/barter/src/lib.rs` ingestion re-export block to include:

```rust
execute_binance_spot_ohlcv_rest, HistoricalRestExecutor,
```

- [ ] **Step 5: Run the offline contract**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract
```

Expected: format check passes and the new offline test passes.

- [ ] **Step 6: Commit Task 1**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs
rtk git commit -m "feat: add barter historical rest executor boundary"
```

---

## Task 2: barter-integration REST Client and Gated Real Smoke

**Files:**
- Modify: `crates/fdc-adapter/barter/Cargo.toml`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add the barter-integration dependency**

Modify `crates/fdc-adapter/barter/Cargo.toml` dependencies:

```toml
barter-integration = { path = "/Volumes/wdata/opensource/mountainsea-lab/barter-rs/barter-integration", features = ["error", "metric", "protocol", "serde"] }
```

Do not add `reqwest` directly to `fdc-barter`; it is used through `barter-integration`.

- [ ] **Step 2: Add barter-integration adapter imports**

At the top of `crates/fdc-adapter/barter/src/ingestion/historical.rs`, add these imports near existing imports:

```rust
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use barter_integration::{
    error::SocketError,
    protocol::http::{
        public::PublicNoHeaders,
        rest::{client::RestClient, RestRequest},
        HttpParser,
    },
};
```

If the file already imports `std::{collections::HashMap, sync::Arc}`, merge the import into the combined import above.

- [ ] **Step 3: Implement private Binance Spot RestRequest adapter types**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `HistoricalRestExecutor`, add:

```rust
#[derive(Debug, Clone, serde::Serialize)]
struct BinanceSpotKlinesQuery {
    symbol: String,
    interval: String,
    #[serde(rename = "startTime")]
    start_time: String,
    #[serde(rename = "endTime")]
    end_time: String,
    limit: String,
}

#[derive(Debug, Clone)]
struct BinanceSpotKlinesRestRequest {
    path: String,
    query: BinanceSpotKlinesQuery,
    timeout: Duration,
}

impl BinanceSpotKlinesRestRequest {
    fn from_descriptor(descriptor: &HistoricalRestRequestDescriptor) -> Result<Self> {
        if descriptor.exchange != "binance_spot" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST exchange {}",
                descriptor.exchange
            )));
        }
        if descriptor.method != "GET" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported historical REST method {}",
                descriptor.method
            )));
        }
        if descriptor.path != "/api/v3/klines" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported Binance Spot historical path {}",
                descriptor.path
            )));
        }

        let query_value = |key: &str| -> Result<String> {
            descriptor
                .query
                .iter()
                .find_map(|(candidate, value)| (candidate == key).then(|| value.clone()))
                .ok_or_else(|| BarterAdapterError::HistoricalRest(format!("missing query param {key}")))
        };

        Ok(Self {
            path: descriptor.path.clone(),
            query: BinanceSpotKlinesQuery {
                symbol: query_value("symbol")?,
                interval: query_value("interval")?,
                start_time: query_value("startTime")?,
                end_time: query_value("endTime")?,
                limit: query_value("limit")?,
            },
            timeout: Duration::from_millis(descriptor.timeout_ms),
        })
    }
}

impl RestRequest for BinanceSpotKlinesRestRequest {
    type Response = serde_json::Value;
    type QueryParams = BinanceSpotKlinesQuery;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Owned(self.path.clone())
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.query)
    }

    fn timeout() -> Duration {
        Duration::from_secs(5)
    }
}
```

Note: `RestRequest::method()` requires `reqwest::Method` because that is part of the `barter-integration` trait contract. If Rust requires a direct `reqwest` import to name this type, use `barter_integration`'s transitive type only if it is re-exported; otherwise add `reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }` as an implementation dependency. Prefer avoiding the direct dependency unless the compiler requires it.

- [ ] **Step 4: Implement the parser and public executor**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `BinanceSpotKlinesRestRequest`, add:

```rust
#[derive(Debug, Clone, serde::Deserialize)]
struct BinanceSpotApiError {
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Debug, Clone)]
struct BinanceSpotHistoricalHttpParser;

#[derive(Debug, thiserror::Error)]
enum BinanceSpotHistoricalRestError {
    #[error(transparent)]
    Socket(#[from] SocketError),
    #[error("Binance Spot historical REST API error status={status}: code={code:?} msg={msg:?}")]
    Api {
        status: reqwest::StatusCode,
        code: Option<i64>,
        msg: Option<String>,
    },
}

impl HttpParser for BinanceSpotHistoricalHttpParser {
    type ApiError = BinanceSpotApiError;
    type OutputError = BinanceSpotHistoricalRestError;

    fn parse_api_error(
        &self,
        status: reqwest::StatusCode,
        error: Self::ApiError,
    ) -> Self::OutputError {
        BinanceSpotHistoricalRestError::Api {
            status,
            code: error.code,
            msg: error.msg,
        }
    }
}

impl From<BinanceSpotHistoricalRestError> for BarterAdapterError {
    fn from(error: BinanceSpotHistoricalRestError) -> Self {
        BarterAdapterError::HistoricalRest(error.to_string())
    }
}

/// Historical REST executor backed by barter-integration's RestClient.
#[derive(Debug)]
pub struct BarterIntegrationHistoricalRestExecutor {
    client: RestClient<PublicNoHeaders, BinanceSpotHistoricalHttpParser>,
}

impl BarterIntegrationHistoricalRestExecutor {
    pub fn binance_spot() -> Self {
        Self {
            client: RestClient::new(
                "https://api.binance.com",
                PublicNoHeaders,
                BinanceSpotHistoricalHttpParser,
            ),
        }
    }
}

#[async_trait]
impl HistoricalRestExecutor for BarterIntegrationHistoricalRestExecutor {
    async fn execute(&self, descriptor: &HistoricalRestRequestDescriptor) -> Result<String> {
        let request = BinanceSpotKlinesRestRequest::from_descriptor(descriptor)?;
        let (payload, _metric) = self.client.execute(request).await.map_err(BarterAdapterError::from)?;
        serde_json::to_string(&payload).map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))
    }
}
```

- [ ] **Step 5: Add the failing ignored smoke test**

Append this test to `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`:

```rust
#[tokio::test]
#[ignore = "requires FDC_BARTER_HISTORICAL_SMOKE=1 and public Binance REST access"]
async fn ignored_live_smoke_fetches_one_binance_spot_ohlcv_candle() {
    if std::env::var("FDC_BARTER_HISTORICAL_SMOKE").as_deref() != Ok("1") {
        eprintln!("set FDC_BARTER_HISTORICAL_SMOKE=1 to run real historical REST smoke");
        return;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - 10 * 60_000;
    let end_ms = now_ms - 9 * 60_000;

    let request = HistoricalBackfillRequest {
        source_id: "barter-binance-spot-history-smoke".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Candle,
        interval: Some("1m".to_string()),
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        limit: Some(1),
        cursor: None,
    };

    let executor = fdc_barter::BarterIntegrationHistoricalRestExecutor::binance_spot();
    let page = execute_binance_spot_ohlcv_rest(&executor, request)
        .await
        .expect("real Binance Spot kline smoke should fetch and parse one page");

    assert!(!page.envelopes.is_empty());
    assert!(page.envelopes[0].quality.is_backfill);
    assert_eq!(page.envelopes[0].event.exchange, "binance_spot");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Candle);
}
```

- [ ] **Step 6: Re-export the barter-integration executor**

Modify `crates/fdc-adapter/barter/src/ingestion/mod.rs` historical re-export block to include:

```rust
BarterIntegrationHistoricalRestExecutor,
```

Modify `crates/fdc-adapter/barter/src/lib.rs` ingestion re-export block to include:

```rust
BarterIntegrationHistoricalRestExecutor,
```

- [ ] **Step 7: Run default offline verification**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected:

```text
binance_spot_historical_rest_execution_contract: offline test passes, smoke ignored
fdc-barter: all non-ignored tests pass
```

- [ ] **Step 8: Optionally run real smoke when network is available**

Run only when public Binance REST is reachable:

```bash
FDC_BARTER_HISTORICAL_SMOKE=1 rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract ignored_live_smoke_fetches_one_binance_spot_ohlcv_candle -- --ignored --nocapture
```

Expected: smoke test passes or reports an external network/API availability error. A network failure is not a default-test failure.

- [ ] **Step 9: Update development status**

Append to `docs/DEVELOPMENT_STATUS.md`:

```markdown
## Worktree Checkpoint: fdc-barter Binance Spot Historical OHLCV REST Execution

Last updated: 2026-06-02 task checkpoint
Branch: `mdb-mqdev`
Plan: `docs/superpowers/plans/2026-06-02-fdc-barter-binance-ohlcv-rest-execution.md`

Completed a gated historical REST execution slice for Binance Spot OHLCV.

Completed capabilities:

- Added `HistoricalRestExecutor` as an adapter-owned HTTP execution boundary.
- Added `execute_binance_spot_ohlcv_rest()` to build the Binance Spot kline descriptor, fetch a response body through an injected executor, parse klines, and return `HistoricalBackfillPage`.
- Added `BarterIntegrationHistoricalRestExecutor` backed by `barter-integration`'s `RestClient<PublicNoHeaders, Parser>`.
- Added an offline fake-executor contract test that verifies default tests do not call the network.
- Added an ignored real Binance Spot OHLCV smoke gated by `FDC_BARTER_HISTORICAL_SMOKE=1`.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_rest_execution_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Boundary note:

- Barter-rs REST networking remains reused through `barter-integration`.
- Default verification remains offline and network-free.
```

- [ ] **Step 10: Commit Task 2**

Run:

```bash
rtk git add crates/fdc-adapter/barter/Cargo.toml \
  crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add binance spot historical ohlcv rest execution"
```

---

## Final Verification

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
rtk git status --short --branch
```

Expected:

- Format check passes.
- `fdc-barter` non-ignored tests pass.
- Working tree is clean after commits.

## Self-Review

- Spec coverage: implements the historical REST design's later ignored smoke path without changing storage, API, SQL, or production runtime.
- Placeholder scan: no placeholder steps remain; all commands and code snippets are concrete.
- Type consistency: executor names are consistent across tests, implementation, and exports.
- Boundary check: real HTTP dependency is reused through `barter-integration`; default tests remain offline.
