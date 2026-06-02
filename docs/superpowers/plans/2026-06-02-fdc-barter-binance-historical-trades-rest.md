# fdc-barter Binance Spot Historical Trades REST Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Binance Spot historical aggregate trades REST acquisition to `fdc-barter` with offline contracts and an ignored real REST smoke.

**Architecture:** Keep all exchange-specific REST request, parser, and provider logic inside `fdc-barter`. Reuse `HistoricalBackfillRequest`, `HistoricalRestExecutor`, and `barter-integration::RestClient` patterns already used by the OHLCV REST slice. Map Binance `/api/v3/aggTrades` rows into adapter-owned `BarterMarketPayload::Trade` envelopes.

**Tech Stack:** Rust, `async-trait`, `barter-integration`, `serde_json`, `rust_decimal`, `tokio`, offline contract tests, ignored/env-gated Binance smoke.

---

## File Structure

### Modify

- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
  - Add Binance Spot aggregate trades capabilities, descriptor, parser/provider, execution helper, and barter-integration request adapter.
- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
  - Re-export new historical trades helpers and provider type.
- `crates/fdc-adapter/barter/src/lib.rs`
  - Re-export new public APIs.
- `docs/DEVELOPMENT_STATUS.md`
  - Append a completed checkpoint after verification.

### Create

- `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`
  - Offline descriptor/parser/provider/fake-executor tests plus ignored real REST smoke.

---

## Task 1: Descriptor and Offline Provider Contract

**Files:**
- Create: `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`

- [ ] **Step 1: Write failing descriptor and provider tests**

Create `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`:

```rust
use fdc_barter::{
    binance_spot_historical_trades_provider_from_response,
    binance_spot_historical_trades_rest_request_descriptor, historical_trade_dedupe_key,
    BarterMarketDataKind, BarterMarketPayload, BarterMarketType, HistoricalBackfillRequest,
    HistoricalExchangeProvider, TradeSide,
};
use fdc_core::types::TimestampNs;

fn trade_request() -> HistoricalBackfillRequest {
    HistoricalBackfillRequest {
        source_id: "barter-binance-spot-trades-history".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(1_700_000_000_000_000_000),
        end: TimestampNs::from_nanos(1_700_000_060_000_000_000),
        limit: Some(2),
        cursor: None,
    }
}

fn sample_agg_trades() -> &'static str {
    r#"[
        {"a":26129,"p":"100.10","q":"0.25000000","f":27781,"l":27781,"T":1700000000000,"m":true,"M":true},
        {"a":26130,"p":"101.20","q":"1.50000000","f":27782,"l":27783,"T":1700000001000,"m":false,"M":true}
    ]"#
}

#[test]
fn binance_spot_historical_trades_descriptor_matches_agg_trades_shape() {
    let descriptor = binance_spot_historical_trades_rest_request_descriptor(&trade_request())
        .expect("valid trade request should build descriptor");

    assert_eq!(descriptor.exchange, "binance_spot");
    assert_eq!(descriptor.method, "GET");
    assert_eq!(descriptor.path, "/api/v3/aggTrades");
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

#[test]
fn binance_spot_historical_trades_descriptor_rejects_unsupported_shape() {
    let mut candle = trade_request();
    candle.kind = BarterMarketDataKind::Candle;
    assert!(binance_spot_historical_trades_rest_request_descriptor(&candle).is_err());

    let mut futures = trade_request();
    futures.market_type = BarterMarketType::Perpetual;
    assert!(binance_spot_historical_trades_rest_request_descriptor(&futures).is_err());

    let mut high_limit = trade_request();
    high_limit.limit = Some(1001);
    let error = binance_spot_historical_trades_rest_request_descriptor(&high_limit)
        .expect_err("Binance aggregate trades max limit is 1000");
    assert!(error.to_string().contains("limit"));
}

#[tokio::test]
async fn binance_spot_historical_trades_provider_maps_agg_trades_to_trade_envelopes() {
    let provider = binance_spot_historical_trades_provider_from_response(sample_agg_trades())
        .expect("sample aggregate trades should parse");

    let page = provider
        .fetch_page(trade_request())
        .await
        .expect("provider should return parsed trades");

    assert_eq!(page.envelopes.len(), 2);
    assert!(!page.complete);
    assert_eq!(
        page.next_cursor.as_ref().unwrap().next_start.unwrap().as_nanos(),
        1_700_000_001_001_000_000
    );

    let first = &page.envelopes[0];
    assert!(first.quality.is_backfill);
    assert_eq!(first.event.exchange, "binance_spot");
    assert_eq!(first.event.symbol.as_str(), "BTCUSDT");
    assert_eq!(first.event.timestamp.as_nanos(), 1_700_000_000_000_000_000);
    assert_eq!(first.event.sequence.as_deref(), Some("26129"));

    let BarterMarketPayload::Trade(trade) = &first.event.payload else {
        panic!("expected trade payload");
    };
    assert_eq!(trade.trade_id.as_deref(), Some("26129"));
    assert_eq!(trade.price.to_f64(), 100.10);
    assert_eq!(trade.quantity.to_string(), "0.25000000");
    assert_eq!(trade.side, Some(TradeSide::Sell));
    assert_eq!(historical_trade_dedupe_key(&first.event).unwrap(), "binance_spot:BTCUSDT:26129");

    let BarterMarketPayload::Trade(second) = &page.envelopes[1].event.payload else {
        panic!("expected trade payload");
    };
    assert_eq!(second.side, Some(TradeSide::Buy));
}

#[tokio::test]
async fn binance_spot_historical_trades_provider_marks_complete_when_short_page() {
    let provider = binance_spot_historical_trades_provider_from_response(sample_agg_trades())
        .expect("sample aggregate trades should parse");

    let mut request = trade_request();
    request.limit = Some(3);
    let page = provider.fetch_page(request).await.expect("provider should return page");

    assert!(page.complete);
    assert!(page.next_cursor.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract
```

Expected: compile failure because the new descriptor/provider functions are not defined.

- [ ] **Step 3: Add capabilities and descriptor**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `binance_spot_ohlcv_capabilities()`, add:

```rust
pub fn binance_spot_historical_trades_capabilities() -> HistoricalProviderCapabilities {
    HistoricalProviderCapabilities {
        exchange: "binance_spot".to_string(),
        market_types: vec![BarterMarketType::Spot],
        kinds: vec![BarterMarketDataKind::Trade],
        intervals: Vec::new(),
        max_limit: Some(1000),
    }
}

pub fn binance_spot_historical_trades_rest_request_descriptor(
    request: &HistoricalBackfillRequest,
) -> Result<HistoricalRestRequestDescriptor> {
    validate_historical_backfill_request(request)?;

    let capabilities = binance_spot_historical_trades_capabilities();
    capabilities.validate_request(request)?;

    if normalize_exchange(&request.exchange) != capabilities.normalized_exchange() {
        return Err(BarterAdapterError::UnsupportedHistoricalExchange(
            request.exchange.clone(),
        ));
    }

    Ok(HistoricalRestRequestDescriptor {
        exchange: capabilities.exchange,
        method: "GET".to_string(),
        path: "/api/v3/aggTrades".to_string(),
        query: vec![
            ("symbol".to_string(), request.symbol.to_ascii_uppercase()),
            (
                "startTime".to_string(),
                nanos_to_millis(request.start).to_string(),
            ),
            (
                "endTime".to_string(),
                nanos_to_millis(request.end).to_string(),
            ),
            (
                "limit".to_string(),
                request.limit.unwrap_or(500).to_string(),
            ),
        ],
        timeout_ms: 5_000,
    })
}
```

- [ ] **Step 4: Add provider and parser**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `BinanceSpotOhlcvProvider` implementation block, add:

```rust
#[derive(Debug, Clone)]
pub struct BinanceSpotHistoricalTradesProvider {
    capabilities: HistoricalProviderCapabilities,
    rows: Vec<BinanceSpotAggTradeRow>,
}

impl BinanceSpotHistoricalTradesProvider {
    pub fn from_response_body(response_body: &str) -> Result<Self> {
        let rows = parse_binance_spot_agg_trades_response(response_body)?;
        Ok(Self {
            capabilities: binance_spot_historical_trades_capabilities(),
            rows,
        })
    }
}

#[async_trait]
impl HistoricalExchangeProvider for BinanceSpotHistoricalTradesProvider {
    fn capabilities(&self) -> &HistoricalProviderCapabilities {
        &self.capabilities
    }

    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        validate_historical_backfill_request(&request)?;
        self.capabilities.validate_request(&request)?;

        let mut envelopes = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let event = row.to_market_event(&request)?;
            envelopes.push(BarterIngestionEnvelope::from_backfill_event(
                request.source_id.clone(),
                event,
            ));
        }

        let complete = request.limit.map(|limit| envelopes.len() < limit).unwrap_or(true);
        let next_cursor = if complete {
            None
        } else {
            self.rows.last().map(|row| {
                HistoricalCursor::next_start(
                    request.exchange.clone(),
                    request.symbol.clone(),
                    request.kind,
                    TimestampNs::from_nanos(millis_to_nanos(row.time_ms + 1)),
                )
            })
        };

        Ok(HistoricalBackfillPage {
            request,
            envelopes,
            next_cursor,
            complete,
        })
    }
}

pub fn binance_spot_historical_trades_provider_from_response(
    response_body: &str,
) -> Result<BinanceSpotHistoricalTradesProvider> {
    BinanceSpotHistoricalTradesProvider::from_response_body(response_body)
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceSpotAggTradeRow {
    #[serde(rename = "a")]
    aggregate_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "T")]
    time_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

impl BinanceSpotAggTradeRow {
    fn to_market_event(&self, request: &HistoricalBackfillRequest) -> Result<BarterMarketEvent> {
        let event_time = TimestampNs::from_nanos(millis_to_nanos(self.time_ms));
        let cursor = HistoricalCursor::next_start(
            request.exchange.clone(),
            request.symbol.clone(),
            request.kind,
            TimestampNs::from_nanos(millis_to_nanos(self.time_ms + 1)),
        );

        Ok(BarterMarketEvent {
            source: request.source_id.clone(),
            mode: BarterMarketDataMode::Historical,
            exchange: normalize_exchange(&request.exchange),
            symbol: Symbol::new(&request.symbol),
            market_type: request.market_type,
            kind: BarterMarketDataKind::Trade,
            timestamp: event_time,
            received_at: TimestampNs::now(),
            payload: BarterMarketPayload::Trade(TradePayload {
                trade_id: Some(self.aggregate_trade_id.to_string()),
                price: Price::new(self.price.parse::<Decimal>().map_err(|_| {
                    BarterAdapterError::HistoricalRest(format!(
                        "invalid numeric value for field price: {}",
                        self.price
                    ))
                })?),
                quantity: self.quantity.parse::<Decimal>().map_err(|_| {
                    BarterAdapterError::HistoricalRest(format!(
                        "invalid numeric value for field quantity: {}",
                        self.quantity
                    ))
                })?,
                side: Some(if self.buyer_is_maker {
                    TradeSide::Sell
                } else {
                    TradeSide::Buy
                }),
            }),
            sequence: Some(self.aggregate_trade_id.to_string()),
            checkpoint: Some(crate::model::BarterCheckpoint {
                source_id: request.source_id.clone(),
                exchange: normalize_exchange(&request.exchange),
                symbol: request.symbol.to_ascii_uppercase(),
                kind: BarterMarketDataKind::Trade,
                mode: BarterMarketDataMode::Historical,
                last_event_time: event_time,
                cursor: Some(cursor),
                updated_at: TimestampNs::now(),
            }),
        })
    }
}

fn parse_binance_spot_agg_trades_response(
    response_body: &str,
) -> Result<Vec<BinanceSpotAggTradeRow>> {
    serde_json::from_str(response_body)
        .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))
}
```

- [ ] **Step 5: Add missing model imports**

At the top of `historical.rs`, extend the `model` import list to include:

```rust
TradePayload, TradeSide,
```

- [ ] **Step 6: Re-export provider APIs**

Add these names to the historical re-export blocks in both `crates/fdc-adapter/barter/src/ingestion/mod.rs` and `crates/fdc-adapter/barter/src/lib.rs`:

```rust
binance_spot_historical_trades_capabilities,
binance_spot_historical_trades_provider_from_response,
binance_spot_historical_trades_rest_request_descriptor,
BinanceSpotHistoricalTradesProvider,
```

- [ ] **Step 7: Verify Task 1**

Run:

```bash
rtk cargo fmt --package fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract
```

Expected: all four new tests pass.

- [ ] **Step 8: Commit Task 1**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs
rtk git commit -m "feat: add binance spot historical trades parser"
```

---

## Task 2: REST Execution Through barter-integration and Smoke

**Files:**
- Modify: `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- Modify: `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- Modify: `crates/fdc-adapter/barter/src/lib.rs`
- Modify: `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add fake executor test and ignored smoke test**

Append to `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`:

```rust
use async_trait::async_trait;
use fdc_barter::{
    execute_binance_spot_historical_trades_rest, HistoricalRestExecutor,
    HistoricalRestRequestDescriptor,
};

struct FakeTradesExecutor;

#[async_trait]
impl HistoricalRestExecutor for FakeTradesExecutor {
    async fn execute(
        &self,
        descriptor: &HistoricalRestRequestDescriptor,
    ) -> fdc_barter::Result<String> {
        assert_eq!(descriptor.exchange, "binance_spot");
        assert_eq!(descriptor.method, "GET");
        assert_eq!(descriptor.path, "/api/v3/aggTrades");
        assert!(descriptor
            .query
            .contains(&("symbol".to_string(), "BTCUSDT".to_string())));
        Ok(sample_agg_trades().to_string())
    }
}

#[tokio::test]
async fn fake_executor_fetches_binance_spot_historical_trades_without_network() {
    let page = execute_binance_spot_historical_trades_rest(&FakeTradesExecutor, trade_request())
        .await
        .expect("fake executor response should parse into historical trades");

    assert_eq!(page.envelopes.len(), 2);
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Trade);
}

#[tokio::test]
#[ignore = "requires FDC_BARTER_HISTORICAL_SMOKE=1 and public Binance REST access"]
async fn ignored_live_smoke_fetches_binance_spot_historical_trades() {
    if std::env::var("FDC_BARTER_HISTORICAL_SMOKE").as_deref() != Ok("1") {
        eprintln!("set FDC_BARTER_HISTORICAL_SMOKE=1 to run real historical trades smoke");
        return;
    }

    let now_ms = chrono::Utc::now().timestamp_millis();
    let start_ms = now_ms - 10 * 60_000;
    let end_ms = now_ms - 9 * 60_000;

    let request = HistoricalBackfillRequest {
        source_id: "barter-binance-spot-trades-history-smoke".to_string(),
        exchange: "binance_spot".to_string(),
        market_type: BarterMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        kind: BarterMarketDataKind::Trade,
        interval: None,
        start: TimestampNs::from_nanos(start_ms * 1_000_000),
        end: TimestampNs::from_nanos(end_ms * 1_000_000),
        limit: Some(10),
        cursor: None,
    };

    let executor = fdc_barter::BarterIntegrationHistoricalRestExecutor::binance_spot();
    let page = execute_binance_spot_historical_trades_rest(&executor, request)
        .await
        .expect("real Binance Spot aggregate trades smoke should fetch and parse one page");

    assert!(!page.envelopes.is_empty());
    assert!(page.envelopes[0].quality.is_backfill);
    assert_eq!(page.envelopes[0].event.exchange, "binance_spot");
    assert_eq!(page.envelopes[0].event.kind, BarterMarketDataKind::Trade);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract
```

Expected: compile failure because `execute_binance_spot_historical_trades_rest` is not defined/exported and `BarterIntegrationHistoricalRestExecutor` does not route aggregate trades.

- [ ] **Step 3: Add aggregate trades RestRequest adapter**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `BinanceSpotKlinesRestRequest`, add:

```rust
#[derive(Debug, Clone, Serialize)]
struct BinanceSpotAggTradesQuery {
    symbol: String,
    #[serde(rename = "startTime")]
    start_time: String,
    #[serde(rename = "endTime")]
    end_time: String,
    limit: String,
}

#[derive(Debug, Clone)]
struct BinanceSpotAggTradesRestRequest {
    path: String,
    query: BinanceSpotAggTradesQuery,
}

impl BinanceSpotAggTradesRestRequest {
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
        if descriptor.path != "/api/v3/aggTrades" {
            return Err(BarterAdapterError::HistoricalRest(format!(
                "unsupported Binance Spot historical trades path {}",
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
            query: BinanceSpotAggTradesQuery {
                symbol: query_value("symbol")?,
                start_time: query_value("startTime")?,
                end_time: query_value("endTime")?,
                limit: query_value("limit")?,
            },
        })
    }
}

impl RestRequest for BinanceSpotAggTradesRestRequest {
    type Response = serde_json::Value;
    type QueryParams = BinanceSpotAggTradesQuery;
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

- [ ] **Step 4: Add execution helper**

In `crates/fdc-adapter/barter/src/ingestion/historical.rs`, after `execute_binance_spot_ohlcv_rest()`, add:

```rust
pub async fn execute_binance_spot_historical_trades_rest(
    executor: &dyn HistoricalRestExecutor,
    request: HistoricalBackfillRequest,
) -> Result<HistoricalBackfillPage> {
    let descriptor = binance_spot_historical_trades_rest_request_descriptor(&request)?;
    let response_body = executor.execute(&descriptor).await?;
    let provider = binance_spot_historical_trades_provider_from_response(&response_body)?;
    provider.fetch_page(request).await
}
```

- [ ] **Step 5: Extend BarterIntegrationHistoricalRestExecutor routing**

Modify `impl HistoricalRestExecutor for BarterIntegrationHistoricalRestExecutor` in `historical.rs`:

```rust
#[async_trait]
impl HistoricalRestExecutor for BarterIntegrationHistoricalRestExecutor {
    async fn execute(&self, descriptor: &HistoricalRestRequestDescriptor) -> Result<String> {
        let payload = match descriptor.path.as_str() {
            "/api/v3/klines" => {
                let request = BinanceSpotKlinesRestRequest::from_descriptor(descriptor)?;
                let (payload, _metric) = self
                    .client
                    .execute(request)
                    .await
                    .map_err(BarterAdapterError::from)?;
                payload
            }
            "/api/v3/aggTrades" => {
                let request = BinanceSpotAggTradesRestRequest::from_descriptor(descriptor)?;
                let (payload, _metric) = self
                    .client
                    .execute(request)
                    .await
                    .map_err(BarterAdapterError::from)?;
                payload
            }
            unsupported => {
                return Err(BarterAdapterError::HistoricalRest(format!(
                    "unsupported Binance Spot historical REST path {unsupported}"
                )));
            }
        };

        serde_json::to_string(&payload)
            .map_err(|error| BarterAdapterError::HistoricalRest(error.to_string()))
    }
}
```

- [ ] **Step 6: Re-export execution helper**

Add this name to historical re-export blocks in both `crates/fdc-adapter/barter/src/ingestion/mod.rs` and `crates/fdc-adapter/barter/src/lib.rs`:

```rust
execute_binance_spot_historical_trades_rest,
```

- [ ] **Step 7: Verify Task 2**

Run:

```bash
rtk cargo fmt --package fdc-barter
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: trades contract passes with one ignored smoke; full `fdc-barter` suite passes with default offline tests.

- [ ] **Step 8: Optional real smoke**

Run only when public Binance REST is reachable:

```bash
FDC_BARTER_HISTORICAL_SMOKE=1 rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract ignored_live_smoke_fetches_binance_spot_historical_trades -- --ignored --nocapture
```

Expected: smoke passes or reports external network/API availability failure. This is not part of default verification.

- [ ] **Step 9: Update development status**

Append to `docs/DEVELOPMENT_STATUS.md`:

```markdown
## Worktree Checkpoint: fdc-barter Binance Spot Historical Trades REST

Last updated: 2026-06-02 task checkpoint
Branch: `main`
Design: `docs/superpowers/specs/2026-06-02-fdc-barter-binance-historical-trades-rest-design.md`
Plan: `docs/superpowers/plans/2026-06-02-fdc-barter-binance-historical-trades-rest.md`

Completed a Binance Spot historical aggregate trades REST slice.

Completed capabilities:

- Added Binance Spot `/api/v3/aggTrades` descriptor for historical trade backfill requests.
- Added `BinanceSpotHistoricalTradesProvider` to parse aggregate trade JSON into historical `TradePayload` envelopes.
- Added `execute_binance_spot_historical_trades_rest()` for executor-backed trade page acquisition.
- Extended `BarterIntegrationHistoricalRestExecutor` to route aggregate trades through `barter-integration::RestClient`.
- Added offline descriptor/parser/provider/fake-executor tests and an ignored real REST smoke gated by `FDC_BARTER_HISTORICAL_SMOKE=1`.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_trades_rest_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```
```

- [ ] **Step 10: Commit Task 2**

Run:

```bash
rtk git add crates/fdc-adapter/barter/src/ingestion/historical.rs \
  crates/fdc-adapter/barter/src/ingestion/mod.rs \
  crates/fdc-adapter/barter/src/lib.rs \
  crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs \
  docs/DEVELOPMENT_STATUS.md
rtk git commit -m "feat: add binance spot historical trades rest execution"
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

- Spec coverage: descriptor, parser/provider, executor, smoke, and status docs are all covered.
- Placeholder scan: no placeholders remain.
- Type consistency: function and provider names match the spec.
- Boundary check: REST networking continues to use `barter-integration`; no downstream crate dependencies are added.
