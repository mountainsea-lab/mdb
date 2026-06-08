# P38 Query API Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden `GET /market-data/trades` with bounded validation, stable response metadata, and deterministic production contract coverage.

**Architecture:** Keep the public route narrow and read-only. Add router-level validation that produces a service-level `ValidatedTradeQuery`, then keep storage access behind the existing `MarketDataQuery` and `QueryableMarketDataStore` abstractions. Extend the trade response model with metadata fields that can later be reused by candle/OHLCV query routes without adding those routes in P38.

**Tech Stack:** Rust, Axum, Tokio, Serde, `fdc-server`, `fdc-storage`, existing production router contract tests.

---

## File Structure

Modify these files only unless a test exposes a genuine boundary bug:

- `crates/fdc-server/src/market_data/model.rs`
  - Extend `MarketDataTradesResponse` with query metadata.
- `crates/fdc-server/src/market_data/service.rs`
  - Add `DEFAULT_TRADE_QUERY_LIMIT`, `MAX_TRADE_QUERY_LIMIT`, `ValidatedTradeQuery`, `TradeQueryValidationError`, `validate_trade_query`, and update `query_trades` to accept validated input.
- `crates/fdc-server/src/market_data/router.rs`
  - Keep `TradeQueryParams` as the HTTP deserialization type, call validation, and return HTTP 400 error envelopes for invalid input.
- `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add P38 route contract tests and small reusable helpers.
- `docs/DEVELOPMENT_STATUS.md`
  - Update only after implementation and verification are complete.

Do not modify `fdc-storage` source for P38 unless the existing query abstraction has a real bug. Do not add `/market-data/candles` or a generic query route.

---

### Task 1: Add failing P38 query metadata and validation contract tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add P38 helper functions near the existing P37 helpers**

Add these helpers after `p37_assert_trade_ids`:

```rust
async fn p38_query_trades_status(
    router: axum::Router,
    uri: &str,
    expected_status: StatusCode,
) -> serde_json::Value {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("trade query request should build"),
        )
        .await
        .expect("trade query should respond");

    assert_eq!(response.status(), expected_status);
    response_body_json(response).await
}

async fn p38_query_trades(router: axum::Router, uri: &str) -> serde_json::Value {
    p38_query_trades_status(router, uri, StatusCode::OK).await
}

async fn p38_ingest_fixture_trades(
    state: &ProductionServerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    state.ingest_test_trade("BTCUSDT", "p38-btc-1").await?;
    state.ingest_test_trade("ETHUSDT", "p38-eth-1").await?;
    state.ingest_test_trade("BTCUSDT", "p38-btc-2").await?;
    Ok(())
}

fn p38_assert_common_trade_metadata(
    json: &serde_json::Value,
    requested_limit: Option<u64>,
    applied_limit: u64,
    symbol: Option<&str>,
) {
    assert_eq!(json["status"], "success");
    assert_eq!(json["message"], serde_json::Value::Null);
    assert_eq!(json["data"]["requested_limit"].as_u64(), requested_limit);
    assert_eq!(json["data"]["applied_limit"], applied_limit);
    assert_eq!(json["data"]["symbol"].as_str(), symbol);
    assert_eq!(json["data"]["data_kind"], "trade");
    assert_eq!(json["data"]["query_source"], "market_data_store");
}
```

- [ ] **Step 2: Add failing default-limit and metadata test**

Add this test near the P37 acceptance tests:

```rust
#[tokio::test]
async fn p38_trades_query_applies_default_limit_and_metadata() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    p38_ingest_fixture_trades(&state)
        .await
        .expect("fixture trades should ingest");

    let router = build_production_router(state);
    let json = p38_query_trades(router, "/market-data/trades").await;

    p38_assert_common_trade_metadata(&json, None, 100, None);
    assert_eq!(json["data"]["returned_records"], 3);
    assert_eq!(p37_trade_ids(&json), BTreeSet::from([
        "p38-btc-1".to_string(),
        "p38-btc-2".to_string(),
        "p38-eth-1".to_string(),
    ]));
}
```

- [ ] **Step 3: Run the new test and verify it fails**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_trades_query_applies_default_limit_and_metadata
```

Expected: FAIL because `requested_limit`, `applied_limit`, `data_kind`, and `query_source` do not exist yet.

- [ ] **Step 4: Add failing normalized symbol test**

Add:

```rust
#[tokio::test]
async fn p38_trades_query_filters_normalized_symbol() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    p38_ingest_fixture_trades(&state)
        .await
        .expect("fixture trades should ingest");

    let router = build_production_router(state);
    let json = p38_query_trades(router, "/market-data/trades?symbol=btcusdt&limit=10").await;

    p38_assert_common_trade_metadata(&json, Some(10), 10, Some("BTCUSDT"));
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(p37_trade_ids(&json), BTreeSet::from([
        "p38-btc-1".to_string(),
        "p38-btc-2".to_string(),
    ]));
}
```

- [ ] **Step 5: Add failing empty-result success test**

Add:

```rust
#[tokio::test]
async fn p38_trades_query_returns_empty_success_for_missing_symbol() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    p38_ingest_fixture_trades(&state)
        .await
        .expect("fixture trades should ingest");

    let router = build_production_router(state);
    let json = p38_query_trades(router, "/market-data/trades?symbol=SOLUSDT&limit=10").await;

    p38_assert_common_trade_metadata(&json, Some(10), 10, Some("SOLUSDT"));
    assert_eq!(json["data"]["returned_records"], 0);
    assert_eq!(json["data"]["records"].as_array().unwrap().len(), 0);
}
```

- [ ] **Step 6: Add failing invalid-limit test**

Add:

```rust
#[tokio::test]
async fn p38_trades_query_rejects_invalid_limits() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    for uri in [
        "/market-data/trades?limit=0",
        "/market-data/trades?limit=1001",
    ] {
        let json = p38_query_trades_status(router.clone(), uri, StatusCode::BAD_REQUEST).await;
        assert_eq!(json["status"], "error");
        assert!(json["message"]
            .as_str()
            .expect("message should be string")
            .contains("limit must be between 1 and 1000"));
        assert_eq!(json["data"]["returned_records"], 0);
        assert_eq!(json["data"]["requested_limit"].is_number(), true);
        assert_eq!(json["data"]["applied_limit"], 100);
        assert_eq!(json["data"]["data_kind"], "trade");
        assert_eq!(json["data"]["query_source"], "market_data_store");
        assert_eq!(json["data"]["records"].as_array().unwrap().len(), 0);
    }
}
```

- [ ] **Step 7: Run the P38 tests and verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_
```

Expected: FAIL because response metadata, validation, and HTTP 400 behavior are not implemented yet.

- [ ] **Step 8: Commit failing tests**

```bash
rtk git add crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "test(server): add p38 query hardening contracts"
```

---

### Task 2: Add query metadata model and validated query service primitives

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Extend `MarketDataTradesResponse`**

In `crates/fdc-server/src/market_data/model.rs`, replace the struct at `MarketDataTradesResponse` with:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataTradesResponse {
    pub returned_records: usize,
    pub requested_limit: Option<usize>,
    pub applied_limit: usize,
    pub symbol: Option<String>,
    pub data_kind: String,
    pub query_source: String,
    pub records: Vec<MarketDataTradeRecord>,
}
```

- [ ] **Step 2: Add validated query types and constants**

In `crates/fdc-server/src/market_data/service.rs`, near the other market-data service helper types, add:

```rust
pub const DEFAULT_TRADE_QUERY_LIMIT: usize = 100;
pub const MAX_TRADE_QUERY_LIMIT: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedTradeQuery {
    pub symbol: Option<String>,
    pub requested_limit: Option<usize>,
    pub applied_limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeQueryValidationError {
    pub message: String,
    pub requested_limit: Option<usize>,
    pub symbol: Option<String>,
}

impl TradeQueryValidationError {
    fn new(
        message: impl Into<String>,
        requested_limit: Option<usize>,
        symbol: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            requested_limit,
            symbol,
        }
    }
}
```

- [ ] **Step 3: Add validation function**

In the same file, add:

```rust
pub fn validate_trade_query(
    symbol: Option<String>,
    limit: Option<usize>,
) -> std::result::Result<ValidatedTradeQuery, TradeQueryValidationError> {
    let normalized_symbol = match symbol {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(TradeQueryValidationError::new(
                    "symbol must not be empty",
                    limit,
                    None,
                ));
            }
            Some(trimmed.to_ascii_uppercase())
        }
        None => None,
    };

    let applied_limit = match limit {
        Some(0) => {
            return Err(TradeQueryValidationError::new(
                "limit must be between 1 and 1000",
                limit,
                normalized_symbol,
            ));
        }
        Some(value) if value > MAX_TRADE_QUERY_LIMIT => {
            return Err(TradeQueryValidationError::new(
                "limit must be between 1 and 1000",
                limit,
                normalized_symbol,
            ));
        }
        Some(value) => value,
        None => DEFAULT_TRADE_QUERY_LIMIT,
    };

    Ok(ValidatedTradeQuery {
        symbol: normalized_symbol,
        requested_limit: limit,
        applied_limit,
    })
}
```

- [ ] **Step 4: Add empty/error response helper**

In `service.rs`, add:

```rust
pub fn empty_trade_query_response(
    requested_limit: Option<usize>,
    applied_limit: usize,
    symbol: Option<String>,
) -> MarketDataTradesResponse {
    MarketDataTradesResponse {
        returned_records: 0,
        requested_limit,
        applied_limit,
        symbol,
        data_kind: "trade".to_string(),
        query_source: "market_data_store".to_string(),
        records: Vec::new(),
    }
}
```

- [ ] **Step 5: Update `query_trades` signature and body**

Replace the existing `query_trades` function with:

```rust
pub fn query_trades(
    state: &ProductionServerState,
    validated: ValidatedTradeQuery,
) -> MarketDataTradesResponse {
    let mut query = MarketDataQuery::for_trades().with_limit(validated.applied_limit);
    if let Some(symbol) = validated.symbol.clone() {
        query = query.with_symbol(symbol);
    }

    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_trade_record)
        .collect();

    MarketDataTradesResponse {
        returned_records: records.len(),
        requested_limit: validated.requested_limit,
        applied_limit: validated.applied_limit,
        symbol: validated.symbol,
        data_kind: "trade".to_string(),
        query_source: "market_data_store".to_string(),
        records,
    }
}
```

- [ ] **Step 6: Run compile-focused test and verify router errors remain**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_trades_query_applies_default_limit_and_metadata
```

Expected: compile FAIL in `router.rs` because `query_trades` now expects `ValidatedTradeQuery` and router has not been updated.

Do not commit until Task 3 updates the router and tests pass.

---

### Task 3: Wire router validation and HTTP 400 error envelopes

**Files:**
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs` only if compiler requires import visibility adjustments

- [ ] **Step 1: Update service imports in router**

In `crates/fdc-server/src/market_data/router.rs`, update the `service::{ ... }` import list to include:

```rust
empty_trade_query_response, validate_trade_query, DEFAULT_TRADE_QUERY_LIMIT,
```

The relevant import block should include these names alongside `query_trades`.

- [ ] **Step 2: Replace query handler signature and body**

Replace `query_trades_handler` with:

```rust
async fn query_trades_handler(
    State(state): State<ProductionServerState>,
    Query(params): Query<TradeQueryParams>,
) -> (StatusCode, Json<ServerApiResponse<MarketDataTradesResponse>>) {
    match validate_trade_query(params.symbol, params.limit) {
        Ok(validated) => (
            StatusCode::OK,
            Json(ServerApiResponse::success(query_trades(&state, validated))),
        ),
        Err(error) => {
            let data = empty_trade_query_response(
                error.requested_limit,
                DEFAULT_TRADE_QUERY_LIMIT,
                error.symbol,
            );
            (
                StatusCode::BAD_REQUEST,
                Json(ServerApiResponse::error(data, error.message)),
            )
        }
    }
}
```

- [ ] **Step 3: Run focused P38 tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_
```

Expected: the P38 tests from Task 1 PASS. If ordering differences make `p37_trade_ids` set assertions pass but returned count differs, inspect whether the existing store applies `with_limit` before or after filtering. Keep the public contract as filter + applied limit.

- [ ] **Step 4: Run existing P37 query acceptance tests**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_
```

Expected: PASS. Existing P37 assertions only depend on `returned_records` and `records`, which remain present.

- [ ] **Step 5: Format server package**

Run:

```bash
rtk cargo fmt -p fdc-server -- --check
```

Expected: PASS. If it fails, run:

```bash
rtk cargo fmt -p fdc-server
rtk cargo fmt -p fdc-server -- --check
```

- [ ] **Step 6: Commit model, service, router implementation**

```bash
rtk git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat(server): harden trade query api"
```

---

### Task 4: Add durable reopen and operational read-only P38 regression tests

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add durable reopen P38 test**

Add this test near P37 durable acceptance coverage:

```rust
#[tokio::test]
async fn p38_trades_query_survives_durable_reopen_with_metadata() {
    let root = unique_test_path("p38-query-reopen");
    let config = p37_durable_config(&root, &[]);

    let state = ProductionServerState::try_new(config.clone())
        .await
        .expect("production state should build");
    p38_ingest_fixture_trades(&state)
        .await
        .expect("fixture trades should ingest");
    drop(state);

    let reopened = ProductionServerState::try_new(config)
        .await
        .expect("reopened production state should build");
    let router = build_production_router(reopened);
    let json = p38_query_trades(router, "/market-data/trades?symbol=BTCUSDT&limit=10").await;

    p38_assert_common_trade_metadata(&json, Some(10), 10, Some("BTCUSDT"));
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(p37_trade_ids(&json), BTreeSet::from([
        "p38-btc-1".to_string(),
        "p38-btc-2".to_string(),
    ]));

    let _ = std::fs::remove_dir_all(root);
}
```

- [ ] **Step 2: Add operational read-only regression test**

Add:

```rust
#[tokio::test]
async fn p38_trades_query_is_read_only_for_operational_controls() {
    let state = tiered_storage_state_with_audit_capacity(8).await;
    p38_ingest_fixture_trades(&state)
        .await
        .expect("fixture trades should ingest");

    let initial_count = state.market_data_store().record_count();
    let router = build_production_router(state.clone());
    let before = p38_query_trades(
        router.clone(),
        "/market-data/trades?symbol=BTCUSDT&limit=10",
    )
    .await;

    run_successful_storage_maintenance(router.clone()).await;

    let after = p38_query_trades(
        router.clone(),
        "/market-data/trades?symbol=BTCUSDT&limit=10",
    )
    .await;

    assert_eq!(state.market_data_store().record_count(), initial_count);
    p38_assert_common_trade_metadata(&after, Some(10), 10, Some("BTCUSDT"));
    assert_eq!(p37_trade_ids(&before), p37_trade_ids(&after));
}
```

This test focuses on the query route plus manual maintenance safety. Do not start public-network live collection.

- [ ] **Step 3: Run P38 suite**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_
```

Expected: all P38 tests PASS.

- [ ] **Step 4: Run P37 suite**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_
```

Expected: PASS.

- [ ] **Step 5: Commit P38 regression tests**

```bash
rtk git add crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "test(server): cover p38 durable and read-only query regressions"
```

---

### Task 5: Run full focused verification and update development status

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused server query verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_
```

Expected: all P38 tests PASS.

- [ ] **Step 2: Run P37 regression verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p37_
```

Expected: all P37 tests PASS.

- [ ] **Step 3: Run live resume regression verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
```

Expected: PASS.

- [ ] **Step 4: Run storage maintenance regression verification**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
```

Expected: both commands PASS.

- [ ] **Step 5: Run storage boundary guard**

Run:

```bash
rtk cargo test -p fdc-storage --test dependency_guard
```

Expected: PASS.

- [ ] **Step 6: Run formatting check**

Run:

```bash
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: PASS.

- [ ] **Step 7: Update `docs/DEVELOPMENT_STATUS.md`**

Add a new section above P37:

```markdown
## 2026-06-08 P38 Query API Production Hardening

Completed:

- Hardened `GET /market-data/trades` with explicit query validation.
- Added default and maximum trade query limits.
- Normalized valid symbol filters and rejected empty symbols.
- Returned stable query metadata: requested limit, applied limit, normalized symbol, data kind, and query source.
- Returned deterministic HTTP 400 error envelopes for invalid limits.
- Verified empty query results remain successful and deterministic.
- Verified tiered/durable readback still returns metadata after reopen.
- Verified query route remains read-only around storage maintenance regression coverage.
- Preserved the `fdc-storage` boundary.

Design and plan:

- `docs/superpowers/specs/2026-06-08-query-api-production-hardening-design.md`
- `docs/superpowers/plans/2026-06-08-query-api-production-hardening.md`

Commits:

After implementation, copy the actual short hashes and subjects from `rtk git log --oneline -n 10` into this section. The expected subjects are:

- `test(server): add p38 query hardening contracts`
- `feat(server): harden trade query api`
- `test(server): cover p38 durable and read-only query regressions`

Verification:

- `rtk cargo test -p fdc-server --test production_server_router_contract p38_` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract p37_` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once` - passed
- `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume` - passed
- `rtk cargo test -p fdc-storage --test dependency_guard` - passed
- `rtk cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- P39 Production Runbook, Config Pack, and Soak Validation.
```

Replace `<commit>` placeholders with actual short commit hashes from:

```bash
rtk git log --oneline -n 10
```

- [ ] **Step 8: Commit status documentation**

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record p38 query hardening completion"
```

---

### Task 6: Final review before branch completion

**Files:**
- No source file changes expected unless verification exposes a bug.

- [ ] **Step 1: Check working tree**

Run:

```bash
rtk git status --short --branch
```

Expected: clean working tree on `mdb-mqdev` or the isolated P38 worktree branch.

- [ ] **Step 2: Review recent commits**

Run:

```bash
rtk git log --oneline -n 8
```

Expected: P38 commits are present in order: tests, implementation, regressions, status docs.

- [ ] **Step 3: Verify no unintended storage source changes**

Run:

```bash
git diff --name-only HEAD~4..HEAD
```

Expected changed files are limited to:

```text
crates/fdc-server/src/market_data/model.rs
crates/fdc-server/src/market_data/router.rs
crates/fdc-server/src/market_data/service.rs
crates/fdc-server/tests/production_server_router_contract.rs
docs/DEVELOPMENT_STATUS.md
```

The plan file and spec file commits may also appear if reviewing a wider commit range.

- [ ] **Step 4: If executing in an isolated worktree, merge or present branch completion options**

Use the finishing workflow after all verification passes. Do not claim P38 complete until the verification commands in Task 5 have passed and `docs/DEVELOPMENT_STATUS.md` has been committed.
