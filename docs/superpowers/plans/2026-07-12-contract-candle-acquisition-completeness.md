# Contract Candle Acquisition Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the contract candle acquisition business loop so operators can trigger Binance Futures USD candle collection, observe it, and prove collected candles are readable from canonical candle storage for future factor research.

**Architecture:** Reuse the existing `ProductionServerState` contract acquisition runner and router/service patterns. Add a manual run-once path that uses runtime config, records last-run/last-error state, and returns a run summary. Validate the canonical data-source contract through tests that query existing candle storage, not checkpoint/audit metadata.

**Tech Stack:** Rust, Axum, Tokio, fdc-server, fdc-storage, fdc-barter historical fetcher abstractions, existing integration-style contract tests.

---

## File Structure

- Modify `crates/fdc-server/src/runtime/app.rs`
  - Add a manual contract acquisition method that runs once when config is enabled without requiring `autostart=true`.
- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add `MarketDataContractAcquisitionRunOnceResponse` or reuse a run status DTO with `market_data_store_records`.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Add `run_contract_acquisition_once` service function that maps success/error into response data and message.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add `POST /market-data/contracts/acquisition/run-once` handler.
- Modify `crates/fdc-server/tests/contract_acquisition_contract.rs`
  - Add/extend canonical candle query validation after acquisition.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route tests for disabled/manual run-once behavior and last-run/status visibility.
- Modify `docs/runbooks/market-data-production-runbook.md`
  - Document run-once operator flow and canonical candle query verification.

---

### Task 1: Add state/service support for manual contract acquisition run-once

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Test: `crates/fdc-server/tests/contract_acquisition_contract.rs`

- [ ] **Step 1: Add a state-level method that does not require autostart**

In `ProductionServerState`, add:

```rust
pub async fn run_contract_acquisition_once_if_enabled(
    &self,
) -> Result<crate::market_data::contract_acquisition::ContractAcquisitionRunStatus> {
    if !self.config.market_data_contract_acquisition.enabled {
        let result = Ok(crate::market_data::contract_acquisition::ContractAcquisitionRunStatus::default());
        self.record_contract_acquisition_result(&result);
        return result;
    }

    let result = crate::market_data::contract_acquisition::run_binance_futures_usd_contract_candle_acquisition_once(
        &self.config.market_data_contract_acquisition,
        self.market_data_store.as_ref(),
    )
    .await;
    self.record_contract_acquisition_result(&result);
    result
}
```

If the file already has long lines nearby, run `rustfmt --edition 2021 crates/fdc-server/src/runtime/app.rs` after editing.

- [ ] **Step 2: Add a run-once response DTO**

In `crates/fdc-server/src/market_data/model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MarketDataContractAcquisitionRunOnceResponse {
    pub enabled: bool,
    pub exchange: String,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
    pub tasks_started: usize,
    pub tasks_completed: usize,
    pub pages_fetched: usize,
    pub envelopes_received: usize,
    pub storage_records_written: usize,
    pub audit_records_written: usize,
    pub final_cursors: usize,
    pub market_data_store_records: usize,
}
```

- [ ] **Step 3: Add service mapping**

In `crates/fdc-server/src/market_data/service.rs`, import the new DTO and add:

```rust
pub async fn run_contract_acquisition_once(
    state: &ProductionServerState,
) -> std::result::Result<MarketDataContractAcquisitionRunOnceResponse, String> {
    let config = &state.config().market_data_contract_acquisition;
    if !config.enabled {
        return Err("contract candle acquisition is disabled".to_string());
    }

    let status = state
        .run_contract_acquisition_once_if_enabled()
        .await
        .map_err(|error| error.to_string())?;

    Ok(MarketDataContractAcquisitionRunOnceResponse {
        enabled: config.enabled,
        exchange: config.exchange.clone(),
        symbols: config.symbols.clone(),
        intervals: config.intervals.clone(),
        tasks_started: status.tasks_started,
        tasks_completed: status.tasks_completed,
        pages_fetched: status.pages_fetched,
        envelopes_received: status.envelopes_received,
        storage_records_written: status.storage_records_written,
        audit_records_written: status.audit_records_written,
        final_cursors: status.final_cursors.len(),
        market_data_store_records: state.market_data_store().record_count(),
    })
}
```

- [ ] **Step 4: Validate compile for service/state changes**

Run:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/contract-candle-completeness
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo check -p fdc-server
```

Expected: `cargo build: 0 errors`. Existing warnings are acceptable.

- [ ] **Step 5: Commit Task 1**

```bash
git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs
git commit -m "feat: add manual contract acquisition service"
```

---

### Task 2: Add POST run-once route and router tests

**Files:**
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add route import and handler**

In `router.rs`, import `MarketDataContractAcquisitionRunOnceResponse` and service `run_contract_acquisition_once`.

Add route:

```rust
.route(
    "/market-data/contracts/acquisition/run-once",
    post(contract_acquisition_run_once_handler),
)
```

Add handler:

```rust
async fn contract_acquisition_run_once_handler(
    State(state): State<ProductionServerState>,
) -> Json<ServerApiResponse<MarketDataContractAcquisitionRunOnceResponse>> {
    match run_contract_acquisition_once(&state).await {
        Ok(data) => Json(ServerApiResponse::success(data)),
        Err(message) => {
            let config = &state.config().market_data_contract_acquisition;
            Json(ServerApiResponse::error(
                MarketDataContractAcquisitionRunOnceResponse {
                    enabled: config.enabled,
                    exchange: config.exchange.clone(),
                    symbols: config.symbols.clone(),
                    intervals: config.intervals.clone(),
                    tasks_started: 0,
                    tasks_completed: 0,
                    pages_fetched: 0,
                    envelopes_received: 0,
                    storage_records_written: 0,
                    audit_records_written: 0,
                    final_cursors: 0,
                    market_data_store_records: state.market_data_store().record_count(),
                },
                message,
            ))
        }
    }
}
```

- [ ] **Step 2: Add disabled route contract test**

In `production_server_router_contract.rs`, add a test that builds default `ProductionServerState`, sends `POST /market-data/contracts/acquisition/run-once`, and asserts:

```rust
assert_eq!(response.status(), StatusCode::OK);
assert_eq!(body["status"], "error");
assert_eq!(body["message"], "contract candle acquisition is disabled");
assert_eq!(body["data"]["enabled"], false);
assert_eq!(body["data"]["tasks_started"], 0);
```

Use existing test helpers in that file for request/response parsing.

- [ ] **Step 3: Run router focused test**

Run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract contract_acquisition_run_once -- --nocapture
```

Expected: new disabled route test passes.

- [ ] **Step 4: Commit Task 2**

```bash
git add crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat: expose contract acquisition run-once route"
```

---

### Task 3: Strengthen canonical candle query validation

**Files:**
- Modify: `crates/fdc-server/tests/contract_acquisition_contract.rs`

- [ ] **Step 1: Extend existing runner test to assert canonical candle fields**

In `contract_acquisition_runner_writes_candles_checkpoints_and_audit_to_storage`, after querying `MarketDataQuery::for_candles().with_symbol("BTCUSDT")`, bind the result and assert:

```rust
let candles = store.query(&MarketDataQuery::for_candles().with_symbol("BTCUSDT"));
assert_eq!(candles.len(), 1);
let candle = &candles[0];
assert_eq!(candle.collection, "candles");
assert_eq!(candle.metadata.tags.get("kind").map(String::as_str), Some("candle"));
assert_eq!(candle.metadata.tags.get("symbol").map(String::as_str), Some("BTCUSDT"));
assert_eq!(candle.metadata.tags.get("exchange").map(String::as_str), Some("binance_futures_usd"));
assert_eq!(candle.metadata.tags.get("market_type").map(String::as_str), Some("perpetual"));
```

If metadata key names differ, inspect the actual storage mapper and assert the existing canonical fields instead. Do not assert against `contract_checkpoints` or `contract_acquisition_audits` as factor inputs.

- [ ] **Step 2: Add metadata separation assertion**

In the same test, assert maintenance records are separate:

```rust
let all_records = store.all_records();
assert!(all_records.iter().any(|record| record.collection == "contract_checkpoints"));
assert!(all_records.iter().any(|record| record.collection == "contract_acquisition_audits"));
assert!(all_records.iter().all(|record| {
    record.collection == "candles"
        || record.collection == "contract_checkpoints"
        || record.collection == "contract_acquisition_audits"
}));
```

- [ ] **Step 3: Run contract acquisition contract tests**

Run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
```

Expected: all tests pass.

- [ ] **Step 4: Commit Task 3**

```bash
git add crates/fdc-server/tests/contract_acquisition_contract.rs
git commit -m "test: verify contract candles query canonical storage"
```

---

### Task 4: Update operator runbook

**Files:**
- Modify: `docs/runbooks/market-data-production-runbook.md`

- [ ] **Step 1: Add manual run-once example**

Under the contract candle acquisition section, add:

```markdown
### Manual run-once

After starting the server with contract acquisition enabled, trigger one bounded run:

```bash
curl --noproxy '*' -sS -X POST \
  'http://127.0.0.1:18080/market-data/contracts/acquisition/run-once'
```

Expected response fields:

- `status=success`
- `data.tasks_started >= 1`
- `data.storage_records_written >= 1` when the selected Binance window has candles
- `data.audit_records_written >= 1`
```

- [ ] **Step 2: Add canonical query example**

Add:

```markdown
### Verify canonical candle availability

Query the existing candle endpoint, not the checkpoint/audit metadata collections:

```bash
curl --noproxy '*' -sS \
  'http://127.0.0.1:18080/market-data/candles?symbol=BTCUSDT&limit=5'
```

Use the returned canonical `candles` records as the future factor-research input surface. The collections `contract_checkpoints` and `contract_acquisition_audits` are maintenance metadata only.
```

- [ ] **Step 3: Run doc sanity grep**

Run:

```bash
grep -n "contracts/acquisition/run-once\|market-data/candles?symbol=BTCUSDT\|maintenance metadata" docs/runbooks/market-data-production-runbook.md
```

Expected: all three topics are present.

- [ ] **Step 4: Commit Task 4**

```bash
git add docs/runbooks/market-data-production-runbook.md
git commit -m "docs: document contract candle run-once flow"
```

---

### Task 5: Final validation and merge cleanup

**Files:**
- No code changes unless validation finds issues.

- [ ] **Step 1: Run final focused validation**

Run:

```bash
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test runtime_config_contract contract_acquisition_config -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo check -p fdc-server
```

Expected: all tests pass and `cargo check` reports 0 errors. Existing warnings from unrelated crates are acceptable.

- [ ] **Step 2: Check git status**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.

- [ ] **Step 3: Merge to `mdb-mqdev` and validate merged result**

From `/Volumes/wdata/opensource/mountainsea-lab/mdb`:

```bash
git checkout mdb-mqdev
git merge --no-ff contract-candle-completeness -m "merge: contract candle acquisition completeness"
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test contract_acquisition_contract -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo test -p fdc-server --test production_server_router_contract market_data_contract_acquisition -- --nocapture
CARGO_TARGET_DIR=/Volumes/wdata/opensource/mountainsea-lab/mdb/target rtk cargo check -p fdc-server
```

Expected: merge succeeds and validation passes.

- [ ] **Step 4: Clean worktree and branch**

After merged validation passes:

```bash
cd /Volumes/wdata/opensource/mountainsea-lab/mdb
git worktree remove /Volumes/wdata/opensource/mountainsea-lab/mdb/.worktrees/contract-candle-completeness
git worktree prune
git branch -d contract-candle-completeness
```

Expected: worktree removed and feature branch deleted.
