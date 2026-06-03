# fdc-barter Docs Calibration and Smoke Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Update stale `fdc-barter` documentation to match current implementation, then run offline and real-network smoke verification without touching unrelated `fdc-server` health changes.

**Architecture:** Keep this slice documentation-and-verification only. Update the module requirements document from observed source/test state, optionally add a short verification checkpoint to `docs/DEVELOPMENT_STATUS.md`, then run deterministic offline checks and opt-in ignored smoke tests. Do not design or implement the `fdc-barter` to `fdc-ingestion` glue.

**Tech Stack:** Markdown, Rust cargo tests/checks, `fdc-barter`, Barter-rs local path crates, Binance public REST/WebSocket smoke tests.

---

## File Structure

- Modify: `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`
  - Responsibility: module-level market-data requirements and current-state description.
  - Change stale claims that mapper is trade-only and historical fetchers do not exist.
  - Preserve explicit remaining gaps.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Responsibility: resumable development checkpoint.
  - Add a small 2026-06-03 checkpoint only after verification completes.
- Do not modify or commit:
  - `crates/fdc-server/src/health/*`
  - `crates/fdc-server/src/bin/health/*`

---

### Task 1: Calibrate `fdc-barter` requirements documentation

**Files:**
- Modify: `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`

- [ ] **Step 1: Inspect stale documentation claims**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path('crates/fdc-adapter/barter/docs/market-data-collection-requirements.md')
text = p.read_text()
for needle in [
    'adapter currently proves realtime crypto trade acquisition',
    'Current live acquisition functions:',
    'The current mapper only maps trades into a structured payload.',
    'no real historical exchange fetcher is implemented',
    'Current status: model exists, mapper is raw placeholder',
    'Current status: mdb only has `RawPayload` placeholder for L2',
]:
    print(f'--- {needle}')
    idx = text.find(needle)
    print('FOUND' if idx >= 0 else 'MISSING')
PY
```

Expected: stale claims are found before editing.

- [ ] **Step 2: Replace the current-state section**

Replace the introductory current-state wording in `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md` so it states:

```markdown
The adapter has moved beyond the original realtime trade proof. It now owns the Barter-rs integration boundary for structured live market data, Binance Spot historical REST acquisition, adapter envelopes, capability declarations, and example binaries. The next module-level work should keep the requirements and capability map accurate while productionizing verification, integration, and operational gaps.
```

Replace section `## 2. Current Module State` through the end of `### 2.2 Current gaps` with:

```markdown
## 2. Current Module State

### 2.1 Implemented in `fdc-barter`

Current models and boundaries:

- `BarterMarketDataKind`
  - `Trade`
  - `OrderBookL1`
  - `OrderBook`
  - `Candle`
  - `Liquidation`
- `BarterMarketDataMode`
  - `Live`
  - `Historical`
- `BarterMarketType`
  - `Spot`
  - `Future`
  - `Perpetual`
  - `Option`
- `BarterMarketEvent`
- `BarterMarketPayload`
  - structured `TradePayload`
  - structured `OrderBookL1Payload`
  - structured `OrderBookPayload`
  - structured `CandlePayload`
  - structured `LiquidationPayload`
  - `RawPayload` fallback
- `BarterIngestionEnvelope`
- `DataQualityFlags`
- `HistoricalBackfillRequest`
- `HistoricalBackfillPage`
- `HistoricalBackfillRunRequest`
- `HistoricalBackfillRunOutcome`
- `HistoricalCursor`
- `HistoricalPageRequest`
- `BarterCheckpoint`
- `BarterSourceCapabilities`
- `HistoricalProviderCapabilities`

Current live acquisition functions:

- `default_binance_spot_trade_subscriptions`
- `default_binance_spot_market_data_subscriptions`
- `default_binance_futures_usd_market_data_subscriptions`
- `init_binance_spot_public_trades`
- `init_binance_spot_market_data`
- `init_binance_futures_usd_market_data`
- `public_trade_result_to_data_kind`
- `map_live_trade_result`
- `map_live_market_data_result`
- `collect_live_trade_envelopes`
- `collect_live_market_data_envelopes`
- `collect_live_envelopes_with_summary`

Current historical acquisition functions:

- `binance_spot_ohlcv_capabilities`
- `binance_spot_historical_trades_capabilities`
- `binance_spot_ohlcv_rest_request_descriptor`
- `binance_spot_historical_trades_rest_request_descriptor`
- `execute_binance_spot_ohlcv_rest`
- `execute_binance_spot_historical_trades_rest`
- `run_historical_backfill_pages`
- `validate_historical_backfill_request`
- `historical_trade_dedupe_key`

Current examples:

- `historical_binance_spot_ohlcv`
- `historical_binance_spot_trades`
- `live_binance_futures_usd_market_data`
- `live_binance_spot_order_books`
- `live_binance_spot_trades`

Current production behavior:

- Binance Spot public trades can be collected live through Barter-rs.
- Binance Spot public trades, Spot L1 order books, Spot L2 order books, and Binance Futures USD liquidations have structured mapper coverage.
- Binance Spot OHLCV and historical trades can be fetched through adapter-owned REST descriptors/executors.
- Bounded live and historical helpers provide finite acquisition outcomes for tests, examples, and future pipeline integration.
- Collected events can be mapped into mdb envelopes and written through the existing orchestrator/storage path where that cross-module path is already wired.
- Production live smoke has already proven real Binance Spot trades are queryable through the production API.

### 2.2 Current gaps

The adapter is no longer trade-only, but several production-level gaps remain:

- Historical REST support is currently Binance Spot focused; multi-exchange historical REST providers are future work.
- Historical support covers Binance Spot OHLCV and trades; historical order-book reconstruction is not implemented.
- L2 order-book payloads preserve snapshot/update, levels, timestamps, and sequence where available, but durable book reconstruction, gap detection, and out-of-order repair are future work.
- Live smoke and historical smoke tests require explicit environment variables and public internet access, so routine CI still relies on offline contract tests by default.
- Cross-module glue from `fdc-barter` bounded helpers to the generic `fdc-ingestion` source pipeline is future work and intentionally outside this document update.
```

- [ ] **Step 3: Update per-kind current status lines**

In the same file, replace stale per-kind status lines:

```markdown
Current status: model exists, mapper is raw placeholder.
```

with:

```markdown
Current status: structured model and mapper coverage exist for live Barter L1 events.
```

Replace:

```markdown
Current status: mdb only has `RawPayload` placeholder for L2. A structured L2 payload is needed before L2 can support factors or backtesting.
```

with:

```markdown
Current status: structured L2 payload and mapper coverage exist for snapshots and updates. Durable order-book reconstruction, gap detection, and storage/query semantics remain future work before L2 can fully support execution simulation and production backtesting.
```

Replace candle gap bullets that say payload lacks `interval` and `trade_count` with:

```markdown
Current status:

- `CandlePayload` includes `interval`, open/close time, OHLC, volume, optional trade count, and optional quote volume.
- Binance Spot historical OHLCV REST execution exists.
- Barter-rs live candle stream maturity should still be verified before relying on it for live candles.
- mdb can also derive candles from trades.
```

Replace liquidation status if it still describes raw-only handling with:

```markdown
Current status: structured liquidation payload and mapper coverage exist for Barter liquidation events. Production use still needs live smoke coverage and downstream storage/query integration.
```

- [ ] **Step 4: Verify no stale contradictions remain**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
text = Path('crates/fdc-adapter/barter/docs/market-data-collection-requirements.md').read_text()
stale = [
    'adapter currently proves realtime crypto trade acquisition',
    'The current mapper only maps trades into a structured payload.',
    'no real historical exchange fetcher is implemented',
    'mapper is raw placeholder',
    'mdb only has `RawPayload` placeholder for L2',
    'payload lacks `interval` and `trade_count`',
]
found = [item for item in stale if item in text]
if found:
    print('STALE CLAIMS REMAIN:')
    for item in found:
        print('-', item)
    raise SystemExit(1)
print('No stale current-state contradictions found')
PY
```

Expected: `No stale current-state contradictions found`.

- [ ] **Step 5: Commit documentation calibration**

Run:

```bash
git add crates/fdc-adapter/barter/docs/market-data-collection-requirements.md
git commit -m 'docs: calibrate barter market data requirements'
```

Expected: one commit that includes only the requirements document.

---

### Task 2: Run offline and example validation

**Files:**
- Read only: `crates/fdc-adapter/barter/Cargo.toml`
- Read only: `crates/fdc-adapter/barter/examples/*.rs`

- [ ] **Step 1: Run offline contract tests**

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
```

Expected: `62 passed, 6 ignored`.

- [ ] **Step 2: Compile all examples offline**

Run:

```bash
for ex in historical_binance_spot_ohlcv historical_binance_spot_trades live_binance_futures_usd_market_data live_binance_spot_order_books live_binance_spot_trades; do
  echo "--- $ex"
  CARGO_NET_OFFLINE=true rtk cargo check -p fdc-barter --example "$ex"
done
```

Expected: each example exits successfully.

---

### Task 3: Run real-network smoke verification

**Files:**
- Read only: `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_execution_contract.rs`
- Read only: `crates/fdc-adapter/barter/tests/binance_spot_historical_trades_rest_contract.rs`
- Read only: `crates/fdc-adapter/barter/tests/binance_futures_live_contract.rs`
- Read only: `crates/fdc-adapter/barter/tests/live_acquisition_contract.rs`

- [ ] **Step 1: Run historical smoke tests**

Run:

```bash
FDC_BARTER_HISTORICAL_SMOKE=1 rtk cargo test -p fdc-barter -- --ignored ignored_live_smoke_fetches_one_binance_spot_ohlcv_candle ignored_live_smoke_fetches_binance_spot_historical_trades --nocapture
```

Expected success when public Binance REST is reachable. If it fails with DNS, TLS, timeout, HTTP 451/418/429, or remote API availability errors, record the exact failure as environment/network/Binance availability.

- [ ] **Step 2: Run live smoke tests**

Run:

```bash
FDC_BARTER_LIVE_SMOKE=1 rtk cargo test -p fdc-barter -- --ignored ignored_live_smoke_can_collect_one_binance_spot_trade ignored_live_smoke_can_initialize_expanded_binance_spot_market_data ignored_live_smoke_can_initialize_binance_futures_usd_market_data --nocapture
```

Expected success when public Binance WebSocket endpoints are reachable and events arrive before test timeouts. If it fails with connection, timeout, regional block, or no-event-before-timeout behavior, record the exact failure as environment/network/Binance availability.

- [ ] **Step 3: Run optional realtime print smoke only if needed for manual observation**

Run only if a human-readable realtime sample is needed:

```bash
FDC_BARTER_LIVE_SMOKE=1 rtk cargo test -p fdc-barter -- --ignored ignored_live_smoke_prints_realtime_binance_spot_trades_for_review --nocapture
```

Expected: realtime trade lines print to stdout. This is optional and should not block completion if Task 3 Step 2 already provides enough evidence.

---

### Task 4: Record verification checkpoint

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Add a 2026-06-03 checkpoint near the top of `docs/DEVELOPMENT_STATUS.md`**

Insert after the opening paragraph and before `## 2026-06-02 Development Checkpoint`:

```markdown
## 2026-06-03 fdc-barter Documentation and Smoke Verification Checkpoint

Completed today:

- Calibrated `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md` so the current-state section matches the implemented structured live mappings, Binance Spot historical REST paths, bounded acquisition helpers, and example binaries.
- Preserved remaining gaps for multi-exchange historical REST, historical order-book reconstruction, L2 gap/out-of-order handling, real-network smoke coverage, and future cross-module source-pipeline glue.

Verification performed:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter`
  - Result: RECORD_RESULT_HERE
- Checked all five fdc-barter examples with `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-barter --example ...`
  - Result: RECORD_RESULT_HERE
- Historical Binance smoke with `FDC_BARTER_HISTORICAL_SMOKE=1`
  - Result: RECORD_RESULT_HERE
- Live Binance smoke with `FDC_BARTER_LIVE_SMOKE=1`
  - Result: RECORD_RESULT_HERE

Scope note:

- No `fdc-barter` to `fdc-ingestion` glue was designed or implemented in this slice.
- Existing uncommitted `fdc-server` health file moves remain unrelated and must be handled separately.
```

Replace each `RECORD_RESULT_HERE` with the actual result from Tasks 2 and 3 before committing. Use concise results such as `62 passed, 6 ignored`, `all five examples built with 0 errors`, `passed`, or `attempted; failed due to Binance HTTP 451 regional restriction`.

- [ ] **Step 2: Verify checkpoint has no placeholders**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
text = Path('docs/DEVELOPMENT_STATUS.md').read_text()
if 'RECORD_RESULT_HERE' in text:
    raise SystemExit('RECORD_RESULT_HERE placeholder remains')
print('Development status checkpoint has no result placeholders')
PY
```

Expected: `Development status checkpoint has no result placeholders`.

- [ ] **Step 3: Commit verification checkpoint**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m 'docs: record barter smoke verification'
```

Expected: one commit that includes only `docs/DEVELOPMENT_STATUS.md`.

---

### Task 5: Final safety checks and report

**Files:**
- Read only: repository git status

- [ ] **Step 1: Confirm unrelated dirty files are not staged**

Run:

```bash
git diff --cached --name-only
rtk git status --short --branch
```

Expected: no staged files. Working tree may still show unrelated `fdc-server` health deletions/untracked files.

- [ ] **Step 2: Summarize commits and verification**

Run:

```bash
rtk git log --oneline --decorate --max-count=6
```

Expected: recent commits include:

- `docs: design barter docs smoke verification`
- `docs: calibrate barter market data requirements`
- `docs: record barter smoke verification`

Final report should include:

- Documentation files changed.
- Offline test result.
- Example compile result.
- Historical smoke result.
- Live smoke result.
- Note that C/glue work was not executed.
- Note that unrelated `fdc-server` health changes remain untouched.
