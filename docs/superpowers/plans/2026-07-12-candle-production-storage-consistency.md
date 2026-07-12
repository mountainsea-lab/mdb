# Candle Production Storage Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist candle acquisition checkpoints and verify audit summaries through the existing market-data storage path, without broadening into non-candle acquisition.

**Architecture:** Keep canonical candles in `candles`. Add storage-backed metadata collections for candle maintenance: `candle_checkpoints` for resume cursors and `candle_verify_audits` for run quality summaries. The server candle acquisition module owns serialization, keys, and store adapters while continuing to use existing `StorageWriteSink`/`QueryableMarketDataStore` APIs.

**Tech Stack:** Rust, fdc-server, fdc-barter HistoricalCursor/CandlePayload, fdc-storage QueryableMarketDataStore/StorageWriteRecord.

---

### Task 1: Storage-backed candle checkpoint store

**Files:**
- Modify: `crates/fdc-server/src/market_data/candle_acquisition.rs`
- Test: `crates/fdc-server/tests/candle_acquisition_contract.rs`

- [x] Write failing test `storage_backed_candle_checkpoint_store_round_trips_cursor` using `QueryableMarketDataStore`.
- [x] Implement `StorageBackedCandleCheckpointStore` using collection `candle_checkpoints`, key `exchange:symbol:interval`, JSON value with `cursor` and `updated_at_ns`.
- [x] Wire `ProductionServerState` candle runs to use storage-backed checkpoints instead of no-checkpoint runner.
- [x] Verify `candle_acquisition_contract` passes.
- [x] Commit `feat: persist candle checkpoints in market storage`.

### Task 2: Candle verify audit persistence

**Files:**
- Modify: `crates/fdc-server/src/market_data/candle_acquisition.rs`
- Test: `crates/fdc-server/tests/candle_acquisition_contract.rs`

- [x] Write failing test `candle_acquisition_persists_verify_audit_summary`.
- [x] Implement verify audit record write to collection `candle_verify_audits` with `run_id`, `symbol`, `interval`, `checked`, `mismatches`, `pages_fetched`, and timestamp.
- [x] Ensure official verify candles are still not written into canonical `candles`.
- [x] Verify `candle_acquisition_contract` and status API tests pass.
- [x] Commit `feat: persist candle verify audits`.

### Task 3: Documentation and final validation

**Files:**
- Modify: `docs/runbooks/market-data-production-runbook.md`
- Modify: `docs/roadmaps/factor-data-stage-status.md`

- [x] Document `candle_checkpoints` and `candle_verify_audits` collections.
- [x] Run final validation commands:
  - `rtk cargo test -p fdc-server --test candle_acquisition_contract -- --nocapture`
  - `rtk cargo test -p fdc-server --test production_server_router_contract market_data_candle_acquisition_status_reports_ -- --nocapture`
  - `rtk cargo check -p fdc-server`
- [x] Commit `docs: record candle storage consistency hardening`.
