# B4b Architecture Audit and Adapter Decoupling Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align completed B4 code with the platform architecture by removing concrete adapter glue from core crates.

**Architecture:** `fdc-barter` owns only Barter acquisition and adapter-owned events/envelopes. `fdc-ingestion` owns generic source validation/batching. `fdc-transform` owns neutral market-data DTOs and transform sink contracts only. Concrete adapter-to-ingestion and adapter-to-transform glue belongs in a future orchestration/integration slice.

**Tech Stack:** Rust 1.95 workspace, Cargo crate boundaries, existing fdc-barter/fdc-ingestion/fdc-transform contract tests.

---

## Tasks

- [x] Remove `fdc-ingestion` and `fdc-transform` from `fdc-barter` dependencies.
- [x] Remove adapter-local source bridge and transform mapper code from `fdc-barter`.
- [x] Remove Barter-specific bridge/mapper code from `fdc-transform` core.
- [x] Keep `fdc-transform` generic `MarketDataDto`, `MarketDataTransformSink`, and recording sink.
- [x] Update `docs/DEVELOPMENT_STATUS.md` to mark B2 source bridge superseded.
- [x] Update B4/B4b docs to state concrete glue is deferred to orchestration/integration.

## Verification

Run:

```bash
rtk cargo fmt --package fdc-transform --package fdc-barter --check
rtk cargo test -p fdc-transform
rtk cargo test -p fdc-barter --test live_acquisition_contract
rtk cargo test -p fdc-barter -p fdc-ingestion
! grep -R "fdc-ingestion\|fdc_ingestion\|fdc-transform\|fdc_transform" -n crates/fdc-adapter/barter/Cargo.toml crates/fdc-adapter/barter/src
! grep -R "fdc-barter\|fdc_barter\|fdc-ingestion\|fdc_ingestion" -n crates/fdc-transform/Cargo.toml crates/fdc-transform/src
! grep -R "fdc-barter\|fdc_barter\|fdc-transform\|fdc_transform" -n crates/fdc-ingestion/Cargo.toml crates/fdc-ingestion/src
```

Expected: all commands exit 0.
