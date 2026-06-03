# fdc-storage S10 P1 Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add module-local P1 production hardening for `fdc-storage` explicit maintenance: re-entry guard, timeout option, audit sink skeleton, typed compaction outcomes, and metrics/Prometheus report DTO.

**Architecture:** Add a focused `maintenance_control.rs` module for options/audit/error classification, extend `maintenance.rs` for compaction and metrics DTOs, and update `TieredStorageStore` to use an atomic guard around maintenance. Keep existing `run_maintenance_once()` compatible by delegating to the options-based method.

**Tech Stack:** Rust, tokio timeout, async-trait, serde, chrono, Arc/AtomicBool, existing `fdc_core::Error`/`Result`.

---

## Files

- Create: `crates/fdc-storage/src/maintenance_control.rs`
- Modify: `crates/fdc-storage/src/maintenance.rs`
- Modify: `crates/fdc-storage/src/tiered_store.rs`
- Modify: `crates/fdc-storage/src/lib.rs`
- Modify: `crates/fdc-storage/docs/production-hardening-followups.md`
- Modify: `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Add tests inside modified modules plus existing integration smoke tests if needed.

## Task 1: Maintenance control types

- [ ] Create `maintenance_control.rs` with `StorageMaintenanceOptions`, `StorageMaintenanceAuditEntry`, `StorageMaintenanceAuditSink`, and `StorageMaintenanceErrorKind`.
- [ ] Unit test default options and audit entry construction.
- [ ] Export the module and public types from `lib.rs`.
- [ ] Run `rtk cargo test -p fdc-storage maintenance_control`.
- [ ] Commit `feat(storage): add maintenance control types`.

## Task 2: Maintenance report outcomes and metrics DTO

- [ ] Extend `maintenance.rs` with `StorageCompactionOutcomeKind`, `StorageCompactionOutcome`, and `StorageMaintenanceMetricsSnapshot`.
- [ ] Add `StorageMaintenanceMetricsSnapshot::from_report()` and `to_prometheus_text()`.
- [ ] Add fields to `StorageMaintenanceReport`: `compaction_outcomes`, `compaction_unsupported`, `compaction_failed` while preserving `compaction_errors`.
- [ ] Unit test metrics snapshot and Prometheus text.
- [ ] Run `rtk cargo test -p fdc-storage maintenance`.
- [ ] Commit `feat(storage): add maintenance metrics export DTO`.

## Task 3: Guarded options-based maintenance

- [ ] Add `maintenance_running: Arc<AtomicBool>` to `TieredStorageStore` and update constructors/clone behavior.
- [ ] Add `run_maintenance_once_with_options(options)` that enforces non-reentrant execution, applies optional timeout, classifies compaction outcomes, and writes optional audit.
- [ ] Keep `run_maintenance_once()` delegating to default options.
- [ ] Add tests for compatibility, audit sink, and report outcome counts.
- [ ] Run `rtk cargo test -p fdc-storage tiered_store maintenance`.
- [ ] Commit `feat(storage): guard explicit maintenance runs`.

## Task 4: Timeout/re-entry tests and docs

- [ ] Add a test proving concurrent calls do not both run. If timing is brittle, use a held guard helper or tiny timeout path.
- [ ] Add a timeout test with zero/near-zero timeout that returns classified timeout and releases guard.
- [ ] Update S9 docs to mark S10 P1 completed items and list remaining production follow-ups.
- [ ] Run focused tests.
- [ ] Commit `docs(storage): update production hardening status`.

## Task 5: Full verification and integration

- [ ] Run `rtk cargo fmt -p fdc-storage && rtk cargo test -p fdc-storage` in isolated worktree.
- [ ] Remove generated `crates/fdc-storage/data`.
- [ ] Fast-forward merge to `mdb-mqdev`.
- [ ] Run `rtk cargo test -p fdc-storage` on `mdb-mqdev`.
- [ ] Remove worktree and branch.
- [ ] Confirm only unrelated `fdc-server health` dirty files remain.

## Self-review

- Covers all S10 spec requirements.
- Keeps storage generic.
- Does not implement scheduler/pipeline/server glue.
- Includes production follow-up notes.
