# fdc-storage S10 P1 Production Hardening Design

Date: 2026-06-03
Module: `crates/fdc-storage`
Status: approved for implementation

## Goal

S10 completes the P1 production-hardening baseline for `fdc-storage` while keeping the module generic and module-local. It improves explicit maintenance safety, report semantics, metrics exportability, and audit handoff without adding background scheduling or pipeline glue.

## Non-goals

- No `fdc-barter -> fdc-ingestion -> fdc-storage` integration.
- No server route or API endpoint wiring.
- No background scheduler loop.
- No market-data-specific DTO/type logic.
- No physical shard routing, backup orchestration, or query index implementation.
- No changes to unrelated `fdc-server health` dirty files.

## Architecture

### Maintenance control

Add `crates/fdc-storage/src/maintenance_control.rs` with:

- `StorageMaintenanceOptions`: caller options for timeout and optional audit sink.
- `StorageMaintenanceAuditEntry`: durable/auditable summary of one maintenance attempt.
- `StorageMaintenanceAuditSink`: async trait for future persistence adapters.
- `StorageMaintenanceErrorKind`: stable classification for skipped, timeout, audit, and internal errors.

`TieredStorageStore` gains an `Arc<AtomicBool>` maintenance guard. `run_maintenance_once()` remains source-compatible and delegates to `run_maintenance_once_with_options(StorageMaintenanceOptions::default())`. The options method rejects concurrent maintenance attempts before entering lifecycle/compaction.

### Timeout and cancellation baseline

S10 does not implement a scheduler or shutdown token. It provides a caller-driven timeout around the actual maintenance future using `tokio::time::timeout`. Timeout returns a classified storage error. The guard is released via RAII/drop even when timeout or internal error happens.

### Compaction semantics

`StorageMaintenanceReport` keeps legacy `compaction_errors` for compatibility and adds typed compaction outcome counts/details:

- unsupported/no-op compaction is counted separately from operational failure.
- failure strings remain available for existing callers.

Classification is string-based for S10 because current `StorageEngine::compact()` returns generic errors. Future engine-level capability/error typing remains a follow-up.

### Metrics exporter baseline

Add `StorageMaintenanceMetricsSnapshot` and a Prometheus text renderer. This is a DTO/export-format helper, not a running exporter. It derives counters/gauges from a maintenance report:

- lifecycle scanned/deleted/demoted/retained/decode errors.
- health tier totals by status.
- compaction success/unsupported/failure counts.
- maintenance duration seconds.

### Audit persistence skeleton

The optional audit sink receives a summary after successful maintenance. If audit write fails, maintenance returns a classified audit error. S10 does not persist to disk by default, but tests can use an in-memory sink.

## Public API additions

Re-export from `lib.rs`:

- `StorageMaintenanceOptions`
- `StorageMaintenanceAuditEntry`
- `StorageMaintenanceAuditSink`
- `StorageMaintenanceErrorKind`
- `StorageMaintenanceMetricsSnapshot`
- `StorageCompactionOutcome`
- `StorageCompactionOutcomeKind`

## Documentation updates

Update:

- `crates/fdc-storage/docs/production-hardening-followups.md`
- `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`

S10 should mark completed P1 items and preserve remaining production-level notes.

## Acceptance criteria

- Concurrent maintenance attempts are guarded and return a classified error.
- Timeout returns a classified timeout error and releases the guard.
- Maintenance report distinguishes compaction unsupported from failure.
- Metrics snapshot renders Prometheus text.
- Optional audit sink receives a summary for successful maintenance.
- Existing `run_maintenance_once()` still works.
- `rtk cargo test -p fdc-storage` passes.

## Production follow-up notes

After S10, remaining work includes actual scheduler loop, shutdown token integration, durable audit sink implementation, engine-level compaction capability/error typing, disk health, exporter wiring, index, shard, and backup orchestration.
