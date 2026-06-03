# fdc-storage Production Hardening Follow-ups

Date: 2026-06-03
Scope: future production-grade improvements after S9 pre-integration closure

## P1: Before broad integration

- Stabilize public API naming and document breaking-change policy.
- Decide lifecycle hard-delete semantics for duplicate keys across tiers.
- Clarify compaction errors: distinguish expected unsupported no-op from operational failure.
- Add maintenance re-entry protection before any scheduler calls `run_maintenance_once()`.
- Add structured tracing spans around write/query/lifecycle/maintenance operations.

## P2: Before production deployment

- Add Prometheus/exporter adapter for `StorageHealthSnapshot` and `StorageMaintenanceReport`.
- Add maintenance scheduler with interval config, timeout, cancellation, and shutdown token.
- Persist maintenance reports to an audit sink or system namespace.
- Add disk usage/path health for durable engines.
- Add engine-specific compaction/vacuum/checkpoint policies.
- Add degraded health states for stale stats, recent write failures, high disk usage, and compaction failures.

## P3: Before high-scale operation

- Design and implement namespace/collection/tag/time indexes.
- Add physical shard routing and rebalancing.
- Add engine-specific backup/restore orchestration.
- Add cold archive formats such as Parquet/Arrow.
- Add quota management and backpressure.

## Deferred by design

S9 does not implement these items. It records them so future production hardening can continue from a clear checklist.
