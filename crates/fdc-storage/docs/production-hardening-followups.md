# fdc-storage Production Hardening Follow-ups

Date: 2026-06-03
Scope: future production-grade improvements after S11 P1 closure

## Done through S11 P1 closure

- Maintenance re-entry protection for explicit `run_maintenance_once*` calls.
- Caller-provided maintenance timeout option, with zero timeout defined as immediate timeout.
- Optional maintenance audit sink trait and audit entry DTO.
- Compaction outcome classification into compacted, unsupported, and failed.
- Maintenance metrics snapshot DTO and Prometheus text rendering helper.
- Engine-level compaction support detection replaces error-string matching.
- Duplicate-key TTL hard-delete semantics are documented and regression-tested.
- Structured tracing spans exist around storage write/query/lifecycle/maintenance boundaries.
- Public API stability and breaking-change policy are documented.

## P2: Before production deployment

- Wire `StorageMaintenanceMetricsSnapshot` into a runtime exporter or metrics adapter.
- Add maintenance scheduler with interval config, timeout, cancellation, and shutdown token.
- Implement durable audit sink persistence to a system namespace, log, or external audit store.
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

S11 does not implement scheduler loops, server/API exporters, durable audit storage, query indexes, physical sharding, or backup orchestration. It records the remaining work so future production hardening can continue from a clear checklist.
