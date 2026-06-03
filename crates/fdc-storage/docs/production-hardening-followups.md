# fdc-storage Production Hardening Follow-ups

Date: 2026-06-03
Scope: future production-grade improvements after S10 P1 hardening baseline

## Done in S10 P1 baseline

- Maintenance re-entry protection for explicit `run_maintenance_once*` calls.
- Caller-provided maintenance timeout option, with zero timeout defined as immediate timeout.
- Optional maintenance audit sink trait and audit entry DTO.
- Compaction outcome classification into compacted, unsupported, and failed.
- Maintenance metrics snapshot DTO and Prometheus text rendering helper.

## Remaining P1: Before broad integration

- Stabilize public API naming and document breaking-change policy.
- Decide lifecycle hard-delete semantics for duplicate keys across tiers.
- Add structured tracing spans around write/query/lifecycle/maintenance operations.
- Replace S10 string-based compaction unsupported detection with engine-level capability/error typing.

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

S10 does not implement scheduler loops, server/API exporters, durable audit storage, query indexes, physical sharding, or backup orchestration. It records the remaining work so future production hardening can continue from a clear checklist.
