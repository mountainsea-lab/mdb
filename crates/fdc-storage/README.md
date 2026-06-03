# fdc-storage

Generic storage boundary for MDB/FDC modules.

`fdc-storage` stores bytes plus storage-owned metadata. Business modules own DTOs/codecs and pass generic write/query inputs such as namespace, collection, key, tags, placement hints, TTL, and encoded payload bytes.

## Current status

Status as of S11: **P1-closed, pre-integration-ready**.

The module is ready for broader business-module integration through its public generic APIs, but it is not yet fully production-deployment ready.

See:

- [`docs/storage-boundary-acceptance-report.md`](docs/storage-boundary-acceptance-report.md) for implemented capability matrix and integration readiness.
- [`docs/public-api-stability.md`](docs/public-api-stability.md) for public API stability and breaking-change policy.
- [`docs/production-hardening-followups.md`](docs/production-hardening-followups.md) for remaining P2/P3 roadmap.

## Completed through S11

- Generic write/query boundary.
- Typed facade with `StorageCodec` and typed read helpers.
- Tier-aware store with hot/warm/cold/archive routing.
- Memory, redb, DuckDB, and RocksDB engine support.
- Explicit lifecycle operations for TTL hard-delete, retention demotion, and retention delete.
- Caller-driven maintenance pass, health snapshot, maintenance metrics snapshot, and Prometheus text rendering helper.
- Engine-level feature typing and typed compaction outcome classification.
- Structured tracing spans/events around write/query/lifecycle/maintenance boundaries.
- Duplicate-key TTL hard-delete semantics documented and regression-tested.
- Public API stability policy documented.

## P2 roadmap: before production deployment

These are the next storage-internal production-hardening items after business integration proves the boundary:

1. Runtime maintenance scheduler with interval config, timeout, cancellation, and shutdown token.
2. Runtime metrics exporter or adapter for `StorageMaintenanceMetricsSnapshot`.
3. Durable audit sink persistence to a system namespace, log, or external audit store.
4. Disk usage/path health for durable engines.
5. Engine-specific compaction/vacuum/checkpoint policies.
6. Degraded health states for stale stats, recent write failures, high disk usage, and compaction failures.

## P3 roadmap: before high-scale operation

These are larger scale/operations features and should not block initial module integration:

1. Namespace/collection/tag/time indexes.
2. Cursor pagination for query APIs.
3. Physical shard routing and rebalancing.
4. Engine-specific backup/restore orchestration.
5. Cold archive formats such as Parquet/Arrow.
6. Quota management and backpressure.

## Dependency rule

`fdc-storage` must remain generic. It must not depend on business modules such as barter, ingestion, transform, api, server, or orchestrator crates. Business modules depend on `fdc-storage`, not the other way around.

Run the guard tests before merging storage changes:

```bash
rtk cargo test -p fdc-storage
```
