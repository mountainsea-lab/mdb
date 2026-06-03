# fdc-storage Storage Boundary Acceptance Report

Date: 2026-06-03
Status: S11 P1-closed pre-integration-ready module baseline

## Scope

`fdc-storage` is a generic storage module. It stores bytes and storage-owned metadata. Business modules provide DTOs through codecs, namespace, collection, tags, and placement hints.

## Implemented Capability Matrix

| Area | Status | Notes |
| --- | --- | --- |
| Generic write boundary | Done | `StorageWriteRecord`, `StorageWriteBatch`, `StorageWriteSink` |
| Generic query boundary | Done | namespace, collection, tags, key filters, time filters, order, limit |
| Typed facade | Done | `StorageCodec`, JSON/bincode codecs, typed read helpers |
| Tier-aware store | Done | `TieredStorageStore` routes writes by placement and queries by tier scope |
| L1 engine | Done | Memory engine |
| L2 engine | Done | redb persistent KV |
| L3 engine | Done | DuckDB KV + SQL query support |
| L4 engine | Done | RocksDB persistent cold KV |
| Tier-scoped query | Done | all/only/hot/warm/cold scope with metrics |
| Lifecycle | Done | explicit TTL hard-delete, retention demotion, retention delete |
| Maintenance/health | Done | explicit maintenance pass, health snapshot, guarded options-based maintenance, typed compaction outcomes, metrics snapshot |
| P1 closure | Done | engine feature-based compaction classification, tracing spans, duplicate-key TTL semantics, API stability docs |
| Business dependency isolation | Guarded | `fdc-storage` must not depend on barter/ingestion/transform/api/server/orchestrator |

## Verification

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected result as of S11: all `fdc-storage` tests pass.

## Integration Readiness Statement

`fdc-storage` is ready for business modules to depend on its generic write/query/typed APIs. It is not yet wired into full ingestion or server runtime in this module-completion phase.

## S11 P1 Closure Notes

- Compaction unsupported classification is based on engine feature support instead of error-string matching.
- TTL hard-delete semantics are explicit: when one logical storage key expires, lifecycle deletes that key from all initialized tiers. Duplicate tier copies are treated as copies of the same logical record, not independent versions.
- Structured tracing spans exist around storage write/query/lifecycle/maintenance boundaries without logging payload bytes or business DTO fields.
- Public API stability rules are documented in `public-api-stability.md`.

## Known Limitations

- Maintenance is explicit and caller-driven, not scheduled.
- Health snapshot and maintenance metrics can render module-local Prometheus text, but are not yet wired into an API/exporter runtime.
- Index, physical shard routing, and backup orchestration are intentionally deferred.
- Durable audit persistence, disk health, and richer degraded health states remain production deployment follow-ups.

## Public API Stability

The integration-facing API stability policy is documented in `public-api-stability.md`. Public re-exports from `crates/fdc-storage/src/lib.rs` define the preferred consumer surface.

## Production Follow-up Reference

See `production-hardening-followups.md` for remaining P2/P3 follow-up items.
