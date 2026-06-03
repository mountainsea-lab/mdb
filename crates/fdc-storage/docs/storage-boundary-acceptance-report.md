# fdc-storage Storage Boundary Acceptance Report

Date: 2026-06-03
Status: pre-integration-ready module baseline

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
| Business dependency isolation | Guarded | `fdc-storage` must not depend on barter/ingestion/transform/api/server/orchestrator |

## Verification

Run:

```bash
rtk cargo test -p fdc-storage
```

Expected result as of S10: all `fdc-storage` tests pass.

## Integration Readiness Statement

`fdc-storage` is ready for business modules to depend on its generic write/query/typed APIs. It is not yet wired into full ingestion or server runtime in this module-completion phase.

## Known Limitations

- Maintenance is explicit and caller-driven, not scheduled.
- Compaction unsupported detection is classified separately from failures, but S10 still uses string-based detection until engine-level capability/error typing is added.
- Health snapshot and maintenance metrics can render module-local Prometheus text, but are not yet wired into an API/exporter runtime.
- Index, physical shard routing, and backup orchestration are intentionally deferred.
- Lifecycle hard-delete deletes all tier copies for the same storage key when TTL is expired.

## Production Follow-up Reference

See `production-hardening-followups.md` for P1/P2/P3 follow-up items.
