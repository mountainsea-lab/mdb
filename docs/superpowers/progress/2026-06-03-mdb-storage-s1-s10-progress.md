# MDB fdc-storage S1-S10 Progress Handoff

Date: 2026-06-03
Branch: `mdb-mqdev`
Workspace: `/Volumes/wdata/opensource/mountainsea-lab/mdb`

## Strategy

Continue module-by-module completion first, then full pipeline integration later.

Do not start full `fdc-barter -> fdc-ingestion -> fdc-storage` glue yet.

Keep `fdc-storage` generic. Business DTO/types must enter via generics, codecs, schema, namespace, collection, tags, and placement hints. Prefer reusing `fdc-types` generic type concepts where appropriate.

Do not touch unrelated `fdc-server health` dirty files.

## fdc-storage completed phases

### S6: tier-scoped query + query metrics

- Added `StorageTierScope`, `StorageQueryMetrics`, `StorageQueryResult`.
- Added scoped prefix scan to `TierManager`.
- Added `TieredStorageStore::query_storage_with_metrics()` while keeping `QueryableStorage::query_storage()` compatible.
- Verified previously with `rtk cargo test -p fdc-storage`.

### S7: tier lifecycle / TTL hard-delete / retention demotion

- Added lifecycle report/action types.
- Added tier-specific put/delete and colder-tier lookup to `TierManager`.
- Added `TieredStorageStore::run_lifecycle_once()`.
- Supports TTL hard-delete, retention demotion, and cold-tier retention delete.

### S8: observability / explicit maintenance

- Added health snapshot and maintenance report types.
- Added `storage_health_snapshot()` and `run_maintenance_once()`.
- Maintenance runs lifecycle, compaction attempts, and health snapshot.

### S9: pre-integration closure baseline

- Added acceptance report:
  - `crates/fdc-storage/docs/storage-boundary-acceptance-report.md`
- Added production followups:
  - `crates/fdc-storage/docs/production-hardening-followups.md`
- Added dependency guard test.
- Added generic business contract test.
- Added public API stability smoke test.

### S10: P1 production hardening baseline

- Added `maintenance_control.rs`.
- Added maintenance re-entry guard.
- Added `run_maintenance_once_with_options()`.
- Added timeout option, with zero timeout defined as immediate timeout.
- Added optional audit sink skeleton and audit entry DTO.
- Added typed compaction outcomes: compacted / unsupported / failed.
- Added maintenance metrics snapshot and Prometheus text rendering helper.
- Updated acceptance report and production followups.

## Latest verification

After S10 merge:

```bash
rtk cargo test -p fdc-storage
```

Result: `111 passed`.

## Latest commits

- `f111215 docs(storage): plan s9 production hardening closure`
- `ed86180 test(storage): close pre-integration storage boundary`
- `153a22e docs(storage): plan s10 production hardening`
- `fd9a664 feat(storage): harden explicit maintenance baseline`

## Current dirty files to avoid

These are pre-existing unrelated files. Do not modify unless explicitly asked:

- Deleted:
  - `crates/fdc-server/src/health/mod.rs`
  - `crates/fdc-server/src/health/model.rs`
  - `crates/fdc-server/src/health/router.rs`
  - `crates/fdc-server/src/health/service.rs`
- Untracked:
  - `crates/fdc-server/src/bin/health/`

## Recommended next development

Next: `fdc-ingestion` module-internal storage contract readiness.

Suggested scope:

1. Explore current `fdc-ingestion` output/write boundary.
2. Design how ingestion output maps to generic `StorageWriteRecord` or typed storage records.
3. Add module-local contract tests proving the mapping works without making `fdc-storage` depend on ingestion.
4. Record production follow-up notes for ingestion.

Still defer:

- full barter -> ingestion -> transform -> storage pipeline glue;
- server/API query integration;
- runtime scheduler/exporter wiring;
- storage P2/P3 items like durable audit persistence, disk health, index, shard, backup.

## Required implementation workflow for next phase

- Use isolated worktree.
- Write spec and plan first.
- Run focused tests and target module full tests.
- Commit changes.
- Fast-forward merge to `mdb-mqdev`.
- Re-run target tests on main worktree.
- Remove generated data/worktree/branch.
