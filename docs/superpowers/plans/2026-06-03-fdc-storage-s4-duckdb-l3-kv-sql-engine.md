# fdc-storage Phase S4 DuckDB L3 KV and SQL Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `DuckDBEngine` as the L3 warm persistent analytical engine for `fdc-storage`, preserving the generic KV `StorageEngine` contract and adding basic SQL query support.

**Approved Spec:** `docs/superpowers/specs/2026-06-03-fdc-storage-s4-duckdb-l3-kv-sql-engine-design.md`

**Architecture:** Store opaque `key`/`value` bytes in a generic DuckDB KV table. Use `spawn_blocking` for synchronous DuckDB operations. Map SQL result values into `fdc_core::types::Value`. Add L3 integration coverage through `TieredStorageStore` placement.

---

## File Structure

- Modify: `crates/fdc-storage/src/engines/duckdb.rs`
  - Replace skeleton with real DuckDB-backed `StorageEngine` implementation.
  - Add focused DuckDB tests.
- Modify: `crates/fdc-storage/src/tiered_store.rs`
  - Add L3 DuckDB integration test.
- Do not modify unrelated `fdc-server health` files.

---

## Task 1: Create isolated worktree and baseline check

- [ ] Create branch/worktree:

```bash
git worktree add .worktrees/fdc-storage-s4-duckdb -b fdc-storage-s4-duckdb
cd .worktrees/fdc-storage-s4-duckdb
```

- [ ] Verify baseline:

```bash
rtk cargo test -p fdc-storage
```

Expected: current storage suite passes before S4 edits.

---

## Task 2: Implement DuckDBEngine persistent KV core

**File:** `crates/fdc-storage/src/engines/duckdb.rs`

- [ ] Replace skeleton fields with:
  - `db_path: PathBuf`
  - thread-safe DuckDB `Connection` holder, for example `Arc<Mutex<Option<Connection>>>`
  - thread-safe `StorageStats`, for example `Arc<Mutex<StorageStats>>`

- [ ] Add helper functions:
  - `duckdb_error(error: impl Display) -> Error`
  - blocking connection accessor that returns validation/storage error if uninitialized.
  - `refresh_stats` that updates `key_count` and approximate `total_size`.

- [ ] Implement `initialize()`:
  - Create parent directory for file path when needed.
  - Open DuckDB connection.
  - Create table:

```sql
CREATE TABLE IF NOT EXISTS fdc_storage_kv (
    key BLOB PRIMARY KEY,
    value BLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

  - Create key index if supported.
  - Refresh stats.

- [ ] Implement `shutdown()`:
  - Drop/take the connection.
  - Return `Ok(())`.

- [ ] Implement `get/put/delete`:
  - `get`: `SELECT value FROM fdc_storage_kv WHERE key = ?1`.
  - `put`: transaction delete+insert or upsert.
  - `delete`: `DELETE FROM fdc_storage_kv WHERE key = ?1`.
  - update stats operation counters.

- [ ] Implement `batch(operations)`:
  - execute operations in a single transaction.
  - update stats after commit.

- [ ] Implement `scan(start_key, end_key, limit)`:
  - return `(key, value)` ordered by `key ASC`.
  - ranges are inclusive.
  - support optional limit.

---

## Task 3: Implement SQL query support

**File:** `crates/fdc-storage/src/engines/duckdb.rs`

- [ ] Override `query(sql)`.

- [ ] Map DuckDB result cells to `fdc_core::types::Value`:
  - NULL -> `Value::Null`
  - bool -> `Value::Bool`
  - signed integer -> `Value::Int64`
  - unsigned integer -> `Value::UInt64`
  - float/double -> `Value::Float64`
  - string -> `Value::String`
  - blob -> `Value::Binary`
  - unsupported/complex values -> `Value::String(format!(...))` if available, or `Value::Null` with explicit test scope avoiding those columns.

- [ ] Increment `stats.queries` for successful query execution.

---

## Task 4: Add DuckDB focused tests

**File:** `crates/fdc-storage/src/engines/duckdb.rs`

- [ ] Add tests using `tempfile::tempdir()` and unique `db_path`.

- [ ] Cover:
  - creation/capabilities.
  - put/get/delete roundtrip.
  - persistence after shutdown/reopen.
  - batch + ordered scan + limit.
  - stats key_count/total_size and operation counters.
  - SQL query returns generic `Value` rows for `COUNT(*)` and/or selected key/value bytes.

- [ ] Run focused tests:

```bash
rtk cargo test -p fdc-storage engines::duckdb::tests -- --nocapture
rtk cargo test -p fdc-storage duckdb -- --nocapture
```

---

## Task 5: Add TieredStorageStore L3 DuckDB integration test

**File:** `crates/fdc-storage/src/tiered_store.rs`

- [ ] Add `tiered_store_can_use_duckdb_l3_for_warm_records`.

Test shape:

1. Create temp DuckDB db path.
2. Configure `TierManager` with L3 `StorageEngineType::DuckDB` and `db_path`.
3. Initialize tier manager and `TieredStorageStore`.
4. Write a generic `StorageWriteRecord` with `StoragePlacementHint::for_tier(StorageTier::L3)`.
5. Query by namespace/collection/key/tags through `StorageQuery`.
6. Assert full raw record content is returned.

- [ ] Run integration test:

```bash
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_can_use_duckdb_l3_for_warm_records -- --nocapture
```

---

## Task 6: Format, full verification, commit, merge, cleanup

- [ ] Format:

```bash
rtk cargo fmt -p fdc-storage
```

- [ ] Full storage verification in worktree:

```bash
rtk cargo test -p fdc-storage
```

- [ ] Commit:

```bash
git add crates/fdc-storage/src/engines/duckdb.rs crates/fdc-storage/src/tiered_store.rs
git commit -m "feat(storage): implement duckdb l3 engine"
```

- [ ] Merge back to `mdb-mqdev`:

```bash
cd ../..
git merge --ff-only fdc-storage-s4-duckdb
rtk cargo test -p fdc-storage
```

- [ ] Clean worktree/branch:

```bash
git worktree remove .worktrees/fdc-storage-s4-duckdb
git worktree prune
git branch -d fdc-storage-s4-duckdb
```

---

## Plan Self-Review

- No placeholders.
- Tasks are module-local and testable.
- No pipeline integration or business DTO coupling.
- Uses current `StorageEngine` and `TieredStorageStore` boundaries.
- Explicitly preserves unrelated dirty files.
