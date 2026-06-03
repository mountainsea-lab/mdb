# fdc-storage Phase S5 RocksDB L4 Persistent KV Engine Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准，待实现

## 1. 背景

`fdc-storage` 已完成模块内基础能力：

- S1：通用 query boundary、typed codec/facade。
- S2：tier-aware `TieredStorageStore` 与 placement routing。
- S3：L2 `RedbEngine` 持久 KV/range engine。
- S4：L3 `DuckDBEngine` 持久 KV + SQL engine。

当前四层存储矩阵中只剩 L4 `RocksDBEngine` 仍是 skeleton。L4 目标是冷数据/长期 KV 归档层，先补齐与 `StorageEngine` 一致的通用 KV 能力，再进入后续 lifecycle/demotion/retention。

## 2. 目标

S5 在 `fdc-storage` 模块内完成 RocksDB L4 engine：

1. 实现 `RocksDBEngine` 的 `initialize/get/put/delete/batch/scan/stats/compact`。
2. 保持通用 bytes KV 边界，不引入业务 DTO 或 market-data 专用列。
3. 支持持久化重开、ordered scan、range scan、limit、batch、stats。
4. 在 `TieredStorageStore` 增加 L4 RocksDB integration test，验证 placement 到 `StorageTier::L4` 后可通过 `StorageQuery` 读回完整 `StorageWriteRecord`。
5. 保持 S1-S4 测试通过。

## 3. 非目标

S5 不做以下事项：

- 不做 `fdc-barter`、`fdc-ingestion`、`fdc-transform`、`fdc-orchestrator`、`fdc-api` glue。
- 不做 tier demotion、retention enforcement、TTL hard delete。后续 S6 处理 lifecycle。
- 不做 RocksDB replication/backup/snapshot 的生产实现。
- 不做 column family 复杂 schema 或业务专用索引。
- 不修改无关 `fdc-server health` dirty files。

## 4. 推荐方案

`RocksDBEngine` 使用单一 default column family，保持和 `StorageEngine` trait 一致：

- key：opaque `Vec<u8>`。
- value：opaque `Vec<u8>`。
- scan：通过 RocksDB iterator 从 `start_key` 起按 key ascending 遍历，`end_key` inclusive，`limit` 可选。
- batch：使用 `rocksdb::WriteBatch` 顺序执行 put/delete。
- stats：维护操作计数，`key_count` 和 `total_size` 通过 iterator 刷新。
- compact：调用 RocksDB range compaction。

## 5. Engine 行为

### 5.1 初始化

`RocksDBEngine::new(config)` 读取：

- `db_path`：默认 `./data/rocksdb`。

`initialize()`：

- 创建 parent directory。
- 使用 `rocksdb::Options::default()`，设置：
  - `create_if_missing(true)`
  - `set_compression_type(DBCompressionType::Lz4)` 或 Snappy/Lz4 可用压缩。
- 打开 DB 并缓存到 engine 内。
- 刷新 stats。

### 5.2 get/put/delete/batch

- `get(key)`：不存在返回 `None`。
- `put(key, value)`：key 不能为空。
- `delete(key)`：不存在不报错。
- `batch(operations)`：使用 RocksDB `WriteBatch`，key 不能为空。

### 5.3 scan

`scan(start_key, end_key, limit)`：

- 使用 `IteratorMode::Start` 或 `IteratorMode::From(start_key, Direction::Forward)`。
- 返回 key ascending。
- `start_key` inclusive。
- `end_key` inclusive。
- `limit` 达到后停止。

### 5.4 stats/compact

- `stats()` 先刷新 key_count/total_size，再返回 clone。
- `compact()` 调用 `compact_range::<&[u8], &[u8]>(None, None)` 并记录成功。
- `snapshot()` 可以继续返回 unimplemented，S5 不承诺生产快照。

## 6. Error handling

新增 helper：

```rust
fn rocksdb_error(error: impl std::fmt::Display) -> Error {
    Error::storage(format!("RocksDB storage error: {error}"))
}
```

所有 RocksDB API error 显式转换为 `fdc_core::Error`。若 engine 未初始化，返回 validation error：`RocksDB engine is not initialized`。

## 7. 测试策略

新增/更新 tests：

1. `test_rocksdb_engine_creation`：capabilities 保持 compression/replication。
2. `rocksdb_put_get_delete_roundtrip`。
3. `rocksdb_persists_records_after_reopen`。
4. `rocksdb_batch_and_ordered_scan_with_limit`。
5. `rocksdb_scan_respects_inclusive_key_range`。
6. `rocksdb_stats_track_key_count_and_size`。
7. `rocksdb_compact_succeeds`。
8. `tiered_store_can_use_rocksdb_l4_for_cold_records`。

验证命令：

```bash
rtk cargo test -p fdc-storage engines::rocksdb::tests -- --nocapture
rtk cargo test -p fdc-storage rocksdb -- --nocapture
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_can_use_rocksdb_l4_for_cold_records -- --nocapture
rtk cargo test -p fdc-storage
```

## 8. 验收标准

S5 完成时应满足：

- `RocksDBEngine` 不再是 skeleton，KV contract 可用。
- L4 支持持久化、batch、range scan、compaction。
- `TieredStorageStore` 可将 record placement 到 L4 RocksDB 并查回。
- `rtk cargo test -p fdc-storage` 通过。
- 没有触碰 `fdc-server health` dirty files。

## 9. 自检

- 无占位符。
- scope 只在 storage 模块内。
- 保持通用 KV，不引入业务 DTO columns。
- S5 为后续 lifecycle/demotion/retention 奠定 L4 目标层。
