# fdc-storage Phase S4 DuckDB L3 KV and SQL Engine Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准方案 B，待实现

## 1. 背景

`fdc-storage` 已完成：

- S1：通用 `StorageQuery`、`QueryableStorage`、typed codec/facade。
- S2：`TieredStorageStore` 与 placement-aware tier routing。
- S3：L2 `RedbEngine` 持久 KV engine。

当前 L3 的 `DuckDBEngine` 仍是 skeleton。设计分析中 L3 的目标是 warm analytical storage：既满足通用 `StorageEngine` KV contract，也提供基础 SQL 查询能力。

## 2. 目标

S4 在 `fdc-storage` 模块内完成 DuckDB L3 engine，不连接任何 pipeline：

1. 实现 `DuckDBEngine` 的 `initialize/get/put/delete/batch/scan/stats`。
2. 实现 `query(sql)`，返回 `Vec<HashMap<String, fdc_core::types::Value>>`。
3. 使用通用 KV 表，不引入业务 DTO 或 market-data 专用列。
4. 验证持久化重开、ordered scan、limit、batch、stats、SQL query。
5. 在 `TieredStorageStore` 增加 L3 DuckDB integration test，验证 placement 到 `StorageTier::L3` 后可通过 storage query 读回完整 `StorageWriteRecord`。

## 3. 非目标

S4 不做以下事项：

- 不做 `fdc-barter`、`fdc-ingestion`、`fdc-transform`、`fdc-orchestrator`、`fdc-api` glue。
- 不把业务字段展开成 DuckDB columns。
- 不实现 SQL planner 或 `fdc-query` 集成。
- 不实现 Arrow/Parquet 导出。
- 不实现 L4 RocksDB。
- 不修改无关 `fdc-server health` dirty files。

## 4. 推荐方案：KV contract + lightweight SQL visibility

DuckDB 表结构保持通用：

```sql
CREATE TABLE IF NOT EXISTS fdc_storage_kv (
    key BLOB PRIMARY KEY,
    value BLOB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

索引：

```sql
CREATE INDEX IF NOT EXISTS idx_fdc_storage_kv_key ON fdc_storage_kv(key);
```

说明：

- `key`/`value` 保持 bytes 边界，与 `StorageEngine` trait 一致。
- `created_at`/`updated_at` 只服务 engine 级 SQL 可见性和调试。
- `StorageWriteRecord` 的 namespace/collection/tags/schema 仍由上层 `TieredStorageStore` 编码进 value，不在 S4 拆列。

## 5. Engine 行为

### 5.1 初始化

`DuckDBEngine::new(config)` 读取：

- `db_path`：默认 `./data/duckdb.db`。

`initialize()`：

- 打开或创建 DuckDB database。
- 创建 KV 表和索引。
- 刷新 stats。

所有阻塞 DuckDB 操作通过 `tokio::task::spawn_blocking` 包装，避免阻塞 async runtime。

### 5.2 get/put/delete/batch

- `get(key)`：按 key 查询 `value`，不存在返回 `None`。
- `put(key, value)`：使用 upsert 语义。如果 DuckDB 版本对 `ON CONFLICT` 支持不稳定，可用 transaction 中 delete+insert 实现。
- `delete(key)`：删除 key，不存在不报错。
- `batch(operations)`：在一个 transaction 中顺序执行 put/delete。

### 5.3 scan

`scan(start_key, end_key, limit)`：

- 按 `key ASC` 返回。
- `start_key` inclusive。
- `end_key` inclusive。
- `limit` 可选。
- 对 bytes key 使用 DuckDB BLOB 比较。如果某些 API 类型转换不支持 BLOB range 参数，使用 SQL 参数绑定并在测试中验证 lexicographic ordering。

### 5.4 stats

`stats()` 返回 `StorageStats`：

- `key_count` 来自 `COUNT(*)`。
- `total_size` 使用 key/value byte length 近似求和。
- 操作计数记录 `reads/writes/deletes/scans/queries`。

### 5.5 query(sql)

`query(sql)` 执行调用方传入 SQL 并映射结果行到 `HashMap<String, Value>`：

- NULL -> `Value::Null`
- bool -> `Value::Bool`
- signed integer -> `Value::Int64`
- unsigned integer -> `Value::UInt64`
- float/double -> `Value::Float64`
- text/varchar -> `Value::String`
- blob -> `Value::Binary`
- timestamp/date/time 等复杂或 API 不易区分类型可先转为 `Value::String`

S4 的 SQL 测试只要求查询 `fdc_storage_kv` 的 `key`/`value`/`COUNT(*)` 能得到稳定结果。

## 6. Error handling

新增 DuckDB error conversion helper：

```rust
fn duckdb_error(error: impl std::fmt::Display) -> Error {
    Error::storage(format!("DuckDB storage error: {error}"))
}
```

所有 DuckDB API error 和 `spawn_blocking` join error 都显式转换为 `fdc_core::Error`。

## 7. 测试策略

新增/更新 tests：

1. `test_duckdb_engine_creation`：capabilities 仍支持 SQL/compression。
2. `duckdb_put_get_delete_roundtrip`。
3. `duckdb_persists_records_after_reopen`。
4. `duckdb_batch_and_ordered_scan_with_limit`。
5. `duckdb_stats_track_key_count_and_size`。
6. `duckdb_query_returns_generic_values`。
7. `tiered_store_can_use_duckdb_l3_for_warm_records`。

验证命令：

```bash
rtk cargo test -p fdc-storage engines::duckdb::tests -- --nocapture
rtk cargo test -p fdc-storage duckdb -- --nocapture
rtk cargo test -p fdc-storage tiered_store::tests::tiered_store_can_use_duckdb_l3_for_warm_records -- --nocapture
rtk cargo test -p fdc-storage
```

## 8. 验收标准

S4 完成时应满足：

- `DuckDBEngine` 不再是 skeleton，KV contract 可用。
- L3 支持持久化和 SQL 查询能力。
- `TieredStorageStore` 可将 record placement 到 L3 DuckDB 并查回。
- `rtk cargo test -p fdc-storage` 通过。
- 没有触碰 `fdc-server health` dirty files。

## 9. 自检

- 无占位符或 TODO 需求。
- scope 只在 storage 模块内。
- 保持通用 KV/SQL 可见性，不引入业务 DTO columns。
- 与 S1/S2/S3 边界兼容。
