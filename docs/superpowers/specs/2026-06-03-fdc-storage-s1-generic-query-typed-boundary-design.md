# fdc-storage Phase S1 Generic Query and Typed Storage Boundary Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：设计规格，基于已审查的模块分析继续推进

## 1. 背景

`fdc-storage` 当前已经有通用存储雏形：

- `StorageEngine` 定义 engine-agnostic KV 能力。
- `StorageWriteRecord` / `StorageWriteBatch` / `StorageWriteSink` 定义 storage-owned 写入边界。
- `StoragePlacementHint` 定义未来冷热分级、durability、shard、TTL hint。
- `QueryableMarketDataStore` 证明了写入后可查询，但查询模型仍是 market-data 专用。
- `fdc-types` 已经提供 `TypeDefinition`、`TypeSchema`、`TypeRegistry`、`TypeValidator`、`SerializationFormat`、`TypeSerializer` 和金融基础类型。

S1 的目标是在不做全流程集成、不绑定市场数据业务的前提下，把 storage 内部提升为通用 typed storage/query 边界。

## 2. 目标

S1 只完成 storage 模块内可测试的通用能力：

1. 引入通用 `StorageQuery`，支持 namespace、collection、key prefix/range、time range、metadata tags、limit、order。
2. 引入通用 `QueryableStorage` trait，返回 raw `StorageWriteRecord`。
3. 引入 typed codec/facade，让业务模块可以用 `T` 读写，但底层仍使用 `StorageWriteRecord` bytes 边界。
4. 优先复用 `fdc-types` 作为 schema/type/serialization 的通用框架，不在 storage 内重复定义类型系统。
5. 保留 `MarketDataQuery` / `QueryableMarketDataStore` 作为兼容 wrapper，不把它作为核心边界继续扩大。
6. 以 in-memory 实现为 S1 合约验证对象，不在 S1 落地 redb/DuckDB/RocksDB。

## 3. 非目标

S1 不做以下事项：

- 不连接 `fdc-barter`、`fdc-ingestion`、`fdc-transform`、`fdc-orchestrator` 或 `fdc-api`。
- 不实现生产级 L2/L3/L4 引擎。
- 不实现 SQL planner，也不依赖 `fdc-query` 的 SQL 语义。
- 不把 market-data DTO 写死进 storage。
- 不迁移或修改无关 `fdc-server health` dirty files。

## 4. 推荐方案

采用“raw storage core + typed facade + fdc-types schema metadata”的方案。

### 4.1 Raw core

`StorageWriteRecord` 继续作为唯一底层写入记录：

- `namespace`：业务域，例如 `market_data`、`factor`、`strategy_state`。
- `collection`：业务域内集合，例如 `trades`、`candles`、`features`。
- `key` / `value`：bytes 边界。
- `timestamp`：通用时间过滤和排序基础。
- `metadata.tags`：业务可扩展索引/过滤标签。
- `metadata.schema` / `schema_version` / `content_type`：typed/schema 信息入口。

### 4.2 Generic query

新增 `query.rs`，定义：

- `StorageQuery`
- `StorageQueryOrder`
- `QueryableStorage`
- query match helper

`StorageQuery` 不认识业务字段，只认识通用 storage 属性。业务字段如果需要过滤，先进入 `metadata.tags`。例如 market data 的 `symbol`、`exchange`、`kind` 都是 tags。

### 4.3 Typed codec/facade

新增 `codec.rs` 和 `typed.rs`：

- `StorageCodec<T>`：把 `T` encode/decode 到 bytes。
- `JsonStorageCodec<T>`：默认 JSON codec。
- `BincodeStorageCodec<T>`：默认 binary codec。
- `StorageTypeDescriptor`：描述 typed record 的 schema/type metadata，复用 `fdc-types::TypeDefinition`、`fdc-types::TypeSchema`、`fdc-types::SerializationFormat`。
- `TypedStorageRecord<T>`：业务值 + namespace/collection/key/timestamp/tags/placement。
- typed helper 把 `TypedStorageRecord<T>` 转成 `StorageWriteRecord`。
- typed query helper 把 raw query 结果 decode 为 `TypedStorageReadRecord<T>`。

### 4.4 fdc-types 复用规则

S1 中 storage 只补充与存储相关的 lightweight descriptor，不新增自己的类型系统：

- 用 `fdc_types::TypeDefinition` 表达单类型定义。
- 用 `fdc_types::TypeSchema` 表达 schema bundle。
- 用 `fdc_types::SerializationFormat` 表达 JSON/Binary 等序列化格式。
- 可选使用 `fdc_types::TypeRegistry` 做类型注册，但 S1 不强制全局 registry。
- 可选使用 `fdc_types::TypeValidator` 做值级校验，但 S1 typed codec 只做 serialization/de-serialization contract。

## 5. 文件结构

S1 建议变更：

- 新增 `crates/fdc-storage/src/query.rs`
  - 通用 query model 和 matching helper。
- 新增 `crates/fdc-storage/src/codec.rs`
  - generic codec trait 和 JSON/Bincode codec。
- 新增 `crates/fdc-storage/src/typed.rs`
  - typed record、type descriptor、typed facade helper。
- 修改 `crates/fdc-storage/src/queryable.rs`
  - 把 in-memory store 泛化为 `InMemoryQueryableStorage`。
  - 保留 `QueryableMarketDataStore` 兼容别名/wrapper。
- 修改 `crates/fdc-storage/src/lib.rs`
  - 导出新模块和常用类型。
- 修改 `crates/fdc-storage/docs/generic-tiered-storage-query-design-analysis.md`
  - 已补充 `fdc-types` 复用约束。

## 6. 数据流

```text
Business DTO T
  ↓ StorageCodec<T> + StorageTypeDescriptor(fd-types)
TypedStorageRecord<T>
  ↓ encode
StorageWriteRecord
  ↓ StorageWriteSink
InMemoryQueryableStorage / future TierManager sink
  ↓ StorageQuery
Vec<StorageWriteRecord>
  ↓ decode_with(codec)
Vec<TypedStorageReadRecord<T>>
```

## 7. 查询语义

`StorageQuery` 应满足：

- namespace 必填且非空。
- collection 可选。
- key_prefix、start_key、end_key 可选。
- start_key/end_key 使用 lexicographic bytes ordering。
- start_time/end_time 使用 inclusive range。
- tags 是 AND 语义，所有指定 tag 都必须匹配。
- limit 可选，应用于排序后的结果。
- order 默认 `Insertion`，可选 `TimestampAsc`、`TimestampDesc`、`KeyAsc`、`KeyDesc`。

## 8. 兼容策略

`MarketDataQuery` 保留，但实现方式调整为转换成 `StorageQuery`：

- `namespace` -> `StorageQuery.namespace`
- `collection` -> `StorageQuery.collection`
- `symbol` -> tag `symbol`
- `kind` -> tag `kind`
- `limit` -> `StorageQuery.limit`

这样现有 market-data smoke/query tests 不需要破坏，后续业务 wrapper 也可以照此模式实现。

## 9. 错误处理

- record/query/typed descriptor 的空 namespace、空 collection、空 key 等错误使用 `fdc_core::error::Error::validation`。
- codec encode/decode 失败直接返回 `fdc_core::Result`。
- schema descriptor 不强制 validate unknown business schema，但如果传入 `TypeSchema`，应允许调用 `SchemaValidation::validate_schema` 的扩展点。

## 10. 测试策略

S1 验证以 unit tests 为主：

1. `StorageQuery` builder 和 validation。
2. query match：namespace、collection、key prefix/range、time range、tags、limit、order。
3. `InMemoryQueryableStorage` 实现 `StorageWriteSink` 和 `QueryableStorage`。
4. `MarketDataQuery` wrapper 兼容现有语义。
5. JSON codec roundtrip。
6. Bincode codec roundtrip。
7. typed record encode 后 metadata 包含 content type、schema name/version、serialization format。
8. typed query decode 返回业务类型。

验证命令：

```bash
rtk cargo test -p fdc-storage
```

## 11. 验收标准

S1 完成时应满足：

- `fdc-storage` 有通用 raw query boundary，而不是只有 market-data query。
- typed storage boundary 能用任意 `T: Serialize + DeserializeOwned` 做 JSON/Bincode roundtrip。
- typed descriptor 复用 `fdc-types` 的 `TypeDefinition` / `TypeSchema` / `SerializationFormat`。
- market-data wrapper 保留且测试通过。
- `rtk cargo test -p fdc-storage` 通过。
- 没有触碰 `fdc-server health` dirty files。

## 12. 自检

- 无占位符。
- scope 聚焦 storage S1，未包含 pipeline glue。
- 设计依赖当前已有 `StorageWriteRecord`、`StorageWriteSink`、`QueryableMarketDataStore`，不是重写 storage。
- `fdc-types` 复用约束已明确。
