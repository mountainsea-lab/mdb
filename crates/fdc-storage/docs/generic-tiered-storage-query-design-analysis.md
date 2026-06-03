# fdc-storage 通用冷热分级存储与查询目标分析

日期：2026-06-03
模块：`crates/fdc-storage`
状态：设计分析，待 review

## 1. 目标修正

本文件基于当前 `fdc-storage` 已有实现，明确后续存储模块目标。设计重点不是只为市场数据写一个专用存储，而是先把 `fdc-storage` 建成一个通用的存储与查询模块：

1. 复用并演进现有 `StorageEngine`、`StorageTier`、`StorageWriteRecord`、`StorageWriteSink`、`QueryableMarketDataStore` 等边界。
2. 提供通用写入、冷热分级、索引和查询能力。
3. 具体业务数据类型由业务侧通过泛型、codec、schema、namespace、collection 和 metadata tags 指定。
4. 框架通用类型优先复用 `fdc-types` 中已有的 `TypeDefinition`、`TypeSchema`、`TypeRegistry`、`TypeValidator`、`SerializationFormat`、`TypeSerializer` 和金融基础类型，避免在 storage 内重复定义类型系统。
5. `fdc-storage` 不依赖 `fdc-barter`、`fdc-ingestion`、`fdc-transform`、`fdc-orchestrator`、`fdc-api`。
6. 全流程贯通暂不执行，先完成 storage 模块内部可测试能力。

因此，市场数据只是第一个高价值业务场景，不应该把存储模块设计成只认识 market data 的专用模块。

## 2. 当前实现盘点

### 2.1 已有公共模块

`crates/fdc-storage/src/lib.rs` 公开了以下模块：

- `engine`：通用存储引擎抽象。
- `tier`：L1-L4 冷热层级管理。
- `write`：storage-owned 写入记录、批次和 placement hints。
- `sink`：写入 sink 边界与内存 recording sink。
- `queryable`：当前 in-memory market-data 查询边界。
- `config`：多层级和存储系统配置。
- `cache`、`compression`、`index`、`metrics`、`backup`、`replication`、`shard`：完整存储系统能力的雏形。
- `engines/*`：Memory、redb、DuckDB、RocksDB 引擎目录。

这说明模块已经按“通用多引擎存储系统”方向组织，而不是单一业务存储。

### 2.2 当前可复用的通用边界

#### `StorageWriteRecord`

当前定义位于 `src/write.rs`：

```rust
pub struct StorageWriteRecord {
    pub namespace: String,
    pub collection: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub metadata: StorageWriteMetadata,
    pub placement: StoragePlacementHint,
}
```

这个结构是通用存储模块的核心资产：

- `namespace` 表示业务域，例如 `market_data`、`factor`、`strategy_state`、`risk`。
- `collection` 表示业务域内集合，例如 `trades`、`candles`、`features`、`signals`。
- `key` 和 `value` 是 engine-agnostic 的二进制边界。
- `timestamp` 可作为通用时间排序和 retention 的基础。
- `metadata` 可承载 schema、source、tags。
- `placement` 可承载冷热层级、TTL、分片、耐久性提示。

因此，后续不应替换它，而应在它上面增加 typed helper / codec / query abstraction。

#### `StorageWriteSink`

当前定义位于 `src/sink.rs`：

```rust
#[async_trait]
pub trait StorageWriteSink: Send + Sync {
    async fn write_batch(&self, batch: StorageWriteBatch) -> Result<StorageWriteOutcome>;
}
```

这是通用批量写入边界。它已经支持 orchestrator 或其他业务模块把业务 DTO 映射为 storage-owned record 后写入。后续应保留这个边界，并增加更通用的 query/read 边界与 tier-aware 实现。

#### `StoragePlacementHint`

当前定义位于 `src/write.rs`：

```rust
pub struct StoragePlacementHint {
    pub target_tier: Option<StorageTier>,
    pub access_pattern: StorageAccessPatternHint,
    pub durability: StorageDurabilityHint,
    pub shard_key: Option<Vec<u8>>,
    pub ttl: Option<Duration>,
}
```

这已经是冷热分级和数据生命周期策略的入口。后续实现应该让 routing policy 读取这些 hints，而不是让业务代码直接选择底层 engine。

### 2.3 当前查询边界的局限

当前 `src/queryable.rs` 提供 `MarketDataQuery` 和 `QueryableMarketDataStore`，但它是市场数据专用 MVP：

```rust
pub struct MarketDataQuery {
    pub namespace: String,
    pub collection: Option<String>,
    pub symbol: Option<String>,
    pub kind: Option<String>,
    pub limit: Option<usize>,
}
```

它的价值在于证明 `StorageWriteSink` 写入的 records 可以被查询，但它还不是通用查询模块。后续应把该能力泛化为：

- 通用 `StorageQuery`，支持 namespace、collection、key range、time range、tags、limit、order。
- 通用 `QueryableStorage` trait，返回 `StorageWriteRecord` 或 typed decoded records。
- 业务可在此之上定义 `MarketDataQuery`、`FactorQuery`、`SignalQuery` 等 wrapper。

## 3. 当前冷热分级设计现状

### 3.1 层级定义

当前 `StorageTier` 定义：

| Tier | 当前默认 engine | 当前含义 | 建议目标 |
| --- | --- | --- | --- |
| L1 | Memory | 超热缓存 | 最近访问数据、短 TTL、latest/read-through cache |
| L2 | redb | 热数据缓存 | 近期持久 KV/range 数据、幂等索引、checkpoint |
| L3 | DuckDB | 温数据存储 | SQL/分析查询、批量历史、时间序列表 |
| L4 | RocksDB | 冷数据存储 | 归档层或长期 KV；未来也可扩展 Parquet/object archive |

当前分层方向合理，但实现还不完整。

### 3.2 当前 TierManager 状态

`TierManager` 已支持：

- 添加 tier config。
- 初始化各 tier engine。
- `get` 按 L1 -> L4 查找。
- `put` 根据访问模式决定初始层级。
- `delete` 从全部层级删除。
- 访问模式跟踪。
- promotion migration queue。
- `process_migrations`。

但它还没有完成生产级冷热分级：

- `StoragePlacementHint` 尚未真正参与 `TierManager::put` 的 routing。
- 默认新数据进入 L2，策略过于简单。
- 有 promotion，但缺少 demotion、TTL expiration、retention enforcement。
- 缺少跨层查询 merge/dedupe/order。
- Redb、DuckDB、RocksDB engine 目前多数方法仍是 `unimplemented`。

### 3.3 引擎可用性

当前真实可用程度：

| Engine | 当前实现状态 | 评价 |
| --- | --- | --- |
| Memory | `get/put/delete/batch/scan/stats` 基本可用 | 可作为 S1 合约和内存实现基础 |
| Redb | skeleton，主要方法未实现 | L2 目标明确，但未落地 |
| DuckDB | skeleton，SQL query 未实现 | L3 分析目标明确，但未落地 |
| RocksDB | skeleton，主要方法未实现 | L4 目标明确，但未落地 |

因此下一阶段应先完成通用语义和内存合约，再逐步落地 L2/L3/L4。

## 4. 通用泛型存储设计方向

### 4.1 不把业务类型写死进 storage

`fdc-storage` 应避免直接定义只服务市场数据的强业务结构，例如只包含 `symbol`、`exchange`、`kind` 的唯一查询类型。更好的方式是：

- storage core 只认识 namespace、collection、key、timestamp、tags、payload bytes、placement。
- 业务模块负责把具体类型编码成 bytes，并提供 schema metadata。
- storage 可以提供 generic typed facade，方便业务用强类型读写。

建议分三层：

```text
业务 DTO
  ↓ codec/schema
TypedStorage<T>
  ↓ encode/decode
StorageWriteRecord / StorageQuery
  ↓ route/query
StorageEngine / TierManager
```

### 4.2 Codec 抽象

建议后续设计一个通用 codec：

```rust
pub trait StorageCodec<T>: Send + Sync {
    fn content_type(&self) -> &'static str;
    fn schema(&self) -> &'static str;
    fn schema_version(&self) -> &'static str;
    fn encode(&self, value: &T) -> Result<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Result<T>;
}
```

默认可提供：

- `JsonStorageCodec<T>`：要求 `T: Serialize + DeserializeOwned`。
- `BincodeStorageCodec<T>`：用于更紧凑的内部数据。

这样 storage 支持泛型，但不需要知道具体业务类型。

### 4.3 Typed write helper

建议在保留 `StorageWriteRecord` 的基础上增加 typed helper，而不是替代现有 record：

```rust
pub struct TypedStorageRecord<T> {
    pub namespace: String,
    pub collection: String,
    pub key: Vec<u8>,
    pub value: T,
    pub timestamp: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub placement: StoragePlacementHint,
}
```

它通过 codec 转成 `StorageWriteRecord`。这样业务模块可以指定 `T`，storage core 仍保持 bytes 边界。

### 4.4 通用查询模型

建议引入通用 `StorageQuery`：

```rust
pub struct StorageQuery {
    pub namespace: String,
    pub collection: Option<String>,
    pub key_prefix: Option<Vec<u8>>,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub tags: BTreeMap<String, String>,
    pub limit: Option<usize>,
    pub order: StorageQueryOrder,
    pub tier_scope: StorageTierScope,
}
```

其中：

```rust
pub enum StorageQueryOrder {
    Asc,
    Desc,
}

pub enum StorageTierScope {
    All,
    Only(StorageTier),
    Hot,
    Warm,
    Cold,
}
```

市场数据查询可以作为业务 wrapper：

```rust
MarketDataQuery::for_trades()
    .with_symbol("BTCUSDT")
    .with_time_range(start, end)
```

最终转换为：

```rust
StorageQuery {
    namespace: "market_data",
    collection: Some("trades"),
    tags: { "symbol": "BTCUSDT", "kind": "trade" },
    start_time: Some(start),
    end_time: Some(end),
    ..
}
```

这样既满足市场数据，也满足未来 factors、signals、risk、strategy state。

### 4.5 通用查询 trait

建议增加：

```rust
#[async_trait]
pub trait QueryableStorage: Send + Sync {
    async fn query_records(&self, query: StorageQuery) -> Result<StorageQueryResult>;
    async fn get_record(&self, namespace: &str, collection: &str, key: &[u8]) -> Result<Option<StorageWriteRecord>>;
}
```

再提供 typed facade：

```rust
pub struct TypedQueryableStorage<T, C> {
    inner: Arc<dyn QueryableStorage>,
    codec: C,
    _marker: PhantomData<T>,
}
```

这样业务侧可以查询 `Vec<T>`，但 storage core 不依赖业务类型。

## 5. 冷热分级目标

### 5.1 写入 routing 目标

后续 routing policy 应按以下优先级决定目标层：

1. `StoragePlacementHint.target_tier` 明确指定时优先使用。
2. `StoragePlacementHint.access_pattern` 指定热度时映射 tier。
3. `StoragePlacementHint.durability` 指定耐久性时映射 tier。
4. `ttl` 较短的数据可进入 L1/L2。
5. 未指定时按默认 policy，例如 persistent warm data 进入 L2 或 L3。

建议映射：

| Hint | 推荐 tier |
| --- | --- |
| `UltraHot + Ephemeral` | L1 |
| `Hot + Cached/Persistent` | L2 |
| `Warm + Persistent` | L3 |
| `Cold + Archival` | L4 |
| explicit `target_tier` | 指定 tier |

### 5.2 查询 routing 目标

查询不应该永远扫全部层。建议：

- latest/recent 查询：优先 L1/L2。
- time range 覆盖近期：L2，必要时补 L3。
- time range 覆盖历史：L3/L4。
- `tier_scope` 明确时只查指定层。
- 跨层查询必须 merge、dedupe、sort、limit。

### 5.3 数据生命周期目标

需要明确三类迁移：

1. Promotion：访问频繁的数据从 L3/L4 提升到 L2/L1。
2. Demotion：TTL/retention 到期或热度降低的数据从 L1/L2 下沉到 L3/L4。
3. Expiration：明确 TTL 到期的数据删除或归档。

当前代码已有 promotion queue 雏形，后续应补 demotion 和 expiration policy。

## 6. 查询功能目标

### S1：通用内存查询合约

先在现有 `QueryableMarketDataStore` 基础上泛化，目标是合约清楚而非数据库落地：

- 通用 `StorageQuery`。
- namespace/collection 过滤。
- exact tag filters。
- key prefix/range。
- timestamp range。
- limit。
- asc/desc ordering。
- query result metadata，例如 scanned/returned/tier hits。
- 继续保持 dependency guard。

### S2：Typed facade 和 codec

在 S1 之上增加：

- `StorageCodec<T>`。
- `JsonStorageCodec<T>`。
- `TypedStorageRecord<T>` helper。
- typed query decode。
- decode error handling。

### S3：Tier-aware storage 实现

把通用写入和查询接入 `TierManager`：

- placement hint routing。
- tier-scoped query。
- cross-tier merge/dedupe/order。
- MemoryEngine 用作 L1 可测实现。
- Redb/DuckDB/RocksDB 仍可逐步实现。

### S4：持久化引擎落地

按顺序实现：

1. Redb 或 RocksDB 的 KV/range scan，用于 L2。
2. DuckDB 的表结构、batch insert 和 SQL/time-range query，用于 L3。
3. Parquet/Arrow 冷归档扩展，用于 L4 长期分析数据。

## 7. 市场数据作为第一个业务用例

虽然 storage 需要通用化，但市场数据仍适合作为第一个验证用例。原因：

- 已有 `fdc-barter` 产出 trade/orderbook/candle/liquidation。
- 已有 `StorageWriteRecord` metadata tags。
- 已有 `QueryableMarketDataStore` 合约。
- 市场数据天然需要时间范围、latest、冷热分级和幂等去重。

但实现时应保持：

- `MarketDataQuery` 是 `StorageQuery` 的 wrapper，不是 storage core 的唯一查询类型。
- market data schema 通过 namespace/collection/tags/codec 表达。
- storage core 不 import `fdc-barter` 或 `fdc-transform`。

建议市场数据 convention：

| 字段 | 表达方式 |
| --- | --- |
| 业务域 | `namespace = "market_data"` |
| 数据集 | `collection = "trades" / "candles" / "order_book_l1" / ...` |
| 业务类型 | `metadata.schema = "market_data.trade"` |
| 版本 | `metadata.schema_version = "1"` |
| symbol/exchange/kind | `metadata.tags` |
| 时间 | `StorageWriteRecord.timestamp`，业务 payload 内可保留 event_time/received_at |
| bytes 编码 | `JsonStorageCodec<T>` 或未来 `BincodeStorageCodec<T>` |
| 冷热分层 | `StoragePlacementHint` |

## 8. 不建议现在做的事情

当前阶段不建议：

- 不做 fdc-barter -> fdc-ingestion -> fdc-storage 全流程贯通。
- 不把 `MarketDataQuery` 扩成一个越来越大的业务专用查询核心。
- 不一次性实现 redb、DuckDB、RocksDB、Parquet 全部引擎。
- 不让 storage 依赖 adapter、ingestion、orchestrator 或 API。
- 不在 storage core 中硬编码交易所、symbol、market type 等金融专用枚举。

## 9. 推荐下一步

建议下一步先写正式设计和计划，目标为：

`fdc-storage Phase S1: Generic Query and Typed Storage Boundary`

S1 验收标准：

1. 基于现有 `StorageWriteRecord` 和 `StorageWriteSink`，新增通用查询边界。
2. 当前 `QueryableMarketDataStore` 要么演进为 `InMemoryQueryableStorage`，要么保留为业务 wrapper。
3. 支持 namespace、collection、tags、time range、key range、limit、order。
4. 支持业务类型通过 codec 泛型读写，但 storage core 仍只存 bytes。
5. 冷热分级只做 policy/placement 合约，不急着落地全部持久化引擎。
6. 用市场数据 contract tests 验证通用边界可承载真实业务场景。

完成 S1 后，再进入 S2/S3 的 tier-aware 和持久化引擎实现。
