# fdc-storage Phase S6 Tier-Scoped Query and Query Metrics Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准，待实现

## 1. 背景

`fdc-storage` 已完成模块内基础存储矩阵：

- S1：通用 `StorageQuery`、`QueryableStorage`、typed codec/facade。
- S2：`TieredStorageStore` 与 placement-aware tier routing。
- S3：L2 `RedbEngine`。
- S4：L3 `DuckDBEngine`。
- S5：L4 `RocksDBEngine`。

当前 `TieredStorageStore::query_storage` 已可跨层扫描并去重，但仍缺少两个能力：

1. 查询方不能指定 tier scope，只能隐式扫描全部已初始化 tier。
2. 查询没有 metrics，无法知道扫描了哪些 tier、扫描了多少 raw entries、返回多少 records。

S6 目标是在不破坏现有 `QueryableStorage` trait 的前提下，补齐 tier-scoped query 与 metrics，为后续 S7 lifecycle/demotion/expiration 提供可验证观察面。

## 2. 目标

1. 在 `StorageQuery` 增加 `tier_scope: StorageTierScope`。
2. 增加 `StorageTierScope`：
   - `All`
   - `Only(StorageTier)`
   - `Hot` = L1/L2
   - `Warm` = L3
   - `Cold` = L4
3. 增加 `StorageQueryResult` 和 `StorageQueryMetrics`，记录 records 与 query statistics。
4. 保持 `QueryableStorage::query_storage(&StorageQuery) -> Vec<StorageWriteRecord>` 向后兼容。
5. 在 `TieredStorageStore` 增加 `query_storage_with_metrics(&StorageQuery) -> StorageQueryResult`。
6. 在 `TierManager` 增加 scoped prefix scan，支持只扫描指定 tiers。
7. 增加测试覆盖 Hot/Warm/Cold/Only scope 和 metrics。

## 3. 非目标

S6 不做以下事项：

- 不做 lifecycle demotion、retention enforcement、TTL hard delete。
- 不做 pipeline glue。
- 不改变业务 DTO/codec/schema 边界。
- 不引入 market-data 专用查询核心。
- 不修改无关 `fdc-server health` dirty files。

## 4. API 设计

### 4.1 StorageTierScope

定义在 `query.rs`，因为它是 query model 的一部分：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTierScope {
    All,
    Only(StorageTier),
    Hot,
    Warm,
    Cold,
}
```

`Default` 为 `All`。

### 4.2 StorageQuery

新增字段：

```rust
pub tier_scope: StorageTierScope,
```

`StorageQuery::new` 默认 `StorageTierScope::All`。

新增 builder：

```rust
pub fn with_tier_scope(mut self, tier_scope: StorageTierScope) -> Self
```

validation 增加：`Only` 不需要额外验证，因为 `StorageTier` 是 enum。

### 4.3 StorageQueryResult / Metrics

定义在 `query.rs`：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQueryMetrics {
    pub scanned_entries: usize,
    pub decoded_records: usize,
    pub returned_records: usize,
    pub tier_hits: BTreeMap<StorageTier, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageQueryResult {
    pub records: Vec<StorageWriteRecord>,
    pub metrics: StorageQueryMetrics,
}
```

语义：

- `scanned_entries`：engine scan 返回的 raw KV entries 数量，不含去重过滤。
- `decoded_records`：成功 decode 的 storage records 数量，不含 TTL/query filter 后剔除。
- `returned_records`：最终返回 records 数量，应用 query filter/order/limit 后。
- `tier_hits`：每个 tier scan 返回的 raw KV entries 数量。

### 4.4 TierManager scoped scan

当前 `scan_prefix(prefix, limit)` 返回 `Vec<(key, value)>`，无法知道 entry 来源 tier。S6 新增：

```rust
pub async fn scan_prefix_in_tiers(
    &self,
    prefix: &[u8],
    tiers: &[StorageTier],
    limit: Option<usize>,
) -> Result<Vec<(StorageTier, Vec<u8>, Vec<u8>)>>
```

`scan_prefix` 保留，内部可调用新方法并丢弃 tier 信息。

新增 helper：

```rust
pub fn tiers_for_scope(&self, scope: &StorageTierScope) -> Vec<StorageTier>
```

只返回已初始化/可用 tier，并按 priority 排序。

### 4.5 TieredStorageStore query flow

`query_storage_with_metrics`：

1. validate query。
2. 计算 namespace/collection prefix。
3. 调用 `tiers_for_scope(query.tier_scope)`。
4. 调用 `scan_prefix_in_tiers`。
5. 用 `BTreeSet` 跨层按 composite key 去重，保留更热 tier 的 first hit。
6. decode record，过滤 TTL expired。
7. 应用 generic query filters。
8. 排序和 limit。
9. 返回 `StorageQueryResult`。

现有 `QueryableStorage::query_storage` 改为调用 `query_storage_with_metrics` 后返回 `.records`。

## 5. Compatibility

- `StorageQuery` 增加字段会影响直接 struct literal，但当前模块测试主要用 builder。新字段 default 在 `StorageQuery::new` 中设置。
- `QueryableStorage` trait 不变。
- `MarketDataQuery` wrapper 不变，因为它转换到 `StorageQuery::new` 后默认 `All`。
- `TierManager::scan_prefix` 不变，避免破坏既有调用方。

## 6. 测试策略

新增 tests：

1. `storage_tier_scope_defaults_to_all`。
2. `storage_query_builder_sets_tier_scope`。
3. `tier_manager_resolves_scope_to_available_tiers`。
4. `tiered_store_query_can_scope_to_only_one_tier`。
5. `tiered_store_query_hot_scope_reads_l1_l2_only`。
6. `tiered_store_query_warm_and_cold_scopes_read_expected_tiers`。
7. `tiered_store_query_metrics_report_scanned_returned_and_tier_hits`。
8. Existing full `fdc-storage` test suite remains passing.

## 7. 验收标准

S6 完成时应满足：

- `StorageQuery` 支持 tier scope。
- `TieredStorageStore` 支持 scoped query with metrics。
- Existing `query_storage` remains backward-compatible。
- Query metrics can distinguish scanned entries, decoded records, returned records, and tier hits。
- Full `rtk cargo test -p fdc-storage` passes。
- No unrelated `fdc-server health` dirty files touched。

## 8. 自检

- 无占位符。
- scope 聚焦 storage query observability，不做 lifecycle。
- 设计保持 storage generic，不引入业务字段。
- 为下一步 lifecycle/demotion/retention 提供验证基础。
