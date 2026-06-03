# fdc-storage Phase S7 Tier Lifecycle, Retention, Demotion, and TTL Hard-Delete Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准，待实现

## 1. 背景

`fdc-storage` 已完成通用 typed/query 边界和 L1-L4 tier engines：

- L1 Memory
- L2 redb
- L3 DuckDB
- L4 RocksDB

S6 增加了 tier-scoped query 和 query metrics，使 storage 能观察查询扫描了哪些 tier。下一步需要补齐 lifecycle 能力：让 storage 在模块内部处理 TTL hard-delete、tier retention 和 demotion，而不是只在查询时过滤过期记录。

当前状态：

- `StoragePlacementHint::ttl` 已存在。
- `TierConfig::retention_duration` 已存在。
- `TierManager` 已有 promotion queue 和 `MigrationTask`。
- `TieredStorageStore::query_storage_with_metrics` 会过滤 expired TTL record，但不会从引擎删除。
- `TierManager` 可跨 tier scan，并已能返回 tier origin。

## 2. 目标

S7 目标是提供一个显式、可测试、通用的 lifecycle pass：

1. TTL hard-delete：对 `StorageWriteRecord` 中明确 TTL 到期的数据，从所有 tier 删除。
2. Retention demotion：对超过当前 tier `retention_duration` 的数据，移动到下一个更冷且可用的 tier。
3. Retention expiration：如果数据已在最冷可用 tier 且超过 retention，则删除。
4. Lifecycle report：返回扫描、删除、下沉、跳过、错误数量，以及 per-tier 统计。
5. 保持 generic storage 边界，不引入市场数据字段。
6. 不启动后台 worker，不做 scheduler，只提供显式 `run_lifecycle_once()` 风格 API。

## 3. 非目标

S7 不做：

- 不做 fdc-barter/ingestion/storage pipeline glue。
- 不做后台定时任务、tokio task supervisor 或 orchestrator 集成。
- 不做复杂热度算法重写。
- 不做压缩、归档格式转换或 Parquet。
- 不修改无关 `fdc-server health` dirty files。

## 4. API 设计

### 4.1 Lifecycle action model

新增 query/store lifecycle 相关类型，建议放在 `tiered_store.rs` 或新文件 `lifecycle.rs`。为保持文件职责清晰，S7 新增 `crates/fdc-storage/src/lifecycle.rs`：

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TierLifecycleAction {
    TtlExpiredDelete,
    RetentionDemote,
    RetentionExpiredDelete,
    Retain,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TierLifecycleTierReport {
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TierLifecycleReport {
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub tier_reports: BTreeMap<StorageTier, TierLifecycleTierReport>,
}
```

### 4.2 TierManager support

`TierManager` 增加三个公开能力：

```rust
pub async fn put_to_specific_tier(
    &self,
    key: &[u8],
    value: &[u8],
    target_tier: &StorageTier,
) -> Result<()>;

pub async fn delete_from_tier(&self, key: &[u8], tier: &StorageTier) -> Result<()>;

pub fn next_colder_available_tier(&self, current_tier: &StorageTier) -> Option<StorageTier>;
```

语义：

- `put_to_specific_tier` 是 `put_to_tier` 的 public wrapper，不重新跑 placement。
- `delete_from_tier` 只删除指定 tier，不影响其它 tier。
- `next_colder_available_tier` 返回 priority 更大、最接近当前 tier 的 initialized tier。

### 4.3 TieredStorageStore lifecycle pass

`TieredStorageStore` 增加：

```rust
pub async fn run_lifecycle_once(&self) -> Result<TierLifecycleReport>;
```

算法：

1. 取 `now = Utc::now()`。
2. 获取所有 initialized tiers，按 priority 从热到冷排序。
3. 对每个 tier 扫描所有 entries。
4. decode `StorageWriteRecord`。
5. 如果 record TTL 到期：调用 `tier_manager.delete(&key)` 从所有 tier 删除，计入 `ttl_deleted`。
6. 否则，如果当前 tier 有 `retention_duration` 且 `record.timestamp + retention_duration < now`：
   - 如果存在更冷 tier：写入更冷 tier，再从当前 tier 删除，计入 `retention_demoted`。
   - 如果不存在更冷 tier：删除当前 tier，计入 `retention_deleted`。
7. 否则计入 `retained`。
8. Decode 失败不删除，计入 `decode_errors` 并继续。

### 4.4 Retention semantics

- TTL 优先于 tier retention。明确 TTL 到期意味着从所有 tier hard-delete。
- Retention 是 tier-local policy，表示“该 tier 最多保留多久”。超过后先尝试下沉，不是直接删除。
- 最冷可用 tier 没有更冷目标时，超过 retention 才删除。
- 如果某个 tier 没有 `retention_duration`，该 tier 不执行 retention demotion/delete。

### 4.5 Duplicate handling

Lifecycle 是 maintenance pass，不是 query pass：

- 不做跨 tier dedupe。
- 如果同一 composite key 存在多个 tier，TTL 到期会从所有 tier 删除。
- Retention demotion 只处理当前扫描到的 tier entry。
- 如果更冷 tier 已有相同 key，写入覆盖即可，保持 storage KV last-write-wins。

## 5. 测试策略

使用 Memory engine 配置 L1-L4，避免持久引擎默认路径和锁冲突，聚焦 lifecycle 语义。

测试覆盖：

1. `next_colder_available_tier` 只返回 initialized colder tier。
2. `delete_from_tier` 只删除指定 tier。
3. `run_lifecycle_once` hard-deletes TTL expired record from all tiers。
4. `run_lifecycle_once` demotes retention-expired L1 record to L2。
5. `run_lifecycle_once` deletes retention-expired record when no colder tier exists。
6. Lifecycle report records scanned/deleted/demoted/retained/per-tier counts。
7. Existing `rtk cargo test -p fdc-storage` remains passing。

## 6. 验收标准

S7 完成后：

- `TieredStorageStore::run_lifecycle_once()` 可显式执行 lifecycle maintenance。
- TTL 到期数据不只是 query-filtered，而是从 storage engines 删除。
- Tier retention 到期数据可以下沉到更冷 tier。
- 最冷 tier retention 到期数据会删除。
- Lifecycle report 可验证行为。
- 不破坏现有 query/write APIs。
- Full `rtk cargo test -p fdc-storage` passes。
- 无关 `fdc-server health` dirty files 未被修改。

## 7. 自检

- 无占位符。
- 范围聚焦 lifecycle，不做 scheduler/pipeline glue。
- API 保持 storage generic。
- Retention、TTL、demotion 优先级明确。
- 测试可用 Memory engine 稳定验证。
