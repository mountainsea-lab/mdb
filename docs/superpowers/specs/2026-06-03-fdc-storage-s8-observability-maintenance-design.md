# fdc-storage Phase S8 Observability and Explicit Maintenance Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准，待实现

## 1. 背景

`fdc-storage` 已完成以下模块内阶段：

- S1/S2：通用 query/typed storage boundary。
- S3/S4/S5：L2 redb、L3 DuckDB、L4 RocksDB 持久 engine。
- S6：tier-scoped query 和 query metrics。
- S7：显式 lifecycle pass，支持 TTL hard-delete、retention demotion、retention expiration 和 lifecycle report。

现在 storage 已有可运行的 tier engines 和维护动作，但缺少统一的模块内观测面：调用方无法一次性获取 tier 健康、engine stats、最近维护结果，也无法执行一个标准 maintenance pass 来收集报告。

S8 目标是在不接入 API/server/pipeline glue 的前提下，为后续生产级运维提供 storage-owned observability 和 explicit maintenance API。

## 2. 目标

1. 新增通用 `StorageHealthSnapshot`：汇总 tier stats、enabled tier、initialized tier、access pattern count、migration queue length。
2. 新增 `StorageMaintenanceReport`：汇总 lifecycle report、tier stats、maintenance 时间、是否执行 compact。
3. 在 `TieredStorageStore` 增加：
   - `storage_health_snapshot()`
   - `run_maintenance_once()`
4. `run_maintenance_once()` 执行：
   - S7 lifecycle pass
   - tier stats refresh/read
   - 可选 engine compaction hook。本阶段先只对所有 tier 调用 `compact()`，由 engine 自身决定是否 no-op。
5. 保持 storage generic，不引入业务字段。
6. 每个环节记录“生产级后续完善注意事项”，方便后续模块接续。

## 3. 非目标

S8 不做：

- 不接 API route、server health route 或 Prometheus exporter。
- 不做后台 scheduler。
- 不做 alerting。
- 不做 distributed health 或 cluster membership。
- 不做 index/shard/backup 实装。
- 不触碰无关 `fdc-server health` dirty files。

## 4. API 设计

### 4.1 新模块 `maintenance.rs`

新增 `crates/fdc-storage/src/maintenance.rs`，职责是保存 storage maintenance/health DTO，而不是执行逻辑。

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTierHealthStatus {
    Healthy,
    MissingEngine,
    StatsUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTierHealth {
    pub tier: StorageTier,
    pub enabled: bool,
    pub initialized: bool,
    pub status: StorageTierHealthStatus,
    pub stats: Option<StorageStats>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageHealthSnapshot {
    pub captured_at: DateTime<Utc>,
    pub tiers: BTreeMap<StorageTier, StorageTierHealth>,
    pub access_patterns: usize,
    pub migration_queue_len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMaintenanceReport {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub lifecycle: TierLifecycleReport,
    pub health: StorageHealthSnapshot,
    pub compacted_tiers: Vec<StorageTier>,
    pub compaction_errors: BTreeMap<StorageTier, String>,
}
```

说明：

- `StorageStats` 已有 `Serialize/Deserialize` 时直接使用；如果没有，S8 会补 derive。
- `StorageTierHealthStatus` 用 enum 表达 module-internal 状态，不绑定 API response。
- `StorageMaintenanceReport` 保存一次显式维护的结果，可被未来 API/exporter 复用。

### 4.2 TierManager observability helpers

`TierManager` 新增：

```rust
pub fn configured_tiers(&self) -> Vec<StorageTier>;
pub fn initialized_tiers(&self) -> Vec<StorageTier>;
pub async fn compact_tier(&self, tier: &StorageTier) -> Result<()>;
```

语义：

- `configured_tiers()` 返回 `tiers` 中所有 configured tier，按 priority 排序。
- `initialized_tiers()` 返回 `engines` 中所有 initialized tier，按 priority 排序。
- `compact_tier()` 调用对应 engine `compact()`；engine 无 compaction 能力时可 no-op 或返回自身错误。

### 4.3 TieredStorageStore health snapshot

`storage_health_snapshot()`：

1. 读取 configured tiers。
2. 读取 initialized tiers。
3. 调用 `get_tier_stats()`。
4. 对每个 configured tier 生成 `StorageTierHealth`：
   - enabled：来自 `TierConfig`。
   - initialized：是否在 initialized tiers。
   - status：
     - enabled 但未 initialized → `MissingEngine`
     - stats 获取失败 → `StatsUnavailable`
     - 否则 `Healthy`
5. 加入 access pattern count 和 migration queue length。

### 4.4 TieredStorageStore maintenance pass

`run_maintenance_once()`：

1. `started_at = Utc::now()`。
2. 执行 `run_lifecycle_once()`。
3. 遍历 initialized tiers 调用 `compact_tier()`。
4. 记录 `compacted_tiers` 和 `compaction_errors`。
5. 获取 `storage_health_snapshot()`。
6. 返回 `StorageMaintenanceReport`。

## 5. 错误策略

- `storage_health_snapshot()` 不应该因为单 tier stats 失败而整体失败。本阶段可通过 `get_tier_stats()` 成功路径覆盖；如未来补 per-tier stats 错误，则落到 `StatsUnavailable`。
- `run_maintenance_once()` lifecycle 失败时应整体返回 error，因为 TTL/retention 是维护核心动作。
- compaction 失败不使 maintenance 整体失败，记录到 `compaction_errors`。

## 6. 测试策略

使用 Memory engine 组合 tier，避免持久 engine 默认路径冲突。

测试覆盖：

1. `StorageHealthSnapshot` 包含 configured initialized tiers。
2. disabled tier 不会 initialized，但仍能出现在 health snapshot 中并标注 enabled=false。
3. `run_maintenance_once()` 返回 lifecycle report 和 health snapshot。
4. `run_maintenance_once()` 会对 initialized tiers 尝试 compact，并记录 compacted tiers。
5. S7 lifecycle 行为仍然在 maintenance pass 中生效。
6. Full `rtk cargo test -p fdc-storage` passes。

## 7. 生产级后续完善注意事项

### 7.1 Metrics/exporter 接续点

S8 只提供 Rust DTO 和方法。生产级应继续：

- 将 `StorageHealthSnapshot` 转换成 Prometheus metrics。
- 暴露 per-tier gauges：records、bytes、initialized、enabled、compaction failures。
- 暴露 maintenance latency histogram。
- 将 lifecycle counters 累积为 monotonic counters，而不是只看单次 report。

### 7.2 Scheduler 接续点

S8 不启动后台任务。生产级应继续：

- 增加 configurable interval scheduler。
- 防止重入：同一 store 同时只能有一个 maintenance pass。
- 增加 cancellation/shutdown token。
- 对长时间 maintenance 加 timeout 和 tracing span。

### 7.3 Compaction 接续点

S8 只调用 engine `compact()`。生产级应继续：

- 根据 engine capabilities 判断是否 compact。
- RocksDB 支持范围 compaction。
- redb/DuckDB 明确 no-op 或实现 vacuum/checkpoint。
- 给 compaction 增加 backoff，避免高峰期阻塞。

### 7.4 Health semantics 接续点

S8 的 health 状态较粗。生产级应继续：

- 增加 degraded 状态，例如 stats stale、disk high-watermark、write failure。
- 区分 configured disabled 与 missing engine。
- 加入最近一次 read/write/lifecycle error。
- 加入 durable engine path、disk usage、file count。

### 7.5 Persistence and audit 接续点

S8 report 只在调用方内存中返回。生产级应继续：

- 将 maintenance report 持久化到 audit log 或 system namespace。
- 记录操作前后 tier counts，支持追踪 demotion/delete。
- 对 TTL hard-delete 记录摘要，不记录 payload。

## 8. 验收标准

S8 完成后：

- `TieredStorageStore::storage_health_snapshot()` 可返回模块内健康快照。
- `TieredStorageStore::run_maintenance_once()` 可执行 lifecycle + compaction + health report。
- Report 类型从 `fdc-storage` re-export。
- Existing query/write/lifecycle APIs 不破坏。
- Full `rtk cargo test -p fdc-storage` passes。
- spec/plan 中保留生产级后续完善注意事项。
- 无关 `fdc-server health` dirty files 未被修改。

## 9. 自检

- 无占位符。
- 范围聚焦 storage observability/maintenance，不做 API glue。
- DTO generic，不引入业务字段。
- 后续生产级完善点已按 metrics/scheduler/compaction/health/audit 分类记录。
