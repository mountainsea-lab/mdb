# fdc-storage Phase S9 Production Hardening and Pre-Integration Closure Baseline Design

日期：2026-06-03
模块：`crates/fdc-storage`
状态：已批准，待实现

## 1. 背景

`fdc-storage` 已完成 S1-S8：

- 通用 query/typed storage boundary。
- L1-L4 tier engines：Memory、redb、DuckDB、RocksDB。
- tier-scoped query 和 query metrics。
- explicit lifecycle maintenance：TTL hard-delete、retention demotion/delete。
- observability/maintenance report：health snapshot、maintenance pass、compaction error capture。

现在 storage 已具备模块内可运行能力。S9 不继续扩展复杂新功能，而是做生产级增强的最小闭环和集成前收口，确保其它模块未来可以安全接入 storage boundary。

## 2. 目标

S9 的目标是形成 “pre-integration closure baseline”：

1. 增加 storage boundary acceptance report 文档，说明 S1-S8 能力、验证命令、限制和后续生产化路线。
2. 增加 contract tests，证明业务模块可通过 namespace/collection/tags/codec/tier placement 使用 storage，不需要 storage import 业务 DTO。
3. 增加 dependency guard，防止 `fdc-storage` 引入 adapter/ingestion/barter/server 等上层依赖。
4. 增加 production hardening checklist 文档，记录 scheduler、metrics exporter、audit persistence、compaction semantics、health degraded semantics 等接续点。
5. 增加小型 API stability tests，锁定关键 public types 和 methods 可用性。
6. 保持模块内范围，不做 pipeline glue。

## 3. 非目标

S9 不做：

- 不接 `fdc-barter -> fdc-ingestion -> fdc-storage` 全链路。
- 不修改 server/API route。
- 不实装完整 backup/restore。
- 不实装 physical shard routing。
- 不实装复杂 query index。
- 不启动后台 scheduler。
- 不修改无关 `fdc-server health` dirty files。

## 4. 实现设计

### 4.1 Acceptance report

新增文档：

`crates/fdc-storage/docs/storage-boundary-acceptance-report.md`

内容包括：

- Public boundary summary。
- Query/filter/tier/lifecycle/maintenance capability matrix。
- Verified command：`rtk cargo test -p fdc-storage`。
- Known limitations。
- Production follow-up checklist。
- Integration readiness statement。

### 4.2 Production hardening checklist

新增文档：

`crates/fdc-storage/docs/production-hardening-followups.md`

按类别记录后续事项：

1. Scheduler：防重入、取消、timeout、interval config。
2. Metrics/exporter：Prometheus text、counters/gauges/histograms。
3. Audit：maintenance report 持久化、TTL delete 摘要。
4. Compaction：engine capabilities、RocksDB range compaction、DuckDB checkpoint/vacuum。
5. Health：degraded/error semantics、stale stats、disk usage、last error。
6. Index：namespace/collection/tags/time index 的未来设计。
7. Backup：engine-specific backup strategy。
8. Shard：logical shard key 到 physical placement 的未来设计。

### 4.3 Dependency guard

新增或扩展 test，确保 `crates/fdc-storage/Cargo.toml` 不包含上层 crates：

- `fdc-barter`
- `fdc-ingestion`
- `fdc-transform`
- `fdc-api`
- `fdc-server`
- `fdc-orchestrator`

测试可放在 `crates/fdc-storage/src/lib.rs` test module 或新 `tests/dependency_guard.rs`。建议放在 `src/lib.rs`，保持简单。

### 4.4 Contract tests

新增 tests，证明 storage 可承载 business-like DTO 但不依赖业务模块：

- 定义 local test struct `BusinessEvent`。
- 用 `JsonStorageCodec<BusinessEvent>` encode/decode。
- 写入 `TypedStorageRecord` 或 `StorageWriteRecord`：
  - namespace = `business_test`
  - collection = `events`
  - tags = `tenant`, `kind`
  - placement = L1/L2/L3/L4 任一 memory-backed tier
- query by namespace/collection/tag。
- decode back to `BusinessEvent`。
- 使用 `StorageTierScope` 验证 tier boundary。

这证明未来市场数据、订单、其它业务模块都可以通过 generic boundary 接入。

### 4.5 API stability smoke tests

在 `fdc-storage` 内增加 public API smoke test：

- 构造 `StorageQuery` with tier scope。
- 构造 `StoragePlacementHint` with ttl/tier。
- 构造 `TierLifecycleReport`。
- 构造 `StorageMaintenanceReport` 或通过 `run_maintenance_once()` 获取。
- 构造 `TieredStorageStore::memory_only()` 并完成 write/query。

目的不是测试内部逻辑，而是锁定外部调用方依赖的 public surface。

## 5. 后续生产级完善注意事项

S9 会把注意事项落到文档，作为后续接续参考。当前建议优先级：

### P1：接入前必须继续关注

- Public API 不稳定项要在 report 中标注。
- Maintenance pass 当前是手动调用，不是后台任务。
- Compaction errors 在 memory/redb/duckdb 上可能是 expected unsupported，需要统一 health semantics。
- Lifecycle TTL hard-delete 会删除所有 tier 中同 key 数据，接入前要确认业务可接受。

### P2：生产部署前建议完成

- Prometheus exporter 或 metrics adapter。
- Maintenance scheduler 防重入。
- Maintenance audit persistence。
- Durable engine path/disk usage health。
- Per-engine compaction/vacuum/checkpoint 策略。

### P3：规模化前再做

- Query index。
- Physical shard routing。
- Backup/restore orchestration。
- Cold archive Parquet/Arrow。

## 6. 验收标准

S9 完成后：

- 有 storage acceptance report。
- 有 production hardening follow-up 文档。
- 有 dependency guard test。
- 有 business-like generic contract test。
- 有 public API stability smoke test。
- `rtk cargo test -p fdc-storage` passes。
- 未触碰 `fdc-server health` dirty files。

## 7. 自检

- 无占位符。
- 范围聚焦收口与可接入性，不做全链路 glue。
- 后续生产级完善点明确分 P1/P2/P3。
- 测试验证 generic boundary，不引入业务 crate 依赖。
