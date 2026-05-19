# fdc-ingestion Phase B1b Source Batch 伪代码 Review

## 目的

本文是 Phase B1b 实现前的业务逻辑 review 输入。B1a 已经在 `fdc-ingestion` 中落地通用结构化 source envelope 和 stateless validator。B1b 的目标是在同一个 `source` 模块下补齐结构化 source batch 层：`SourceBatchItem<T>`、`SourceBatchSink<T>`、`SourceBatchProcessor<T>`、`SourceBatchResult` 和 batch stats。

B1b 仍然只处理 `fdc-ingestion` 的通用 source path，不让 `fdc-ingestion` 依赖 `fdc-barter`，不接真实网络，不接 `fdc-transform`，不写真实 storage。

## 当前基础

B1a 已提供：

```rust
SourceEnvelope<T>
SourceType
SourceCheckpoint
SourcePartition
SourcePosition
SourceQualityFlags
SourceMetadata
SourceValidator
SourceValidationResult
SourceValidationError
SourceValidationWarning
SourceValidatorStats
```

现有 network byte path 仍然是：

```text
ReceivedData
        ↓
DataParser
        ↓
DataValidator
        ↓
BatchItem
        ↓
BatchProcessor
        ↓
SimpleStorage
```

B1b 新增的是并行 source batch path：

```text
SourceEnvelope<T>
        ↓
SourceValidator::validate
        ↓
SourceBatchItem<T>
        ↓
SourceBatchProcessor<T>
        ↓
SourceBatchSink<T>
```

## 设计约束

- 不修改现有 `BatchItem` / `BatchProcessor` 行为。
- 不把 `SourceEnvelope<T>` 转成 `ParsedData`。
- 不把 source batch 写入 `SimpleStorage`。
- 不实现 `fdc-transform`，只定义 `SourceBatchSink<T>` 作为后续交接边界。
- 不引入 `fdc-barter` 依赖。
- 不实现 long-running pipeline loop，B1c 再实现 bounded pipeline glue。
- B1b 的 processor 接收已经完成 validation 的 `SourceBatchItem<T>`。

## 成功标准

- `SourceBatchItem<T>` 能携带 `SourceEnvelope<T>` 和 `SourceValidationResult`。
- `SourceBatchItem::is_valid()` 只由 `validation_result.is_valid` 决定。
- `SourceBatchProcessor<T>` 能按 `BatchConfig.batch_size` 触发 flush。
- `flush()` 能把 valid item 交给 `SourceBatchSink<T>`。
- invalid item 不交给 sink，并计入 failure。
- `SourceBatchResult` 能表达 batch id、processed/success/failure counts、batch size、耗时、错误。
- `SourceBatchProcessorStats` 能累计 batches、total/success/failed counts 和成功率。
- 测试使用 dummy payload 和 in-memory test sink，不需要真实 storage 或 transform。

## 核心类型草图

### 1. SourceBatchItem

```rust
struct SourceBatchItem<T> {
    item_id: String,
    envelope: SourceEnvelope<T>,
    validation_result: SourceValidationResult,
}

impl<T> SourceBatchItem<T> {
    fn new(envelope: SourceEnvelope<T>, validation_result: SourceValidationResult) -> Self {
        Self {
            item_id: Uuid::new_v4().to_string(),
            envelope,
            validation_result,
        }
    }

    fn is_valid(&self) -> bool {
        self.validation_result.is_valid
    }
}
```

业务语义：

- `item_id` 是 batch 层 trace id，不等于 `envelope.envelope_id`。
- `envelope` 保留完整 source envelope 和 payload。
- `validation_result` 固化进入 batch 前的验证结果。
- invalid item 可以进入 processor，但不会进入 sink。

### 2. SourceBatchSink

```rust
#[async_trait]
trait SourceBatchSink<T>: Send + Sync {
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize>;
}
```

业务语义：

- `SourceBatchSink<T>` 是 source batch 和后续 transform/storage 的交接边界。
- B1b 只用 test sink 或 no-op sink。
- sink 返回成功交接的 item 数。
- sink 如果返回错误，整个 batch processor 返回 `Err`，不吞掉下游错误。

### 3. SourceBatchResult

```rust
struct SourceBatchResult {
    batch_id: String,
    processed_count: usize,
    success_count: usize,
    failure_count: usize,
    batch_size: usize,
    processing_time_ms: u64,
    processed_at: DateTime<Utc>,
    errors: Vec<String>,
}
```

业务规则：

- `processed_count` 等于进入 processor 的 item 数。
- `success_count` 等于 sink 成功接收的 valid item 数。
- `failure_count` 至少包含 invalid item 数。
- 如果 sink 返回数量小于 valid item 数，差额计入 failure，并记录 error message。
- 如果 sink 返回错误，`process_batch` 返回 `Err`，不构造 partial success result。

### 4. SourceBatchProcessorStats

```rust
struct SourceBatchProcessorStats {
    batches_processed: u64,
    total_messages: u64,
    successful_messages: u64,
    failed_messages: u64,
    total_processing_time_ms: u64,
    avg_batch_size: f64,
    avg_processing_time_ms: f64,
    throughput_msg_per_sec: f64,
}
```

业务语义与现有 `BatchProcessorStats` 对齐，但统计对象是 `SourceBatchItem<T>`。

### 5. SourceBatchProcessor

```rust
struct SourceBatchProcessor<T> {
    config: BatchConfig,
    sink: Arc<dyn SourceBatchSink<T>>,
    stats: Arc<RwLock<SourceBatchProcessorStats>>,
    current_batch: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
    batch_timer: Arc<RwLock<Option<Instant>>>,
}
```

B1b 采用与现有 `BatchProcessor` 相同的内部结构，降低认知成本。

## 构造和基础方法伪代码

```rust
impl<T> SourceBatchProcessor<T> {
    fn new(config: BatchConfig, sink: Arc<dyn SourceBatchSink<T>>) -> Self {
        Self {
            config,
            sink,
            stats: Arc::new(RwLock::new(SourceBatchProcessorStats::default())),
            current_batch: Arc::new(RwLock::new(Vec::new())),
            batch_timer: Arc::new(RwLock::new(None)),
        }
    }

    async fn get_stats(&self) -> SourceBatchProcessorStats {
        self.stats.read().await.clone()
    }

    async fn reset_stats(&self) {
        *self.stats.write().await = SourceBatchProcessorStats::default();
    }
}
```

## add_item 伪代码

```rust
async fn add_item(&self, item: SourceBatchItem<T>) -> Result<Option<SourceBatchResult>> {
    let mut batch = self.current_batch.write().await;
    let mut timer = self.batch_timer.write().await;

    if batch.is_empty() {
        *timer = Some(Instant::now());
    }

    batch.push(item);

    let reached_size = batch.len() >= self.config.batch_size;
    let reached_timeout = timer
        .as_ref()
        .map(|started| started.elapsed() >= self.config.batch_timeout)
        .unwrap_or(false);

    if reached_size || reached_timeout {
        let items = batch.drain(..).collect::<Vec<_>>();
        *timer = None;
        drop(batch);
        drop(timer);
        return Ok(Some(self.process_batch(items).await?));
    }

    Ok(None)
}
```

设计要点：

- 不在持有 batch lock 时 await sink。
- size trigger 是 B1b 的主要测试路径。
- timeout trigger 保留，但不作为第一批测试重点，避免 sleep 测试不稳定。

## flush 伪代码

```rust
async fn flush(&self) -> Result<Option<SourceBatchResult>> {
    let mut batch = self.current_batch.write().await;
    let mut timer = self.batch_timer.write().await;

    if batch.is_empty() {
        return Ok(None);
    }

    let items = batch.drain(..).collect::<Vec<_>>();
    *timer = None;
    drop(batch);
    drop(timer);

    Ok(Some(self.process_batch(items).await?))
}
```

业务语义：

- `flush()` 用于 source runtime shutdown 或 bounded pipeline helper。
- 空 batch 返回 `Ok(None)`。
- 非空 batch 返回 `Ok(Some(SourceBatchResult))`。

## process_batch 伪代码

```rust
async fn process_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<SourceBatchResult> {
    let batch_id = Uuid::new_v4().to_string();
    let batch_size = items.len();
    let start = Instant::now();

    let mut valid_items = Vec::new();
    let mut invalid_count = 0;
    let mut errors = Vec::new();

    for item in items {
        if item.is_valid() {
            valid_items.push(item);
        } else {
            invalid_count += 1;
            errors.push(format!("invalid source batch item: {}", item.item_id));
        }
    }

    let valid_count = valid_items.len();
    let written_count = if valid_items.is_empty() {
        0
    } else {
        self.sink.write_batch(valid_items).await?
    };

    if written_count < valid_count {
        errors.push(format!(
            "source sink accepted {} of {} valid items",
            written_count,
            valid_count,
        ));
    }

    let failure_count = invalid_count + valid_count.saturating_sub(written_count);

    let result = SourceBatchResult {
        batch_id,
        processed_count: batch_size,
        success_count: written_count,
        failure_count,
        batch_size,
        processing_time_ms: start.elapsed().as_millis() as u64,
        processed_at: Utc::now(),
        errors,
    };

    self.stats.write().await.record_batch(&result);
    Ok(result)
}
```

Review 要点：

- invalid item 不进入 sink。
- sink 错误向上传播。
- sink short write 不是 panic，而是 result failure。
- B1b 不做 dead-letter queue，后续 recovery/dead-letter 设计时再处理。

## Test sink 伪代码

测试使用 in-memory sink，避免真实 transform/storage：

```rust
#[derive(Default)]
struct RecordingSourceBatchSink<T> {
    written: Arc<RwLock<Vec<SourceBatchItem<T>>>>,
}

#[async_trait]
impl<T> SourceBatchSink<T> for RecordingSourceBatchSink<T>
where
    T: Send + Sync + 'static,
{
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize> {
        let count = items.len();
        self.written.write().await.extend(items);
        Ok(count)
    }
}
```

测试重点：

- valid item flush 后进入 sink。
- invalid item flush 后不进入 sink。
- batch size 达到 `BatchConfig.batch_size` 自动触发处理。
- stats 记录 batches、total、success、failure。

## 推荐文件变更

```text
crates/fdc-ingestion/src/source/
  batch.rs      # 新增 SourceBatchItem/Result/Sink/Processor/Stats
  mod.rs        # re-export batch types

crates/fdc-ingestion/src/lib.rs
  # re-export source batch types from crate root

crates/fdc-ingestion/tests/
  source_batch_contract.rs
```

## Public API 目标

```rust
use fdc_ingestion::{
    SourceBatchItem,
    SourceBatchProcessor,
    SourceBatchSink,
    SourceEnvelope,
    SourceType,
    SourceValidator,
};
```

使用流程：

```rust
let envelope = SourceEnvelope::new(
    "dummy-source",
    SourceType::MarketData,
    TimestampNs::from_nanos(1_000),
    TimestampNs::from_nanos(1_100),
    DummyMarketEvent { symbol: "BTCUSDT".to_string() },
);

let validation = SourceValidator::default().validate(&envelope).await;
let item = SourceBatchItem::new(envelope, validation);
let result = processor.add_item(item).await?;
```

## Not Doing

- 不实现 `run_source_pipeline_once` 或 long-running pipeline。B1c 处理。
- 不实现 transform sink。
- 不实现 storage sink。
- 不引入 `fdc-barter`。
- 不实现 dead-letter queue。
- 不实现 checkpoint persistence。
- 不实现跨事件 dedupe/gap detection。
- 不修改 `BatchProcessor` / `BatchItem`。

## Review 关注点

1. 是否同意 B1b 新增并行 `source::batch`，不泛化或修改现有 `batch.rs`？
2. 是否同意 invalid item 不进入 sink，只在 `SourceBatchResult` 里计 failure？
3. 是否同意 sink error 直接返回 `Err`，不生成 partial success result？
4. 是否同意 short write 生成 `SourceBatchResult` failure，而不是 `Err`？
5. 是否同意 timeout trigger 保留实现，但测试主要覆盖 size trigger 和 explicit `flush()`？

## B1b 实现前验收标准

implementation plan 应覆盖：

- `SourceBatchItem::new` 和 `is_valid` contract test。
- `SourceBatchProcessor::add_item` size trigger contract test。
- `SourceBatchProcessor::flush` valid/invalid filtering contract test。
- `SourceBatchProcessorStats` contract test。
- no `fdc-barter` dependency scan。
- `cargo test -p fdc-ingestion` 和 `cargo check -p fdc-ingestion --all-targets`。
