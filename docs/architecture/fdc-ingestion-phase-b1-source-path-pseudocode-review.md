# fdc-ingestion Phase B1 Source Path 伪代码 Review

## 目的

本文是 Phase B1 实现前的业务逻辑 review 输入。目标是在 `fdc-ingestion` 中设计一条结构化 source path，用于接收 `fdc-barter` 等 adapter 已经标准化的事件 envelope，并复用 ingestion 的 buffer、validation、batch、metrics 思路。

本阶段只设计和后续实现 `fdc-ingestion` 的通用 source path 骨架，不让 `fdc-ingestion` 直接依赖 `fdc-barter`，不接真实 Barter WebSocket，不接 `fdc-transform`，不写真实 storage。

## 背景

当前 `fdc-ingestion` 已有网络字节流路径：

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

这条路径适合 TCP/WebSocket/UDP 等原始字节输入。`fdc-barter` 的 Phase A 已经产出结构化对象：

```text
BarterMarketEvent
        ↓
BarterIngestionEnvelope
```

如果把 `BarterIngestionEnvelope` 再序列化成 bytes 伪装成 `ReceivedData`，会重复解析、丢失类型信息，也会让 realtime/historical checkpoint 和 quality flags 难以下沉到 ingestion。因此 Phase B1 推荐新增并行 source path：

```text
Adapter-specific envelope, for example BarterIngestionEnvelope
        ↓
SourceEnvelope<T>
        ↓
DataBuffer<SourceEnvelope<T>>
        ↓
SourceValidator<T>
        ↓
SourceBatchItem<T>
        ↓
SourceBatchProcessor<T>
        ↓
future fdc-transform sink
```

## 设计约束

- `fdc-ingestion` 不依赖 `fdc-barter`。
- `fdc-barter` 后续可以在 dev/test 中适配 `SourceEnvelope<BarterIngestionEnvelope>`，但该类型不进入 `fdc-ingestion` crate 的正常依赖图。
- `SourceEnvelope<T>` 中的 `T` 必须是已结构化事件或 envelope，不是原始 bytes。
- Phase B1 不修改现有 `ReceivedData -> DataParser -> BatchProcessor` 路径。
- Phase B1 不实现 `fdc-transform` sink，只预留 trait 或 handoff 位置。
- Phase B1 不实现 checkpoint 持久化，只在模型中携带 checkpoint 引用或 opaque checkpoint metadata。

## 成功标准

- 能表达一个通用 source envelope，携带 source id、sequence、timestamps、quality、checkpoint 和 payload。
- 能用 `DataBuffer<SourceEnvelope<T>>` 缓冲结构化事件。
- 能验证 source envelope 的基础约束：source id 非空、event time 合法、payload 存在、duplicate/gap/out-of-order 标记可传递。
- 能把通过验证的 envelope 包装为 `SourceBatchItem<T>`。
- 能批量处理 `SourceBatchItem<T>`，输出 `SourceBatchResult`，但不写真实 storage。
- 不破坏现有 ingestion demo 和现有 tests。

## 核心类型草图

### 1. SourceEnvelope

```rust
struct SourceEnvelope<T> {
    envelope_id: String,
    source_id: String,
    source_type: SourceType,
    sequence: Option<String>,
    event_time: TimestampNs,
    received_at: TimestampNs,
    emitted_at: TimestampNs,
    payload: T,
    checkpoint: Option<SourceCheckpoint>,
    quality: SourceQualityFlags,
    metadata: SourceMetadata,
}
```

业务语义：

- `envelope_id` 是 ingestion 级 trace/dedupe id。
- `source_id` 是运行时 source id，例如 `barter-binance-live`。
- `source_type` 用于区分 `MarketData`、`ReferenceData`、`Replay`、`Custom`。
- `event_time` 是交易所或源数据事件时间。
- `received_at` 是 adapter 收到数据的时间。
- `emitted_at` 是 adapter/source 输出 envelope 的时间。
- `payload` 是结构化对象，例如 future `BarterIngestionEnvelope` 或其他 adapter envelope。
- `checkpoint` 是 source 恢复用 metadata，不要求 ingestion 理解具体交易所 cursor。
- `quality` 是数据质量标记。
- `metadata` 是轻量扩展字段，避免频繁改 struct。

### 2. SourceType

```rust
enum SourceType {
    MarketData,
    ReferenceData,
    Replay,
    Custom(String),
}
```

Phase B1 主要使用 `MarketData`。保留 `Custom` 是为了支持非行情 adapter，但不在本阶段扩展业务逻辑。

### 3. SourceCheckpoint

```rust
struct SourceCheckpoint {
    checkpoint_id: String,
    source_id: String,
    partition: SourcePartition,
    position: SourcePosition,
    updated_at: TimestampNs,
}

struct SourcePartition {
    exchange: Option<String>,
    symbol: Option<String>,
    kind: Option<String>,
    shard: Option<String>,
}

enum SourcePosition {
    Timestamp(TimestampNs),
    Sequence(String),
    PageToken(String),
    Opaque(serde_json::Value),
}
```

业务语义：

- `SourceCheckpoint` 是 ingestion 通用 checkpoint 外壳。
- 对 Barter historical cursor，`SourcePosition::Opaque` 可以承载交易所特有 cursor。
- 对 live stream，`SourcePosition::Sequence` 或 `Timestamp` 可用于恢复和 dedupe。
- Phase B1 不检查 payload 是否存在。`SourceEnvelope<T>` 的 payload 是非 `Option<T>`，编译期已保证构造出的 envelope 带 payload。

### 4. SourceQualityFlags

```rust
struct SourceQualityFlags {
    is_replay: bool,
    is_backfill: bool,
    is_duplicate_candidate: bool,
    has_gap_before: bool,
    is_out_of_order: bool,
}
```

业务规则：

- adapter 可以设置 flags。
- ingestion validator 可以基于本地状态补充 warnings，但 Phase B1 不做跨事件状态机。
- `is_duplicate_candidate` 不代表必须丢弃，只表示下游 dedupe 需要注意。

### 5. SourceMetadata

```rust
struct SourceMetadata {
    adapter: Option<String>,
    exchange: Option<String>,
    symbol: Option<String>,
    kind: Option<String>,
    attributes: BTreeMap<String, String>,
}
```

业务规则：

- 常用 routing 字段放为 top-level option。
- 额外属性进入 `attributes`。
- 不在 metadata 中放大 payload 或完整原始响应。

## SourceEnvelope 构造伪代码

```rust
impl<T> SourceEnvelope<T> {
    fn new(
        source_id: impl Into<String>,
        source_type: SourceType,
        event_time: TimestampNs,
        received_at: TimestampNs,
        payload: T,
    ) -> Self {
        Self {
            envelope_id: Uuid::new_v4().to_string(),
            source_id: source_id.into(),
            source_type,
            sequence: None,
            event_time,
            received_at,
            emitted_at: TimestampNs::now(),
            payload,
            checkpoint: None,
            quality: SourceQualityFlags::default(),
            metadata: SourceMetadata::default(),
        }
    }

    fn with_optional_checkpoint(mut self, checkpoint: Option<SourceCheckpoint>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    fn with_checkpoint(mut self, checkpoint: SourceCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    fn with_quality(mut self, quality: SourceQualityFlags) -> Self {
        self.quality = quality;
        self
    }

    fn with_metadata(mut self, metadata: SourceMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}
```

## Barter envelope 适配示例伪代码

该适配逻辑建议放在 `fdc-barter` 或 integration test fixture 中，而不是 `fdc-ingestion` 内部。

```rust
fn source_envelope_from_barter(
    barter: BarterIngestionEnvelope,
) -> SourceEnvelope<BarterIngestionEnvelope> {
    let event_time = barter.event.timestamp;
    let received_at = barter.event.received_at;

    let checkpoint = barter.checkpoint.as_ref().map(|checkpoint| SourceCheckpoint {
        checkpoint_id: format!(
            "{}:{}:{}:{:?}",
            checkpoint.source_id,
            checkpoint.exchange,
            checkpoint.symbol,
            checkpoint.kind,
        ),
        source_id: checkpoint.source_id.clone(),
        partition: SourcePartition {
            exchange: Some(checkpoint.exchange.clone()),
            symbol: Some(checkpoint.symbol.clone()),
            kind: Some(format!("{:?}", checkpoint.kind)),
            shard: None,
        },
        position: checkpoint
            .cursor
            .as_ref()
            .map(|cursor| SourcePosition::Opaque(serde_json::to_value(cursor).unwrap()))
            .unwrap_or(SourcePosition::Timestamp(checkpoint.last_event_time)),
        updated_at: checkpoint.updated_at,
    });

    SourceEnvelope::new(
        barter.source_id.clone(),
        SourceType::MarketData,
        event_time,
        received_at,
        barter,
    )
    .with_optional_checkpoint(checkpoint)
    .with_metadata(SourceMetadata {
        adapter: Some("fdc-barter".to_string()),
        exchange: Some(barter.event.exchange.clone()),
        symbol: Some(barter.event.symbol.as_str().to_string()),
        kind: Some(format!("{:?}", barter.event.kind)),
        attributes: BTreeMap::new(),
    })
}
```

Review 要点：上面只是适配示例，不表示 `fdc-ingestion` 要依赖 `fdc-barter`。

## SourceValidator 伪代码

### 类型草图

```rust
struct SourceValidationResult {
    is_valid: bool,
    errors: Vec<SourceValidationError>,
    warnings: Vec<SourceValidationWarning>,
    validation_time_us: u64,
    validated_at: DateTime<Utc>,
}

enum SourceValidationErrorType {
    EmptySourceId,
    EmptyEnvelopeId,
    InvalidEventTime,
    EventTimeAfterReceivedAt,
}

struct SourceValidationError {
    error_type: SourceValidationErrorType,
    field_path: String,
    message: String,
}

struct SourceValidationWarning {
    warning_type: String,
    field_path: String,
    message: String,
}

struct SourceValidator {
    config: SourceValidatorConfig,
    stats: SourceValidatorStats,
}
```

### 验证流程

```rust
impl SourceValidator {
    async fn validate<T>(&self, envelope: &SourceEnvelope<T>) -> SourceValidationResult {
        let start = Instant::now();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        if envelope.envelope_id.trim().is_empty() {
            errors.push(SourceValidationError::empty_envelope_id());
        }

        if envelope.source_id.trim().is_empty() {
            errors.push(SourceValidationError::empty_source_id());
        }

        if envelope.event_time.as_nanos() <= 0 {
            errors.push(SourceValidationError::invalid_event_time(envelope.event_time));
        }

        if envelope.event_time > envelope.received_at {
            warnings.push(SourceValidationWarning::event_time_after_received_at(
                envelope.event_time,
                envelope.received_at,
            ));
        }

        if envelope.quality.is_duplicate_candidate {
            warnings.push(SourceValidationWarning::duplicate_candidate());
        }

        if envelope.quality.has_gap_before {
            warnings.push(SourceValidationWarning::gap_before());
        }

        let result = SourceValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            validation_time_us: start.elapsed().as_micros() as u64,
            validated_at: Utc::now(),
        };

        self.stats.record_validation(&result).await;
        result
    }
}
```

Phase B1 的 validator 是 stateless validation。跨事件顺序、gap detection、dedupe cache 放到后续阶段。

## SourceBatchItem 伪代码

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

## SourceBatchProcessor 伪代码

### 输出 sink trait

```rust
#[async_trait]
trait SourceBatchSink<T>: Send + Sync {
    async fn write_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<usize>;
}
```

业务语义：

- Phase B1 可以提供 `NoopSourceBatchSink` 或 test sink。
- 后续 `fdc-transform` 可实现这个 trait 或通过 adapter 桥接。
- sink 返回成功写入/交接的 item 数。

### processor 类型

```rust
struct SourceBatchProcessor<T> {
    config: BatchConfig,
    sink: Arc<dyn SourceBatchSink<T>>,
    stats: SourceBatchProcessorStats,
    current_batch: Vec<SourceBatchItem<T>>,
    batch_timer: Option<Instant>,
}
```

### add item 流程

```rust
impl<T> SourceBatchProcessor<T>
where
    T: Send + Sync + 'static,
{
    async fn add_item(&self, item: SourceBatchItem<T>) -> Result<Option<SourceBatchResult>> {
        self.current_batch.push(item);

        let should_process = self.current_batch.len() >= self.config.batch_size
            || self.batch_timer.elapsed() >= self.config.batch_timeout;

        if should_process {
            return self.flush().await;
        }

        Ok(None)
    }

    async fn flush(&self) -> Result<Option<SourceBatchResult>> {
        if self.current_batch.is_empty() {
            return Ok(None);
        }

        let batch = self.current_batch.drain(..).collect::<Vec<_>>();
        let result = self.process_batch(batch).await?;
        Ok(Some(result))
    }

    async fn process_batch(&self, items: Vec<SourceBatchItem<T>>) -> Result<SourceBatchResult> {
        let batch_id = Uuid::new_v4().to_string();
        let batch_size = items.len();
        let valid_items = items.into_iter().filter(SourceBatchItem::is_valid).collect::<Vec<_>>();
        let invalid_count = batch_size - valid_items.len();

        let written_count = self.sink.write_batch(valid_items).await?;

        let result = SourceBatchResult {
            batch_id,
            processed_count: batch_size,
            success_count: written_count,
            failure_count: invalid_count,
            batch_size,
            processing_time_ms,
            errors,
        };

        self.stats.record_batch(&result).await;
        Ok(result)
    }
}
```

Review 要点：Phase B1 先过滤 invalid item，不把 invalid item 交给 sink。是否要另建 dead-letter queue 放到后续阶段决定。

## Source pipeline 伪代码

```rust
async fn run_source_pipeline<T>(
    buffer: DataBuffer<SourceEnvelope<T>>,
    validator: SourceValidator,
    batch_processor: SourceBatchProcessor<T>,
) -> Result<()>
where
    T: Send + Sync + 'static,
{
    loop {
        match buffer.dequeue().await {
            Some(envelope) => {
                let validation = validator.validate(&envelope).await;
                let item = SourceBatchItem::new(envelope, validation);
                batch_processor.add_item(item).await?;
            }
            None => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}
```

Phase B1 是否实现 `run_source_pipeline` 可以作为后续实现时的最小闭环任务。伪代码重点是展示 buffer、validator、batch processor 的连接方式。

## 与现有模块的关系

| 现有模块 | Phase B1 关系 | 说明 |
| --- | --- | --- |
| `buffer.rs` | 复用 | `DataBuffer<T>` 已是 generic，可直接用于 `SourceEnvelope<T>` |
| `validator.rs` | 并行新增 | 现有 `DataValidator` 面向 `ParsedData`，不强行泛化 |
| `batch.rs` | 并行新增 | 现有 `BatchProcessor` 面向 `BatchItem`/storage，Phase B1 新增 source batch 类型 |
| `metrics.rs` | 后续扩展 | Phase B1 可先保留 stats struct，不强改全局 metrics |
| `recovery.rs` | 后续扩展 | checkpoint 持有但不持久化 |
| `parser.rs` | 不使用 | source path 接收结构化 payload，不走 parser |
| `receiver.rs` | 不使用 | adapter/source runtime 后续直接 enqueue source envelope |

## 推荐文件结构

```text
crates/fdc-ingestion/src/
  source/
    mod.rs
    envelope.rs
    checkpoint.rs
    quality.rs
    validator.rs
    batch.rs
    pipeline.rs
```

re-export：

```rust
// crates/fdc-ingestion/src/lib.rs
pub mod source;

pub use source::{
    SourceEnvelope,
    SourceType,
    SourceCheckpoint,
    SourcePartition,
    SourcePosition,
    SourceQualityFlags,
    SourceMetadata,
    SourceValidator,
    SourceValidationResult,
    SourceBatchItem,
    SourceBatchProcessor,
    SourceBatchResult,
    SourceBatchSink,
};
```

## Scope 分阶段建议

### Phase B1a：类型和 validator

- 新增 source envelope/checkpoint/quality/metadata 类型。
- 新增 SourceValidator 和 SourceValidationResult。
- 测试：合法 envelope 通过；空 source id 失败；duplicate/gap flags 生成 warnings。

### Phase B1b：batch item 和 processor

- 新增 SourceBatchItem。
- 新增 SourceBatchSink trait。
- 新增 SourceBatchProcessor。
- 测试：valid item 被 sink 接收；invalid item 不进 sink；flush 生成 result。

### Phase B1c：pipeline glue 和 demo fixture

- 新增 `run_source_pipeline_once` 或 bounded pipeline helper，避免测试无限 loop。
- 用 dummy payload 构造 `SourceEnvelope<DummyMarketEvent>`，不依赖 `fdc-barter`。
- 可选：在 integration test 中用 feature/dev-dependency 适配 `BarterIngestionEnvelope`，但不作为第一步。

## Not Doing

- 不让 `fdc-ingestion` 依赖 `fdc-barter`。
- 不实现真实 Barter stream。
- 不实现 Binance/OKX historical REST。
- 不实现 `fdc-transform` schema conversion。
- 不写真实 storage。
- 不改现有 network receiver path。
- 不实现跨事件 dedupe/gap detection 状态机。
- 不实现 checkpoint persistence。

## Review 关注点

1. 是否同意 `fdc-ingestion` 新增通用 source path，而不是直接依赖 `fdc-barter`？
2. 是否同意 `SourceEnvelope<T>` 泛型 payload 设计？
3. 是否同意 Phase B1 validator 只做 stateless validation，dedupe/gap detection 后续再做？
4. 是否同意 batch processor 先通过 `SourceBatchSink<T>` 抽象交接给 future transform？
5. 是否同意 Phase B1 实现拆成 B1a/B1b/B1c 三个小阶段，避免超时？

## 实现前验收标准

伪代码 review 通过后，implementation plan 应覆盖：

- 每个新类型的 public constructor 或 builder。
- 每个 validator 错误/警告的测试。
- batch processor 对 valid/invalid item 的测试。
- 不依赖 `fdc-barter` 的 compile 验证。
- `cargo test -p fdc-ingestion` 通过。
- `cargo check -p fdc-ingestion --all-targets` 通过。
- `cargo check --workspace --all-targets` 通过，仅允许既有 warning。
