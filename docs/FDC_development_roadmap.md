# FDC Development Roadmap

本文档定义 FDC 新架构的开发路线图、模块状态、阶段验收标准和模块衔接方式。

`docs/FDC_architecture_design.md` 是长期架构边界文档；本文档是执行层路线图。后续开发应先更新本文档状态，再进入模块详细设计和实现。

---

## 1. 路线图原则

FDC 后续开发不采用“一个模块完全做完，再做下一个模块”的纯横向方式。

推荐采用：

```text
contract-first + vertical slice + module milestone
```

含义：

1. **Contract-first**：先定义模块职责、输入输出、依赖方向和 canonical type，不先写内部实现。
2. **Vertical slice**：用一条最小端到端数据链路验证架构，例如 `adapter -> ingestion -> fdc-data -> storage -> query`。
3. **Module milestone**：每个模块仍有独立里程碑，但模块完成标准必须包含与上下游的衔接验证。
4. **No isolated perfection**：不要在某个模块内部追求完整功能后才集成，避免接口假设失真。
5. **Canonical model first**：所有链路以 `fdc-data` 的 canonical financial data model 为核心，不让 transform、analytics、adapter 或 storage 自行定义重复数据模型。

---

## 2. 文档分层

```text
docs/FDC_architecture_design.md
  长期架构原则、模块边界、依赖方向、canonical model 定位

docs/FDC_development_roadmap.md
  开发路线图、模块状态、阶段目标、集成顺序、验收标准

docs/modules/<module>.md
  单个模块详细设计，包括 public API、数据结构、错误模型、测试策略
```

推荐后续新增模块设计文档：

```text
docs/modules/fdc-data.md
docs/modules/fdc-adapter.md
docs/modules/fdc-ingestion.md
docs/modules/fdc-storage.md
docs/modules/fdc-query.md
docs/modules/fdc-transform.md
docs/modules/fdc-feature.md
docs/modules/fdc-factor.md
docs/modules/fdc-analytics.md
```

---

## 3. 模块状态枚举

每个模块使用统一状态枚举。

```text
Proposed
  已进入架构规划，但尚未确认边界。

Designing
  正在编写模块详细设计，接口和依赖方向仍可能变化。

Contract Ready
  模块职责、输入输出、public API、依赖方向、验收标准已确认，可以进入实现。

Implementing
  正在实现模块内部功能，但尚未完成上下游衔接验证。

Integrated
  已完成至少一条与上下游模块的 vertical slice 验证。

Stable
  API 和行为经过多场景验证，可以作为其他模块稳定依赖。
```

状态推进规则：

```text
Proposed -> Designing -> Contract Ready -> Implementing -> Integrated -> Stable
```

禁止跳过 `Contract Ready` 直接实现核心模块。

---

## 4. 当前模块状态表

| Module | Status | Depends On | Downstream Users | Next Integration |
| --- | --- | --- | --- | --- |
| `fdc-core` | Existing | none | all crates | clarify long-term overlap with `fdc-types` |
| `fdc-types` | Existing | `fdc-core` | `fdc-data`, query, storage, wasm | define base financial value boundaries |
| `fdc-data` | Designing | `fdc-core`, `fdc-types` | ingestion, adapter, transform, storage, query, analytics | design `common`, `reference`, `market` first |
| `fdc-adapter` | Proposed | `fdc-data`, `fdc-ingestion` contracts | ingestion | design adapter interface, use barter-rs as reference |
| `fdc-ingestion` | Existing | `fdc-core`, future `fdc-data` | storage, transform | accept canonical events/envelopes |
| `fdc-storage` | Existing | `fdc-core`, future `fdc-data` | query, analytics | store canonical market events and bars |
| `fdc-query` | Existing | `fdc-storage`, future `fdc-data` | api, analytics | query by instrument/time/kind |
| `fdc-transform` | Existing placeholder | future `fdc-data` | feature, analytics | `Trade -> Bar` first transform |
| `fdc-feature` | Proposed | `fdc-data`, `fdc-transform` | factor, analytics, strategy | define feature model after market slice |
| `fdc-factor` | Proposed | `fdc-data`, `fdc-feature`, `fdc-transform` | strategy, analytics | define factor contracts after feature model |
| `fdc-analytics` | Existing | future `fdc-data`, `fdc-query`, `fdc-feature` | api, reports | remove duplicated canonical market model |
| `fdc-api` | Existing | query, ingestion, future `fdc-data` | server, cli | expose canonical query and ingestion APIs |
| `fdc-proto` | Existing placeholder | `fdc-data` conversion boundary | external clients | define wire DTO after Rust canonical model |
| `fdc-orchestrator` | Proposed | api, adapter, ingestion, storage, wasm | server/runtime | defer until vertical slice works |

说明：

- `Existing` 表示当前 upstream/main 已有 crate 或 scaffold，但不等于架构稳定。
- `Proposed` 表示架构需要该模块，但第一阶段可以只做设计和边界。
- `fdc-data` 是当前路线图的第一优先级，因为它决定后续所有模块的公共语言。

---

## 5. 总体阶段路线

### Phase 0：架构治理与状态基线

目标：建立可持续推进机制。

范围：

- `FDC_architecture_design.md` 作为长期架构边界；
- `FDC_development_roadmap.md` 作为路线图和状态源；
- 后续每个核心模块进入实现前，必须先有 `docs/modules/<module>.md`；
- 明确所有模块状态和 next integration。

验收标准：

- 架构文档明确 `fdc-data`、`fdc-types`、`fdc-adapter`、`fdc-ingestion` 边界；
- 路线图列出所有核心模块状态；
- 能回答“下一步开发哪个模块，为什么，如何验收”。

当前状态：进行中。

---

### Phase 1：fdc-data canonical model

目标：建立统一标准金融数据模型 crate。

优先范围：

```text
fdc-data
  common
  reference
  market
```

暂不展开完整实现：

```text
news
macro_data
fundamental
onchain
altdata
portfolio
```

这些模块只预留边界，避免第一阶段过度设计。

关键设计点：

- `common`：`DataId`、`SourceId`、`Domain`、`SchemaVersion`、时间戳语义、质量标记、lineage、entity links；
- `reference`：instrument registry、exchange、calendar、entity mapping；
- `market`：instrument、trade、bar、order book、derivatives、market event；
- crate root 不平铺所有领域对象，领域对象通过 `fdc_data::<domain>::Type` 访问。

验收标准：

- `fdc-data` 不依赖 storage/query/transform/wasm/api；
- 能表达最小 market event；
- 能表达 instrument/reference 信息；
- 能作为 ingestion、adapter、transform、storage 的共同输入输出语言。

状态目标：`Designing -> Contract Ready`。

---

### Phase 2：market vertical slice

目标：用最小行情链路验证架构，而不是孤立完成各模块。

最小链路：

```text
sample external source
  -> fdc-adapter demo mapper
  -> fdc_data::market::MarketEvent
  -> fdc-ingestion SourceEnvelope
  -> fdc-storage write
  -> fdc-query read by instrument/time
```

第一版 adapter 可以是 demo/static/csv source，不需要立即接真实 Binance/OKX。

关键设计点：

- `fdc-adapter` 负责 source-specific protocol/raw payload/mapping；
- `fdc-ingestion` 负责统一 pipeline、buffer、validator、checkpoint、backpressure；
- `fdc-storage` 负责 canonical event/bar 的持久化；
- `fdc-query` 负责 canonical query semantics。

验收标准：

- 一条 `Trade` 或 `Bar` 能从 sample source 进入系统并被查询出来；
- 中间不引入重复 market data model；
- adapter 输出边界是 `fdc_data::market::MarketEvent` 或明确 envelope；
- storage/query 不依赖 adapter-specific raw model。

状态目标：`fdc-data Integrated`，`fdc-ingestion/storage/query` 对 market slice 达到 `Integrated`。

---

### Phase 3：fdc-adapter 真实数据源设计

目标：在 vertical slice 验证后，再设计真实 adapter 体系。

参考：

- 可参考 `barter-rs` 的 exchange connector、stream lifecycle、subscription、normalized event 分层；
- 不直接采用 barter-rs 内部类型作为 FDC canonical model；
- barter-rs 只能作为 adapter 设计参考或 source integration 参考。

优先 adapter：

```text
fdc-adapter
  traits
  source config
  subscription model
  normalized mapper
  error model
  demo source
  barter/binance/okx reference implementation later
```

验收标准：

- adapter trait 不泄漏具体交易所 raw type；
- adapter 输出可进入 ingestion；
- adapter 错误、重连、订阅、时间语义有明确边界；
- 至少一个 demo adapter 完成 vertical slice。

状态目标：`Proposed -> Designing -> Contract Ready`。

---

### Phase 4：transform and feature foundation

目标：建立从 canonical market data 到派生数据和特征的第一条链路。

最小链路：

```text
fdc_data::market::Trade
  -> fdc-transform TradeToBar
  -> fdc_data::market::Bar
  -> fdc-feature FeatureSet / FeatureVector
```

关键边界：

- `fdc-transform` 不定义 canonical market model；
- `fdc-feature` 负责特征结果如何表达、组织和存储接口；
- `fdc-factor` 负责因子计算定义和执行，不拥有 feature storage 语义。

验收标准：

- `Trade -> Bar` 转换使用 `fdc_data::market` 类型；
- feature 类型不与 market canonical model 重复；
- analytics 可以消费 Bar 或 Feature，而不是自定义重复 MarketData。

状态目标：`fdc-transform Integrated`，`fdc-feature Contract Ready`。

---

### Phase 5：analytics, factor, strategy runtime

目标：让研究、回测、风控和策略运行建立在 canonical data + feature 之上。

优先顺序：

```text
fdc-analytics
  consume fdc_data::market::Bar
  consume fdc_feature::FeatureSet
  produce AnalyticsResult/RiskMetrics/Report

fdc-factor
  define factor specs and factor calculation contracts

strategy runtime / orchestrator
  coordinate adapter, ingestion, storage, query, transform, factor, wasm
```

验收标准：

- `fdc-analytics::models::MarketData` 不再作为 canonical market model；
- analytics/factor/strategy 只消费 canonical data 或 feature outputs；
- orchestrator 不持有 domain model，只编排模块。

状态目标：`fdc-analytics Integrated`，`fdc-factor Contract Ready`，`fdc-orchestrator Designing`。

---

### Phase 6：扩展非 market 数据域

目标：在 market slice 稳定后扩展多金融数据域。

扩展顺序建议：

1. `reference` 深化：entity graph、issuer、asset、calendar；
2. `portfolio`：order、fill、position、PnL、strategy run metadata；
3. `fundamental`：financial statement、metric、filing metadata；
4. `news`：article、announcement、event extraction；
5. `macro_data`：indicator、release、revision；
6. `onchain` / `altdata`：等前面链路稳定后再进入。

验收标准：

- 新数据域仍在 `fdc-data` 内部 module；
- 跨域关联通过 `common` 和 `reference`，不形成 domain cycles；
- storage/query 支持 domain-aware schema；
- factor/analytics 可以跨域 join。

状态目标：按数据域逐步从 `Proposed` 到 `Integrated`。

---

## 6. 模块衔接规则

### 6.1 每个模块必须声明 contract

每个模块设计文档必须包含：

```text
Purpose
Inputs
Outputs
Public API
Dependencies
Non-goals
Error model
Testing strategy
Integration target
```

没有 contract 的模块不能进入实现。

### 6.2 每个实现必须有 integration target

例：

```text
fdc-data integration target:
  fdc-ingestion and fdc-transform can compile against fdc_data::market types

fdc-storage integration target:
  can persist and read fdc_data::market::Bar by instrument/time

fdc-query integration target:
  can query market data without knowing adapter raw model
```

### 6.3 不接受孤立完成

模块不能只以“单元测试通过”作为完成标准。

完成至少需要：

```text
unit tests
+ contract tests
+ one upstream/downstream integration test or demo
```

---

## 7. 状态更新流程

每次完成设计或实现后，应更新本文档中的模块状态表。

推荐提交节奏：

```text
1. docs: update roadmap status for <module>
2. docs: add module design for <module>
3. feat: implement <module> contract
4. test: add integration slice for <module>
5. docs: mark <module> integrated
```

状态更新必须说明：

```text
previous status
new status
evidence
next integration
```

示例：

```text
Module: fdc-data
Previous: Designing
New: Contract Ready
Evidence:
  - docs/modules/fdc-data.md approved
  - common/reference/market public API specified
  - dependency direction validated
Next integration:
  - fdc-ingestion SourceEnvelope<fdc_data::market::MarketEvent>
```

---

## 8. 推荐近期工作顺序

近期不要直接进入大规模实现。建议顺序：

```text
1. 完成本文档并作为 roadmap baseline
2. 编写 docs/modules/fdc-data.md
3. 审核 fdc-data contract
4. 实现最小 fdc-data crate
5. 编写 docs/modules/market-vertical-slice.md 或 docs/modules/fdc-ingestion-market-slice.md
6. 实现 sample source -> ingestion -> storage -> query 的最小 vertical slice
7. 再设计真实 fdc-adapter
```

第一阶段目标不是功能多，而是把核心边界打穿。

---

## 9. 当前推荐下一步

下一步应进入：

```text
docs/modules/fdc-data.md
```

该文档只设计 `fdc-data`，不实现代码。重点回答：

- `common` 放哪些跨域类型；
- `reference` 第一版定义哪些实体；
- `market` 第一版定义哪些 canonical market objects；
- 哪些类型暂时继续复用 `fdc-core` / `fdc-types`；
- public API 路径如何稳定；
- 第一版不做哪些事情。

完成 `fdc-data` 模块设计并确认后，再进入具体 implementation plan。
