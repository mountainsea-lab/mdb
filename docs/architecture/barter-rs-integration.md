# Barter-rs 集成方向分析

## 背景

mdb 当前定位是金融级高频交易数据中心，核心能力包括接入、转换、存储、查询和分析。`plan1.md` 中最新状态显示，核心组件大多已有实现，后续关键缺口是 `fdc-transform` 数据转换管道和系统级集成。

本地 `/Volumes/wdata/mountainsea-lab/barter-rs` 项目提供完整的交易生态，其中：

- `barter-data`：多交易所公共行情 WebSocket 实时数据流，输出标准化 `MarketEvent`。
- `barter-integration`：底层 REST/WebSocket/Stream 抽象，包含 `DataArgs<Live>` 与 `DataArgs<Historical>` 模式抽象。
- `barter-instrument`：交易所、资产、交易对和索引化 instrument 数据结构。

目标是在 mdb 中复用 barter-rs 的多交易所实时行情能力，并为历史数据获取预留统一抽象。

## 方向结论

建议采用 **mdb 侧适配器模式**：在 mdb 中新增统一适配器目录 `crates/fdc-adapter`，并在其中放置 Barter 集成 crate `crates/fdc-adapter/barter`（crate 名保持 `fdc-barter`），依赖 barter-rs 相关 crate，将 Barter 的行情流和事件模型转换为 mdb 的接入/转换模型。

不建议优先在 barter-rs 中增加 mdb 专用模块。

## 推荐架构

```text
barter-data / barter-integration / barter-instrument
        ↓
crates/fdc-adapter/barter (`fdc-barter`)
        ↓
Barter MarketEvent / Subscription / ExchangeId
        ↓
mdb BarterMarketEvent / RawData / TickData / transform input
        ↓
fdc-transform
        ↓
fdc-storage / fdc-query / fdc-analytics
```

`fdc-barter` 应只承担适配职责，不承担存储、查询、交易策略或执行职责。

## 方案对比

### 方案 A：在 mdb 的 `crates/fdc-adapter/barter` 中维护 `fdc-barter` 适配器 crate，推荐

优点：

- 职责边界清晰，Barter 做行情源，mdb 做数据中心。
- 避免 mdb 业务模型污染 barter-rs。
- mdb 可独立维护自己的类型映射、错误处理、配置和数据转换管道。
- 后续可通过 path/git dependency 独立升级 barter-rs。
- 适合当前 `fdc-transform` 缺口，能快速形成真实数据闭环。

缺点：

- mdb 需要维护一层转换代码。
- Barter API 变更时，适配层需要跟随更新。

### 方案 B：在 barter-rs 中新增 mdb 模块，不推荐当前阶段采用

优点：

- 如果未来 mdb 要成为 Barter 官方生态存储后端，可让 Barter 用户直接使用。
- 有机会沉淀为 `barter-mdb-sink` 或类似生态 crate。

缺点：

- 会让 barter-rs 理解 mdb 类型和数据中心语义，职责方向反转。
- 两个项目发布、测试和版本节奏耦合。
- 当前 mdb 仍在建设核心转换/存储闭环，过早反向集成会增加复杂度。

适用条件：

- mdb 已具备稳定写入 API。
- 有明确需求让 Barter 用户直接将数据写入 mdb。
- 可将集成模块设计为独立 Barter 生态 crate，而非侵入 Barter 核心。

### 方案 C：先在 mdb 中做 `fdc-barter`，成熟后反向贡献独立生态 crate

这是推荐路线的长期演进版本。

阶段：

1. mdb 内部实现 `fdc-barter`。
2. 验证实时行情接入、转换、存储、查询闭环。
3. 补齐历史数据获取与回放。
4. 如果 API 稳定且有外部复用价值，再抽出或贡献为 Barter 生态插件。

## 历史数据判断

当前观察到 `barter-integration` 有 `DataArgs<Historical>` 抽象，但 `barter-data` 主线能力仍以 WebSocket 实时流为核心。历史数据建议先在 `fdc-barter` 中定义 mdb 侧统一接口，初期可以按交易所 REST Kline/Trades 拉取实现。

后续如果 barter-rs 补齐通用历史数据客户端，`fdc-barter` 可以切换为复用 Barter 的历史数据抽象。

## 第一阶段范围

第一阶段只做准备和骨架，不接真实交易所网络。

包含：

- 新增 `crates/fdc-adapter/barter` crate，crate 名为 `fdc-barter`。
- 接入 workspace。
- 定义适配器配置、数据模式、错误类型和标准化事件结构。
- 定义实时数据客户端边界，但不启动真实 WebSocket。
- 提供 Barter trade 事件到 mdb 标准事件的最小映射工具。
- 提供单元测试验证类型和映射行为。

不包含：

- 真实 WebSocket 连接。
- 历史 REST 拉取实现。
- 写入 storage。
- API/CLI 暴露。
- 性能优化和 benchmark。

## 后续路线

1. `fdc-barter` 骨架与基础类型。
2. Barter 实时 public trades / L1 order book 映射。
3. 与 `fdc-transform` 的输入类型打通。
4. 端到端流：Barter stream -> fdc-barter -> fdc-transform -> fdc-storage。
5. 历史数据接口与交易所 REST 实现。
6. 配置化订阅和运行时管理。
7. 如有外部复用价值，再考虑反向贡献到 barter-rs 生态。

## 维护原则

- `fdc-barter` 不引入 mdb 存储和查询依赖，避免适配层变重。
- Barter 类型只在适配边界出现，mdb 内部使用自己的标准事件和类型。
- 历史和实时使用统一输出模型，便于后续回放与实时混合处理。
- 优先保证编译、测试和边界清晰，再逐步接入真实网络能力。
