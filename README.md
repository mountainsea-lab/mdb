# Financial Data Center (FDC)

面向金融行情数据采集、分层存储、查询和生产化运行验证的 Rust 数据中心项目。

当前分支 `mdb-mqdev` 的开发重点已经从早期架构蓝图推进到 **内部 MVP / 受控生产化验证阶段**：项目可以构建 `fdc_server`，通过 Docker Compose 或本地二进制启动，使用四层行情存储配置，提供健康检查、就绪检查、版本信息、行情查询、live 采集控制和存储维护接口。

> 状态来源：`docs/DEVELOPMENT_STATUS.md`、`docs/runbooks/market-data-production-runbook.md`、`docs/runbooks/market-data-package-deployment-verification.md`。

## 当前开发阶段

- **阶段定位**：内部 MVP、生产运行时装配、部署包验证和短周期 smoke/soak 验证。
- **主服务**：`fdc-server` 包中的 `fdc_server` 二进制。
- **默认部署方式**：Docker Compose，本地监听 `127.0.0.1:18080`，容器内监听 `0.0.0.0:18080`。
- **默认存储形态**：tiered market-data storage，L1 内存层加 L2 redb、L3 DuckDB、L4 RocksDB 持久化层。
- **默认安全模型**：live acquisition 可手动启用但不自动启动；resume/reset 等恢复类操作通过环境变量显式开关保护。
- **近期完成重点**：P38 查询 API 加固、P39 生产 runbook 与配置包、P40 生产运行时装配修复与 `/version` 就绪端点。

## 已具备的可验证能力

### 生产运行时与部署

- `fdc_server` 会从运行时配置装配 `ProductionServerState`，生产配置中的 tiered storage 会驱动实际服务状态。
- Dockerfile 使用多阶段构建，最终镜像只保留运行依赖和 `/usr/local/bin/fdc_server`。
- `docker-compose.yml` 挂载 `./data/fdc-market-data` 到容器内 `/app/var/fdc-market-data`，用于持久化行情存储。
- 配置示例位于：
  - `config/docker.env.example`
  - `config/production.local.example.env`
  - `config/production.auto-live.local.example.env`

### 行情采集与查询

- 支持通过 `crates/fdc-adapter/barter` 接入 Barter 生态的 Binance spot/futures live 与 historical 数据能力。
- `GET /market-data/trades` 已完成生产化查询加固：限制 `limit` 范围、规范化 symbol、返回稳定查询元数据，并对非法参数返回确定的 HTTP 400 JSON 错误。
- live 采集支持手动 start/stop/status/resume，并暴露失败次数、重试状态、suppressed 状态和安全恢复门禁信息。

### 存储与维护

- `fdc-storage` 提供面向行情数据的四层存储边界和查询接口。
- 生产服务暴露存储状态、健康检查、手动 maintenance、maintenance audit 和 scheduler 状态/恢复接口。
- 默认 Docker 配置启用手动 maintenance，关闭后台 scheduler 自动运行。

## 市场数据采集能力矩阵

能力来源主要在 `crates/fdc-adapter/barter`。下表区分 **当前已接入/已验证主线** 和 **能力矩阵声明**：前者是 README 面向当前阶段推荐优先使用的路径，后者表示 adapter capability map 中声明的 crypto live 覆盖范围，不等同于所有交易所都已完成生产 smoke。

### 当前已接入/已验证主线

| Source | 市场类型 | 模式 | 数据类型 | 默认/已验证标的 | 状态 |
|---|---|---|---|---|---|
| `binance_spot` | Spot | Live | Trade | BTC/USDT, ETH/USDT | 已接入；生产 live smoke 已证明真实 Binance Spot trades 可通过生产 API 查询 |
| `binance_spot` | Spot | Live | OrderBookL1, OrderBook | BTC/USDT, ETH/USDT | 已接入结构化 mapper；适合 bounded live 验证 |
| `binance_futures_usd` | Perpetual | Live | Trade, OrderBookL1, OrderBook, Liquidation | BTC/USDT, ETH/USDT | 已接入结构化 mapper 与示例 |
| `binance_spot` | Spot | Historical REST | Candle/OHLCV | 请求指定 symbol/interval/time range | 已接入 adapter-owned REST descriptor/executor |
| `binance_spot` | Spot | Historical REST | Trade | 请求指定 symbol/time range/cursor | 已接入 adapter-owned REST descriptor/executor |

### 能力矩阵声明/规划支持

| Source | 市场类型 | Live 数据类型 | Historical 数据类型 | 说明 |
|---|---|---|---|---|
| `binance_spot` | Spot | Trade, OrderBookL1, OrderBook | Candle, Trade | 当前主线，historical 已实现 Binance Spot focused REST |
| `binance_futures_usd` | Perpetual | Trade, OrderBookL1, OrderBook, Liquidation | - | 当前 live 主线之一 |
| `bybit_spot` | Spot | Trade, OrderBookL1, OrderBook | - | capability map 声明，生产验证程度低于 Binance 主线 |
| `bybit_perpetuals_usd` | Perpetual | Trade, OrderBookL1, OrderBook | - | capability map 声明 |
| `kraken` | Spot | Trade, OrderBookL1 | - | capability map 声明 |
| `coinbase` | Spot | Trade | - | capability map 声明 |
| `bitfinex` | Spot | Trade | - | capability map 声明 |
| `bitmex` | Perpetual | Trade | - | capability map 声明 |
| `gateio_spot` | Spot | Trade | - | capability map 声明 |
| `gateio_futures_usd` | Future | Trade | - | capability map 声明 |
| `gateio_futures_btc` | Future | Trade | - | capability map 声明 |
| `gateio_perpetuals_usd` | Perpetual | Trade | - | capability map 声明 |
| `gateio_perpetuals_btc` | Perpetual | Trade | - | capability map 声明 |
| `gateio_options` | Option | Trade | - | capability map 声明 |
| `okx` | Spot | Trade | - | capability map 声明 |

当前限制：

- Historical REST 支持目前主要集中在 Binance Spot；多交易所 historical provider 是后续工作。
- Historical 已覆盖 Binance Spot OHLCV 和 trades，但 historical order-book reconstruction 尚未实现。
- L2 order book payload 会保留 snapshot/update、levels、timestamps 和 sequence where available；durable book reconstruction、gap detection、out-of-order repair 仍是后续工作。
- 外部网络 live/historical smoke 需要显式环境变量和公网访问，常规验证默认依赖 offline contract tests。
- `fdc-barter` bounded helpers 到通用 `fdc-ingestion` source pipeline 的跨模块 glue 仍是后续工作。

## 项目结构

```text
mdb/
├── Cargo.toml                         # Rust workspace 配置
├── Dockerfile                         # fdc_server 生产镜像多阶段构建
├── docker-compose.yml                 # 本地/受控环境 Compose 部署
├── README.md                          # 项目入口文档
├── config/
│   ├── docker.env.example             # Compose 默认安全配置
│   ├── production.local.example.env   # 本地生产 smoke 配置
│   └── production.auto-live.local.example.env
├── crates/
│   ├── fdc-core/                      # 核心数据类型、时间、指标等基础能力
│   ├── fdc-storage/                   # 行情数据分层存储、维护和查询边界
│   ├── fdc-query/                     # 查询引擎相关模块
│   ├── fdc-ingestion/                 # 数据接入流水线基础模块
│   ├── fdc-api/                       # API/demo 层与早期服务接口
│   ├── fdc-analytics/                 # 聚合、指标、风险、批/流分析模块
│   ├── fdc-wasm/                      # WASM 插件系统基础模块
│   ├── fdc-types/                     # 自定义类型系统基础模块
│   ├── fdc-transform/                 # 数据转换模块
│   ├── fdc-adapter/
│   │   └── barter/                    # Barter 行情适配器、模型、mapper、采集能力
│   ├── fdc-common/                    # 通用工具与共享类型
│   ├── fdc-proto/                     # Protocol Buffers / gRPC 相关定义
│   ├── fdc-cli/                       # 命令行入口
│   ├── fdc-server/                    # 当前生产运行时主服务 fdc_server
│   └── fdc-orchestrator/              # 编排与调度相关模块
└── docs/
    ├── DEVELOPMENT_STATUS.md          # 当前开发状态与恢复入口
    ├── runbooks/                      # 生产运行、部署验证、操作检查清单
    └── superpowers/                   # 设计与实施计划归档
```

## 快速开始

### 前置要求

- Rust workspace 声明的 MSRV：Rust `1.75`
- Docker 镜像构建使用：Rust `1.95-bookworm`
- Cargo
- Docker 与 Docker Compose（如需容器部署）
- 首次构建 DuckDB/RocksDB 相关依赖时建议预留至少 10 GiB 可用空间

### 本地构建与测试

```bash
# 构建 workspace
cargo build

# 运行全部测试
cargo test

# 构建当前主服务二进制
cargo build -p fdc-server --release --bin fdc_server
```

常用聚焦验证命令：

```bash
# 生产配置解析合同
cargo test -p fdc-server --test runtime_config_contract

# 生产路由合同
cargo test -p fdc-server --test production_server_router_contract

# 生产二进制运行时装配合同
cargo test -p fdc-server --test production_binary_runtime_contract

# fdc-storage 依赖边界守卫
cargo test -p fdc-storage dependency_guard
```

## Docker Compose 部署

项目提供生产镜像构建文件和本地 Compose 配置。默认服务地址为 `http://127.0.0.1:18080`。

```bash
# 构建镜像并启动服务
docker compose up -d --build

# 查看容器状态和日志
docker compose ps
docker compose logs -f fdc-server
```

默认 Compose 配置：

- 镜像：`fdc-server:local`
- 环境文件：`config/docker.env.example`
- 端口映射：`127.0.0.1:18080 -> container:18080`
- 持久化目录：`./data/fdc-market-data:/app/var/fdc-market-data`
- 健康检查：`GET /health`
- live acquisition：可手动启动，默认不 autostart
- storage maintenance：手动 maintenance 开启，scheduler 默认关闭

如果需要私有化配置：

```bash
cp config/docker.env.example config/docker.env
# 编辑 config/docker.env 后，将 docker-compose.yml 的 env_file 改为 config/docker.env
```

停止服务：

```bash
# 停止容器，保留持久化数据
docker compose down

# 如需清理本地持久化行情数据
rm -rf ./data/fdc-market-data
```

## 运行时检查

服务启动后，可以按以下顺序执行 smoke 检查：

```bash
# 基础健康与就绪
curl --noproxy '*' -sS http://127.0.0.1:18080/health
curl --noproxy '*' -sS http://127.0.0.1:18080/ready
curl --noproxy '*' -sS http://127.0.0.1:18080/version

# 存储状态与四层健康
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/status
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/storage/health

# live 行情状态
curl --noproxy '*' -sS http://127.0.0.1:18080/market-data/live/status

# 查询最近交易数据
curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/trades?limit=5'
```

手动启动一次有界 live 采集：

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":10,"max_envelopes":5}'
```

手动运行一次存储维护：

```bash
curl --noproxy '*' -sS -X POST http://127.0.0.1:18080/market-data/storage/maintenance/run-once \
  -H 'content-type: application/json' \
  -d '{"confirmation":"run_storage_maintenance_once"}'

curl --noproxy '*' -sS 'http://127.0.0.1:18080/market-data/storage/maintenance/audit?limit=10'
```

更完整的操作流程请使用 runbook，而不是只依赖 README。

## 主要 API 端点

| 端点 | 用途 |
|---|---|
| `GET /health` | 进程健康检查 |
| `GET /ready` | 服务就绪检查，包含关键运行时状态 |
| `GET /version` | 服务名称和包版本元数据 |
| `GET /market-data/trades` | 查询交易数据，支持 `limit` 与 symbol 过滤 |
| `POST /market-data/live/start` | 手动启动有界 live 采集 |
| `POST /market-data/live/stop` | 停止 live 采集 |
| `POST /market-data/live/resume` | 受门禁保护的 live 恢复操作 |
| `GET /market-data/live/status` | 查看 live runner 状态、失败、重试和 suppressed 信息 |
| `GET /market-data/storage/status` | 查看存储 backend 与 tier 配置状态 |
| `GET /market-data/storage/health` | 查看四层存储健康状态 |
| `POST /market-data/storage/maintenance/run-once` | 手动运行一次存储维护 |
| `GET /market-data/storage/maintenance/audit` | 查看维护审计记录 |
| `GET /market-data/storage/maintenance/scheduler/status` | 查看 maintenance scheduler 状态 |

## 文档入口

- 当前开发状态与恢复入口：[`docs/DEVELOPMENT_STATUS.md`](docs/DEVELOPMENT_STATUS.md)
- 生产运行手册：[`docs/runbooks/market-data-production-runbook.md`](docs/runbooks/market-data-production-runbook.md)
- 部署包验证清单：[`docs/runbooks/market-data-package-deployment-verification.md`](docs/runbooks/market-data-package-deployment-verification.md)
- Barter 行情采集需求与计划：
  - [`crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`](crates/fdc-adapter/barter/docs/market-data-collection-requirements.md)
  - [`crates/fdc-adapter/barter/docs/market-data-collection-implementation-plan.md`](crates/fdc-adapter/barter/docs/market-data-collection-implementation-plan.md)
- 存储模块状态：[`crates/fdc-storage/README.md`](crates/fdc-storage/README.md)

## 当前路线图

### 已完成或已具备验证入口

- [x] Rust workspace 与核心 crate 拆分
- [x] `fdc_server` 生产运行时启动路径
- [x] 本地生产配置包与 Docker Compose 配置
- [x] `/health`、`/ready`、`/version` 基础运行检查
- [x] tiered storage 运行时装配、状态检查与健康检查
- [x] `GET /market-data/trades` 查询参数校验与稳定元数据
- [x] live acquisition 手动控制、重试、suppressed 状态和受保护 resume
- [x] storage maintenance 手动执行、audit 与 scheduler 状态接口
- [x] 部署验证清单和生产 runbook

### 下一阶段重点

- [ ] 按 P40 后建议重新执行内部 MVP smoke，记录实际部署结果。
- [ ] 基于 smoke/soak 结果选择下一项生产就绪缺口。
- [ ] 扩展 candle/OHLCV 等查询路由，目前生产加固范围主要覆盖 `/market-data/trades`。
- [ ] 将更多真实网络 live 采集验证保持为显式 opt-in，避免默认测试依赖外部网络。
- [ ] 持续补齐 CLI、orchestrator、analytics、wasm/types 等模块的生产化边界。

## 贡献与开发约定

- 优先阅读 `docs/DEVELOPMENT_STATUS.md`，确认当前分支状态和推荐下一步。
- 对生产运行时、存储、live acquisition、维护门禁相关改动，应优先增加或更新合同测试。
- 默认保持危险操作关闭，通过环境变量和确认字符串显式启用。
- 生产验证命令建议参考 runbook 中的 `curl --noproxy '*'` 写法，避免代理导致本地 smoke 误判。

## 许可证

Cargo workspace 当前声明许可证为 `MIT OR Apache-2.0`。仓库根目录暂未包含独立许可证文本文件，正式发布前应补齐对应 LICENSE 文件。
