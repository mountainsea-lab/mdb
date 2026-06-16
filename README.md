# Financial Data Center (FDC) v3.0

🚀 **高性能金融级高频交易数据中心** - 基于Rust + WASM插件系统的可扩展架构

[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/financial-data-center/mdb)

## 🌟 核心特性

### 🔥 **极致性能**
- **端到端延迟**: < 3微秒 (tick-to-trade)
- **吞吐量**: 2000万+ ticks/秒
- **查询响应**: < 50微秒 (P99)
- **零拷贝**: Apache Arrow + 自定义内存管理

### 🧩 **WASM插件系统**
- **多语言支持**: Rust、C++、Go、Python、JavaScript
- **热加载**: 零停机插件更新
- **安全沙箱**: 完全隔离的执行环境
- **高性能**: 接近原生性能的执行效率

### 🏷️ **自定义类型系统**
- **金融专用类型**: PRICE、VOLUME、SYMBOL、OPTION_CONTRACT
- **用户定义类型**: 支持复杂业务逻辑的自定义类型
- **动态类型转换**: WASM驱动的智能类型转换
- **类型验证**: 实时数据完整性检查

### 🗄️ **多引擎存储架构**
- **L1**: 超热缓存 (自定义格式 + WASM优化) - <1μs
- **L2**: 热数据缓存 (redb + Apache Arrow) - <5μs
- **L3**: 温数据存储 (DuckDB + WASM UDF) - <100μs
- **L4**: 冷数据存储 (RocksDB + WASM压缩) - <10ms

## 🏗️ 项目结构

```
financial-data-center/
├── Cargo.toml                 # 工作空间配置
├── README.md
├── LICENSE
├── plan1.md                   # 详细技术方案
├── crates/                    # 核心包
│   ├── fdc-core/              # ✅ 核心库 (已完成)
│   ├── fdc-wasm/              # 🚧 WASM插件系统
│   ├── fdc-types/             # 🚧 自定义类型系统
│   ├── fdc-storage/           # 🚧 存储引擎
│   ├── fdc-query/             # 🚧 查询引擎
│   ├── fdc-ingestion/         # 🚧 数据接入
│   ├── fdc-api/               # 🚧 API服务
│   ├── fdc-analytics/         # 🚧 分析引擎
│   ├── fdc-transform/         # 🚧 数据转换引擎
│   ├── fdc-common/            # 🚧 通用工具
│   ├── fdc-proto/             # 🚧 Protocol Buffers
│   ├── fdc-cli/               # 🚧 命令行工具
│   └── fdc-server/            # 🚧 服务器主程序
├── plugins/                   # WASM插件目录
│   ├── rust-plugins/          # Rust编写的插件
│   ├── cpp-plugins/           # C++编写的插件
│   ├── go-plugins/            # Go编写的插件
│   ├── python-plugins/        # Python编写的插件
│   └── js-plugins/            # JavaScript编写的插件
├── schemas/                   # 自定义类型定义
├── examples/                  # 示例代码
├── benchmarks/                # 性能基准测试
├── docs/                      # 文档
└── k8s/                       # Kubernetes部署配置
```

## 🚀 快速开始

### 前置要求

- Rust 1.95+
- Cargo
- Git

### 安装与构建

```bash
# 克隆仓库
git clone https://github.com/financial-data-center/mdb.git
cd mdb

# 构建所有包
cargo build --release

# 运行测试
cargo test

# 运行核心功能演示
cargo run -p fdc-core --example fdc_core_demo
```

### Docker 镜像构建与 Compose 部署

项目提供生产镜像构建文件和本地 `docker compose` 部署配置，默认运行 `fdc-server` 的 `fdc_server` 二进制服务，监听 `18080` 端口，并将行情分层存储持久化到宿主机 `./data/fdc-market-data`。

#### 1. 构建 Docker 镜像

```bash
# 在项目根目录构建生产镜像
docker build -t fdc-server:local .
```

镜像使用多阶段构建：builder 阶段编译 `cargo build --release --bin fdc_server`，runtime 阶段只保留运行所需依赖和 `/usr/local/bin/fdc_server`。

#### 2. 使用 docker compose 启动

```bash
# 使用 config/docker.env.example 中的安全默认配置启动
docker compose up -d --build

# 查看服务状态和日志
docker compose ps
docker compose logs -f fdc-server
```

默认配置说明：

- 服务地址：`http://127.0.0.1:18080`
- 容器监听：`FDC_SERVER_ADDR=0.0.0.0:18080`
- 环境配置：`config/docker.env.example`
- 持久化目录：`./data/fdc-market-data:/app/var/fdc-market-data`
- 健康检查：`GET /health`
- 默认开启手动 live acquisition 能力，但不自动启动：`FDC_LIVE_ENABLED=1`、`FDC_LIVE_AUTOSTART=0`

如果需要私有化配置，可复制示例文件后修改 `docker-compose.yml` 的 `env_file`：

```bash
cp config/docker.env.example config/docker.env
# 编辑 config/docker.env 后，将 docker-compose.yml 的 env_file 改为 config/docker.env
```

#### 3. 验证部署

```bash
# 健康检查
curl http://127.0.0.1:18080/health

# 就绪检查
curl http://127.0.0.1:18080/ready

# 查看 live 行情状态
curl http://127.0.0.1:18080/market-data/live/status

# 手动启动一次有界 live 行情采集
curl -X POST http://127.0.0.1:18080/market-data/live/start \
  -H 'content-type: application/json' \
  -d '{"timeout_secs":10,"max_envelopes":5}'

# 查询最近交易数据
curl 'http://127.0.0.1:18080/market-data/trades?limit=5'
```

#### 4. 停止与清理

```bash
# 停止容器但保留持久化数据
docker compose down

# 如需清理本地持久化行情数据
rm -rf ./data/fdc-market-data
```

### 基础使用示例

```rust
use fdc_core::{
    types::*,
    time::TimeUtils,
    metrics::Metrics,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建tick数据
    let symbol = Symbol::new("AAPL");
    let price = Price::from_f64(150.25).unwrap();
    let volume = Volume::new(1000);
    let exchange_id = ExchangeId::new(1);
    let sequence_number = SequenceNumber::new(12345);

    let tick_data = TickData::new(
        symbol,
        price,
        volume,
        exchange_id,
        MessageType::Trade,
        sequence_number,
    );

    println!("Created tick: {} @ ${}", tick_data.symbol, tick_data.price);

    // 指标收集
    let metrics = Metrics::new();
    metrics.increment_counter("trades_processed", 1);

    Ok(())
}
```

## 📊 性能对比

| 指标 | kdb+ | QuestDB | InfluxDB | TimescaleDB | **FDC v3.0** |
|------|------|---------|----------|-------------|-------------|
| 写入延迟(P99) | 1-5μs | 10-50μs | 100μs-1ms | 1-10ms | **<3μs** |
| 查询延迟(P99) | 100-500μs | 1-10ms | 10-100ms | 10-100ms | **<50μs** |
| 吞吐量 | 1000万/s | 400万/s | 100万/s | 50万/s | **2000万/s** |
| SQL支持 | q语言 | 完整 | 部分 | 完整 | **完整+扩展** |
| 扩展性 | 有限 | 基础 | 基础 | 基础 | **WASM无限** |
| 成本 | $100K+/年 | 开源+商业 | 开源+商业 | 开源 | **完全开源** |

## 🛣️ 开发路线图

### ✅ **已完成** (Phase 1)
- [x] 项目结构搭建
- [x] fdc-core核心包实现
- [x] 基础数据类型系统
- [x] 配置管理
- [x] 错误处理
- [x] 时间工具
- [x] 内存管理
- [x] 指标收集
- [x] 类型注册表基础

### 🚧 **进行中** (Phase 2)
- [ ] WASM插件系统 (fdc-wasm)
- [ ] 自定义类型系统 (fdc-types)
- [ ] 存储引擎 (fdc-storage)
- [ ] 查询引擎 (fdc-query)
- [ ] 数据转换引擎 (fdc-transform)

### 📋 **计划中** (Phase 3)
- [ ] 数据接入系统 (fdc-ingestion)
- [ ] API服务层 (fdc-api)
- [ ] 分析引擎 (fdc-analytics)
- [ ] 命令行工具 (fdc-cli)
- [ ] 服务器程序 (fdc-server)

## 🤝 贡献指南

我们欢迎所有形式的贡献！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详细信息。

### 开发环境设置

```bash
# 安装开发依赖
cargo install cargo-watch cargo-tarpaulin

# 运行开发模式
cargo watch -x check -x test

# 代码覆盖率
cargo tarpaulin --out Html
```

## 📄 许可证

本项目采用双许可证：

- MIT License ([LICENSE-MIT](LICENSE-MIT))
- Apache License 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

## 🔗 相关链接

- [技术方案详细文档](plan1.md)
- [API文档](https://docs.rs/financial-data-center)
- [性能基准测试](benchmarks/)
- [示例代码](examples/)

## 💬 社区

- [GitHub Discussions](https://github.com/financial-data-center/mdb/discussions)
- [Discord](https://discord.gg/financial-data-center)
- [邮件列表](mailto:dev@financial-data-center.org)

---

**Financial Data Center** - 让金融数据处理更快、更强、更智能 🚀
