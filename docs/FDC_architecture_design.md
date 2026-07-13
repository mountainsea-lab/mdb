# 金融数据中心（FDC）标准数据模型与系统架构设计

## 1. 核心设计思想

金融数据中心不应该首先设计数据库，而应该首先设计：

> 统一金融市场数据模型（Canonical Market Data Model）

原因：

-   不同交易所（Binance、OKX等）数据格式不同
-   因子工厂和策略系统需要统一输入
-   存储、查询、分析模块都依赖标准数据结构

整体流程：

外部数据源\
→ fdc-ingestion\
→ fdc-market（标准数据模型）\
→ fdc-transform\
→ fdc-storage\
→ fdc-query\
→ fdc-factor / fdc-analytics / Strategy

------------------------------------------------------------------------

# 2. 推荐系统架构

                     外部数据源

     Binance     OKX      Bybit      第三方数据

                     |
                     v

              fdc-ingestion

              数据采集适配层

              - Exchange Adapter
              - WebSocket
              - REST API
              - Parser
              - Buffer

                     |

                     v

            =====================
                  fdc-market
            =====================

            Instrument
            Trade
            Bar
            OrderBook
            FundingRate
            OpenInterest
            MarketEvent

                     |

            ---------------------

            |                   |

            v                   v

     fdc-transform        fdc-storage

     数据加工层             存储层

     Tick->Bar             MDB
     TradeFlow             Arrow
     Indicator             Parquet
     Feature               RocksDB

                     |

                     v

                 fdc-query

                 查询服务

                     |

         -------------------------

         |                       |

         v                       v

     fdc-factor             fdc-analytics

     因子工厂               回测分析

                     |

                     v

                 Strategy Engine

------------------------------------------------------------------------

# 3. 第一阶段：fdc-market 标准数据模型

新增模块：

    fdc-market

负责金融领域对象定义。

## Instrument

所有市场数据的根对象。

``` rust
pub struct Instrument {

    pub id: u64,

    pub exchange: Exchange,

    pub symbol: String,

    pub market_type: MarketType,

    pub price_scale: u8,

    pub quantity_scale: u8,
}
```

设计原则：

不要在高频数据中重复保存：

-   exchange string
-   symbol string

使用：

    instrument_id
            |
            |
     Instrument Registry
            |
     BTC-USDT Binance

减少内存占用，提高查询性能。

------------------------------------------------------------------------

# 4. fdc-ingestion 数据采集层

职责：

将所有外部数据转换成统一结构。

例如：

Binance：

    BinanceTrade

          |

          v

    fdc-market::Trade

OKX：

    OKXTrade

          |

          v

    fdc-market::Trade

系统内部无需关心交易所差异。

核心接口：

``` rust
pub trait MarketDataAdapter {

    async fn subscribe_trade();

    async fn subscribe_orderbook();

}
```

------------------------------------------------------------------------

# 5. fdc-transform 数据转换层

职责：

市场原始数据加工。

## Tick生成K线

    Trade

     |

     v

    1m Bar

## 订单流计算

输入：

    Trade
    OrderBook

输出：

    TradeFlow

    buy_volume
    sell_volume
    delta
    CVD

## 指标计算

    Bar

     |

    RSI
    MACD
    ATR
    ADX

------------------------------------------------------------------------

# 6. fdc-storage 存储层

存储层只关心标准数据。

不关心：

-   Binance
-   OKX

只保存：

    Trade
    Bar
    OrderBook
    Factor
    Feature

## L1 超热数据

    RingBuffer
    VecDeque

用于：

-   实盘策略
-   高频查询

## L2 热数据

    Arrow
    redb

用于：

-   快速查询
-   零拷贝访问

## L3 温数据

    Parquet
    DuckDB

用于：

-   回测
-   分析

## L4 冷数据

    RocksDB

用于：

-   历史归档

------------------------------------------------------------------------

# 7. fdc-query 查询层

提供统一查询接口。

例如：

获取BTC最近500根K线：

    instrument=BTCUSDT

    range=500min

返回：

    Vec<Bar>

支持：

-   SQL
-   API
-   策略查询

------------------------------------------------------------------------

# 8. Factor Factory

建议独立：

    fdc-factor

负责：

-   因子定义
-   因子计算
-   因子存储

接口：

``` rust
trait Factor {

    fn compute(
        &self,
        data:&MarketData
    )->Feature;

}
```

支持：

-   Momentum
-   Volatility
-   Liquidity
-   OrderFlow
-   MarketRegime

------------------------------------------------------------------------

# 9. 推荐最终Workspace

    fdc-core

    fdc-market

    fdc-types

    fdc-ingestion

    fdc-transform

    fdc-storage

    fdc-query

    fdc-factor

    fdc-feature

    fdc-analytics

    fdc-wasm

    fdc-api

    fdc-server

    fdc-cli

------------------------------------------------------------------------

# 10. 开发优先级

## Phase 1

基础：

    fdc-core
    fdc-common

## Phase 2

核心数据：

    1. fdc-market

    2. fdc-ingestion

    3. fdc-storage

    4. fdc-transform

    5. fdc-query

## Phase 3

量化能力：

    fdc-factor

    fdc-feature

    fdc-analytics

## Phase 4

扩展：

    fdc-wasm

用于：

-   自定义因子
-   自定义转换
-   自定义查询函数

------------------------------------------------------------------------

# 11. 最终定位

FDC 不应该只是数据库。

它应该成为：

> Rust 实现的量化金融数据基础设施

类似：

-   kdb+ 的时序能力
-   QuestDB 的SQL能力
-   Arrow/DataFusion的数据处理能力
-   Factor Factory的研究能力

核心建设顺序：

    数据标准
        ↓
    数据采集
        ↓
    数据存储
        ↓
    数据计算
        ↓
    因子
        ↓
    策略

其中：

**fdc-market 是整个系统最重要的基础。**
