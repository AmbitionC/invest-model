# invest-model Notebooks

基于 Jupyter Notebook 的交易系统实现，模块化设计，每个大模块有 `main.ipynb` 统一调用。

---

## 📁 目录结构

```
notebooks/
├── 00_setup/                    # 环境配置
│   ├── main.ipynb               # ⭐ 主入口
│   ├── 01_environment_setup.ipynb
│   ├── 02_database_init.ipynb
│   └── 03_environment_check.ipynb
│
├── 01_data_collection/          # 数据采集
│   ├── main.ipynb               # ⭐ 主入口
│   ├── 01_fetch_stock_list.ipynb
│   ├── 02_fetch_daily_data.ipynb
│   ├── 03_fetch_fundamentals.ipynb (待创建)
│   └── 04_fetch_events.ipynb (待创建)
│
├── 02_technical_factors/        # 技术因子
│   ├── main.ipynb               # ⭐ 主入口
│   ├── 01_calculate_bollinger.ipynb
│   ├── 02_calculate_macd_rsi.ipynb
│   ├── 03_calculate_other_factors.ipynb
│   └── 04_save_factors.ipynb (待创建)
│
├── 03_fundamental_factors/      # 基本面因子（待创建）
│   ├── main.ipynb
│   ├── 01_valuation_factors.ipynb
│   ├── 02_growth_factors.ipynb
│   └── 03_quality_factors.ipynb
│
├── 04_event_factors/            # 事件因子（待创建）
│   ├── main.ipynb
│   ├── 01_holder_reduce.ipynb
│   └── 02_lockup_release.ipynb
│
└── 05_confidence/               # 综合置信度（待创建）
    ├── main.ipynb
    ├── 01_calculate_confidence.ipynb
    └── 02_generate_signals.ipynb
```

---

## 🚀 快速开始

### 1. 环境配置

```bash
cd notebooks/00_setup
jupyter notebook main.ipynb
```

执行顺序：
1. `01_environment_setup.ipynb` - 配置环境变量
2. `02_database_init.ipynb` - 初始化数据库
3. `03_environment_check.ipynb` - 验证配置

### 2. 数据采集

```bash
cd notebooks/01_data_collection
jupyter notebook main.ipynb
```

执行顺序：
1. `01_fetch_stock_list.ipynb` - 获取股票列表
2. `02_fetch_daily_data.ipynb` - 采集日线数据

### 3. 技术因子计算

```bash
cd notebooks/02_technical_factors
jupyter notebook main.ipynb
```

执行顺序：
1. `01_calculate_bollinger.ipynb` - 布林带
2. `02_calculate_macd_rsi.ipynb` - MACD + RSI
3. `03_calculate_other_factors.ipynb` - 其他因子

---

## 📊 模块说明

### 00_setup - 环境配置

| 文件 | 功能 | 状态 |
|------|------|------|
| `01_environment_setup.ipynb` | 加载环境变量和配置文件 | ✅ |
| `02_database_init.ipynb` | 创建 SQLite 数据库和表 | ✅ |
| `03_environment_check.ipynb` | 验证所有配置 | ✅ |
| `main.ipynb` | 统一入口 | ✅ |

### 01_data_collection - 数据采集

| 文件 | 功能 | 状态 |
|------|------|------|
| `01_fetch_stock_list.ipynb` | 获取 A 股股票列表 | ✅ |
| `02_fetch_daily_data.ipynb` | 采集日线数据（5 年） | ✅ |
| `03_fetch_fundamentals.ipynb` | 采集财务指标 | ⏳ |
| `04_fetch_events.ipynb` | 采集事件数据 | ⏳ |
| `main.ipynb` | 统一入口 | ✅ |

### 02_technical_factors - 技术因子

| 文件 | 功能 | 状态 |
|------|------|------|
| `01_calculate_bollinger.ipynb` | 布林带因子 | ✅ |
| `02_calculate_macd_rsi.ipynb` | MACD + RSI 因子 | ✅ |
| `03_calculate_other_factors.ipynb` | 动量/成交量/波动率/偏离度 | ✅ |
| `04_save_factors.ipynb` | 保存计算结果 | ⏳ |
| `main.ipynb` | 统一入口 | ✅ |

---

## 🎯 核心设计

### 1. 模块化
- 每个大模块独立目录
- `main.ipynb` 统一入口
- 子 Notebook 功能单一

### 2. 语义化命名
- `fetch_stock_list` - 获取股票列表
- `calculate_bollinger` - 计算布林带
- `save_factors` - 保存因子

### 3. 可视化验证
- 每个计算 Notebook 都有可视化
- 因子是否正确，一眼就能看出来

### 4. 因子相对化
- 所有因子用分位数标准化（0-1）
- 不用绝对值判断（如 MACD>0、RSI>70）

---

## 📖 相关文档

- [架构概览](../docs/00-architecture-overview.md)
- [数据层架构](../docs/01-data-layer-architecture.md)
- [信号层架构](../docs/02-signal-layer-architecture.md)
- [实现方案](../IMPLEMENTATION.md)

---

**版本**: v0.3  
**最后更新**: 2026-04-02
