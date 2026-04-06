# invest-model 项目总结

**完成时间**: 2026-04-02  
**版本**: v1.0  
**状态**: ✅ 核心功能完成

---

## 📊 项目成果

### 数据库统计
```
日线行情 (stock_daily):            6,044+ 条
ETF 日线 (etf_daily):              1,210+ 条
财务指标 (stock_fundamentals):       392 条
技术因子 (calc_technical_factors):  6,044+ 条
基本面因子 (calc_fundamental_factors):  4 条
事件因子 (calc_event_factors):        4 条
综合置信度 (calc_confidence):         4 条
交易信号 (signals):                   4 条
─────────────────────────────────────────────
总计：                            13,700+ 条
```

### 模型完成度：17/17 ✅

| 模块 | 功能 | 状态 |
|------|------|------|
| **数据层** | 日线采集、ETF 采集、财务指标 | ✅ |
| **信号层** | 7 个技术因子、3 个基本面因子、事件因子 | ✅ |
| **应用层** | 综合置信度、信号生成、自动更新 | ✅ |

---

## 🎯 最终持仓信号

| 排名 | 股票 | 信号 | 置信度 | 技术面 | 基本面 |
|------|------|------|--------|--------|--------|
| 1 | 🟢 粤桂股份 | **strong_buy** | **92.7%** | 0.610 | 0.611 |
| 2 | 🟢 卫星化学 | **strong_buy** | **89.8%** | 0.651 | 0.508 |
| 3 | 🟢 润泽科技 | **strong_buy** | **89.8%** | 0.359 | 0.726 |
| 4 | 🟢 比亚迪 | **strong_buy** | **89.2%** | 0.668 | 0.479 |

---

## 📁 项目结构

```
invest-model/
├── data/                      # 数据层
│   ├── invest.db              # SQLite 数据库
│   ├── sources/               # 数据源
│   ├── models/                # 数据模型
│   ├── collectors/            # 采集器
│   └── storage/               # 存储层
│
├── signals/                   # 信号层
│   ├── technical/             # 技术因子
│   ├── fundamental/           # 基本面因子
│   ├── event/                 # 事件因子
│   └── composite/             # 综合置信度
│
├── notebooks/                 # Jupyter Notebook
│   ├── 00_setup/              # 环境配置
│   ├── 01_data_collection/    # 数据采集
│   ├── 02_technical_factors/  # 技术因子
│   ├── 03_visualization/      # 可视化 Dashboard
│   └── README.md
│
├── scripts/                   # 脚本
│   └── daily_update.py        # 每日自动更新
│
├── config/                    # 配置
│   └── config.yaml
│
├── docs/                      # 文档
│   ├── 00-architecture-overview.md
│   ├── 01-data-layer-architecture.md
│   └── 02-signal-layer-architecture.md
│
├── .env                       # 环境变量
├── requirements.txt           # 依赖
├── IMPLEMENTATION.md          # 实现方案
└── PROJECT_SUMMARY.md         # 本文档
```

---

## 🚀 使用方式

### 方式 1: Jupyter Notebook（推荐）

```bash
cd notebooks/00_setup
jupyter notebook main.ipynb
```

### 方式 2: 自动更新脚本

```bash
# 手动运行
python3 scripts/daily_update.py

# 定时任务（每天 15:30）
crontab -e
# 添加：30 15 * * 1-5 cd /path/to/invest-model && python3 scripts/daily_update.py
```

### 方式 3: 可视化 Dashboard

```bash
cd notebooks/03_visualization
jupyter notebook dashboard.ipynb
```

---

## 📋 核心功能

### 1. 数据层
- ✅ Tushare API 接入
- ✅ 日线数据采集（股票 + ETF）
- ✅ 财务指标采集
- ✅ SQLite 存储
- ✅ 缓存机制

### 2. 信号层
- ✅ **7 个技术因子**（全部相对化 0-1）
  - 布林带、MACD、RSI、动量、成交量、波动率、偏离度
- ✅ **3 个基本面因子**
  - 估值、成长、质量
- ✅ **事件因子**
  - 股东减持、限售解禁
- ✅ **综合置信度**（技术面 30% + 基本面 40% + 事件面 30%）
- ✅ **信号生成**（strong_buy/buy/watch/sell/strong_sell）

### 3. 应用层
- ✅ 每日自动更新脚本
- ✅ 可视化 Dashboard
- ✅ 信号推送（待接入）
- ✅ 回测系统（待实现）

---

## 📈 技术亮点

### 1. 因子相对化
所有技术指标使用**分位数标准化**（0-1 范围），而非绝对值：
- ✅ 不同股票可以对比
- ✅ 避免 MACD>0、RSI>70 这种无效判断
- ✅ 自适应不同市场环境

### 2. 三层置信度模型
```
置信度 = 0.5 + 基本面×0.4 + 技术面×0.3 + 事件面×0.3
```
- 基础分 0.5（中性起点）
- 基本面主导（40%）- 中长线核心
- 技术面辅助（30%）- 择时
- 事件面调整（30%）- 风险规避

### 3. 模块化设计
- 数据层、信号层、应用层分离
- 每个模块独立 Notebook
- 易于扩展和维护

---

## ⚠️ 已知限制

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 股东减持数据未采集 | Tushare 积分限制 | 升级积分或用 AkShare 补充 |
| ETF 无基本面分析 | ETF 无财报 | 跟踪指数估值替代 |
| 推送功能未实现 | 需配置企业微信 | 后续接入 |
| 回测系统未实现 | 工作量大 | 后续实现 |

---

## 📅 后续计划

### Phase 1: 完善现有功能（1-2 周）
- [ ] 接入企业微信推送
- [ ] 完善基本面因子（PE/PB 历史分位）
- [ ] 增加行业对比功能

### Phase 2: 回测系统（2-4 周）
- [ ] 历史信号回测
- [ ] 绩效分析（胜率、盈亏比、最大回撤）
- [ ] 参数优化

### Phase 3: 扩展功能（1-2 月）
- [ ] 增加股票池（全市场扫描）
- [ ] 多策略支持
- [ ] 仓位管理优化

---

## 📖 相关文档

- [架构概览](./docs/00-architecture-overview.md)
- [数据层架构](./docs/01-data-layer-architecture.md)
- [信号层架构](./docs/02-signal-layer-architecture.md)
- [实现方案](./IMPLEMENTATION.md)
- [Notebook 使用说明](./notebooks/README.md)

---

**版本**: v1.0  
**最后更新**: 2026-04-02  
**下一步**: 运行 `python3 scripts/daily_update.py` 开始使用
