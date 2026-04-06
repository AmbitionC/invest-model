# invest-model 实现方案

**版本**: v0.2  
**创建时间**: 2026-04-02  
**状态**: 实现中

---

## 🎯 项目定位

`invest-model` 是 invest-journey 交易系统的**核心引擎**，基于 Jupyter Notebook 实现：
- **数据层**：采集、存储、管理 A 股/ETF 多源异构数据
- **信号层**：基于多因子模型生成交易信号和置信度
- **可视化**：通过 Notebook 交互式验证因子计算

---

## 📁 目录结构

```
invest-model/
├── notebooks/                 # Jupyter Notebook（核心实现）
│   ├── 00_setup.ipynb         # 环境配置
│   ├── 01_data_fetch.ipynb    # 数据采集
│   ├── 02_technical_factors.ipynb  # 技术因子
│   ├── 03_fundamental_factors.ipynb # 基本面因子（待创建）
│   ├── 04_event_factors.ipynb # 事件因子（待创建）
│   ├── 05_confidence.ipynb    # 综合置信度（待创建）
│   └── README.md              # Notebook 使用说明
│
├── data/                      # 数据层（Python 模块）
│   ├── sources/               # 数据源适配
│   ├── models/                # 数据模型
│   ├── collectors/            # 数据采集器
│   ├── storage/               # 存储层
│   │   ├── migrations/        # 数据库迁移
│   │   └── repositories/      # 数据访问层
│   └── cache/                 # 缓存层
│
├── signals/                   # 信号层（Python 模块）
│   ├── technical/             # 技术因子
│   ├── fundamental/           # 基本面因子
│   ├── event/                 # 事件因子
│   ├── normalizers/           # 标准化器
│   └── composite/             # 因子合成
│
├── config/                    # 配置文件
│   ├── config.yaml            # 主配置
│   └── config.example.yaml    # 配置示例
│
├── docs/                      # 架构文档
│   ├── 00-architecture-overview.md
│   ├── 01-data-layer-architecture.md
│   └── 02-signal-layer-architecture.md
│
├── models/                    # 现有模型目录（保留）
│   ├── deviation-rates/
│   └── finace-balance-stock-value/
│
├── tests/                     # 测试
├── requirements.txt           # Python 依赖
├── .env                       # 环境变量（Git 忽略）
├── .env.example               # 环境变量示例
└── IMPLEMENTATION.md          # 本文档
```

---

## 🚀 快速开始

### 1. 安装依赖

```bash
cd ~/Code/invest-journey/invest-model
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
# 复制示例文件
cp .env.example .env

# 编辑 .env，填入 Tushare Token
# TUSHARE_TOKEN=your_token_here
```

### 3. 启动 Jupyter

```bash
cd notebooks
jupyter notebook
```

### 4. 按顺序执行 Notebook

1. `00_setup.ipynb` - 环境配置
2. `01_data_fetch.ipynb` - 数据采集
3. `02_technical_factors.ipynb` - 技术因子
4. `03_fundamental_factors.ipynb` - 基本面因子
5. `04_event_factors.ipynb` - 事件因子
6. `05_confidence.ipynb` - 综合置信度

---

## 📊 数据流

```
Tushare API
    ↓
Notebook 01 (数据采集)
    ↓
SQLite 数据库 (原始数据)
    ↓
Notebook 02-04 (因子计算)
    ↓
SQLite 数据库 (计算指标)
    ↓
Notebook 05 (置信度)
    ↓
signals 表 (交易信号)
```

---

## 🎯 核心设计原则

### 1. Notebook 优先
- **为什么**: 边写边看，可视化验证，适合探索和调试
- **好处**: 因子计算是否正确，一眼就能看出来

### 2. 原始/计算分离
- **原始数据**: 直接从 Tushare 获取，不做计算
- **计算指标**: 基于原始数据计算，保存结果
- **好处**: 可追溯、性能好、便于回测

### 3. 因子相对化
- **所有指标**: 用分位数标准化（0-1 范围）
- **不用绝对值**: MACD>0、RSI>70 这种绝对值无意义
- **好处**: 不同股票可以对比

### 4. 版本控制
- **数据版本**: 财报修正后保留历史版本
- **计算版本**: 因子公式修改后保留旧版本
- **好处**: 回测时避免未来函数

---

## 📐 因子框架

### 技术因子（30% 权重）

| 因子 | 文件 | 核心指标 | 信号逻辑 |
|------|------|---------|---------|
| 布林带 | `02_technical_factors.ipynb` | position, bandwidth_pct | 带宽收缩 + 突破 |
| MACD | `02_technical_factors.ipynb` | dif_pct, divergence | 底背离 + 低分位 |
| RSI | `02_technical_factors.ipynb` | rsi_pct, is_strong | 低分位 + 底背离 |
| 动量 | `02_technical_factors.ipynb` | momentum_pct, accel | 高分位 + 加速 |
| 成交量 | `02_technical_factors.ipynb` | volume_ratio, volume_pct | 放量上涨 |
| 波动率 | `02_technical_factors.ipynb` | volatility_pct | 高分位减仓 |
| 偏离度 | `02_technical_factors.ipynb` | deviation_pct | 低分位超跌 |

### 基本面因子（40% 权重）

| 因子 | 文件 | 核心指标 | 信号逻辑 |
|------|------|---------|---------|
| 估值 | `03_fundamental_factors.ipynb` | pe_pct, pb_pct, peg | 低分位低估 |
| 成长 | `03_fundamental_factors.ipynb` | revenue_growth, profit_growth | 高增速 + 加速 |
| 质量 | `03_fundamental_factors.ipynb` | roe, gross_margin | 高 ROE+ 改善 |

### 事件因子（30% 权重）

| 因子 | 文件 | 核心指标 | 信号逻辑 |
|------|------|---------|---------|
| 股东减持 | `04_event_factors.ipynb` | reduce_type, reduce_ratio | 扣分制 |
| 限售解禁 | `04_event_factors.ipynb` | lockup_days, lockup_ratio | 扣分制 |

---

## 🧮 综合置信度公式

```
置信度 = 0.5 + 基本面分 × 0.4 + 技术面分 × 0.3 + 事件面分 × 0.3

基本面分 = 估值分 × 0.375 + 成长分 × 0.375 + 质量分 × 0.25
技术面分 = Σ(各技术因子信号 × 权重)
事件面分 = -Σ(事件扣分)
```

### 信号阈值

| 置信度 | 信号类型 | 操作建议 |
|--------|---------|---------|
| > 0.75 | strong_buy | 仓位 15-20% |
| 0.65-0.75 | buy | 仓位 10-15% |
| 0.55-0.65 | watch | 观望 |
| 0.35-0.45 | sell | 减仓 50% |
| < 0.35 | strong_sell | 清仓 |

---

## 📝 实现进度

| Notebook | 状态 | 完成度 | 备注 |
|---------|------|--------|------|
| 00_setup | ✅ 完成 | 100% | 环境配置 |
| 01_data_fetch | ✅ 完成 | 100% | 数据采集 + 可视化 |
| 02_technical_factors | ✅ 完成 | 100% | 技术因子 + 可视化 |
| 03_fundamental_factors | ⏳ 待创建 | 0% | 基本面因子 |
| 04_event_factors | ⏳ 待创建 | 0% | 事件因子 |
| 05_confidence | ⏳ 待创建 | 0% | 综合置信度 |

---

## 🔧 待办事项

### Phase 1: 数据层（已完成）
- [x] 创建目录结构
- [x] 创建 00_setup.ipynb
- [x] 创建 01_data_fetch.ipynb
- [x] 创建数据库表设计
- [ ] 测试 Tushare 数据采集

### Phase 2: 技术因子（已完成）
- [x] 创建 02_technical_factors.ipynb
- [x] 实现 6 个技术因子
- [x] 实现分位数标准化
- [x] 可视化验证
- [ ] 扩展到所有股票

### Phase 3: 基本面因子（待开始）
- [ ] 创建 03_fundamental_factors.ipynb
- [ ] 实现估值因子
- [ ] 实现成长因子
- [ ] 实现质量因子
- [ ] 行业标准化

### Phase 4: 事件因子（待开始）
- [ ] 创建 04_event_factors.ipynb
- [ ] 实现股东减持因子
- [ ] 实现限售解禁因子
- [ ] 扣分规则配置

### Phase 5: 综合置信度（待开始）
- [ ] 创建 05_confidence.ipynb
- [ ] 实现因子合成
- [ ] 实现信号生成
- [ ] 可视化信号分布

---

## 📖 相关文档

- [架构概览](./docs/00-architecture-overview.md)
- [数据层架构](./docs/01-data-layer-architecture.md)
- [信号层架构](./docs/02-signal-layer-architecture.md)
- [Notebook 使用说明](./notebooks/README.md)

---

**版本**: v0.2  
**最后更新**: 2026-04-02  
**下一步**: 运行 `00_setup.ipynb` 开始使用
