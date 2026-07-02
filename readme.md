# invest-model

A 股**截面多因子自主闭环**投资系统。系统每次调仓自动从全 A 流动性池筛选标的、
多因子打分排名、构建近满仓约 30 只组合并轻度择时——**无需手工维护标的池**。

## 为什么重构

旧系统对 8 只手工标的各自单独回归预测 3/5/10 日收益，叠加多层风控连乘把仓位
压成 0：回测 2025–2026 仅 +6.3%（基准 +25%）、**beta 0.13、常年 96% 空仓**。
根因是范式错误（单票回归）+ 风控连乘空仓陷阱 + 手工小池子。

新系统改为业界主流的**截面多因子选股**：

- **自主 universe**：每个调仓日从全 A 动态筛选（剔 ST/次新/停牌/低流动性），不手工维护。
- **多因子打分**：价值(EP/BP/SP)、质量(ROE/ROA/毛利)、成长(营收/净利同比)、
  动量(60/120 日)、反转、低波、规模、低换手；截面去极值 + 行业市值中性化 + 标准化。
- **滚动 IC 加权合成**：用过去 N 期 rank-IC（IC_IR）加权，权重符号自动学习因子方向，
  稳健、可解释、不易过拟合。
- **组合构建**：选 top-N（默认 30），rank 加权 + 单票/行业上限。
- **轻度择时**：指数趋势/宽度/低波融合，把总仓位线性调在 [0.5, 1.0]，
  **下限 0.5、不连乘、无死区**——针对性修复旧系统空仓陷阱。
- **截面回测**：A 股成本（手续费 0.03% + 卖出印花税 0.1%、2023-08-28 起自动减半 +
  滑点 0.05%）；涨停不买/跌停不卖（阈值按板块：主板 10% / 创业板·科创板 20% /
  北交所 30%）；停牌不可交易；退市/永停持仓按最后成交价强制清算（损失落地，
  不冻结）；换仓先卖后买、买入受可用现金约束（Σ权重 ≤ 1，无隐性杠杆）；
  月频 T+1，基准沪深300。

## 数据后端可移植

同一套代码支持两种后端，由 `--db` 切换：

- **生产**：MySQL（阿里云 RDS，连接信息读 `.env`）。
- **本地/验证**：SQLite 文件库（`sqlite:///./data/local.db`）。

`invest_model/repositories/base.py` 的 upsert 与 `invest_model/data/schema.py`
（SQLAlchemy Core）对两种方言一致。

## 快速开始

```bash
pip install -e .

# A) 本地合成数据验证（无需外网/数据库）
python scripts/gen_synthetic_sample.py --db sqlite:///./data/local.db \
    --n-stocks 250 --start 20210101 --end 20260613
python scripts/run_pipeline.py --mode all --db sqlite:///./data/local.db \
    --start 20210101 --end 20260613 --top-n 30

# B) 真实数据（需可访问 Tushare，.env 配 TUSHARE_TOKEN）
python scripts/fetch_local_sample.py --db sqlite:///./data/real.db \
    --start 20210101 --end 20260613
python scripts/run_pipeline.py --mode all --db sqlite:///./data/real.db \
    --start 20210101 --end 20260613

# C) 生产（MySQL，.env 配 MYSQL_*）
python scripts/run_pipeline.py --mode update --start 20190101   # 增量更新数据
python scripts/run_pipeline.py --mode all                       # 全流程 + 回测 + 导出
```

`--mode`：`update`（拉数据）/`universe`/`factors`/`train`/`predict`/`backtest`/`all`。
结果导出至 `results/latest.json`（含最新调仓组合）。

## 合成数据验证结果（示例）

合成市场内嵌真实因子结构，跑通后典型指标（证明机制成立）：

| 指标 | 旧系统 | 新系统 |
|---|---|---|
| beta | 0.13（空仓） | ~0.83 |
| 平均仓位 | ~4% | ~83% |
| 平均持仓数 | ~4 | ~30 |
| 年化 alpha | — | 正 |

> 合成数据仅用于验证**机制正确性**（空仓陷阱消除、近满仓、约 30 只、因子 IC 有效、
> 组合单调跑赢基准），真实 alpha 需在真实数据上回测。

## 项目结构

```
invest_model/
  data/          引擎工厂 + 可移植 schema（MySQL/SQLite）
  sources/       Tushare 客户端（含 daily_bulk / fina_indicator_vip / index_weight）
  universe/      全 A 流动性投资域构建 + 过滤器
  factors/       因子库 / 加载 / 去极值中性化标准化 / 流水线
  model/         调仓日历 / 前瞻标签 / 因子 IC / IC 加权合成 / 预测
  portfolio/     组合构建 + 轻度指数择时
  backtest/      截面回测引擎 + 绩效指标
  orchestration/ 闭环编排（数据更新→…→回测→导出）+ 健康监控
  repositories/  数据访问（dialect 感知 upsert + 各表 repo）
scripts/         run_pipeline.py / gen_synthetic_sample.py / fetch_local_sample.py
tests/           端到端冒烟测试
```

## 回测口径 vs 实盘口径

两者刻意存在差异，解读回测数字时须知：

- **回测**（`--mode backtest`）默认**纯量化**：每个调仓日严格调到目标组合
  （掉出 top-N 即卖出），风控默认关闭（`RiskConfig(enabled=False)`）。
  它衡量的是**多因子选股 + 轻度择时**这一机制本身的有效性。
- **实盘操作计划**（`build_action_plan`）默认**投顾为主 + 持有惯性**：
  风控未触发就持有（不因掉出 top-N 强制换出）、投顾标的需买点触发才建议买入。
  它的组合演化与回测不同，回测指标**不能**直接背书实盘计划的预期收益。
  若需评估实盘口径，应在回测中开启风控（`LoopConfig(risk=RiskConfig(enabled=True))`）
  并理解买点闸门只存在于实盘层。

另注：ST 过滤基于 stock_info 当前名称快照（非 point-in-time），历史 ST 状态
存在少量误差；财务数据取 ann_date ≤ t 且不早于 t-540 天的最新一期（超期视为失效）。

## 测试

```bash
pytest tests/ -q
```

## 可选：截面 ML 排序模型

默认引擎是多因子 IC 加权合成（稳健、可解释）。另提供一个 XGBoost 截面排序模型作为
增强，严格 walk-forward（预测日只用历史已实现样本，无未来函数），历史不足时自动回退
到 IC 合成：

```bash
pip install "invest-model[ml]"      # 装 xgboost
python scripts/run_pipeline.py --mode all --db sqlite:///./data/local.db \
    --start 20210101 --end 20260613 --model ranker --version ranker_v1
```

## 路线图

- 资金流因子（北向/大单/融资）接入（需对应数据采集）。
- 多 universe 口径（沪深300/中证800 指数成分，已具备 `index_weight` 采集接口）。
