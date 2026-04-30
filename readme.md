# invest-model

A 股量化交易系统 — 数据层模块。

## 功能

- **多数据源**：Tushare（主）+ Baostock（补充），统一接口可切换
- **MySQL 存储**：12 张表覆盖行情、财务、事件、市场数据
- **自选股池**：数据库表管理，支持分组与标签
- **增量更新**：基于交易日历的智能增量采集
- **数据校验**：缺失检测、连续性验证、业务规则检查

## 快速开始

```bash
# 1. 安装依赖
pip install -e ".[dev]"

# 2. 配置环境变量
# 新建 .env，至少包含 TUSHARE_TOKEN、MySQL_*（参见 notebooks/00_setup.ipynb）
#
# Token：登录 https://tushare.pro → 右上角用户名 → 「接口TOKEN」复制到 TUSHARE_TOKEN=
#
# 若官网示例要求使用 http 镜像，已在 config/config.yaml 的 sources.tushare.http_url
# 中给出文档地址；也可在 .env 里设置 TUSHARE_HTTP_URL= 覆盖。
# 若仅需官方默认 HTTPS 接口，将 config 中 http_url 改为空字符串 "" 即可。

# 3. 按顺序运行 notebooks/
jupyter notebook notebooks/00_setup.ipynb
```

## 项目结构

```
invest_model/         核心 Python 包
├── sources/          数据源客户端（Tushare / Baostock）
├── models/           数据模型（dataclass）
├── repositories/     数据访问层（Repository 模式）
├── collectors/       数据采集器
├── validators/       数据校验
└── pipeline/         每日管线

notebooks/            Jupyter Notebook（设计文档 + 可运行代码）
config/               配置文件
```

## Notebooks

| 序号 | 文件 | 内容 |
|------|------|------|
| 00 | `00_setup.ipynb` | 环境配置与连通性验证 |
| 01 | `01_database.ipynb` | 数据库设计与表初始化 |
| 02 | `02_stock_pool.ipynb` | 股票池管理 |
| 03 | `03_daily_kline.ipynb` | 日线行情采集 |
| 04 | `04_fundamentals.ipynb` | 财务指标采集 |
| 05 | `05_etf_data.ipynb` | ETF 数据采集 |
| 06 | `06_events.ipynb` | 事件数据采集 |
| 07 | `07_market_data.ipynb` | 融资融券与指数数据 |
| 08 | `08_validation.ipynb` | 数据质量校验 |
| 09 | `09_daily_pipeline.ipynb` | 每日增量管线 |
