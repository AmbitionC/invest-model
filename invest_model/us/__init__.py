"""美股模块（与 A 股链路完全隔离）。

方法论迁移自 life-teachers `insights/us-stock-investing-methodology.md`：
跨市场六公理 + $20k 三层结构（核心锚/期权造血/卫星α）+ US 版风控铁律。
规则溯源：docs/us_rulebook.md。

隔离边界：本包不 import invest_model.factors/model/portfolio/orchestration 等
A 股链路模块；只共用 data(schema/engine) 与 repositories.base 两个底座。
运行通道：GitHub Actions（.github/workflows/us-update.yml），不进阿里云 FC。
"""
