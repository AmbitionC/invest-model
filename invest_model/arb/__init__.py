"""套利模块（arbitrage）：文档《套利方案 v2 —— 追踪本轮红利流向》的落地。

与现有截面多因子选股系统（引擎 B / 交易）同属一个资金池的一体两面：
  - 引擎 A 防守底盘（carry）：国债逆回购 / 红利 / 可转债双低 / 恐慌弹药。
  - 三水表（信贷/财政/政策资本）资金流 overlay：倾斜引擎 B + 生成盲区 α。
  - 盲区 α：错杀/未定价环节，用「剥离股价·只看产业侧资金到没到」证伪。
统一资金账本按 A(50-60%)/B(30-40%)/α(5-15%) 分配，强制零杠杆（Σ≤100%）。

默认以观察态/影子上线（ArbConfig.enabled=False / watermeter_tilt=False），
经 docs/model_change_proposals.md 三关治理后一键切换启用。
"""

from invest_model.arb.config import ArbConfig  # noqa: F401
