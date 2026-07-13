"""美股模块参数（全部可用环境变量覆盖，默认值即 $20k 账户 V1 方案）。

三层结构与铁律出处：life-teachers insights/us-stock-investing-methodology.md 第四节；
每条规则的编号溯源见 docs/us_rulebook.md。
"""

from __future__ import annotations

import os


def _f(env: str, default: float) -> float:
    try:
        return float(os.getenv(env, "") or default)
    except ValueError:
        return default


# ── 账户与三层结构（规则 US-A1）────────────────────────────────
START_CASH = _f("US_START_CASH", 20_000.0)      # 起步资金（USD）
SLEEVE_CORE = _f("US_SLEEVE_CORE", 0.50)        # 核心锚（宽基 ETF + 趋势纪律）
SLEEVE_INCOME = _f("US_SLEEVE_INCOME", 0.30)    # 造血（现金担保 put / 备兑 call）
SLEEVE_SATELLITE = _f("US_SLEEVE_SATELLITE", 0.15)  # 卫星 α（个股，确定性分级）
SLEEVE_CASH = _f("US_SLEEVE_CASH", 0.05)        # 恐慌抄底弹药

# 单一标的总敞口上限（股票市值 + put 担保）/ 总资产。小账户适配值，
# 账户 >$100k 应收紧回 0.10（规则 US-R2，偏离重远"单票5%"的声明见 rulebook）。
MAX_SINGLE_EXPOSURE = _f("US_MAX_SINGLE_EXPOSURE", 0.25)

# ── 核心仓趋势纪律（规则 US-C1）────────────────────────────────
CORE_ETF = os.getenv("US_CORE_ETF", "QQQ")      # 核心锚标的
TREND_BENCH = os.getenv("US_TREND_BENCH", "SPY")  # 趋势/回撤基准
MA_TREND = int(_f("US_MA_TREND", 200))          # 趋势线（日）
CORE_BELOW_TREND = _f("US_CORE_BELOW_TREND", 0.5)  # 破线时核心仓保留比例

# ── 恐慌择时（规则 US-T1，人性模型 US 版）────────────────────────
VIX_ALERT = _f("US_VIX_ALERT", 20.0)            # 警惕
VIX_PANIC = _f("US_VIX_PANIC", 30.0)            # 恐慌（暂停新卖 put + 抄底观察窗）
PANIC_DRAWDOWN = _f("US_PANIC_DRAWDOWN", 0.15)  # 基准距一年高点回撤阈值

# ── 期权造血（规则 US-O1~O4）───────────────────────────────────
OPT_DTE_MIN = int(_f("US_OPT_DTE_MIN", 20))     # 到期天数窗口
OPT_DTE_MAX = int(_f("US_OPT_DTE_MAX", 45))
OPT_MIN_SAFETY = _f("US_OPT_MIN_SAFETY", 0.05)  # CSP 行权价至少低于现价 5%
OPT_MIN_ANNUAL_YIELD = _f("US_OPT_MIN_ANNUAL_YIELD", 0.10)  # 年化权利金下限
OPT_MAX_ANNUAL_YIELD = _f("US_OPT_MAX_ANNUAL_YIELD", 0.60)  # 上限（高到离谱=市场在给风险定价）
OPT_MIN_OI = int(_f("US_OPT_MIN_OI", 100))      # 最小未平仓（流动性）
OPT_MAX_CANDIDATES = int(_f("US_OPT_MAX_CANDIDATES", 12))   # 每日候选条数上限

# ── 卫星仓基本面探针（规则 US-F1~F3）────────────────────────────
ACCEL_WARN = _f("US_ACCEL_WARN", -0.15)   # 单季净利同比一阶差分低于 -15pp = 失速预警
PROBE_FCF_NI = _f("US_PROBE_FCF_NI", 0.5)  # FCF < 净利 50% 连续两季 = 红旗

# ── 数据 ────────────────────────────────────────────────────
HISTORY_PERIOD = os.getenv("US_HISTORY_PERIOD", "2y")   # 首次回填长度
VIX_CODE = "^VIX"

# ── 估值锚（V2 新增，全哥体系；规则 US-V1~V3）──────────────────
VAL_CHEAP_YEARS = _f("US_VAL_CHEAP_YEARS", 15.0)   # 回本 ≤15年 = cheap（~6.7%收益率）
VAL_FAIR_YEARS = _f("US_VAL_FAIR_YEARS", 25.0)     # 15-25年 = fair；>25 或不赚真钱 = expensive
VAL_CHASE_RALLY = _f("US_VAL_CHASE_RALLY", 0.80)   # 距一年低点涨幅>80%且非cheap = 追高禁买
PROBE_CAPEX_NI = _f("US_PROBE_CAPEX_NI", 1.2)      # capex/净利>1.2 = 以战养战黑洞探针

# ── 负 Gamma×到期 挤压探针（规则 US-O5 / 提案 P17；参数与 E13 冻结判据镜像）──
# 窗口 = 距月度 OpEx（标准第三个周五）≤GAMMA_DTE_MAX 交易日 且（VIX 单日涨幅≥GAMMA_VIX_SPIKE
# 或 VIX≥GAMMA_VIX_ABS）且 基准收于 GAMMA_MA 日线下方。overlay 默认 off——
# **E13 未证出显著恶化前不接线**，卖 put 行为零改动；E13 达标+高置信直升才切 strict/observe。
GAMMA_DTE_MAX = int(_f("US_GAMMA_DTE_MAX", 2))       # 距月度 OpEx 交易日数
GAMMA_VIX_SPIKE = _f("US_GAMMA_VIX_SPIKE", 0.15)     # VIX 单日涨幅阈值
GAMMA_VIX_ABS = _f("US_GAMMA_VIX_ABS", 35.0)         # VIX 绝对高位阈值
GAMMA_MA = int(_f("US_GAMMA_MA", 10))                # 基准短均线（破位）
GAMMA_OVERLAY = os.getenv("US_GAMMA_OVERLAY", "off")  # off | observe（仅提示）| strict（收紧卖put）
