"""估值锚引擎（V2 新增，全哥体系核心）：回本周期 → 分档 → 心甘情愿接盘价。

方法论出处（life-teachers 全哥）：
- 买股票=买公司=算回本周期（《买股票就是买公司》，〔验证〕茅台4%→25年等算术成立）
- 第一前提永远是"便宜"；追高=亏钱根本（《普通人股票赚钱方法》）
- Sell Put 基于估值找"心甘情愿接盘的价格"（《期权课程》）
规则编号：US-V1（回本分档）/ US-V2（接盘价锚）/ US-V3（追高禁令）——docs/us_rulebook.md。
"""

from __future__ import annotations

import pandas as pd

from invest_model.us import config as C


def payback_years(market_cap: float | None, net_cash: float | None,
                  fcf: float | None, net_income: float | None) -> float | None:
    """去现金回本周期 = (市值 − 净现金) / 年可分配利润（保守取 min(FCF, 净利) 的正值）。

    全哥口径：投100万开饭馆每年分几万、几年回本；股票同理。现金减掉是因为
    那部分买来就是你的（拼多多去现金3倍PE例）。利润取保守口径防"假利润"。
    """
    if not market_cap or market_cap <= 0:
        return None
    ev = market_cap - (net_cash or 0.0)
    if ev <= 0:
        return 0.0                       # 市值低于净现金：白送，回本即刻
    earns = [x for x in (fcf, net_income) if x is not None]
    if not earns:
        return None
    e = min(earns)                       # 保守：FCF 与净利取小
    if e <= 0:
        return float("inf")              # 不赚真钱：永不回本
    return ev / e


def verdict(pb_years: float | None) -> str:
    """估值分档：cheap（≤15年）/ fair（15-25年）/ expensive（>25年或不赚真钱）/ unknown。

    15 年≈6.7% 收益率对应全哥"饭馆10年回本很好、茅台25年回本靠成长补"的中间带；
    〔分析〕阈值是 Claude 定的工程参数（可 env 覆盖），全哥只给了算法与案例。
    """
    if pb_years is None:
        return "unknown"
    if pb_years <= C.VAL_CHEAP_YEARS:
        return "cheap"
    if pb_years <= C.VAL_FAIR_YEARS:
        return "fair"
    return "expensive"


def anchor_price(close: float | None, pb_years: float | None) -> float | None:
    """心甘情愿接盘价：使回本周期回到 cheap 阈值的价格（市值∝价格的线性近似）。

    回本 30 年的公司，接盘价 = 现价 × 15/30 = 打对折；回本 12 年的公司，
    锚在现价上方 → 取现价（已经便宜，不需要更深折扣）。
    """
    if not close or close <= 0 or pb_years is None:
        return None
    if pb_years == float("inf"):
        return None                      # 不赚真钱的公司没有"心甘情愿"价
    if pb_years <= 0:
        return close
    return round(min(close, close * C.VAL_CHEAP_YEARS / pb_years), 2)


def chase_high_flag(closes: pd.Series, vd: str) -> bool:
    """追高禁令（US-V3）：距一年低点涨幅 > 阈值 且 估值非 cheap → 禁 buy。

    "套牢普通人的不是低谷，是买贵了"——涨了一大截又不便宜的，宁可错过。
    cheap 档豁免：极便宜的反弹不算追高（周期股底部翻倍仍可能 PE≈10）。
    """
    if vd == "cheap":
        return False
    s = pd.to_numeric(closes, errors="coerce").dropna().tail(252)
    if len(s) < 60:
        return False
    lo = float(s.min())
    if lo <= 0:
        return False
    return (float(s.iloc[-1]) / lo - 1) > C.VAL_CHASE_RALLY
