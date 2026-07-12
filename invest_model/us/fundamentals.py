"""卫星仓基本面：增速二阶导（防戴维斯双杀）+ 排雷探针 US 版 + 确定性分级（纯函数）。

规则溯源：US-F1（二阶导）、US-F2（排雷探针）、US-F3（分级映射）——docs/us_rulebook.md。
验证依据：life-teachers verification/growth-deceleration-davis-killer（PE 端跌 78% 的数学）。
"""

from __future__ import annotations

import pandas as pd

from invest_model.us import config as C


def yoy_series(q: pd.DataFrame, col: str) -> pd.Series:
    """季度长表（quarter_end 升序）→ 同比序列（对齐 4 季前）。"""
    s = pd.to_numeric(q.sort_values("quarter_end")[col], errors="coerce")
    base = s.shift(4)
    return (s - base) / base.abs()


def growth_accel(q: pd.DataFrame, col: str = "net_income") -> float | None:
    """增速二阶导：最近一季同比 - 上一季同比（一阶差分，单位 pp/100）。
    数据不足（<6 季）返回 None——探针不装数据。"""
    if q is None or len(q) < 6 or col not in q.columns:
        return None
    yoy = yoy_series(q, col).dropna()
    if len(yoy) < 2:
        return None
    return float(yoy.iloc[-1] - yoy.iloc[-2])


def mine_probes(q: pd.DataFrame) -> list[str]:
    """排雷探针（触发=深挖信号，非定罪；宁错杀口径只用于组合决策）。
    ① FCF/净利背离连续两季；② 净债务连升且为正；③ 毛利率连续两季下滑。"""
    flags: list[str] = []
    if q is None or q.empty:
        return flags
    q = q.sort_values("quarter_end")
    ni = pd.to_numeric(q.get("net_income"), errors="coerce")
    fcf = pd.to_numeric(q.get("fcf"), errors="coerce")
    if ni is not None and fcf is not None and len(q) >= 2:
        recent = [(f, n) for f, n in zip(fcf.tail(2), ni.tail(2))
                  if pd.notna(f) and pd.notna(n) and n > 0]
        if len(recent) == 2 and all(f < n * C.PROBE_FCF_NI for f, n in recent):
            flags.append("FCF与净利背离(连续两季FCF<50%净利)")
    nd = pd.to_numeric(q.get("net_debt"), errors="coerce").dropna()
    if len(nd) >= 3 and nd.iloc[-1] > 0 and nd.iloc[-1] > nd.iloc[-2] > nd.iloc[-3]:
        flags.append("净债务连续两季上升")
    gm = pd.to_numeric(q.get("gross_margin"), errors="coerce").dropna()
    if len(gm) >= 3 and gm.iloc[-1] < gm.iloc[-2] < gm.iloc[-3]:
        flags.append("毛利率连续两季下滑")
    return flags


def certainty_grade(accel: float | None, flags: list[str],
                    ni_yoy: float | None) -> tuple[str, str]:
    """确定性分级（A/B/C）+ 理由。宁错杀：任一红旗直接 C。
    A=增速为正且加速、零红旗；B=增速为正、无失速、零红旗；C=其余（仅追踪）。"""
    if flags:
        return "C", f"排雷探针触发：{'；'.join(flags)}〔规则US-F2〕"
    if accel is not None and accel <= C.ACCEL_WARN:
        return "C", (f"增速失速预警：同比一阶差分 {accel:+.0%}（戴维斯双杀风险，"
                     f"PE端主导下跌的数学见验证库）〔规则US-F1〕")
    if ni_yoy is None:
        return "C", "基本面数据不足，降级仅追踪（数据为王：不装数据）〔规则US-F3〕"
    if ni_yoy <= 0:
        return "C", f"净利同比 {ni_yoy:+.0%} 为负，仅追踪〔规则US-F3〕"
    if accel is None:
        # 同比为正、零红旗，但 yfinance 季报深度不足以算加速度——B 是"基本确信"档，
        # 加速度是 A 档的额外要求；深度不足只挡 A 不挡 B（不装数据、也不无谓错杀）。
        return "B", (f"净利同比 {ni_yoy:+.0%}、零红旗（历史深度不足以判加速度，"
                     f"仅挡A不挡B）〔规则US-F3〕")
    if accel > 0:
        return "A", f"净利同比 {ni_yoy:+.0%} 且加速 {accel:+.0%}，零红旗〔规则US-F3〕"
    return "B", f"净利同比 {ni_yoy:+.0%}、未失速，零红旗〔规则US-F3〕"
