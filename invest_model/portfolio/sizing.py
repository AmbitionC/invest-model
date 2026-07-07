"""A 股买入股数换算（可执行口径）。

交易所约束：主板/创业板整手 100 股；科创板(688/689)单笔最低 200 股、
达到 200 股后可按 1 股递增。高价股 + 小目标权重时「一手都买不起」或
「一手远超目标」是常态——必须显式判不可执行并让调用方明示跳过，
而不是四舍五入出 0 股（计划里出现无股数的死指令）或硬塞一手
（一手占比可达权益 15%+，远超目标权重；科创板塞 100 股更是废单）。
"""

from __future__ import annotations

LOT_TOLERANCE = 1.2   # 不足最小一笔时：一手金额 ≤ 目标增量×此倍数 → 允许补足一手


def min_lot(code: str) -> int:
    """该代码的最小可买股数：科创板 200，其余 100。"""
    return 200 if str(code)[:3] in ("688", "689") else 100


def buy_shares(code: str, budget: float, px: float) -> float:
    """按目标增量金额(元)与现价计算可执行买入股数；返回 0 = 不可执行。

    主板/创业板向下取整手；科创板 ≥200 股后按 1 股递增向下取整。
    向下取整为 0 时，若最小一笔金额 ≤ budget×LOT_TOLERANCE → 补足最小一笔
    （轻度前置建仓，仍在目标带内）；否则判不可执行。
    """
    if not px or px <= 0 or not budget or budget <= 0:
        return 0.0
    lot = min_lot(code)
    sh = budget / px
    if lot == 200:
        n = float(int(sh)) if sh >= 200 else 0.0
    else:
        n = float(int(sh // 100) * 100)
    if n <= 0 and lot * px <= budget * LOT_TOLERANCE:
        n = float(lot)
    return n
