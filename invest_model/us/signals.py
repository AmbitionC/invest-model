"""美股市场信号：趋势闸 / VIX 恐慌分档 / 回撤 / 抄底观察窗（纯函数，可离线测试）。

规则溯源：US-C1（趋势纪律）、US-T1（恐慌择时）——docs/us_rulebook.md。
"""

from __future__ import annotations

import pandas as pd

from invest_model.us import config as C


def ma_trend(closes: pd.Series, window: int | None = None) -> str:
    """收盘价序列 → above/below（最新价 vs N 日均线）。数据不足按 above（不惩罚新标的）。"""
    window = window or C.MA_TREND
    s = pd.to_numeric(closes, errors="coerce").dropna()
    if len(s) < window:
        return "above"
    return "above" if float(s.iloc[-1]) >= float(s.tail(window).mean()) else "below"


def vix_regime(vix: float | None) -> str:
    """VIX 分档：calm(<20) / alert(20-30) / panic(>30)。缺数据按 alert（保守）。"""
    if vix is None:
        return "alert"
    if vix >= C.VIX_PANIC:
        return "panic"
    if vix >= C.VIX_ALERT:
        return "alert"
    return "calm"


def drawdown_from_high(closes: pd.Series, lookback: int = 252) -> float:
    """最新价距过去一年高点的回撤（正数，0.12=回撤12%）。"""
    s = pd.to_numeric(closes, errors="coerce").dropna().tail(lookback)
    if s.empty:
        return 0.0
    return max(0.0, 1.0 - float(s.iloc[-1]) / float(s.max()))


def dip_window(vix: float | None, dd: float) -> bool:
    """恐慌抄底观察窗（下跌二分法 US 版的"估值/情绪驱动"分支）：
    VIX 恐慌档 且 基准回撤足够深 → 现金弹药进入观察状态。
    个股是否可抄还要过基本面闸（增速探针无失速红旗）——业绩驱动的跌不接。"""
    return vix_regime(vix) == "panic" and dd >= C.PANIC_DRAWDOWN


def core_target_ratio(trend: str) -> float:
    """核心仓目标比例：趋势线上=满配该 sleeve；线下=保留 CORE_BELOW_TREND。"""
    return 1.0 if trend == "above" else C.CORE_BELOW_TREND


def selling_puts_allowed(vix: float | None, trend: str) -> bool:
    """新卖 put 闸（US-O4，对手盘思维）：VIX>panic 或基准破 200 日线 → 暂停新卖。
    恐慌时权利金最诱人、被行权风险也最大——先问对手为什么高价买保险。"""
    return vix_regime(vix) != "panic" and trend == "above"
