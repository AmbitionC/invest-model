"""美股市场信号：趋势闸 / VIX 恐慌分档 / 回撤 / 抄底观察窗（纯函数，可离线测试）。

规则溯源：US-C1（趋势纪律）、US-T1（恐慌择时）——docs/us_rulebook.md。
"""

from __future__ import annotations

import datetime as _dt

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


def _third_friday(year: int, month: int) -> _dt.date:
    """标准月度期权到期日 = 当月第三个周五。"""
    d = _dt.date(year, month, 1)
    first_friday = 1 + (4 - d.weekday()) % 7   # weekday: Mon=0..Fri=4
    return _dt.date(year, month, first_friday + 14)


def _opex_trading_days(dates_sorted: list[str]) -> set[str]:
    """在给定交易日历里，每个月映射到"标准 OpEx（第三个周五）当天或之前最近的交易日"。

    第三个周五若恰逢假日休市，取该月内 ≤ 第三个周五的最后一个交易日（贴近真实到期结算）。
    """
    out: set[str] = set()
    for ym in sorted({d[:6] for d in dates_sorted}):
        tf = _third_friday(int(ym[:4]), int(ym[4:6])).strftime("%Y%m%d")
        cand = [d for d in dates_sorted if d[:6] == ym and d <= tf]
        if cand:
            out.add(cand[-1])
    return out


def _near_opex_mask(dates_sorted: list[str], days_to_expiry_max: int) -> list[bool]:
    """标记"距月度 OpEx ≤days_to_expiry_max 交易日（含到期日当天）"的日期。"""
    n = len(dates_sorted)
    near = [False] * n
    pos = {d: i for i, d in enumerate(dates_sorted)}
    for od in _opex_trading_days(dates_sorted):
        oi = pos[od]
        for j in range(max(0, oi - days_to_expiry_max), oi + 1):
            near[j] = True
    return near


def label_squeeze_windows(bench_close: pd.Series, vix_close: pd.Series, *,
                          days_to_expiry_max: int | None = None,
                          vix_spike_pct: float | None = None,
                          vix_abs: float | None = None,
                          ma_window: int | None = None) -> pd.Series:
    """负 Gamma×到期 挤压窗口（US-O5 / P17 核心逻辑，纯函数，供 E13 全历史验证 + 生产同用）。

    窗口日 = 距月度 OpEx ≤days_to_expiry_max 交易日 **且**（VIX 单日涨幅 ≥vix_spike_pct
    或 VIX ≥vix_abs）**且** 基准收于 ma_window 日线下方。

    入参 bench_close / vix_close：index 为 YYYYMMDD 字符串的收盘序列。返回同 index 的 bool 序列。
    """
    days_to_expiry_max = C.GAMMA_DTE_MAX if days_to_expiry_max is None else days_to_expiry_max
    vix_spike_pct = C.GAMMA_VIX_SPIKE if vix_spike_pct is None else vix_spike_pct
    vix_abs = C.GAMMA_VIX_ABS if vix_abs is None else vix_abs
    ma_window = C.GAMMA_MA if ma_window is None else ma_window

    bench = pd.to_numeric(bench_close, errors="coerce").dropna()
    vix = pd.to_numeric(vix_close, errors="coerce").dropna()
    idx = bench.index.intersection(vix.index)
    idx = sorted(str(x) for x in idx)
    if not idx:
        return pd.Series(dtype=bool)
    bench = bench.reindex(idx)
    vix = vix.reindex(idx)

    ma = bench.rolling(ma_window).mean()
    below = bench < ma
    vix_chg = vix.pct_change()
    vix_spike = (vix_chg >= vix_spike_pct) | (vix >= vix_abs)
    near = pd.Series(_near_opex_mask(idx, days_to_expiry_max), index=idx)

    window = near & vix_spike.fillna(False) & below.fillna(False)
    return window.astype(bool)


def gamma_squeeze_now(bench_close: pd.Series, vix_close: pd.Series, **kw) -> bool:
    """最新交易日是否处于负 Gamma×到期 挤压窗口（生产侧单点判定）。"""
    w = label_squeeze_windows(bench_close, vix_close, **kw)
    return bool(w.iloc[-1]) if len(w) else False


def selling_puts_mode(vix: float | None, trend: str) -> str:
    """新卖 put 模式（US-O4 V2）：normal / strict。

    V1 恐慌停卖（重远对手盘思维）；V2 采信全哥（期权实战者）——"市场情绪化最重、
    分歧最大时=期权最好的时候"（IV 最高权利金最厚 + 人弃我取）。但叠加守本金收紧：
    恐慌/破线时切 strict——只允许 cheap 档标的 + 行权价 ≤ 估值锚×0.9（恐慌吃厚
    权利金的前提是接货价便宜到心甘情愿）。两位博主的调和：重远警告的是"贪权利金
    卖在危险价位"，估值锚恰好排除了这种卖法。"""
    if vix_regime(vix) == "panic" or trend == "below":
        return "strict"
    return "normal"
