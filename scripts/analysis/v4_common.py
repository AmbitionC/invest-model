"""V4 独立验证路线（事件级/统计推断）——共用数据与口径工具。

只读 results/ 下 CSV，不落库、不联网、不改生产代码。
口径来源：scratchpad/verdict/SPEC.md（不得改动）。
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd

RESULTS = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "results")


def _load_index(fname: str, col: str = "close") -> pd.Series:
    df = pd.read_csv(os.path.join(RESULTS, fname))
    df["trade_date"] = df["trade_date"].astype(int)
    df = df.sort_values("trade_date").drop_duplicates("trade_date")
    return pd.Series(df[col].to_numpy(float), index=df["trade_date"].to_numpy(int))


def load_legs() -> dict:
    """四条腿：signal 用价格序列，px 用计价序列（红利腿计价用全收益）。"""
    hs300 = _load_index("index_dump_000300_SH.csv")
    kc50 = _load_index("index_dump_000688_SH.csv")
    sp = pd.read_csv(os.path.join(RESULTS, "spread_full_history.csv"))
    sp["trade_date"] = sp["trade_date"].astype(int)
    sp = sp.sort_values("trade_date").drop_duplicates("trade_date")
    cyb = pd.Series(sp["chinext"].to_numpy(float), index=sp["trade_date"].to_numpy(int))
    div_p = _load_index("index_dump_000922_CSI.csv")
    div_tr = _load_index("index_dump_H00922_CSI.csv")
    return {
        "沪深300": dict(sig=hs300, px=hs300, M=1.00, S=1.00, kind="anchor"),
        "创业板": dict(sig=cyb, px=cyb, M=0.90, S=1.10, kind="anchor"),
        "科创50": dict(sig=kc50, px=kc50, M=1.00, S=1.00, kind="ladder"),
        "红利": dict(sig=div_p, px=div_tr, M=1.00, S=1.00, kind="anchor"),
    }


def load_fear() -> pd.Series:
    df = pd.read_csv(os.path.join(RESULTS, "fear_daily_dump.csv"))
    df["trade_date"] = df["trade_date"].astype(int)
    df = df.sort_values("trade_date").drop_duplicates("trade_date")
    return pd.Series(df["score"].to_numpy(float), index=df["trade_date"].to_numpy(int))


def load_amount() -> pd.Series:
    df = pd.read_csv(os.path.join(RESULTS, "crowding_daily.csv"))
    df["trade_date"] = df["trade_date"].astype(int)
    df = df.sort_values("trade_date").drop_duplicates("trade_date")
    s = pd.Series(df["total_amt_yi"].to_numpy(float), index=df["trade_date"].to_numpy(int))
    return s.dropna()


def rolling_pct(s: pd.Series, window: int = 750, min_periods: int = 250) -> pd.Series:
    """滚动分位：仅用 [t-window+1, t] 窗口内数据（含当日），无未来函数。

    pct_t = #{x in win : x <= s_t} / len(win)。窗口不足 min_periods 时为 NaN。
    """
    v = s.to_numpy(float)
    n = len(v)
    out = np.full(n, np.nan)
    for i in range(n):
        lo = max(0, i - window + 1)
        win = v[lo:i + 1]
        if len(win) < min_periods:
            continue
        out[i] = float((win <= v[i]).sum()) / len(win)
    return pd.Series(out, index=s.index)


def to_dt(ymd) -> pd.Timestamp:
    return pd.to_datetime(str(int(ymd)), format="%Y%m%d")


def year_of(ymd) -> int:
    return int(ymd) // 10000


def ann_return(nav0: float, nav1: float, d0, d1) -> float:
    yrs = (to_dt(d1) - to_dt(d0)).days / 365.25
    return (nav1 / nav0) ** (1.0 / yrs) - 1.0


def max_drawdown(nav: np.ndarray) -> float:
    peak = np.maximum.accumulate(nav)
    return float((nav / peak - 1.0).min())


def sharpe(nav: np.ndarray, rf: float = 0.02) -> float:
    r = np.diff(np.log(nav))
    if r.std() == 0:
        return float("nan")
    ann_r = r.mean() * 250
    ann_v = r.std() * np.sqrt(250)
    return float((ann_r - rf) / ann_v)


def episodes_from_flags(dates: np.ndarray, flags: np.ndarray, gap: int = 60):
    """连续触发合并为 episode，间隔 >gap 个交易日才算新 episode。

    返回 [(start_idx, end_idx, n_trigger_days)]，idx 为在 dates 中的位置。
    """
    idx = np.where(flags)[0]
    if len(idx) == 0:
        return []
    eps = []
    start = idx[0]
    prev = idx[0]
    cnt = 1
    for i in idx[1:]:
        if i - prev > gap:
            eps.append((start, prev, cnt))
            start = i
            cnt = 0
        prev = i
        cnt += 1
    eps.append((start, prev, cnt))
    return eps


def eff_sample_size(x: np.ndarray) -> float:
    """一阶自相关修正的等效样本量 N*(1-r)/(1+r)。"""
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    n = len(x)
    if n < 3:
        return float(n)
    r = np.corrcoef(x[:-1], x[1:])[0, 1]
    r = min(max(r, -0.99), 0.99)
    return n * (1 - r) / (1 + r)
