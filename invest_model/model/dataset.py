"""调仓日历、前瞻收益标签、因子 rank-IC 计算。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.factor_repo import FactorRepository

logger = get_logger()


def all_trade_dates(engine, start: str, end: str) -> list[str]:
    repo = BaseRepository(engine)
    df = repo.read_sql(
        "SELECT cal_date FROM trade_calendar WHERE is_open=1 AND cal_date>=:s AND cal_date<=:e ORDER BY cal_date",
        {"s": start, "e": end},
    )
    if df.empty:
        # 合成数据可能无 is_open 过滤需求；回退用 stock_daily 的 distinct 日期
        df = repo.read_sql(
            "SELECT DISTINCT trade_date AS cal_date FROM stock_daily "
            "WHERE trade_date>=:s AND trade_date<=:e ORDER BY trade_date",
            {"s": start, "e": end},
        )
    return df["cal_date"].tolist()


def rebalance_dates(engine, start: str, end: str, freq: str = "monthly") -> list[str]:
    """返回调仓日：每月（或每两周）首个交易日。"""
    tds = all_trade_dates(engine, start, end)
    if not tds:
        return []
    s = pd.Series(tds)
    dt = pd.to_datetime(s, format="%Y%m%d")
    if freq == "biweekly":
        # 每 10 个交易日取一个
        return s.iloc[::10].tolist()
    # monthly：每个 (年,月) 的首个交易日
    ym = dt.dt.strftime("%Y%m")
    first = s.groupby(ym.values).first()
    return sorted(first.tolist())


def next_rebalance_map(reb_dates: list[str]) -> dict[str, str]:
    """{调仓日 t: 下一个调仓日}，用于对齐前瞻收益与回测持有期。"""
    return {reb_dates[i]: reb_dates[i + 1] for i in range(len(reb_dates) - 1)}


def forward_returns(engine, t0: str, t1: str, codes: list[str]) -> pd.Series:
    """从 t0 收盘到 t1 收盘的区间收益，index=code。

    t1 无行情的票（区间内停牌/退市）用其 ≤t1 的最后成交价计算，而非剔除——
    否则退市前的崩跌会被排除出 IC 与训练标签，系统性高估因子有效性。
    """
    repo = BaseRepository(engine)
    df = repo.read_sql(
        "SELECT code, trade_date, close FROM stock_daily "
        "WHERE trade_date>=:a AND trade_date<=:b",
        {"a": t0, "b": t1},
    )
    if df.empty:
        return pd.Series(dtype=float)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    wide = df.pivot(index="trade_date", columns="code", values="close").sort_index()
    if t0 not in wide.index:
        return pd.Series(dtype=float)
    p0 = wide.loc[t0]
    p1 = wide.ffill().iloc[-1]          # 每票 ≤t1 的最后成交价
    ret = p1 / p0 - 1.0
    if codes:
        ret = ret.reindex(codes)
    return ret.dropna()


def compute_factor_ic(engine, reb_dates: list[str], horizon: int = 21,
                      persist: bool = True) -> pd.DataFrame:
    """对每个调仓日计算各因子的 rank-IC（因子暴露 vs 到下一调仓日的前瞻收益）。

    返回长表 trade_date, factor_name, ic, rank_ic。同时落 factor_ic_log。
    horizon 仅作记录（实际用 next-rebalance 区间收益）。
    """
    frepo = FactorRepository(engine)
    nxt = next_rebalance_map(reb_dates)
    rows = []
    for t in reb_dates:
        if t not in nxt:
            continue
        expo = frepo.get_exposures_wide(t)        # index=code, cols=factors
        if expo.empty:
            continue
        fwd = forward_returns(engine, t, nxt[t], expo.index.tolist())
        if len(fwd) < 8:
            continue
        common = expo.index.intersection(fwd.index)
        if len(common) < 8:
            continue
        fwd_r = fwd.loc[common].rank()
        for f in expo.columns:
            x = expo.loc[common, f]
            if x.notna().sum() < 8 or x.std() == 0:
                continue
            pear = np.corrcoef(x.fillna(x.mean()), fwd.loc[common])[0, 1]
            rank_ic = x.rank().corr(fwd_r)
            rows.append({"trade_date": t, "factor_name": f,
                         "horizon": horizon,
                         "ic": float(pear) if np.isfinite(pear) else None,
                         "rank_ic": float(rank_ic) if pd.notna(rank_ic) else None})
    ic_df = pd.DataFrame(rows)
    if persist and not ic_df.empty:
        frepo.save_ic_log(ic_df[["trade_date", "factor_name", "horizon", "ic", "rank_ic"]])
    return ic_df
