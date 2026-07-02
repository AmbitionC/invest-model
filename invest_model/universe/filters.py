"""投资域过滤器：纯函数，作用于截面 DataFrame。

输入截面约定列：code, name, list_date, trade_date, amount_20d, circ_mv, volume。
过滤项：ST、次新（上市不足 N 个自然日）、当日停牌（无成交）、流动性/市值。
流动性与市值默认用**截面百分位**过滤（尺度无关，真实/合成数据通用），
另支持可选的绝对下限（生产可在 config 配置）。
"""

from __future__ import annotations

import pandas as pd


def exclude_st(df: pd.DataFrame) -> pd.DataFrame:
    """剔除 ST/*ST。

    已知局限：name 来自 stock_info 当前快照，非 point-in-time——历史上曾 ST
    现已摘帽的票会被历史截面误放行，反之被追溯误杀。严格修复需引入
    namechange 历史名称表；退市股（stock_basic list_status=D）保留的是
    退市时名称，多数带 *ST，能被本过滤器正确识别。
    """
    if "name" not in df.columns:
        return df
    mask = ~df["name"].fillna("").str.contains("ST", case=False)
    return df[mask]


def exclude_new_listings(df: pd.DataFrame, trade_date: str, min_calendar_days: int = 365) -> pd.DataFrame:
    """剔除上市不足 N 个自然日的次新股。

    注意防幸存者偏差：list_date 缺失（多为已退市、不在当前 stock_basic 列表中的票）
    **不应**被当作次新剔除，否则会把历史上存在、后来退市的股票从回测中抹掉，
    系统性地高估历史收益。这里仅剔除「明确知道上市日且不足 1 年」的票。
    """
    if "list_date" not in df.columns:
        return df
    cutoff = (pd.to_datetime(trade_date) - pd.Timedelta(days=min_calendar_days)).strftime("%Y%m%d")
    ld = df["list_date"]
    too_new = ld.notna() & (ld > cutoff)
    return df[~too_new]


def exclude_suspended(df: pd.DataFrame) -> pd.DataFrame:
    if "volume" not in df.columns:
        return df
    return df[pd.to_numeric(df["volume"], errors="coerce").fillna(0) > 0]


def liquidity_filter(
    df: pd.DataFrame,
    liquidity_pct: float = 0.20,
    size_pct: float = 0.10,
    min_amount: float = 0.0,
    min_circ_mv: float = 0.0,
) -> pd.DataFrame:
    """剔除流动性最差的 ``liquidity_pct`` 与市值最小的 ``size_pct``；
    再施加可选绝对下限（min_amount 千元、min_circ_mv 万元）。"""
    out = df.copy()
    amt = pd.to_numeric(out.get("amount_20d"), errors="coerce")
    mv = pd.to_numeric(out.get("circ_mv"), errors="coerce")
    if amt is not None and amt.notna().any():
        out = out[amt >= amt.quantile(liquidity_pct)]
    if mv is not None and mv.notna().any():
        mv = pd.to_numeric(out["circ_mv"], errors="coerce")
        out = out[mv >= mv.quantile(size_pct)]
    if min_amount > 0:
        out = out[pd.to_numeric(out["amount_20d"], errors="coerce").fillna(0) >= min_amount]
    if min_circ_mv > 0:
        out = out[pd.to_numeric(out["circ_mv"], errors="coerce").fillna(0) >= min_circ_mv]
    return out
