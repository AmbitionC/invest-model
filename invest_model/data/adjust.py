"""股票收盘序列的运行时前复权（P11）。

背景：`stock_daily` 存的是 **未复权** 原始价（tushare `pro.daily`）。除权除息日
价格机械跳空——分红 1~3% 的缺口足以在止损线附近假触发「硬止损 / 破MA20」，
送转（如 10 转 10 → 价格减半）直接假崩清仓；均线与动量同被污染。ETF 入库时
已做前复权（`ingest_etf_daily`：fund_daily × fund_adj），股票此前漏了同款处理。

方案：读取时用 `stock_adj`（tushare adj_factor，`update.run_data_update` 每日拉取）
前复权：``close_qfq = close × adj_factor / 窗口内最新 adj_factor``——最新价 == 原始价，
可直接与券商现价/摊薄成本比较，历史按比例折算、除权缺口被抹平。

Fail-open：`stock_adj` 缺表 / 该票无因子（含 ETF——其行情本身已复权、不在
adj_factor 覆盖内，天然不会二次复权）/ 因子异常 → 原样返回未复权序列，
行为与修复前逐字一致。窗口起点早于库内首个因子日时用首个因子回填（bfill）——
等价于假设窗口开头到首因子日之间无除权，与不复权同风险、不会更差。
"""

from __future__ import annotations

import pandas as pd


def qfq_close_hist(repo, code: str, start: str, dt: str) -> pd.Series:
    """读取 [start, dt] 收盘序列（index=trade_date 升序），并按 stock_adj 前复权。"""
    df = repo.read_sql(
        "SELECT trade_date, close FROM stock_daily "
        "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
        {"c": code, "s": start, "d": dt},
    )
    if df.empty:
        return pd.Series(dtype=float)
    s = pd.to_numeric(df.set_index("trade_date")["close"], errors="coerce")
    return apply_qfq(repo, code, s, start, dt)


def apply_qfq(repo, code: str, s: pd.Series, start: str, dt: str) -> pd.Series:
    """对已取好的收盘序列套用前复权因子；任何异常 fail-open 返回原序列。"""
    if s.empty:
        return s
    try:
        if not repo.table_exists("stock_adj"):
            return s
        adj = repo.read_sql(
            "SELECT trade_date, adj_factor FROM stock_adj "
            "WHERE code=:c AND trade_date>=:s AND trade_date<=:d ORDER BY trade_date",
            {"c": code, "s": start, "d": dt},
        )
    except Exception:  # noqa: BLE001 — 复权失败不阻断风控，退回未复权
        return s
    if adj is None or adj.empty:
        return s
    fac = pd.to_numeric(adj.set_index("trade_date")["adj_factor"], errors="coerce").dropna()
    if fac.empty:
        return s
    fac = fac[~fac.index.duplicated(keep="last")]
    f = fac.reindex(s.index).ffill().bfill()
    last = pd.to_numeric(f.iloc[-1], errors="coerce")
    if not pd.notna(last) or float(last) <= 0 or f.isna().any():
        return s
    return s * f.astype(float) / float(last)
