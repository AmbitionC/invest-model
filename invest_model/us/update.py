"""美股数据更新：观察清单日线 + VIX + 季度基本面 → us_* 表；账户快照日更。

在 GitHub Actions 上运行（yfinance 需要美国出口）。全程幂等 upsert。
"""

from __future__ import annotations

import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository
from invest_model.us import config as C
from invest_model.us import datasource as ds

logger = get_logger()


def load_watchlist(path: str) -> list[dict]:
    """config/us_watchlist.txt：`CODE,sleeve_hint` 每行一条，# 注释。"""
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            rows.append({"code": parts[0].upper(),
                         "sleeve_hint": parts[1] if len(parts) > 1 else "satellite"})
    return rows


def run_update(engine, watchlist_path: str) -> dict:
    repo = BaseRepository(engine)
    from invest_model.data import create_schema
    create_schema(engine)
    stats: dict[str, int] = {}

    watch = load_watchlist(watchlist_path)
    codes = [w["code"] for w in watch]

    # 静态信息（best-effort，逐票）
    info_rows = []
    for w in watch:
        info = ds.fetch_info(w["code"])
        info_rows.append({"code": w["code"],
                          "name": info.get("name", w["code"]),
                          "kind": info.get("kind", "stock"),
                          "sector": info.get("sector", ""),
                          "sleeve_hint": w["sleeve_hint"]})
    stats["us_stock_info"] = repo.upsert(
        "us_stock_info", pd.DataFrame(info_rows), ["code"])

    # 日线（含 VIX；已有数据则只拉近 3 个月增量，首跑拉 HISTORY_PERIOD）
    have = repo.read_sql("SELECT COUNT(*) n FROM us_stock_daily")["n"].iloc[0] \
        if repo.table_exists("us_stock_daily") else 0
    period = C.HISTORY_PERIOD if not have else "3mo"
    bars = ds.fetch_daily(codes + [C.VIX_CODE], period=period)
    if not bars.empty:
        stats["us_stock_daily"] = repo.upsert(
            "us_stock_daily", bars, ["code", "trade_date"])

    # 季度基本面（仅个股）
    n = 0
    for w in watch:
        if w["sleeve_hint"] == "core":
            continue
        q = ds.fetch_fundamentals_q(w["code"])
        if q.empty:
            continue
        q = q.sort_values("quarter_end")
        for col, out in (("revenue", "revenue_yoy"), ("net_income", "ni_yoy")):
            s = pd.to_numeric(q[col], errors="coerce")
            base = s.shift(4)
            q[out] = (s - base) / base.abs()
        n += repo.upsert("us_fundamental_q", q, ["code", "quarter_end"])
    stats["us_fundamental_q"] = n

    stats["us_account_snapshot"] = _persist_account_snapshot(repo)
    logger.info(f"us update 完成：{stats}")
    return stats


def _persist_account_snapshot(repo: BaseRepository) -> int:
    """账户快照：无快照则以 START_CASH 播种；随后每日按收盘×持仓重估补齐断档
    （与 A 股 _persist_account_snapshot_daily 同思想的简化版：补洞式）。"""
    latest = repo.read_sql(
        "SELECT MAX(trade_date) d FROM us_stock_daily WHERE code!='^VIX'")["d"].iloc[0]
    if latest is None:
        return 0
    seed = repo.read_sql("SELECT COUNT(*) n FROM us_account_snapshot")["n"].iloc[0]
    if not seed:
        repo.upsert("us_account_snapshot", pd.DataFrame([{
            "snapshot_date": str(latest), "cash": C.START_CASH,
            "market_value": 0.0, "total_asset": C.START_CASH}]), ["snapshot_date"])
        return 1
    ch = repo.read_sql("SELECT code, shares FROM us_current_holding")
    first = repo.read_sql("SELECT MIN(snapshot_date) d FROM us_account_snapshot")["d"].iloc[0]
    days = repo.read_sql(
        "SELECT DISTINCT trade_date d FROM us_stock_daily "
        "WHERE code!='^VIX' AND trade_date>=:s AND trade_date<=:e ORDER BY trade_date",
        {"s": str(first), "e": str(latest)})
    have = repo.read_sql(
        "SELECT DISTINCT snapshot_date d FROM us_account_snapshot")
    have_set = {str(x) for x in have["d"].tolist()}
    last_cash = repo.read_sql(
        "SELECT cash FROM us_account_snapshot ORDER BY snapshot_date DESC LIMIT 1")
    cash = float(last_cash["cash"].iloc[0]) if not last_cash.empty else C.START_CASH
    n = 0
    targets = [str(x) for x in days["d"].tolist() if str(x) not in have_set][-90:]
    for d in targets:
        mv = 0.0
        for _, r in ch.iterrows():
            px = repo.read_sql(
                "SELECT close FROM us_stock_daily WHERE code=:c AND trade_date<=:d "
                "ORDER BY trade_date DESC LIMIT 1", {"c": str(r["code"]), "d": d})
            if not px.empty and px["close"].iloc[0] is not None:
                mv += float(r["shares"]) * float(px["close"].iloc[0])
        n += repo.upsert("us_account_snapshot", pd.DataFrame([{
            "snapshot_date": d, "cash": round(cash, 2),
            "market_value": round(mv, 2),
            "total_asset": round(mv + cash, 2)}]), ["snapshot_date"])
    return n
