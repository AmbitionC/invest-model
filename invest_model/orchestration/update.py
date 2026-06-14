"""生产数据更新（Tushare 路径）：全市场日线 + 估值 + 财务 + 指数，增量入库。

仅在能访问 Tushare 的环境（用户本地 / 已开网络白名单）下生效；
合成/离线环境调用会因 TushareClient 初始化失败而被上层捕获并跳过。
"""

from __future__ import annotations

import pandas as pd

from invest_model.logger import get_logger
from invest_model.repositories.base import BaseRepository

logger = get_logger()

BENCHMARKS = ["000300.SH", "000905.SH", "000906.SH"]
_FINA_COLS = ["code", "report_date", "ann_date", "roe", "roa", "gross_margin",
              "revenue_yoy", "profit_yoy"]


def _missing_dates(repo: BaseRepository, table: str, all_dates: list[str]) -> list[str]:
    if not all_dates:
        return []
    have = repo.read_sql(
        f"SELECT DISTINCT trade_date FROM {table} WHERE trade_date>=:s AND trade_date<=:e",
        {"s": all_dates[0], "e": all_dates[-1]},
    )
    have_set = set(have["trade_date"].tolist()) if not have.empty else set()
    return [d for d in all_dates if d not in have_set]


def run_data_update(engine, start: str, end: str, quarters: list[str] | None = None) -> dict:
    """增量更新行情底座。返回各表写入行数统计。"""
    from invest_model.sources.tushare_client import TushareClient

    client = TushareClient()
    repo = BaseRepository(engine)
    stats: dict[str, int] = {}

    # 交易日历
    cal = client.get_trade_calendar(start, end)
    if not cal.empty:
        cal = cal.rename(columns={"cal_date": "cal_date"})
        cal["is_open"] = pd.to_numeric(cal.get("is_open"), errors="coerce").fillna(0).astype(int)
        stats["trade_calendar"] = repo.upsert("trade_calendar",
                                              cal[["cal_date", "is_open", "pretrade_date"]],
                                              ["cal_date"])
    open_dates = cal.loc[cal["is_open"] == 1, "cal_date"].tolist() if not cal.empty else []

    # 股票列表
    info = client.get_stock_list()
    if not info.empty:
        stats["stock_info"] = repo.upsert(
            "stock_info",
            info[["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]],
            ["ts_code"])

    # 全市场日线（按缺失日 bulk）
    n = 0
    for d in _missing_dates(repo, "stock_daily", open_dates):
        df = client.get_daily_bulk(d)
        if not df.empty:
            df["trade_date"] = d
            cols = ["code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "change", "pct_chg", "volume", "amount"]
            n += repo.upsert("stock_daily", df[[c for c in cols if c in df.columns]],
                             ["code", "trade_date"])
    stats["stock_daily"] = n

    # 全市场估值
    n = 0
    for d in _missing_dates(repo, "stock_fundamental", open_dates):
        df = client.get_daily_basic(d)
        if not df.empty:
            n += repo.upsert("stock_fundamental", df, ["code", "trade_date"])
    stats["stock_fundamental"] = n

    # 季度财务（VIP 批量；失败则跳过，交由调用方提示）
    if quarters:
        n = 0
        for q in quarters:
            try:
                df = client.get_fina_indicator_bulk(q)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"fina_indicator_vip({q}) 失败：{e}")
                continue
            if not df.empty:
                df = df[[c for c in _FINA_COLS if c in df.columns]].drop_duplicates(["code", "report_date"])
                n += repo.upsert("stock_fina_indicator", df, ["code", "report_date"])
        stats["stock_fina_indicator"] = n

    # 指数日线（基准）
    n = 0
    for code in BENCHMARKS:
        df = client.get_index_daily(code, start, end)
        if not df.empty:
            n += repo.upsert("index_daily", df, ["code", "trade_date"])
    stats["index_daily"] = n

    logger.info(f"数据更新完成：{stats}")
    return stats
