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
              "revenue_yoy", "profit_yoy", "q_sales_yoy", "q_profit_yoy", "ocfps"]
_FINA_EXT_COLS = ["code", "report_date", "ann_date", "goodwill", "minority_int",
                  "eq_exc_min", "accounts_receiv", "revenue", "n_income",
                  "n_income_attr_p", "sell_exp", "admin_exp", "fin_exp",
                  "n_cashflow_act"]


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
    from invest_model.data import create_schema
    from invest_model.sources.tushare_client import TushareClient

    client = TushareClient()
    repo = BaseRepository(engine)
    create_schema(engine)   # 幂等补建新增表（stock_namechange / stock_hk_hold 等）
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

    # 历史名称变更（point-in-time ST 识别）：表小，每次全量刷、幂等 upsert。
    # 拉取失败不阻断主流程——universe 构建会自动回退用 stock_info 现名。
    try:
        nc = client.get_namechange()
        if not nc.empty:
            nc = nc.drop_duplicates(["ts_code", "start_date"])
            stats["stock_namechange"] = repo.upsert(
                "stock_namechange",
                nc[["ts_code", "name", "start_date", "end_date", "change_reason"]],
                ["ts_code", "start_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"namechange 拉取失败（跳过，PIT ST 过滤回退现名）：{e}")

    # 北向个股持股（hk_hold）：候选因子 nb_ratio_chg_20 数据源，按缺失日增量。
    # 港股通闭市日返回空属正常（该日会在下次运行时再试一次，成本可忽略）。
    n = 0
    try:
        for d in _missing_dates(repo, "stock_hk_hold", open_dates):
            df = client.get_hk_hold(d)
            if not df.empty:
                df["ratio"] = pd.to_numeric(df.get("ratio"), errors="coerce")
                n += repo.upsert("stock_hk_hold",
                                 df[["code", "trade_date", "vol", "ratio"]],
                                 ["code", "trade_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"hk_hold 拉取中断（已入库 {n} 行，下次续拉）：{e}")
    stats["stock_hk_hold"] = n

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

        # 三大报表扩展项（排雷/扣商誉原料；任一接口无权限即跳过该部分，影子层自然缺省）
        n = 0
        for q in quarters:
            merged: pd.DataFrame | None = None
            for fetch in (client.get_balancesheet_bulk, client.get_income_bulk,
                          client.get_cashflow_bulk):
                try:
                    part = fetch(q)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"{fetch.__name__}({q}) 失败（排雷数据降级）：{e}")
                    continue
                if part is None or part.empty:
                    continue
                part = part.drop_duplicates(["code", "report_date"])
                if merged is None:
                    merged = part
                else:
                    dup = [c for c in part.columns
                           if c in merged.columns and c not in ("code", "report_date")]
                    merged = merged.merge(part.drop(columns=dup), on=["code", "report_date"],
                                          how="outer")
            if merged is not None and not merged.empty:
                cols = [c for c in _FINA_EXT_COLS if c in merged.columns]
                n += repo.upsert("stock_fina_ext", merged[cols], ["code", "report_date"])
        stats["stock_fina_ext"] = n

    # 指数日线（基准）
    n = 0
    for code in BENCHMARKS:
        df = client.get_index_daily(code, start, end)
        if not df.empty:
            n += repo.upsert("index_daily", df, ["code", "trade_date"])
    stats["index_daily"] = n

    # 个股两融明细（信贷水表：融资余额按行业聚合）。按缺失日增量。
    n = 0
    try:
        for d in _missing_dates(repo, "stock_margin_detail", open_dates):
            df = client.get_margin_detail(d)
            if not df.empty:
                cols = ["code", "trade_date", "rzye", "rqye", "rzmre", "rzche"]
                n += repo.upsert("stock_margin_detail",
                                 df[[c for c in cols if c in df.columns]],
                                 ["code", "trade_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"margin_detail 拉取中断（已入库 {n} 行，信贷水表降级）：{e}")
    stats["stock_margin_detail"] = n

    # ── 套利模块数据（best-effort，权限缺失即跳过，对应 sleeve 后续降级为现金）──

    # 国债逆回购日行情（defense_A carry）
    n = 0
    try:
        for d in _missing_dates(repo, "reverse_repo_daily", open_dates):
            df = client.get_reverse_repo_daily(d)
            if not df.empty:
                df["interest_days"] = _repo_interest_days(df["code"], d, cal)
                cols = ["code", "trade_date", "rate", "close", "pre_close",
                        "amount", "interest_days"]
                n += repo.upsert("reverse_repo_daily",
                                 df[[c for c in cols if c in df.columns]],
                                 ["code", "trade_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"reverse_repo_daily 拉取失败（跳过，逆回购 sleeve 降级）：{e}")
    stats["reverse_repo_daily"] = n

    # 可转债基础信息（静态，全量刷）
    try:
        cb = client.get_cb_basic()
        if not cb.empty:
            cb = cb.drop_duplicates(["ts_code"])
            cols = ["ts_code", "bond_short_name", "stk_code", "list_date",
                    "delist_date", "conv_price", "maturity_date", "remain_size",
                    "call_status"]
            stats["cb_basic"] = repo.upsert("cb_basic",
                                            cb[[c for c in cols if c in cb.columns]],
                                            ["ts_code"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"cb_basic 拉取失败（跳过，可转债 sleeve 降级）：{e}")

    # 可转债日行情
    n = 0
    try:
        for d in _missing_dates(repo, "cb_daily", open_dates):
            df = client.get_cb_daily(d)
            if not df.empty:
                cols = ["code", "trade_date", "open", "high", "low", "close",
                        "pre_close", "pct_chg", "vol", "amount", "cb_value",
                        "cb_over_rate"]
                n += repo.upsert("cb_daily", df[[c for c in cols if c in df.columns]],
                                 ["code", "trade_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"cb_daily 拉取中断（已入库 {n} 行，下次续拉）：{e}")
    stats["cb_daily"] = n

    # 分红/除权事件（红利 carry 精确除权日；按缺失公告日增量）。
    # 注意：dividend_event 主键为 (code, ex_date)、无 trade_date 列，故不能用
    # _missing_dates（它查 trade_date）；改为按 ann_date 自增量。
    n = 0
    try:
        have = repo.read_sql("SELECT DISTINCT ann_date FROM dividend_event "
                             "WHERE ann_date>=:s", {"s": open_dates[0]}) \
            if open_dates and repo.table_exists("dividend_event") else pd.DataFrame()
        have_set = set(have["ann_date"].tolist()) if not have.empty else set()
        for d in [x for x in open_dates if x not in have_set]:
            df = client.get_dividend(d)
            if not df.empty:
                df["ann_date"] = d
                cols = ["code", "ex_date", "ann_date", "end_date", "div_proc",
                        "cash_div", "cash_div_tax", "record_date", "pay_date"]
                n += repo.upsert("dividend_event",
                                 df[[c for c in cols if c in df.columns]],
                                 ["code", "ex_date"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"dividend_event 拉取中断（已入库 {n} 行，下次续拉）：{e}")
    stats["dividend_event"] = n

    # 业绩快报/预告（时效层：预报<快报<定期报告——财报跟踪法）。按公告日增量。
    n = 0
    try:
        for d in _missing_ann_dates(repo, "fina_express", open_dates):
            frames = []
            for kind, fetch in (("express", client.get_express_by_date),
                                ("forecast", client.get_forecast_by_date)):
                try:
                    df = fetch(d)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"{kind}({d}) 拉取失败（跳过该日）：{e}")
                    continue
                if df is not None and not df.empty:
                    df["kind"] = kind
                    frames.append(df)
            if frames:
                allf = pd.concat(frames, ignore_index=True)
                cols = ["code", "report_date", "kind", "ann_date", "revenue",
                        "n_income", "profit_yoy", "forecast_type"]
                allf = allf[[c for c in cols if c in allf.columns]] \
                    .drop_duplicates(["code", "report_date", "kind"])
                n += repo.upsert("fina_express", allf, ["code", "report_date", "kind"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"fina_express 拉取中断（已入库 {n} 行，下次续拉）：{e}")
    stats["fina_express"] = n

    # 重要股东/高管增减持（跟庄信号：insider_conviction 候选因子数据源）。按公告日增量。
    n = 0
    try:
        for d in _missing_ann_dates(repo, "holder_trade", open_dates):
            df = client.get_holder_trade_by_date(d)
            if df is not None and not df.empty:
                cols = ["code", "ann_date", "holder_name", "holder_type", "in_de",
                        "change_vol", "change_ratio", "after_ratio", "avg_price"]
                df = df[[c for c in cols if c in df.columns]] \
                    .drop_duplicates(["code", "ann_date", "holder_name", "in_de"])
                if "holder_name" in df.columns:
                    df["holder_name"] = df["holder_name"].astype(str).str.slice(0, 64)
                n += repo.upsert("holder_trade", df,
                                 ["code", "ann_date", "holder_name", "in_de"])
    except Exception as e:  # noqa: BLE001
        logger.warning(f"holder_trade 拉取中断（已入库 {n} 行，下次续拉）：{e}")
    stats["holder_trade"] = n

    logger.info(f"数据更新完成：{stats}")
    return stats


def _missing_ann_dates(repo: BaseRepository, table: str, all_dates: list[str]) -> list[str]:
    """按 ann_date 口径的缺失公告日（表主键无 trade_date 时用）。"""
    if not all_dates:
        return []
    if not repo.table_exists(table):
        return list(all_dates)
    have = repo.read_sql(
        f"SELECT DISTINCT ann_date FROM {table} WHERE ann_date>=:s AND ann_date<=:e",
        {"s": all_dates[0], "e": all_dates[-1]},
    )
    have_set = set(have["ann_date"].tolist()) if not have.empty else set()
    return [d for d in all_dates if d not in have_set]


def _repo_interest_days(codes: pd.Series, trade_date: str, cal: pd.DataFrame) -> pd.Series:
    """逆回购计息天数：资金占用 1 日但计息到下一交易日，跨周末/节假日自动多计。

    GC001（1 天期）在交易日 t 计息天数 = 下一交易日 - t 的自然日差（周四通常为 3）。
    多天期品种（GC007 等）取品种固有期限与该自然日差的较大值近似。
    """
    tenor = {
        "204001.SH": 1, "131810.SZ": 1,
        "204002.SH": 2, "131811.SZ": 2,
        "204003.SH": 3, "204004.SH": 4,
        "204007.SH": 7, "131800.SZ": 7,
        "204014.SH": 14, "131809.SZ": 14,
        "204028.SH": 28, "204091.SH": 91, "204182.SH": 182,
    }
    gap = 1
    try:
        if not cal.empty:
            opens = sorted(cal.loc[cal["is_open"] == 1, "cal_date"].tolist())
            if trade_date in opens:
                i = opens.index(trade_date)
                if i + 1 < len(opens):
                    gap = max(1, (pd.to_datetime(opens[i + 1]) -
                                  pd.to_datetime(trade_date)).days)
    except Exception:  # noqa: BLE001
        gap = 1
    return codes.map(lambda c: max(tenor.get(c, 1), gap))
