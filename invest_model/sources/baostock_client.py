"""Baostock 数据源客户端（补充数据源，免费无限制）"""

import baostock as bs
import pandas as pd

from invest_model.config import load_config
from invest_model.logger import get_logger
from invest_model.sources.base import BaseSource

logger = get_logger()


def _to_tushare_code(bao_code: str) -> str:
    """baostock 代码 (sh.600000) -> tushare 代码 (600000.SH)"""
    parts = bao_code.split(".")
    if len(parts) == 2:
        return f"{parts[1]}.{parts[0].upper()}"
    return bao_code


def _to_bao_code(ts_code: str) -> str:
    """tushare 代码 (600000.SH) -> baostock 代码 (sh.600000)"""
    parts = ts_code.split(".")
    if len(parts) == 2:
        return f"{parts[1].lower()}.{parts[0]}"
    return ts_code


def _to_dash_date(d: str) -> str:
    """YYYYMMDD -> YYYY-MM-DD"""
    if len(d) == 8 and "-" not in d:
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def _to_compact_date(d: str) -> str:
    """YYYY-MM-DD -> YYYYMMDD"""
    return d.replace("-", "")


class BaostockClient(BaseSource):
    """Baostock 数据源（免费，日线 + 财务）"""

    def __init__(self):
        result = bs.login()
        if result.error_code != "0":
            raise ConnectionError(f"Baostock 登录失败: {result.error_msg}")
        logger.info("Baostock 客户端初始化完成")

    def __del__(self):
        try:
            bs.logout()
        except Exception:
            pass

    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        rs = bs.query_trade_dates(
            start_date=_to_dash_date(start_date),
            end_date=_to_dash_date(end_date),
        )
        rows = []
        while (rs.error_code == "0") and rs.next():
            rows.append(rs.get_row_data())
        df = pd.DataFrame(rows, columns=rs.fields)
        df = df.rename(columns={"calendar_date": "cal_date", "is_trading_day": "is_open"})
        df["cal_date"] = df["cal_date"].apply(_to_compact_date)
        df["is_open"] = df["is_open"].astype(int)
        return df

    def get_stock_list(self) -> pd.DataFrame:
        raise NotImplementedError("Baostock 不支持获取完整股票列表，请使用 Tushare")

    def get_stock_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        bao_code = _to_bao_code(code)
        rs = bs.query_history_k_data_plus(
            bao_code,
            "date,code,open,high,low,close,preclose,pctChg,volume,amount",
            start_date=_to_dash_date(start_date),
            end_date=_to_dash_date(end_date),
            frequency="d",
            adjustflag="3",
        )
        rows = []
        while (rs.error_code == "0") and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=rs.fields)
        df = df.rename(columns={
            "date": "trade_date",
            "preclose": "pre_close",
            "pctChg": "pct_chg",
        })
        df["trade_date"] = df["trade_date"].apply(_to_compact_date)
        df["code"] = code

        for col in ["open", "high", "low", "close", "pre_close", "pct_chg", "volume", "amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # baostock volume 单位是股，转换为手
        df["volume"] = df["volume"] / 100
        # change = close - pre_close
        df["change"] = df["close"] - df["pre_close"]

        return df[["code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "change", "pct_chg", "volume", "amount"]]

    def get_index_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        bao_code = _to_bao_code(code)
        rs = bs.query_history_k_data_plus(
            bao_code,
            "date,code,open,high,low,close,preclose,pctChg,volume,amount",
            start_date=_to_dash_date(start_date),
            end_date=_to_dash_date(end_date),
            frequency="d",
        )
        rows = []
        while (rs.error_code == "0") and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=rs.fields)
        df = df.rename(columns={
            "date": "trade_date",
            "preclose": "pre_close",
            "pctChg": "pct_chg",
        })
        df["trade_date"] = df["trade_date"].apply(_to_compact_date)
        df["code"] = code

        for col in ["open", "high", "low", "close", "pre_close", "pct_chg", "volume", "amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["change"] = df["close"] - df["pre_close"]
        return df[["code", "trade_date", "open", "high", "low", "close",
                    "pre_close", "change", "pct_chg", "volume", "amount"]]
