"""ETF 数据 Repository"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class ETFRepository(BaseRepository):
    """ETF 数据访问：日线、基础信息、重仓股"""

    def save_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "volume", "amount"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("etf_daily", save_df, unique_keys=["code", "trade_date"])

    def save_holding(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "report_date", "stock_code", "stock_name",
                "holding_amount", "holding_mv", "holding_ratio"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("etf_holding", save_df, unique_keys=["code", "report_date", "stock_code"])

    def get_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM etf_daily WHERE code = :code AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"code": code, "s": start_date, "e": end_date},
        )

    def get_holding(self, code: str, report_date: str | None = None) -> pd.DataFrame:
        if report_date:
            return self.read_sql(
                "SELECT * FROM etf_holding WHERE code = :code AND report_date = :rd ORDER BY holding_ratio DESC",
                {"code": code, "rd": report_date},
            )
        return self.read_sql(
            "SELECT * FROM etf_holding WHERE code = :code ORDER BY report_date DESC, holding_ratio DESC",
            {"code": code},
        )

    def get_latest_date(self, code: str) -> str | None:
        df = self.read_sql(
            "SELECT MAX(trade_date) as max_date FROM etf_daily WHERE code = :code",
            {"code": code},
        )
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])
