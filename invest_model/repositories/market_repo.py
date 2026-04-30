"""市场数据 Repository（融资融券 + 指数日线）"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class MarketRepository(BaseRepository):
    """市场级别数据访问"""

    # ── stock_margin ──

    def save_margin(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "trade_date", "rzye", "rqye", "rzmre", "rqyl", "rzche", "rqchl"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_margin", save_df, unique_keys=["code", "trade_date"])

    def get_margin(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM stock_margin WHERE code = :code AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"code": code, "s": start_date, "e": end_date},
        )

    # ── index_daily ──

    def save_index_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "volume", "amount"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("index_daily", save_df, unique_keys=["code", "trade_date"])

    def get_index_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM index_daily WHERE code = :code AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"code": code, "s": start_date, "e": end_date},
        )

    def get_index_latest_date(self, code: str) -> str | None:
        df = self.read_sql(
            "SELECT MAX(trade_date) as max_date FROM index_daily WHERE code = :code",
            {"code": code},
        )
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])
