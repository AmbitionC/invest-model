"""个股日线 Repository"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class StockDailyRepository(BaseRepository):
    """个股日线数据访问"""

    TABLE = "stock_daily"
    COLUMNS = ["code", "trade_date", "open", "high", "low", "close",
               "pre_close", "change", "pct_chg", "volume", "amount"]

    def save(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        save_df = df[[c for c in self.COLUMNS if c in df.columns]].copy()
        return self.upsert(self.TABLE, save_df, unique_keys=["code", "trade_date"])

    def get_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        sql = f"""
            SELECT * FROM {self.TABLE}
            WHERE code = :code AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date
        """
        return self.read_sql(sql, {"code": code, "start": start_date, "end": end_date})

    def get_latest_date(self, table: str = None, code: str = "", **kwargs) -> str | None:
        sql = f"SELECT MAX(trade_date) as max_date FROM {self.TABLE} WHERE code = :code"
        df = self.read_sql(sql, {"code": code})
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])
