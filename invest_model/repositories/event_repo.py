"""事件数据 Repository"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class EventRepository(BaseRepository):
    """事件数据访问：股东增减持 + 股东户数"""

    # ── stock_holder_trade ──

    def save_holder_trade(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "ann_date", "holder_name", "holder_type", "trade_type",
                "change_vol", "change_ratio", "after_share", "after_ratio",
                "avg_price", "begin_date", "close_date"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_holder_trade", save_df,
                           unique_keys=["code", "holder_name", "begin_date", "close_date"])

    def get_holder_trade(self, code: str, start_date: str | None = None,
                         end_date: str | None = None) -> pd.DataFrame:
        sql = "SELECT * FROM stock_holder_trade WHERE code = :code"
        params: dict = {"code": code}
        if start_date:
            sql += " AND ann_date >= :start"
            params["start"] = start_date
        if end_date:
            sql += " AND ann_date <= :end"
            params["end"] = end_date
        sql += " ORDER BY ann_date DESC"
        return self.read_sql(sql, params)

    # ── stock_holder_count ──

    def save_holder_count(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "ann_date", "end_date", "holder_num", "holder_num_change"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_holder_count", save_df, unique_keys=["code", "end_date"])

    def get_holder_count(self, code: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM stock_holder_count WHERE code = :code ORDER BY end_date",
            {"code": code},
        )
