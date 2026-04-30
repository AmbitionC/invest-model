"""交易日历 Repository"""

from typing import Optional

import pandas as pd

from invest_model.repositories.base import BaseRepository


class CalendarRepository(BaseRepository):
    """交易日历数据访问"""

    TABLE = "trade_calendar"

    def get_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        """获取指定范围内的交易日列表"""
        sql = f"""
            SELECT cal_date FROM {self.TABLE}
            WHERE cal_date BETWEEN :start AND :end AND is_open = 1
            ORDER BY cal_date
        """
        df = self.read_sql(sql, {"start": start_date, "end": end_date})
        return df["cal_date"].tolist()

    def get_latest_date(self, **kwargs) -> Optional[str]:
        """获取日历中最新日期"""
        sql = f"SELECT MAX(cal_date) as max_date FROM {self.TABLE}"
        df = self.read_sql(sql)
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])

    def get_missing_trade_dates(self, table: str, code: str, start_date: str, end_date: str) -> list[str]:
        """
        对比交易日历，找出某张表中某只股票缺失的交易日。
        这是增量更新的核心方法。
        """
        sql = f"""
            SELECT c.cal_date
            FROM {self.TABLE} c
            LEFT JOIN {table} t ON c.cal_date = t.trade_date AND t.code = :code
            WHERE c.cal_date BETWEEN :start AND :end
              AND c.is_open = 1
              AND t.trade_date IS NULL
            ORDER BY c.cal_date
        """
        df = self.read_sql(sql, {"code": code, "start": start_date, "end": end_date})
        return df["cal_date"].tolist()

    def save(self, df: pd.DataFrame) -> int:
        """保存交易日历数据"""
        if df.empty:
            return 0
        cols = ["cal_date", "is_open"]
        if "pretrade_date" in df.columns:
            cols.append("pretrade_date")
        save_df = df[cols].copy()
        return self.upsert(self.TABLE, save_df, unique_keys=["cal_date"])
