"""universe_snapshot 读写。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository


class UniverseRepository(BaseRepository):
    TABLE = "universe_snapshot"
    KEYS = ["trade_date", "method", "code"]

    def save_snapshot(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_universe(self, trade_date: str, method: str) -> list[str]:
        df = self.read_sql(
            f"SELECT code FROM {self.TABLE} WHERE trade_date=:d AND method=:m ORDER BY code",
            {"d": trade_date, "m": method},
        )
        return df["code"].tolist() if not df.empty else []

    def get_snapshot(self, trade_date: str, method: str) -> pd.DataFrame:
        return self.read_sql(
            f"SELECT * FROM {self.TABLE} WHERE trade_date=:d AND method=:m",
            {"d": trade_date, "m": method},
        )

    def get_dates(self, method: str) -> list[str]:
        df = self.read_sql(
            f"SELECT DISTINCT trade_date FROM {self.TABLE} WHERE method=:m ORDER BY trade_date",
            {"m": method},
        )
        return df["trade_date"].tolist() if not df.empty else []
