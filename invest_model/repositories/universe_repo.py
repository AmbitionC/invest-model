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

