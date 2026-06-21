"""当前持仓读写（实盘：股数 + 成本价 + 建仓日）。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository


class HoldingRepo(BaseRepository):
    TABLE = "current_holding"
    KEYS = ["code"]

    def save(self, df: pd.DataFrame) -> int:
        """df 列：code, shares, cost_price, entry_date。"""
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_all(self) -> pd.DataFrame:
        df = self.read_sql(
            f"SELECT code, shares, cost_price, entry_date FROM {self.TABLE}"
        )
        for c in ("shares", "cost_price"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    def clear(self) -> None:
        self.execute_sql(f"DELETE FROM {self.TABLE}")
