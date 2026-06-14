"""model_prediction 读写。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository


class PredictionRepository(BaseRepository):
    TABLE = "model_prediction"
    KEYS = ["trade_date", "version", "code"]

    def save_predictions(self, df: pd.DataFrame) -> int:
        """df 列：trade_date, version, code, score, rank_pct。"""
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_predictions(self, trade_date: str, version: str) -> pd.DataFrame:
        df = self.read_sql(
            f"SELECT code, score, rank_pct FROM {self.TABLE} "
            f"WHERE trade_date=:d AND version=:v ORDER BY score DESC",
            {"d": trade_date, "v": version},
        )
        for c in ("score", "rank_pct"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    def get_dates(self, version: str) -> list[str]:
        df = self.read_sql(
            f"SELECT DISTINCT trade_date FROM {self.TABLE} WHERE version=:v ORDER BY trade_date",
            {"v": version},
        )
        return df["trade_date"].tolist() if not df.empty else []
