"""model_prediction 读写。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository


class PredictionRepository(BaseRepository):
    TABLE = "model_prediction"
    KEYS = ["trade_date", "version", "code"]

    def save_predictions(self, df: pd.DataFrame) -> int:
        """df 列：trade_date, version, code, score, rank_pct[, top_factors]。"""
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_predictions(self, trade_date: str, version: str) -> pd.DataFrame:
        try:
            df = self.read_sql(
                f"SELECT code, score, rank_pct, top_factors FROM {self.TABLE} "
                f"WHERE trade_date=:d AND version=:v ORDER BY score DESC",
                {"d": trade_date, "v": version},
            )
        except Exception:  # noqa: BLE001 — 旧库无 top_factors 列（因子归因）时退回旧口径
            df = self.read_sql(
                f"SELECT code, score, rank_pct FROM {self.TABLE} "
                f"WHERE trade_date=:d AND version=:v ORDER BY score DESC",
                {"d": trade_date, "v": version},
            )
        for c in ("score", "rank_pct"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
