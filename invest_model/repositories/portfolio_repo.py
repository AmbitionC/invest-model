"""portfolio_target 与 model_registry 读写。"""

from __future__ import annotations

import json

import pandas as pd

from invest_model.repositories.base import BaseRepository


class PortfolioRepository(BaseRepository):
    TABLE = "portfolio_target"
    KEYS = ["trade_date", "version", "code"]

    def save_targets(self, df: pd.DataFrame) -> int:
        """df 列：trade_date, version, code, weight, rank, gross_exposure。"""
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_targets(self, trade_date: str, version: str) -> pd.DataFrame:
        df = self.read_sql(
            f"SELECT code, weight, `rank`, gross_exposure FROM {self.TABLE} "
            f"WHERE trade_date=:d AND version=:v ORDER BY weight DESC",
            {"d": trade_date, "v": version},
        )
        if "weight" in df.columns:
            df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        return df


class ModelRegistryRepository(BaseRepository):
    TABLE = "model_registry"
    KEYS = ["version"]

    def register(self, meta: dict) -> int:
        rec = dict(meta)
        if isinstance(rec.get("factor_cols"), (list, dict)):
            rec["factor_cols"] = json.dumps(rec["factor_cols"], ensure_ascii=False)
        return self.upsert(self.TABLE, pd.DataFrame([rec]), self.KEYS)

    def get(self, version: str) -> dict | None:
        df = self.read_sql(
            f"SELECT * FROM {self.TABLE} WHERE version=:v", {"v": version}
        )
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def list_all(self) -> pd.DataFrame:
        return self.read_sql(f"SELECT * FROM {self.TABLE} ORDER BY created_at DESC")
