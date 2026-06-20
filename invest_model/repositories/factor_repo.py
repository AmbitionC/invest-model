"""factor_exposure（长表）与 factor_ic_log 读写。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository


class FactorRepository(BaseRepository):
    TABLE = "factor_exposure"
    KEYS = ["trade_date", "code", "factor"]
    IC_TABLE = "factor_ic_log"
    IC_KEYS = ["trade_date", "factor_name", "horizon"]

    def save_exposures_long(self, df: pd.DataFrame) -> int:
        """df 列：trade_date, code, factor, value。"""
        return self.upsert(self.TABLE, df, self.KEYS)

    def save_exposures_wide(self, trade_date: str, wide: pd.DataFrame, factor_cols: list[str]) -> int:
        """wide：index=code，columns 含各因子。转长表后落库。"""
        if wide.empty:
            return 0
        long = wide[factor_cols].copy()
        long = long.reset_index().melt(
            id_vars=[wide.index.name or "code"], var_name="factor", value_name="value"
        )
        long = long.rename(columns={wide.index.name or "code": "code"})
        long["trade_date"] = trade_date
        long = long.dropna(subset=["value"])
        return self.save_exposures_long(long[["trade_date", "code", "factor", "value"]])

    def get_exposures_wide(self, trade_date: str) -> pd.DataFrame:
        """返回 index=code, columns=各因子 的宽表。"""
        df = self.read_sql(
            f"SELECT code, factor, value FROM {self.TABLE} WHERE trade_date=:d",
            {"d": trade_date},
        )
        if df.empty:
            return pd.DataFrame()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df.pivot(index="code", columns="factor", values="value")

    def save_ic_log(self, df: pd.DataFrame) -> int:
        """df 列：trade_date, factor_name, horizon, ic, rank_ic。"""
        return self.upsert(self.IC_TABLE, df, self.IC_KEYS)

    def get_ic_log(self, horizon: int | None = None) -> pd.DataFrame:
        if horizon is None:
            return self.read_sql(f"SELECT * FROM {self.IC_TABLE} ORDER BY trade_date")
        return self.read_sql(
            f"SELECT * FROM {self.IC_TABLE} WHERE horizon=:h ORDER BY trade_date",
            {"h": horizon},
        )
