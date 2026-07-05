"""套利模块数据访问：水表 / carry / 盲区 α / 统一资金账本。

沿用 AdvisorRepo 的 ``rec_date<=dt AND (valid_until 空 or >=dt)`` 当期有效惯用法，
所有派生行按 version 命名空间隔离。
"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository

VALID_METERS = {"credit", "fiscal", "policy_capital"}
VALID_DIMENSIONS = {"sector", "theme"}
VALID_FLOW_DIRECTIONS = {"in", "out", "flat"}
VALID_SLEEVES = {"defense_A", "offense_B", "alpha", "cash"}
VALID_CARRY_SLEEVES = {"reverse_repo", "dividend_carry", "convertible"}


class WaterMeterRepo(BaseRepository):
    TABLE = "watermeter_signal"
    KEYS = ["as_of_date", "meter", "dimension", "key"]

    def save(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_active(self, dt: str) -> pd.DataFrame:
        """当期有效水表信号：as_of_date<=dt 且（valid_until 空或 >=dt）。"""
        df = self.read_sql(
            f"SELECT as_of_date, meter, dimension, key, flow_score, direction, "
            f"evidence, source, valid_until FROM {self.TABLE} "
            f"WHERE as_of_date<=:d AND (valid_until IS NULL OR valid_until='' "
            f"OR valid_until>=:d)",
            {"d": dt},
        )
        if df.empty:
            return df
        # 同一 (meter,dimension,key) 取 as_of_date 最新一条
        df = df.sort_values("as_of_date").drop_duplicates(
            ["meter", "dimension", "key"], keep="last")
        return df.reset_index(drop=True)


class FlowRepo(BaseRepository):
    TABLE = "flow_score"
    KEYS = ["trade_date", "dimension", "key", "version"]

    def save(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get(self, dt: str, version: str) -> pd.DataFrame:
        return self.read_sql(
            f"SELECT * FROM {self.TABLE} WHERE trade_date=:d AND version=:v",
            {"d": dt, "v": version})

    def latest_on_or_before(self, dt: str, version: str) -> pd.DataFrame:
        """<=dt 的最近一个 trade_date 的 flow_score（跟水不跟价的复读用）。"""
        row = self.read_sql(
            f"SELECT MAX(trade_date) m FROM {self.TABLE} "
            f"WHERE trade_date<=:d AND version=:v", {"d": dt, "v": version})
        if row.empty or not row["m"].iloc[0]:
            return pd.DataFrame()
        return self.get(str(row["m"].iloc[0]), version)


class CarryRepo(BaseRepository):
    TABLE = "carry_signal"
    KEYS = ["trade_date", "sleeve", "code", "version"]

    def save(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get(self, dt: str, version: str, sleeve: str | None = None) -> pd.DataFrame:
        sql = (f"SELECT * FROM {self.TABLE} WHERE trade_date=:d AND version=:v")
        params: dict = {"d": dt, "v": version}
        if sleeve:
            sql += " AND sleeve=:s"
            params["s"] = sleeve
        return self.read_sql(sql + " ORDER BY rank", params)


class AlphaRepo(BaseRepository):
    TABLE = "alpha_candidate"
    KEYS = ["as_of_date", "code", "version"]

    def save(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get_active(self, dt: str, version: str) -> pd.DataFrame:
        df = self.read_sql(
            f"SELECT * FROM {self.TABLE} WHERE as_of_date<=:d AND version=:v "
            f"AND (valid_until IS NULL OR valid_until='' OR valid_until>=:d)",
            {"d": dt, "v": version})
        if df.empty:
            return df
        df = df.sort_values("as_of_date").drop_duplicates("code", keep="last")
        return df.reset_index(drop=True)

    def set_falsified(self, as_of: str, code: str, version: str, flag: int) -> None:
        self.execute_sql(
            f"UPDATE {self.TABLE} SET falsified=:f "
            f"WHERE as_of_date=:d AND code=:c AND version=:v",
            {"f": flag, "d": as_of, "c": code, "v": version})


class LedgerRepo(BaseRepository):
    TABLE = "sleeve_target"
    KEYS = ["plan_date", "sleeve", "version"]

    def save(self, df: pd.DataFrame) -> int:
        return self.upsert(self.TABLE, df, self.KEYS)

    def get(self, plan_date: str, version: str) -> pd.DataFrame:
        return self.read_sql(
            f"SELECT * FROM {self.TABLE} WHERE plan_date=:d AND version=:v",
            {"d": plan_date, "v": version})
