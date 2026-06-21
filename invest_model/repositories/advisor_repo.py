"""投顾信号读写：individual 个股推荐 + theme 行业/主题。"""

from __future__ import annotations

import pandas as pd

from invest_model.repositories.base import BaseRepository

VALID_GRADES = {"A", "B", "C"}
VALID_DIRECTIONS = {"long", "reduce", "avoid", "exit"}
VALID_SOURCE_TYPES = {"research", "intraday"}


class AdvisorRepo(BaseRepository):
    RECO_TABLE = "advisor_reco"
    RECO_KEYS = ["rec_date", "code", "source_type"]
    THEME_TABLE = "advisor_theme"
    THEME_KEYS = ["rec_date", "theme", "source_type"]

    def save_reco(self, df: pd.DataFrame) -> int:
        return self.upsert(self.RECO_TABLE, df, self.RECO_KEYS)

    def save_theme(self, df: pd.DataFrame) -> int:
        return self.upsert(self.THEME_TABLE, df, self.THEME_KEYS)

    def get_active_reco(self, dt: str) -> pd.DataFrame:
        """当期有效个股信号：rec_date<=dt 且（valid_until 空或 >=dt）。"""
        df = self.read_sql(
            f"SELECT rec_date, code, source_type, grade, direction, catalyst, "
            f"valid_until, source FROM {self.RECO_TABLE} "
            f"WHERE rec_date<=:d AND (valid_until IS NULL OR valid_until='' OR valid_until>=:d)",
            {"d": dt},
        )
        if df.empty:
            return df
        # 同票多条（研报+早午盘）取 rec_date 最新一条
        df = df.sort_values("rec_date").drop_duplicates("code", keep="last")
        return df.reset_index(drop=True)

    def get_active_theme(self, dt: str) -> pd.DataFrame:
        return self.read_sql(
            f"SELECT rec_date, theme, source_type, direction, thesis, valid_until "
            f"FROM {self.THEME_TABLE} "
            f"WHERE rec_date<=:d AND (valid_until IS NULL OR valid_until='' OR valid_until>=:d)",
            {"d": dt},
        )

    def get_exit_codes(self, dt: str) -> set[str]:
        """当期 direction∈{avoid,exit} 的 code（逻辑止损 / 选股排除）。"""
        df = self.get_active_reco(dt)
        if df.empty:
            return set()
        return set(df.loc[df["direction"].isin({"avoid", "exit"}), "code"])
