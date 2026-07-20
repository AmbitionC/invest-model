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

    # 主题有效期时间窗（自然日）：盘中主题短效、研报/周末主题长效。
    # 主题录入不像个股信号那样自动补 valid_until，此前空 valid_until=永不过期，
    # 令投顾风向行无限累计历史主题。改为查询时按 source_type 兜底时间窗（2026-07-20）。
    THEME_INTRADAY_DAYS = 5      # 盘中主题：rec_date + 5 自然日（≈3 交易日，板块轮动短命）
    THEME_RESEARCH_DAYS = 14     # 研报/周末主题：rec_date + 14 自然日（≈10 交易日，仍相关会被重提刷新）

    def get_active_theme(self, dt: str) -> pd.DataFrame:
        """当期有效主题。有效期两层：
        ① 时间窗——显式 valid_until 优先；否则按 source_type 兜底（盘中短/研报长）。
        ② 方向演化——同一主题名只保留 rec_date 最新那天的行（更新的 reduce 取代更早的
           long，反之亦然）；不同主题名的分歧如实并记、不互相取代。"""
        intra_floor = (pd.Timestamp(str(dt)) - pd.Timedelta(days=self.THEME_INTRADAY_DAYS)
                       ).strftime("%Y%m%d")
        res_floor = (pd.Timestamp(str(dt)) - pd.Timedelta(days=self.THEME_RESEARCH_DAYS)
                     ).strftime("%Y%m%d")
        df = self.read_sql(
            f"SELECT rec_date, theme, source_type, direction, thesis, valid_until "
            f"FROM {self.THEME_TABLE} WHERE rec_date<=:d AND ("
            f"  (valid_until IS NOT NULL AND valid_until<>'' AND valid_until>=:d)"
            f"  OR ((valid_until IS NULL OR valid_until='') AND ("
            f"       (source_type='intraday' AND rec_date>=:intra)"
            f"       OR (source_type<>'intraday' AND rec_date>=:res)))"
            f")",
            {"d": dt, "intra": intra_floor, "res": res_floor},
        )
        if df.empty:
            return df
        # 方向演化：同主题名只留最新 rec_date 的行（同日多源保留=共振；跨日反向被取代）
        df["rec_date"] = df["rec_date"].astype(str)
        latest = df.groupby("theme")["rec_date"].transform("max")
        return df[df["rec_date"] == latest].reset_index(drop=True)

    def get_exit_codes(self, dt: str) -> set[str]:
        """当期 direction∈{avoid,exit} 的 code（逻辑止损 / 选股排除）。"""
        df = self.get_active_reco(dt)
        if df.empty:
            return set()
        return set(df.loc[df["direction"].isin({"avoid", "exit"}), "code"])
