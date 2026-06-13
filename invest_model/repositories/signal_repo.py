"""信号与综合评分 Repository。

对应两张表：
  - stock_signal_snapshot  (code, trade_date, signal_name) 明细
  - stock_composite_score  (code, trade_date) 综合评分
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class SignalRepository(BaseRepository):
    """信号与综合评分数据访问。"""

    SNAPSHOT_TABLE = "stock_signal_snapshot"
    COMPOSITE_TABLE = "stock_composite_score"

    SNAPSHOT_COLUMNS = [
        "code", "trade_date", "signal_name", "category",
        "direction", "score", "strength", "label", "indicator_values",
    ]
    COMPOSITE_COLUMNS = [
        "code", "trade_date",
        "tech_score", "tech_rev_score", "fund_score", "flow_score", "sent_score",
        "composite", "rank_pct", "summary", "generated_at",
    ]

    # ── write ────────────────────────────────────────────

    def upsert_signal_snapshot(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = [c for c in self.SNAPSHOT_COLUMNS if c in df.columns]
        save_df = df[cols].copy()
        return self.upsert(
            self.SNAPSHOT_TABLE, save_df,
            unique_keys=["code", "trade_date", "signal_name"],
        )

    def upsert_composite_score(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = [c for c in self.COMPOSITE_COLUMNS if c in df.columns]
        save_df = df[cols].copy()
        return self.upsert(
            self.COMPOSITE_TABLE, save_df,
            unique_keys=["code", "trade_date"],
        )

    # ── read ─────────────────────────────────────────────

    def get_latest_composite_score(self, code: str) -> Optional[pd.Series]:
        """获取一只股票最新一日的综合评分行。"""
        sql = f"""
            SELECT * FROM {self.COMPOSITE_TABLE}
            WHERE code = :code
            ORDER BY trade_date DESC
            LIMIT 1
        """
        df = self.read_sql(sql, {"code": code})
        if df.empty:
            return None
        return df.iloc[0]

    def get_composite_score_range(
        self,
        code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """时序拉取单票的综合评分。"""
        sql = f"""
            SELECT * FROM {self.COMPOSITE_TABLE}
            WHERE code = :code AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date
        """
        return self.read_sql(sql, {"code": code, "s": start_date, "e": end_date})

    def get_composite_cross_section(self, trade_date: str) -> pd.DataFrame:
        """获取某日的全量综合评分截面（用于选股/回测）。"""
        sql = f"""
            SELECT * FROM {self.COMPOSITE_TABLE}
            WHERE trade_date = :date
            ORDER BY composite DESC
        """
        return self.read_sql(sql, {"date": trade_date})

    def get_daily_top(
        self,
        trade_date: str,
        n: int = 10,
        direction: str = "bullish",
    ) -> pd.DataFrame:
        """获取某日排名最靠前（bullish）或最靠后（bearish）的 n 只股票。"""
        if direction not in ("bullish", "bearish"):
            raise ValueError("direction 必须是 'bullish' 或 'bearish'")
        order = "DESC" if direction == "bullish" else "ASC"
        sql = f"""
            SELECT cs.code, si.name, cs.composite, cs.rank_pct,
                   cs.tech_score, cs.fund_score, cs.flow_score, cs.sent_score,
                   cs.summary
            FROM {self.COMPOSITE_TABLE} cs
            LEFT JOIN stock_info si ON cs.code = si.ts_code
            WHERE cs.trade_date = :date
            ORDER BY cs.composite {order}
            LIMIT :n
        """
        return self.read_sql(sql, {"date": trade_date, "n": n})

    def get_signal_snapshot(
        self,
        code: str,
        trade_date: str,
    ) -> pd.DataFrame:
        """获取某只股票在某日的全部单信号。"""
        sql = f"""
            SELECT * FROM {self.SNAPSHOT_TABLE}
            WHERE code = :code AND trade_date = :date
            ORDER BY category, signal_name
        """
        return self.read_sql(sql, {"code": code, "date": trade_date})

    def get_score_series(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """批量取一段时间内多只股票的 composite（供回测使用）。"""
        if not codes:
            return pd.DataFrame()
        placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
        params = {f"c{i}": c for i, c in enumerate(codes)}
        params.update({"s": start_date, "e": end_date})
        sql = f"""
            SELECT code, trade_date, composite, rank_pct,
                   tech_score, fund_score, flow_score, sent_score
            FROM {self.COMPOSITE_TABLE}
            WHERE code IN ({placeholders})
              AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date, code
        """
        return self.read_sql(sql, params)

    def get_latest_trade_date(self) -> Optional[str]:
        df = self.read_sql(f"SELECT MAX(trade_date) AS d FROM {self.COMPOSITE_TABLE}")
        if df.empty or df["d"].iloc[0] is None:
            return None
        return str(df["d"].iloc[0])
