"""财务数据 Repository"""

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()


class FundamentalRepository(BaseRepository):
    """财务数据访问（两张表：每日估值 + 季度财务指标）"""

    # ── stock_fundamental: 每日估值指标（daily_basic） ──

    def save_daily_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "trade_date", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv",
                "turnover_rate", "turnover_rate_f"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_fundamental", save_df, unique_keys=["code", "trade_date"])

    def get_daily_basic(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM stock_fundamental WHERE code = :code AND trade_date BETWEEN :s AND :e ORDER BY trade_date",
            {"code": code, "s": start_date, "e": end_date},
        )

    # ── stock_fina_indicator: 季度财务指标 ──

    def save_fina_indicator(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "ann_date", "report_date", "eps", "bps", "roe", "roa",
                "gross_margin", "debt_to_asset", "revenue_yoy", "profit_yoy",
                "revenue", "net_profit", "ocfps"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_fina_indicator", save_df, unique_keys=["code", "report_date"])

    def get_fina_indicator(self, code: str) -> pd.DataFrame:
        return self.read_sql(
            "SELECT * FROM stock_fina_indicator WHERE code = :code ORDER BY report_date",
            {"code": code},
        )

    # ── stock_cashflow: 资金流向 ──

    def save_cashflow(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        cols = ["code", "trade_date", "buy_sm_vol", "sell_sm_vol",
                "buy_md_vol", "sell_md_vol", "buy_lg_vol", "sell_lg_vol",
                "buy_elg_vol", "sell_elg_vol", "net_mf_vol"]
        save_df = df[[c for c in cols if c in df.columns]].copy()
        return self.upsert("stock_cashflow", save_df, unique_keys=["code", "trade_date"])
