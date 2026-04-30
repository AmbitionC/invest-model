"""技术指标 Repository"""

from typing import Optional

import pandas as pd

from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()

INDICATOR_COLUMNS = [
    "code", "trade_date",
    "boll_upper", "boll_mid", "boll_lower",
    "macd_dif", "macd_dea", "macd_hist",
    "rsi_6", "rsi_14",
    "ma60_bias", "vol_ratio",
    "momentum_20", "volatility_20",
    "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
    "is_limit_up", "is_limit_down", "is_st",
]

FEATURE_COLUMNS = [
    "boll_upper", "boll_mid", "boll_lower",
    "macd_dif", "macd_dea", "macd_hist",
    "rsi_6", "rsi_14",
    "ma60_bias", "vol_ratio",
    "momentum_20", "volatility_20",
    "ma5", "ma10", "ma20", "ma60", "ma120", "ma250",
]


class TechnicalRepository(BaseRepository):
    """stock_technical 表的读写 + 模型层特征获取接口"""

    TABLE = "stock_technical"

    def save(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        save_df = df[[c for c in INDICATOR_COLUMNS if c in df.columns]].copy()
        return self.upsert(self.TABLE, save_df, unique_keys=["code", "trade_date"])

    def get_technical(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        sql = f"""
            SELECT * FROM {self.TABLE}
            WHERE code = :code AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date
        """
        return self.read_sql(sql, {"code": code, "start": start_date, "end": end_date})

    def get_latest_date(self, table: str = None, code: str = "", **kwargs) -> str | None:
        sql = f"SELECT MAX(trade_date) as max_date FROM {self.TABLE} WHERE code = :code"
        df = self.read_sql(sql, {"code": code})
        if df.empty or df["max_date"].iloc[0] is None:
            return None
        return str(df["max_date"].iloc[0])

    # ── 模型层预留接口 ──────────────────────────────────────────

    def get_feature_vector(self, code: str, trade_date: str) -> Optional[pd.Series]:
        """获取某只股票在某日的完整特征向量。

        返回一个 Series，索引为 FEATURE_COLUMNS 中的指标名。
        如果该日无数据则返回 None。
        """
        cols = ", ".join([f"`{c}`" for c in FEATURE_COLUMNS])
        sql = f"""
            SELECT {cols} FROM {self.TABLE}
            WHERE code = :code AND trade_date = :date
            LIMIT 1
        """
        df = self.read_sql(sql, {"code": code, "date": trade_date})
        if df.empty:
            return None
        return df.iloc[0]

    def get_cross_section(self, trade_date: str,
                          codes: list[str] | None = None) -> pd.DataFrame:
        """获取某日全市场（或指定股票池）的截面数据。

        返回 DataFrame，每行一只股票，列包含 code + 全部技术特征。
        用于截面排名、行业对比等场景。
        """
        cols = "code, " + ", ".join([f"`{c}`" for c in FEATURE_COLUMNS])
        if codes:
            placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
            sql = f"""
                SELECT {cols} FROM {self.TABLE}
                WHERE trade_date = :date AND code IN ({placeholders})
                ORDER BY code
            """
            params = {"date": trade_date}
            params.update({f"c{i}": c for i, c in enumerate(codes)})
        else:
            sql = f"""
                SELECT {cols} FROM {self.TABLE}
                WHERE trade_date = :date
                ORDER BY code
            """
            params = {"date": trade_date}
        return self.read_sql(sql, params)

    def get_feature_matrix(self, code: str, start_date: str,
                           end_date: str) -> pd.DataFrame:
        """获取某只股票在一段时间内的特征矩阵。

        返回 DataFrame，每行一个交易日，列为 trade_date + FEATURE_COLUMNS。
        用于时序特征工程和模型训练窗口。
        """
        cols = "trade_date, " + ", ".join([f"`{c}`" for c in FEATURE_COLUMNS])
        sql = f"""
            SELECT {cols} FROM {self.TABLE}
            WHERE code = :code AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date
        """
        return self.read_sql(sql, {"code": code, "start": start_date, "end": end_date})

    def get_latest_snapshot(self, code: str) -> Optional[pd.Series]:
        """获取某只股票最新一日的全部技术指标（含 close 联表查询）。

        用于信号生成器实时消费。
        """
        sql = f"""
            SELECT t.*, d.close, d.volume, d.pct_chg
            FROM {self.TABLE} t
            JOIN stock_daily d ON t.code = d.code AND t.trade_date = d.trade_date
            WHERE t.code = :code
            ORDER BY t.trade_date DESC
            LIMIT 1
        """
        df = self.read_sql(sql, {"code": code})
        if df.empty:
            return None
        return df.iloc[0]
