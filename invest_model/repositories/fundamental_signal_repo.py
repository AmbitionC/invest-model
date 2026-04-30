"""基本面信号专用数据访问层

提供截面估值查询和最新财务指标查询，供 FundamentalSignalGenerator 消费。
"""

import pandas as pd

from invest_model.repositories.base import BaseRepository


class FundamentalSignalRepository(BaseRepository):
    """基本面信号所需的数据查询"""

    def get_valuation_cross_section(self, trade_date: str) -> pd.DataFrame:
        """获取某日全市场截面估值数据（PE/PB/PS/市值/换手率）。

        用于截面排名归一化，判断个股估值在市场中的相对位置。
        """
        sql = """
            SELECT f.code, f.pe_ttm, f.pb, f.ps_ttm, f.total_mv, f.circ_mv,
                   f.turnover_rate, f.turnover_rate_f,
                   si.industry
            FROM stock_fundamental f
            LEFT JOIN stock_info si ON f.code = si.ts_code
            WHERE f.trade_date = :date
              AND f.pe_ttm IS NOT NULL
        """
        return self.read_sql(sql, {"date": trade_date})

    def get_latest_valuation(self, code: str) -> pd.Series | None:
        """获取某只股票最新一日的估值数据。"""
        sql = """
            SELECT f.*, si.industry
            FROM stock_fundamental f
            LEFT JOIN stock_info si ON f.code = si.ts_code
            WHERE f.code = :code
            ORDER BY f.trade_date DESC
            LIMIT 1
        """
        df = self.read_sql(sql, {"code": code})
        if df.empty:
            return None
        return df.iloc[0]

    def get_latest_fina(self, code: str) -> pd.Series | None:
        """获取某只股票最新一期的季度财务指标。

        按 report_date 降序取最新一条（最近一个报告期）。
        """
        sql = """
            SELECT * FROM stock_fina_indicator
            WHERE code = :code
            ORDER BY report_date DESC
            LIMIT 1
        """
        df = self.read_sql(sql, {"code": code})
        if df.empty:
            return None
        return df.iloc[0]

    def get_latest_trade_date_with_fundamental(self) -> str | None:
        """获取 stock_fundamental 表中最新的交易日期。"""
        df = self.read_sql("SELECT MAX(trade_date) as d FROM stock_fundamental")
        if df.empty or df["d"].iloc[0] is None:
            return None
        return str(df["d"].iloc[0])
