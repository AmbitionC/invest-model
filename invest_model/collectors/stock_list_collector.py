"""股票/ETF 列表采集器"""

import pandas as pd

from invest_model.collectors.base import BaseCollector, logger
from invest_model.repositories.stock_pool_repo import StockPoolRepository


class StockListCollector(BaseCollector):
    """采集并同步 A 股列表和 ETF 列表到数据库"""

    def collect_stock_list(self) -> pd.DataFrame:
        """采集全部 A 股基础信息并写入 stock_info"""
        logger.info("开始采集 A 股股票列表...")
        df = self.source.get_stock_list()
        if df.empty:
            logger.warning("获取到空的股票列表")
            return df

        logger.info(f"获取到 {len(df)} 只 A 股")

        if self.engine:
            repo = StockPoolRepository(self.engine)
            cols = ["ts_code", "symbol", "name", "area", "industry", "market", "list_date"]
            save_df = df[[c for c in cols if c in df.columns]]
            n = repo.save_stock_info(save_df)
            logger.info(f"写入 stock_info: {n} 条")

        return df

    def collect_etf_list(self) -> pd.DataFrame:
        """采集全部 ETF 基础信息并写入 etf_info"""
        logger.info("开始采集 ETF 列表...")
        df = self.source.get_etf_list()
        if df.empty:
            logger.warning("获取到空的 ETF 列表")
            return df

        logger.info(f"获取到 {len(df)} 只 ETF")

        if self.engine:
            repo = StockPoolRepository(self.engine)
            cols = ["ts_code", "name", "management", "fund_type",
                    "found_date", "list_date", "invest_type", "market"]
            save_df = df[[c for c in cols if c in df.columns]]
            n = repo.save_etf_info(save_df)
            logger.info(f"写入 etf_info: {n} 条")

        return df
