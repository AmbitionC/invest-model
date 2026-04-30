"""数据源抽象基类"""

from abc import ABC, abstractmethod

import pandas as pd


class BaseSource(ABC):
    """所有数据源的统一接口"""

    @abstractmethod
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取交易日历 -> columns: [cal_date, is_open]"""

    @abstractmethod
    def get_stock_list(self) -> pd.DataFrame:
        """获取全部 A 股列表 -> columns: [ts_code, name, industry, list_date, ...]"""

    @abstractmethod
    def get_stock_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取个股日线 -> columns: [code, trade_date, open, high, low, close, pre_close, change, pct_chg, volume, amount]"""

    @abstractmethod
    def get_index_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指数日线"""

    def get_stock_fundamental(self, code: str, period: str) -> pd.DataFrame:
        """获取财务指标（默认不支持，子类可选实现）"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_stock_fundamental")

    def get_cashflow(self, code: str, trade_date: str) -> pd.DataFrame:
        """获取资金流向"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_cashflow")

    def get_holder_trade(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取股东增减持"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_holder_trade")

    def get_holder_count(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取股东户数"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_holder_count")

    def get_margin(self, trade_date: str) -> pd.DataFrame:
        """获取融资融券"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_margin")

    def get_etf_list(self) -> pd.DataFrame:
        """获取 ETF 列表"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_etf_list")

    def get_etf_daily(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 ETF 日线"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_etf_daily")

    def get_etf_holding(self, code: str, report_date: str) -> pd.DataFrame:
        """获取 ETF 重仓股"""
        raise NotImplementedError(f"{self.__class__.__name__} 不支持 get_etf_holding")
