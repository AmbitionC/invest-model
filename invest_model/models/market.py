"""市场级别数据模型"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TradeCalendar:
    """交易日历"""
    cal_date: str
    is_open: int
    pretrade_date: Optional[str] = None


@dataclass
class IndexDaily:
    """指数日线行情"""
    code: str
    trade_date: str
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pre_close: Optional[float] = None
    change: Optional[float] = None
    pct_chg: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None


@dataclass
class StockMargin:
    """融资融券"""
    code: str
    trade_date: str
    rzye: Optional[float] = None       # 融资余额
    rqye: Optional[float] = None       # 融券余额
    rzmre: Optional[float] = None      # 融资买入额
    rqyl: Optional[float] = None       # 融券余量
    rzche: Optional[float] = None      # 融资偿还额
    rqchl: Optional[float] = None      # 融券偿还量
