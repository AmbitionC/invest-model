"""ETF 相关数据模型"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ETFInfo:
    """ETF 基础信息"""
    ts_code: str
    name: Optional[str] = None
    management: Optional[str] = None
    fund_type: Optional[str] = None
    found_date: Optional[str] = None
    list_date: Optional[str] = None
    invest_type: Optional[str] = None
    market: Optional[str] = None


@dataclass
class ETFDaily:
    """ETF 日线行情"""
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
class ETFHolding:
    """ETF 重仓股持仓"""
    code: str
    report_date: str
    stock_code: Optional[str] = None
    stock_name: Optional[str] = None
    holding_amount: Optional[float] = None
    holding_mv: Optional[float] = None
    holding_ratio: Optional[float] = None
