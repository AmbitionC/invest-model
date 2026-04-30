"""股票相关数据模型"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class StockInfo:
    """股票基础信息"""
    ts_code: str
    symbol: str
    name: str
    area: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
    list_date: Optional[str] = None


@dataclass
class StockDaily:
    """个股日线行情"""
    code: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    pre_close: Optional[float] = None
    change: Optional[float] = None
    pct_chg: Optional[float] = None
    volume: Optional[float] = None
    amount: Optional[float] = None


@dataclass
class StockTechnical:
    """个股技术指标"""
    code: str
    trade_date: str
    boll_upper: Optional[float] = None
    boll_mid: Optional[float] = None
    boll_lower: Optional[float] = None
    macd_dif: Optional[float] = None
    macd_dea: Optional[float] = None
    macd_hist: Optional[float] = None
    rsi_6: Optional[float] = None
    rsi_14: Optional[float] = None
    ma60_bias: Optional[float] = None
    vol_ratio: Optional[float] = None
    momentum_20: Optional[float] = None
    volatility_20: Optional[float] = None
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    ma120: Optional[float] = None
    ma250: Optional[float] = None
    is_limit_up: Optional[int] = None
    is_limit_down: Optional[int] = None
    is_st: Optional[int] = None


@dataclass
class StockPool:
    """股票池"""
    code: str
    name: Optional[str] = None
    pool_group: str = "default"
    tags: Optional[str] = None
    notes: Optional[str] = None
