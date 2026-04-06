#!/usr/bin/env python3
"""
数据模型定义 - 个股、ETF、股东数据
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional, List

@dataclass
class StockDaily:
    """个股日线数据"""
    ts_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    change: float
    pct_chg: float
    vol: float
    amount: float

@dataclass
class StockFundamentals:
    """个股财务指标"""
    ts_code: str
    ann_date: date
    end_date: date
    pe: Optional[float]
    pb: Optional[float]
    ps: Optional[float]
    roe: Optional[float]
    roa: Optional[float]
    revenue_growth: Optional[float]
    profit_growth: Optional[float]
    gross_margin: Optional[float]
    debt_to_asset: Optional[float]

@dataclass
class HolderTrade:
    """股东增减持"""
    ts_code: str
    holder_name: str
    holder_type: str
    trade_date: date
    trade_type: str
    trade_volume: float
    trade_price: float

@dataclass
class ETFDaily:
    """ETF 日线数据"""
    ts_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    change: float
    pct_chg: float
    vol: float
    amount: float
    iopv: Optional[float]

print("✅ 数据模型定义完成")
