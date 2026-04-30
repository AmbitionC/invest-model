"""财务数据模型"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class StockFundamental:
    """财务指标（季报）"""
    code: str
    ann_date: Optional[str] = None
    report_date: Optional[str] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    ps_ttm: Optional[float] = None
    total_mv: Optional[float] = None
    circ_mv: Optional[float] = None
    turnover_rate: Optional[float] = None
    turnover_rate_f: Optional[float] = None
    eps: Optional[float] = None
    bps: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    gross_margin: Optional[float] = None
    debt_to_asset: Optional[float] = None
    revenue_yoy: Optional[float] = None
    profit_yoy: Optional[float] = None
    revenue: Optional[float] = None
    net_profit: Optional[float] = None
    ocfps: Optional[float] = None


@dataclass
class StockCashflow:
    """资金流向"""
    code: str
    trade_date: str
    buy_sm_vol: Optional[float] = None
    sell_sm_vol: Optional[float] = None
    buy_md_vol: Optional[float] = None
    sell_md_vol: Optional[float] = None
    buy_lg_vol: Optional[float] = None
    sell_lg_vol: Optional[float] = None
    buy_elg_vol: Optional[float] = None
    sell_elg_vol: Optional[float] = None
    net_mf_vol: Optional[float] = None
