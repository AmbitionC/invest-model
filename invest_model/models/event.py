"""事件数据模型"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class HolderTrade:
    """股东增减持"""
    code: str
    holder_name: Optional[str] = None
    holder_type: Optional[str] = None
    trade_type: Optional[str] = None
    change_vol: Optional[float] = None
    change_ratio: Optional[float] = None
    after_share: Optional[float] = None
    after_ratio: Optional[float] = None
    avg_price: Optional[float] = None
    begin_date: Optional[str] = None
    close_date: Optional[str] = None
    ann_date: Optional[str] = None


@dataclass
class HolderCount:
    """股东户数"""
    code: str
    ann_date: Optional[str] = None
    end_date: Optional[str] = None
    holder_num: Optional[int] = None
    holder_num_change: Optional[float] = None
