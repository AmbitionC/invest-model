"""信号数据模型与抽象接口

定义信号层的核心数据结构和生成器抽象基类，
为技术指标信号、基本面信号、情绪信号等提供统一范式。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import pandas as pd


class SignalDirection(Enum):
    """信号方向"""
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class SignalStrength(Enum):
    """信号强度等级"""
    STRONG = "strong"
    MODERATE = "moderate"
    WEAK = "weak"


@dataclass
class Signal:
    """单个信号的结构化表示

    Attributes
    ----------
    name : str
        信号名称，如 "macd_cross", "rsi_oversold"
    direction : SignalDirection
        看多 / 看空 / 中性
    score : float
        量化评分，范围 [-1.0, 1.0]。正值看多，负值看空，
        绝对值越大信号越强。
    strength : SignalStrength
        信号强度等级（由 score 绝对值映射）
    label : str
        人类可读的信号描述，如 "MACD 金叉向上"
    indicator_values : dict
        产生该信号的原始指标值，便于追溯
    """
    name: str
    direction: SignalDirection
    score: float
    strength: SignalStrength
    label: str
    indicator_values: dict = field(default_factory=dict)

    def __post_init__(self):
        self.score = max(-1.0, min(1.0, self.score))


@dataclass
class SignalSnapshot:
    """某只股票在某一时点的完整信号快照

    Attributes
    ----------
    code : str
        股票代码
    trade_date : str
        交易日期 (YYYYMMDD)
    signals : list[Signal]
        所有信号列表
    composite_score : float
        综合评分 [-1.0, 1.0]，由各信号加权汇总
    generated_at : str
        生成时间戳
    """
    code: str
    trade_date: str
    signals: list[Signal] = field(default_factory=list)
    composite_score: float = 0.0
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def bullish_signals(self) -> list[Signal]:
        return [s for s in self.signals if s.direction == SignalDirection.BULLISH]

    def bearish_signals(self) -> list[Signal]:
        return [s for s in self.signals if s.direction == SignalDirection.BEARISH]

    def summary(self) -> str:
        """生成人类可读的综合摘要"""
        parts = [s.label for s in self.signals if s.strength != SignalStrength.WEAK]
        return "；".join(parts) if parts else "各指标表现平稳，无明显信号"

    def to_dict(self) -> dict:
        """序列化为字典，便于 JSON 输出或存储"""
        return {
            "code": self.code,
            "trade_date": self.trade_date,
            "composite_score": round(self.composite_score, 4),
            "summary": self.summary(),
            "generated_at": self.generated_at,
            "signals": [
                {
                    "name": s.name,
                    "direction": s.direction.value,
                    "score": round(s.score, 4),
                    "strength": s.strength.value,
                    "label": s.label,
                    "indicator_values": s.indicator_values,
                }
                for s in self.signals
            ],
        }


def score_to_strength(score: float) -> SignalStrength:
    """将 [-1, 1] 评分的绝对值映射为强度等级"""
    abs_score = abs(score)
    if abs_score >= 0.6:
        return SignalStrength.STRONG
    elif abs_score >= 0.3:
        return SignalStrength.MODERATE
    return SignalStrength.WEAK


class SignalGenerator(ABC):
    """信号生成器抽象基类

    所有类型的信号源（技术面、基本面、情绪面等）都应实现此接口，
    以便模型层统一消费。
    """

    @abstractmethod
    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:
        """从原始数据生成信号列表

        Parameters
        ----------
        code : str
            股票代码
        data : DataFrame
            包含所需指标列的 DataFrame（至少含最新一行）

        Returns
        -------
        list[Signal]
            生成的信号列表
        """

    @abstractmethod
    def required_columns(self) -> list[str]:
        """声明该生成器所需的 DataFrame 列名，
        用于校验输入数据完整性和文档化依赖关系。"""

    def validate_input(self, data: pd.DataFrame) -> bool:
        """校验输入数据是否包含所有必需列"""
        missing = set(self.required_columns()) - set(data.columns)
        if missing:
            raise ValueError(
                f"{self.__class__.__name__} 缺少必需列: {missing}，"
                f"现有列: {list(data.columns)}"
            )
        return True
