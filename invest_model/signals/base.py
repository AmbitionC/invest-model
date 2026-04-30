"""信号层通用基类与扩展元数据。

复用 `invest_model.technical.signals` 中已有的 `Signal / SignalSnapshot / SignalGenerator`，
在此为其补充模型层所需的元数据约定：
- `category`: 信号类别（technical / fundamental / money_flow / sentiment）
- `scope`: 数据作用域（time_series / cross_section / history），
  供 CompositeScorer 决定批量预加载策略
- `required_tables`: 依赖的数据库表，供 pipeline 做前置校验

所有新老 Generator 都应实现这些属性，并通过 `signals.registry` 注册。
"""

from __future__ import annotations

from abc import abstractmethod
from typing import ClassVar

import pandas as pd

from invest_model.technical.signals import (
    Signal,
    SignalDirection,
    SignalGenerator,
    SignalSnapshot,
    SignalStrength,
    score_to_strength,
)

__all__ = [
    "Signal",
    "SignalDirection",
    "SignalGenerator",
    "SignalSnapshot",
    "SignalStrength",
    "score_to_strength",
    "CATEGORIES",
    "SCOPES",
    "CategorizedSignalGenerator",
]


CATEGORIES = ("technical", "fundamental", "money_flow", "sentiment")
SCOPES = ("time_series", "cross_section", "history")


class CategorizedSignalGenerator(SignalGenerator):
    """带有类别/作用域元数据的 SignalGenerator 基类。

    Subclass 需要至少声明：
        - `category` 类属性
        - `scope` 类属性
        - `required_tables` 类属性
        - `required_columns()` 方法
        - `generate()` 方法

    为了与 CompositeScorer 配合批量计算，子类可以选择覆盖
    `generate_for_date(code, trade_date, context)`，通过 context 复用预加载好的
    截面/历史数据，避免重复查库。
    """

    category: ClassVar[str] = "technical"
    scope: ClassVar[str] = "time_series"
    required_tables: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def generate(self, code: str, data: pd.DataFrame) -> list[Signal]:  # pragma: no cover - abstract
        ...

    def generate_for_date(
        self,
        code: str,
        trade_date: str,
        context: dict,
    ) -> list[Signal]:
        """批量评分时的默认实现：从 context 里取对应 DataFrame 走 generate。

        `context` 在不同 scope 下包含的键不同（由 CompositeScorer 约定）：
          - time_series:  {"time_series": {code -> DataFrame}}
          - cross_section: {"cross_section": DataFrame(以 code 为 index)}
          - history: 同 time_series
        子类如果有更高效的实现（例如一次算全市场）可以覆盖此方法。
        """
        if self.scope == "cross_section":
            cs: pd.DataFrame = context.get("cross_section")
            if cs is None or cs.empty or code not in cs.index:
                return []
            row = cs.loc[code]
            df = pd.DataFrame([row])
            return self.generate(code, df)

        ts_map: dict[str, pd.DataFrame] = context.get("time_series", {})
        df = ts_map.get(code)
        if df is None or df.empty:
            return []
        return self.generate(code, df)
