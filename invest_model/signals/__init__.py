"""信号层入口。

使用方式：
    from invest_model.signals import get_all_generators, CompositeScorer
    gens = get_all_generators()  # 已加载 technical / fundamental / money_flow / sentiment
"""

from invest_model.signals.base import (
    CATEGORIES,
    SCOPES,
    CategorizedSignalGenerator,
    Signal,
    SignalDirection,
    SignalSnapshot,
    SignalStrength,
    score_to_strength,
)
from invest_model.signals.registry import (
    REGISTRY,
    ensure_default_generators_loaded,
    get_all_generators,
    get_by_category,
    get_generator,
    register,
)

__all__ = [
    "CATEGORIES",
    "SCOPES",
    "CategorizedSignalGenerator",
    "Signal",
    "SignalDirection",
    "SignalSnapshot",
    "SignalStrength",
    "score_to_strength",
    "REGISTRY",
    "register",
    "get_all_generators",
    "get_by_category",
    "get_generator",
    "ensure_default_generators_loaded",
]
