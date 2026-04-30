"""综合评分层入口。"""

from invest_model.scoring.attribution import (
    narrative,
    top_bearish,
    top_bullish,
)
from invest_model.scoring.persistence import (
    save_composite_scores,
    save_signal_snapshots,
)
from invest_model.scoring.scorer import CompositeScorer, DEFAULT_CATEGORY_WEIGHTS

__all__ = [
    "CompositeScorer",
    "DEFAULT_CATEGORY_WEIGHTS",
    "top_bullish",
    "top_bearish",
    "narrative",
    "save_signal_snapshots",
    "save_composite_scores",
]
