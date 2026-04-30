"""信号与综合评分的落库工具。

供 CompositeScorer 产出的结果：
  - dict[code -> SignalSnapshot]  → 展平到 stock_signal_snapshot
  - 打分 DataFrame               → upsert 到 stock_composite_score
"""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.repositories.signal_repo import SignalRepository
from invest_model.scoring.scorer import _category_of  # noqa: PLC2701 (internal)
from invest_model.signals.base import SignalSnapshot

logger = get_logger()


def _snapshots_to_dataframe(snapshots: dict[str, SignalSnapshot]) -> pd.DataFrame:
    rows: list[dict] = []
    for code, snap in snapshots.items():
        for sig in snap.signals:
            rows.append(
                {
                    "code": code,
                    "trade_date": snap.trade_date,
                    "signal_name": sig.name,
                    "category": _category_of(sig),
                    "direction": sig.direction.value,
                    "score": float(sig.score),
                    "strength": sig.strength.value,
                    "label": sig.label,
                    "indicator_values": json.dumps(
                        sig.indicator_values, ensure_ascii=False, default=str
                    ),
                }
            )
    return pd.DataFrame(rows)


def save_signal_snapshots(engine: Engine, snapshots: dict[str, SignalSnapshot]) -> int:
    """把单信号明细 upsert 到 stock_signal_snapshot。"""
    if not snapshots:
        return 0
    df = _snapshots_to_dataframe(snapshots)
    if df.empty:
        return 0
    repo = SignalRepository(engine)
    return repo.upsert_signal_snapshot(df)


def save_composite_scores(engine: Engine, df: pd.DataFrame) -> int:
    """把综合评分 DataFrame upsert 到 stock_composite_score。

    传入 df 需至少包含：code, trade_date, composite。其它列：tech_score / fund_score /
    flow_score / sent_score / rank_pct / summary / generated_at 可选。
    """
    if df is None or df.empty:
        return 0
    repo = SignalRepository(engine)
    return repo.upsert_composite_score(df)
