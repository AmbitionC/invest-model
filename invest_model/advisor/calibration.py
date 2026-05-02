"""标的历史分位数校准。

基于每只标的过去 N 个交易日的 composite 历史分布做分位数归一化，
并通过目标出手频率反推 confidence 阈值。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger

logger = get_logger()


@dataclass
class CalibrationProfile:
    """单票校准画像。"""
    code: str
    calibrated_at: str
    window_days: int
    composite_mean: float
    composite_std: float
    composite_p75: float
    composite_p90: float
    composite_p95: float
    abs_values: list[float] = field(default_factory=list)
    action_threshold: int = 60

    def percentile_rank(self, abs_composite: float) -> float:
        """当前 |composite| 在历史 |composite| 分布中的百分位 (0.0~1.0)。"""
        if not self.abs_values:
            return 0.0
        arr = np.array(self.abs_values)
        return float(np.searchsorted(arr, abs_composite, side="right") / len(arr))


def calibrate_single(
    engine: Engine,
    code: str,
    end_date: str,
    window: int = 500,
) -> CalibrationProfile | None:
    """从 stock_composite_score 表读取历史 composite，计算分位数 profile。"""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT composite FROM stock_composite_score
                WHERE code = :code AND trade_date <= :end_date
                ORDER BY trade_date DESC
                LIMIT :window
            """),
            conn,
            params={"code": code, "end_date": end_date, "window": window},
        )
    if df.empty or len(df) < 30:
        return None

    composites = df["composite"].astype(float).values
    abs_composites = np.abs(composites)
    abs_sorted = np.sort(abs_composites).tolist()

    return CalibrationProfile(
        code=code,
        calibrated_at=end_date,
        window_days=len(df),
        composite_mean=float(np.mean(composites)),
        composite_std=float(np.std(composites)),
        composite_p75=float(np.percentile(abs_composites, 75)),
        composite_p90=float(np.percentile(abs_composites, 90)),
        composite_p95=float(np.percentile(abs_composites, 95)),
        abs_values=abs_sorted,
        action_threshold=60,
    )


def calibrate_batch(
    engine: Engine,
    codes: list[str],
    end_date: str,
    window: int = 500,
) -> dict[str, CalibrationProfile]:
    """批量校准。"""
    profiles: dict[str, CalibrationProfile] = {}
    for code in codes:
        p = calibrate_single(engine, code, end_date, window)
        if p:
            profiles[code] = p
    return profiles


def find_threshold_for_frequency(
    profile: CalibrationProfile,
    target_actions_per_year: int = 17,
    consistency_bonus_avg: float = 18.0,
    trigger_bonus_avg: float = 4.0,
    base_max: float = 60.0,
) -> int:
    """根据目标出手频率反推 confidence 阈值。

    逻辑：模拟对每个历史 |composite| 计算 confidence（base + 平均一致性/触发加分），
    找到使年化出手次数最接近 target 的阈值。
    """
    if not profile.abs_values:
        return 60

    n_days = profile.window_days
    years = n_days / 250.0

    avg_bonus = consistency_bonus_avg + trigger_bonus_avg

    # 预计算所有 estimated confidence
    arr = np.array(profile.abs_values)
    pct_ranks = np.searchsorted(arr, arr, side="right") / len(arr)
    estimated_confidences = pct_ranks * base_max + avg_bonus

    best_threshold = 60
    best_diff = float("inf")

    for threshold in range(50, 96):
        count = int(np.sum(estimated_confidences >= threshold))
        actions_per_year = count / years if years > 0 else 0
        diff = abs(actions_per_year - target_actions_per_year)
        if diff < best_diff:
            best_diff = diff
            best_threshold = threshold

    return best_threshold


def calibrate_with_frequency(
    engine: Engine,
    codes: list[str],
    end_date: str,
    target_actions_per_year: int = 17,
    window: int = 500,
) -> dict[str, CalibrationProfile]:
    """校准 + 频率调参一步完成。"""
    profiles = calibrate_batch(engine, codes, end_date, window)
    for code, profile in profiles.items():
        profile.action_threshold = find_threshold_for_frequency(
            profile, target_actions_per_year
        )
        logger.info(
            f"校准 {code}: p90={profile.composite_p90:.4f}, "
            f"threshold={profile.action_threshold}"
        )
    return profiles
