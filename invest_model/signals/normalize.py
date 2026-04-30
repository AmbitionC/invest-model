"""截面归一化工具。

提供 quantile_rank / zscore / industry_neutral / clip_zscore 等函数，
供各类基本面、资金流、情绪信号把原始指标映射到统一评分区间。

约定：
    - 所有函数对输入做 copy，不在原 Series 上写
    - 遇到全空/常数序列时返回 0（NEUTRAL）而不是 NaN
    - 极端值默认 3σ 截断再映射
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


__all__ = [
    "quantile_rank",
    "zscore",
    "clip_zscore",
    "industry_neutral_zscore",
    "map_rank_to_score",
    "safe_series",
]


def safe_series(s: pd.Series) -> pd.Series:
    """把 Series 转为 float 并用 NaN 替换 None/非数值。"""
    return pd.to_numeric(s, errors="coerce")


def quantile_rank(s: pd.Series) -> pd.Series:
    """返回 [0, 1] 分位（1 表示最大）。全常数/全空时返回 0.5。"""
    x = safe_series(s)
    if x.dropna().empty or x.nunique(dropna=True) <= 1:
        return pd.Series(0.5, index=s.index)
    return x.rank(pct=True, method="average").fillna(0.5)


def zscore(s: pd.Series) -> pd.Series:
    """标准化。全常数/全空时返回 0。"""
    x = safe_series(s)
    std = x.std(ddof=0)
    if not np.isfinite(std) or std == 0:
        return pd.Series(0.0, index=s.index)
    return ((x - x.mean()) / std).fillna(0.0)


def clip_zscore(s: pd.Series, n_std: float = 3.0) -> pd.Series:
    """3σ 截断 zscore。"""
    z = zscore(s)
    return z.clip(-n_std, n_std)


def industry_neutral_zscore(
    df: pd.DataFrame,
    value_col: str,
    industry_col: str = "industry",
    n_std: float = 3.0,
) -> pd.Series:
    """按行业分组做 zscore，得到"行业内相对位置"。

    行业为空或组内仅 1 条样本时退化为全市场 zscore。
    """
    if df.empty or value_col not in df.columns:
        return pd.Series(dtype=float)

    out = pd.Series(0.0, index=df.index)
    global_z = clip_zscore(df[value_col], n_std)

    if industry_col not in df.columns:
        return global_z

    for ind, sub in df.groupby(industry_col, dropna=False):
        if pd.isna(ind) or len(sub) < 5:
            out.loc[sub.index] = global_z.loc[sub.index]
        else:
            out.loc[sub.index] = clip_zscore(sub[value_col], n_std)
    return out.fillna(0.0)


def map_rank_to_score(
    rank: float,
    bullish_when_low: bool = True,
    max_abs_score: float = 1.0,
) -> float:
    """把 [0, 1] 分位映射到 [-max_abs_score, +max_abs_score] 评分。

    - bullish_when_low=True: 低分位看多（例如 PE/PB 越低越好）。
      映射：rank=0 → +max_abs_score, rank=1 → -max_abs_score
    - bullish_when_low=False: 高分位看多（例如 ROE 越高越好）。
      映射：rank=0 → -max_abs_score, rank=1 → +max_abs_score
    """
    if rank is None or not np.isfinite(rank):
        return 0.0
    rank = max(0.0, min(1.0, float(rank)))
    scaled = (0.5 - rank) * 2 * max_abs_score if bullish_when_low else (rank - 0.5) * 2 * max_abs_score
    return float(max(-max_abs_score, min(max_abs_score, scaled)))


def zscore_to_score(
    z: float,
    bullish_when_high: bool = True,
    cap: float = 2.0,
    max_abs_score: float = 1.0,
) -> float:
    """把 zscore 截断到 ±cap 后线性映射到 ±max_abs_score。"""
    if z is None or not np.isfinite(z):
        return 0.0
    z = max(-cap, min(cap, float(z)))
    base = z / cap
    return float(max(-max_abs_score, min(max_abs_score, base * max_abs_score))) * (1 if bullish_when_high else -1)


def value_to_score(
    value: Optional[float],
    thresholds: list[tuple[float, float]],
    default: float = 0.0,
) -> float:
    """根据阈值表把绝对值映射到评分。

    thresholds 是升序的 [(lower_bound, score), ...]，取第一个满足 value <= lower_bound 之前的最后一段。
    示例（debt_to_asset）:
        [(0.3, 0.3), (0.5, 0.0), (0.7, -0.3), (1.0, -0.6)]
    value=0.25 → 0.3；value=0.4 → 0.0；value=0.8 → -0.6
    """
    if value is None or not np.isfinite(value):
        return default
    score = default
    for bound, s in thresholds:
        if value <= bound:
            return s
        score = s
    return score
