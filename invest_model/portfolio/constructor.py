"""组合构建：从截面打分选 top-N、定权重、单票上限、按 gross 缩放。"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class PortfolioConfig:
    top_n: int = 30
    scheme: str = "rank_weight"     # equal | rank_weight | score_weight
    max_weight: float = 0.08        # 单票上限
    industry_cap: float | None = 0.30  # 单行业上限（可选，None=不限）


def _cap_weights(w: pd.Series, max_weight: float) -> pd.Series:
    """迭代再分配，使所有权重 <= max_weight 且总和保持为原总和。"""
    total = w.sum()
    if total <= 0:
        return w
    w = w / total
    for _ in range(50):
        over = w > max_weight + 1e-12
        if not over.any():
            break
        excess = (w[over] - max_weight).sum()
        w[over] = max_weight
        under = ~over
        if not under.any() or w[under].sum() <= 0:
            break
        w[under] += excess * w[under] / w[under].sum()
    return w * total


def build_targets(
    scores: pd.DataFrame,
    cfg: PortfolioConfig,
    gross: float = 1.0,
    industry_map: dict[str, str] | None = None,
) -> dict[str, float]:
    """scores：含 code, score（可选 rank_pct）。返回 {code: weight}，Σ=gross。"""
    if scores is None or scores.empty:
        return {}
    s = scores.dropna(subset=["score"]).sort_values("score", ascending=False)
    if cfg.industry_cap and industry_map:
        s = _apply_industry_cap(s, cfg, industry_map)
    top = s.head(cfg.top_n).copy()
    if top.empty:
        return {}

    n = len(top)
    if cfg.scheme == "equal":
        raw = pd.Series(1.0, index=top["code"].values)
    elif cfg.scheme == "score_weight":
        v = top["score"].to_numpy()
        v = v - v.min() + 1e-6
        raw = pd.Series(v, index=top["code"].values)
    else:  # rank_weight
        raw = pd.Series(np.arange(n, 0, -1, dtype=float), index=top["code"].values)

    w = raw / raw.sum()
    w = _cap_weights(w, cfg.max_weight)
    w = w / w.sum() * gross
    return {c: float(x) for c, x in w.items() if x > 1e-6}


def _apply_industry_cap(s: pd.DataFrame, cfg: PortfolioConfig,
                        industry_map: dict[str, str]) -> pd.DataFrame:
    """贪心限制单行业入选数量（上限按 top_n×industry_cap 估算），保持打分顺序。"""
    max_per_ind = max(1, int(cfg.top_n * cfg.industry_cap))
    counts: dict[str, int] = {}
    keep = []
    for _, row in s.iterrows():
        ind = industry_map.get(row["code"], "NA")
        if counts.get(ind, 0) >= max_per_ind:
            continue
        counts[ind] = counts.get(ind, 0) + 1
        keep.append(row)
        if len(keep) >= cfg.top_n * 3:  # 收集足够候选即停
            break
    return pd.DataFrame(keep)
