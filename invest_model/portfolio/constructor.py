"""组合构建：从截面打分选 top-N、定权重、单票上限、按 gross 缩放。"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd


@dataclass
class PortfolioConfig:
    top_n: int = 30
    scheme: str = "rank_weight"     # equal | rank_weight | score_weight
    max_weight: float = 0.08        # 单票上限
    industry_cap: float | None = 0.30  # 单行业上限（可选，None=不限）
    # ── 投顾为主融合（advisor_led=True 时生效）──
    advisor_led: bool = False
    grade_target: dict = field(default_factory=lambda: {"A": 0.65, "B": 0.50})  # 分级 conviction 权重
    advisory_name_cap: float = 0.20      # 投顾单票上限
    advisory_sleeve_cap: float = 1.0     # 投顾仓位池占 gross 的上限
    include_grade_c: bool = False        # 是否纳入 C 级
    theme_boost: float = 1.0             # 命中投顾看多主题的票，conviction 乘数（1.0=关）


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


def fuse_targets(
    scores: pd.DataFrame,
    cfg: PortfolioConfig,
    advisor_df: pd.DataFrame | None,
    gross: float = 1.0,
    *,
    trend_ok_codes: set[str] | None = None,
    exit_codes: set[str] | None = None,
    theme_industries: set[str] | None = None,
    industry_map: dict[str, str] | None = None,
) -> tuple[dict[str, float], dict[str, dict]]:
    """投顾为主 + 量化补充。返回 (weights {code:w}, meta {code:{'grade','source'}})。

    流程：投顾 long 票按分级 conviction 定权重（单票上限 + 仓位池上限）→ 量化用现有
    ``build_targets`` 填满剩余仓位（排除投顾占用 / 排除集 / 趋势闸外的票）。
    """
    exit_codes = set(exit_codes or set())
    meta: dict[str, dict] = {}
    adv_weights: dict[str, float] = {}

    if advisor_df is not None and not advisor_df.empty:
        grades_ok = {"A", "B"} | ({"C"} if cfg.include_grade_c else set())
        df = advisor_df.copy()
        df = df[df["direction"] == "long"]
        df = df[df["grade"].isin(grades_ok)]
        df = df[~df["code"].isin(exit_codes)]
        if trend_ok_codes is not None:
            df = df[df["code"].isin(trend_ok_codes)]
        raw: dict[str, float] = {}
        for _, r in df.iterrows():
            conv = float(cfg.grade_target.get(r["grade"], 0.0))
            if theme_industries and industry_map and industry_map.get(r["code"]) in theme_industries:
                conv *= cfg.theme_boost
            if conv <= 0:
                continue
            raw[r["code"]] = min(conv, cfg.advisory_name_cap)
        total = sum(raw.values())
        sleeve_cap = gross * cfg.advisory_sleeve_cap
        scale = (sleeve_cap / total) if total > sleeve_cap and total > 0 else 1.0
        for c, w in raw.items():
            adv_weights[c] = w * scale
            meta[c] = {"grade": dict(advisor_df.set_index("code")["grade"]).get(c), "source": "advisor"}

    # 量化补充：填满剩余仓位
    used = sum(adv_weights.values())
    remaining = max(0.0, gross - used)
    quant_weights: dict[str, float] = {}
    if remaining > 1e-6 and scores is not None and not scores.empty:
        qs = scores.copy()
        drop = set(adv_weights) | exit_codes
        qs = qs[~qs["code"].isin(drop)]
        if trend_ok_codes is not None:
            qs = qs[qs["code"].isin(trend_ok_codes)]
        quant_weights = build_targets(qs, cfg, gross=remaining, industry_map=industry_map)
        for c in quant_weights:
            meta.setdefault(c, {"grade": None, "source": "quant"})

    weights = {**adv_weights, **quant_weights}
    return {c: float(w) for c, w in weights.items() if w > 1e-6}, meta


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
