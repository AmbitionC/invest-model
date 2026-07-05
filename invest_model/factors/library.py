"""因子库：把原始截面映射为带方向约定的因子暴露。

方向（FACTOR_DIRECTION）+1 表示「值越大越好」，-1 表示「值越小越好」。
方向仅作记录/参考；最终合成权重由滚动 IC 的符号自动学习，无需硬编码。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# 因子名 → 方向（先验）
FACTOR_DIRECTION: dict[str, int] = {
    # 价值（低估值好）
    "ep": +1,      # 1/PE
    "bp": +1,      # 1/PB
    "sp": +1,      # 1/PS
    # 质量
    "roe": +1,
    "roa": +1,
    "gross_margin": +1,
    # 成长
    "rev_yoy": +1,
    "profit_yoy": +1,
    # 动量 / 反转
    "mom_60": +1,
    "mom_120": +1,
    "reversal_5": +1,   # = -ret_5，近 5 日跌得多更好
    # 风险
    "lowvol_20": +1,    # = -vol_20，低波动好
    # 规模（小市值溢价，A 股先验为正：小好）
    "small_size": +1,   # = -ln(circ_mv)
    # 流动性（低换手好）
    "low_turnover": +1, # = -turnover_rate
}

FACTORS: list[str] = list(FACTOR_DIRECTION.keys())

# ── 候选因子（影子观察）──
# 与正式因子同样计算暴露、落库、记 rank-IC（health/复盘可见），但**不参与**
# 合成打分与 ranker 训练——晋升前先攒 ≥12 期 IC 观察其有效性与稳定性。
# 晋升 = 把因子从 CANDIDATE_DIRECTION 移入 FACTOR_DIRECTION（属模型层变更，需评审）。
CANDIDATE_DIRECTION: dict[str, int] = {
    # 北向持股占比 ~20 交易日变化（百分点）。先验：外资加仓为正。
    # 注意数据依赖 hk_hold（港交所披露）；若披露口径变化导致断供，
    # 该因子暴露为 NaN，影子模式下无任何下游影响。
    "nb_ratio_chg_20": +1,
    # 投顾立场信号（long A/B/C 打正、reduce/avoid/exit 打负）。先验：看多为正。
    # E6 实测（issue #14）：对量化 rank_pct 有独立增量偏 IC（+0.077，聚类稳健 t=+2.2）、
    # 与量化正交（相关 -0.19）；研究亦支持「投顾+因子」融合最优（IR~1.23, arXiv 2502.20489）。
    # 影子累积 ≥12 期 IC 达门槛后，再提议移入 FACTOR_DIRECTION 或经 fuse_targets/元标签融合。
    # 数据依赖 advisor_reco；覆盖面窄（仅投顾点名标的），其余截面 NaN=中性。
    "adv_stance": +1,
}

CANDIDATE_FACTORS: list[str] = list(CANDIDATE_DIRECTION.keys())


def _safe_inv(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.ndim == 0:             # 单票时退化为标量
        s = pd.Series([s])
    out = 1.0 / s
    out[s <= 0] = np.nan        # 负 PE/PB/PS 无意义
    return out.replace([np.inf, -np.inf], np.nan)


def _to_series(val, index) -> pd.Series:
    """确保返回值始终为 Series（单票时 raw.get 可能退化为标量）。"""
    s = pd.to_numeric(val, errors="coerce")
    if not isinstance(s, pd.Series):
        s = pd.Series([s] * len(index), index=index)
    return s


def compute_factors(raw: pd.DataFrame) -> pd.DataFrame:
    """raw：index=code，列见 loader。返回 index=code、列=FACTORS 的因子宽表（含 industry, ln_circ_mv 辅助列）。"""
    idx = raw.index
    out = pd.DataFrame(index=idx)

    out["ep"] = _safe_inv(raw.get("pe_ttm"))
    out["bp"] = _safe_inv(raw.get("pb"))
    out["sp"] = _safe_inv(raw.get("ps_ttm"))

    out["roe"] = _to_series(raw.get("roe"), idx)
    out["roa"] = _to_series(raw.get("roa"), idx)
    out["gross_margin"] = _to_series(raw.get("gross_margin"), idx)

    out["rev_yoy"] = _to_series(raw.get("revenue_yoy"), idx)
    out["profit_yoy"] = _to_series(raw.get("profit_yoy"), idx)

    out["mom_60"] = _to_series(raw.get("mom_60"), idx)
    out["mom_120"] = _to_series(raw.get("mom_120"), idx)
    out["reversal_5"] = -_to_series(raw.get("ret_5"), idx)

    out["lowvol_20"] = -_to_series(raw.get("vol_20"), idx)

    circ = _to_series(raw.get("circ_mv"), idx)
    ln_circ = np.log(circ.where(circ > 0))
    out["small_size"] = -ln_circ

    out["low_turnover"] = -_to_series(raw.get("turnover_rate"), idx)

    # 候选因子（影子观察，不参与打分；无数据时整列 NaN，落库时被 dropna 自然跳过）
    out["nb_ratio_chg_20"] = _to_series(raw.get("nb_ratio_chg_20"), idx)
    out["adv_stance"] = _to_series(raw.get("adv_stance"), idx)

    # 辅助列（供中性化使用）
    ind = raw.get("industry")
    if not isinstance(ind, pd.Series):
        ind = pd.Series([ind] * len(idx), index=idx)
    out["industry"] = ind
    out["ln_circ_mv"] = ln_circ
    return out
