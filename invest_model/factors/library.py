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

    # 辅助列（供中性化使用）
    ind = raw.get("industry")
    if not isinstance(ind, pd.Series):
        ind = pd.Series([ind] * len(idx), index=idx)
    out["industry"] = ind
    out["ln_circ_mv"] = ln_circ
    return out
