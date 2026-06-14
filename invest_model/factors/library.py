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
    out = 1.0 / s
    out[s <= 0] = np.nan        # 负 PE/PB/PS 无意义
    return out.replace([np.inf, -np.inf], np.nan)


def compute_factors(raw: pd.DataFrame) -> pd.DataFrame:
    """raw：index=code，列见 loader。返回 index=code、列=FACTORS 的因子宽表（含 industry, ln_circ_mv 辅助列）。"""
    out = pd.DataFrame(index=raw.index)

    out["ep"] = _safe_inv(raw.get("pe_ttm"))
    out["bp"] = _safe_inv(raw.get("pb"))
    out["sp"] = _safe_inv(raw.get("ps_ttm"))

    out["roe"] = pd.to_numeric(raw.get("roe"), errors="coerce")
    out["roa"] = pd.to_numeric(raw.get("roa"), errors="coerce")
    out["gross_margin"] = pd.to_numeric(raw.get("gross_margin"), errors="coerce")

    out["rev_yoy"] = pd.to_numeric(raw.get("revenue_yoy"), errors="coerce")
    out["profit_yoy"] = pd.to_numeric(raw.get("profit_yoy"), errors="coerce")

    out["mom_60"] = pd.to_numeric(raw.get("mom_60"), errors="coerce")
    out["mom_120"] = pd.to_numeric(raw.get("mom_120"), errors="coerce")
    out["reversal_5"] = -pd.to_numeric(raw.get("ret_5"), errors="coerce")

    out["lowvol_20"] = -pd.to_numeric(raw.get("vol_20"), errors="coerce")

    circ = pd.to_numeric(raw.get("circ_mv"), errors="coerce")
    ln_circ = np.log(circ.where(circ > 0))
    out["small_size"] = -ln_circ

    out["low_turnover"] = -pd.to_numeric(raw.get("turnover_rate"), errors="coerce")

    # 辅助列（供中性化使用）
    out["industry"] = raw.get("industry")
    out["ln_circ_mv"] = ln_circ
    return out
