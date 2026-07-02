"""因子处理：去极值(MAD) → 行业/市值中性化(OLS 残差) → 截面标准化(zscore)。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.factors.library import CANDIDATE_FACTORS, FACTORS

# 不做市值中性化的因子（规模因子本身）
_SKIP_SIZE_NEUTRAL = {"small_size"}


def winsorize_mad(s: pd.Series, k: float = 3.0) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    med = s.median()
    mad = (s - med).abs().median()
    if mad == 0 or np.isnan(mad):
        return s
    upper, lower = med + k * 1.4826 * mad, med - k * 1.4826 * mad
    return s.clip(lower, upper)


def zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu, sd = s.mean(), s.std()
    if sd == 0 or np.isnan(sd):
        return s * 0.0
    return (s - mu) / sd


def _neutralize(y: pd.Series, dummies: pd.DataFrame, size: pd.Series | None) -> pd.Series:
    """OLS 残差中性化：y ~ 行业哑变量 (+ ln市值)。返回残差，index 对齐 y。"""
    X_parts = [dummies]
    if size is not None:
        X_parts.append(zscore(size).rename("size").to_frame())
    X = pd.concat(X_parts, axis=1)
    X.insert(0, "_const", 1.0)

    valid = y.notna() & X.notna().all(axis=1)
    if valid.sum() < X.shape[1] + 2:
        return y  # 样本不足，跳过中性化
    Xv = X[valid].to_numpy(dtype=float)
    yv = y[valid].to_numpy(dtype=float)
    try:
        beta, *_ = np.linalg.lstsq(Xv, yv, rcond=None)
    except np.linalg.LinAlgError:
        return y
    resid = pd.Series(np.nan, index=y.index)
    resid[valid] = yv - Xv @ beta
    return resid


def process_factors(factor_df: pd.DataFrame, neutralize: bool = True) -> pd.DataFrame:
    """factor_df：index=code，列含 FACTORS + industry + ln_circ_mv。
    返回 index=code、列=FACTORS 的处理后暴露（zscore，均值≈0 标准差≈1）。"""
    industry = factor_df.get("industry")
    size = factor_df.get("ln_circ_mv")
    dummies = None
    if neutralize and industry is not None and industry.notna().any():
        dummies = pd.get_dummies(industry.fillna("NA"), prefix="ind", dtype=float)

    out = pd.DataFrame(index=factor_df.index)
    # 候选因子与正式因子同流程处理（去极值/中性化/标准化），但打分层不使用
    for f in FACTORS + CANDIDATE_FACTORS:
        if f not in factor_df.columns:
            continue
        s = winsorize_mad(factor_df[f])
        if neutralize and dummies is not None:
            use_size = size if f not in _SKIP_SIZE_NEUTRAL else None
            s = _neutralize(s, dummies, use_size)
        out[f] = zscore(s)
    return out
