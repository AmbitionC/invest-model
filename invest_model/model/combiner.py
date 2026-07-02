"""ICCombiner：滚动 IC（或 IC_IR）加权合成多因子为单一截面打分。

权重 = 过去 K 个调仓日 rank-IC 的均值（IC 模式）或 均值/标准差（ICIR 模式）。
合成分 = Σ_f w_f · 标准化暴露_f。w_f 的符号自动吸收因子方向，无需硬编码。
预测当日若历史 IC 不足，回退到等权（按先验方向）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.factors.library import FACTOR_DIRECTION, FACTORS
from invest_model.logger import get_logger
from invest_model.repositories.factor_repo import FactorRepository

logger = get_logger()


class ICCombiner:
    def __init__(self, engine, window: int = 12, mode: str = "icir",
                 min_history: int = 3):
        self.engine = engine
        self.window = window
        self.mode = mode
        self.min_history = min_history
        self.frepo = FactorRepository(engine)

    def weights(self, as_of: str) -> pd.Series:
        """计算截至 as_of（不含当日）的滚动因子权重。"""
        ic = self.frepo.get_ic_log()
        if not ic.empty:
            ic = ic[ic["trade_date"] < as_of]
        if ic.empty or ic["trade_date"].nunique() < self.min_history:
            # 回退：等权 + 先验方向
            logger.debug(f"ICCombiner[{as_of}] 历史 IC 不足，回退等权方向先验")
            return pd.Series({f: float(FACTOR_DIRECTION.get(f, 1)) for f in FACTORS})

        ic["rank_ic"] = pd.to_numeric(ic["rank_ic"], errors="coerce")
        recent_dates = sorted(ic["trade_date"].unique())[-self.window:]
        ic = ic[ic["trade_date"].isin(recent_dates)]
        grp = ic.groupby("factor_name")["rank_ic"]
        mean_ic = grp.mean()
        if self.mode == "icir":
            std_ic = grp.std().replace(0, np.nan)
            w = (mean_ic / std_ic).fillna(mean_ic)
        else:
            w = mean_ic
        w = w.reindex(FACTORS).fillna(0.0)
        # 数值稳健：截断极端权重（小样本期某因子 IC 序列 std 极小时 ICIR 会爆表，
        # 不截断会让单因子主导整个合成分），保留符号强度。
        return w.clip(-3.0, 3.0)

    def score(self, exposures: pd.DataFrame, weights: pd.Series) -> pd.Series:
        """exposures：index=code, cols=factors。返回合成分（再做一次截面 zscore）。

        只用 FACTORS 列——候选因子（CANDIDATE_FACTORS）的暴露虽同表落库、IC 同表
        记录，但晋升前不进入打分（影子观察机制，见 factors/library.py）。
        """
        cols = [f for f in FACTORS if f in exposures.columns]
        X = exposures[cols].copy()
        # 缺失填 0（已是 zscore，0=截面均值，中性）
        X = X.fillna(0.0)
        w = weights.reindex(cols).fillna(0.0).to_numpy()
        raw = pd.Series(X.to_numpy() @ w, index=X.index)
        sd = raw.std()
        if sd and np.isfinite(sd) and sd > 0:
            raw = (raw - raw.mean()) / sd
        return raw
