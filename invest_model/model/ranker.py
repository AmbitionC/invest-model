"""CSRanker：截面 ML 排序模型（可选增强，--model ranker）。

用全期因子暴露 + 前瞻收益（截面标准化）作池化样本训练 XGBoost 回归，
输出截面打分。严格 walk-forward：预测日 t 只用 t 之前已实现的样本训练，无未来函数。
接口与 CSPredictor 对齐（predict / predict_dates），可在闭环中互换。

依赖 xgboost（可选）：pip install "invest-model[ml]" 或 pip install xgboost。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from invest_model.factors.library import FACTORS
from invest_model.logger import get_logger
from invest_model.model.dataset import forward_returns, next_rebalance_map
from invest_model.repositories.factor_repo import FactorRepository
from invest_model.repositories.prediction_repo import PredictionRepository

logger = get_logger()

_DEFAULT_PARAMS = dict(
    n_estimators=200, max_depth=4, learning_rate=0.03,
    subsample=0.8, colsample_bytree=0.7,
    reg_alpha=0.5, reg_lambda=2.0, min_child_weight=10,
    objective="reg:squarederror", n_jobs=2,
)


def _zscore(s: pd.Series) -> pd.Series:
    sd = s.std()
    return (s - s.mean()) / sd if sd and np.isfinite(sd) and sd > 0 else s * 0.0


class CSRanker:
    def __init__(self, engine, reb_dates: list[str], version: str = "ranker_v1",
                 min_train_periods: int = 6, params: dict | None = None):
        self.engine = engine
        self.reb_dates = sorted(reb_dates)
        self.version = version
        self.min_train_periods = min_train_periods
        self.params = params or _DEFAULT_PARAMS
        self.frepo = FactorRepository(engine)
        self.prepo = PredictionRepository(engine)
        self._nxt = next_rebalance_map(self.reb_dates)
        self._factor_cols = list(FACTORS)

    def _training_panel(self, as_of: str) -> tuple[pd.DataFrame, pd.Series]:
        """汇总 as_of 之前所有「暴露 + 已实现前瞻收益(截面 zscore)」样本。"""
        X_parts, y_parts = [], []
        for d in self.reb_dates:
            if d >= as_of or d not in self._nxt:
                continue
            t1 = self._nxt[d]
            if t1 > as_of:                 # 标签未实现，跳过（防未来函数）
                continue
            expo = self.frepo.get_exposures_wide(d)
            if expo.empty:
                continue
            fwd = forward_returns(self.engine, d, t1, expo.index.tolist())
            common = expo.index.intersection(fwd.index)
            if len(common) < 10:
                continue
            X_parts.append(expo.loc[common].reindex(columns=self._factor_cols))
            y_parts.append(_zscore(fwd.loc[common]))
        if not X_parts:
            return pd.DataFrame(), pd.Series(dtype=float)
        return pd.concat(X_parts), pd.concat(y_parts)

    def predict(self, trade_date: str, persist: bool = True) -> pd.DataFrame:
        try:
            from xgboost import XGBRegressor
        except ImportError as e:  # noqa: BLE001
            raise RuntimeError("CSRanker 需要 xgboost：pip install xgboost") from e

        expo_now = self.frepo.get_exposures_wide(trade_date)
        if expo_now.empty:
            return pd.DataFrame()

        Xtr, ytr = self._training_panel(trade_date)
        n_periods = sum(1 for d in self.reb_dates if d < trade_date and d in self._nxt
                        and self._nxt[d] <= trade_date)
        if Xtr.empty or n_periods < self.min_train_periods:
            return pd.DataFrame()      # 历史不足，交由上层回退（如 IC 合成）

        model = XGBRegressor(**self.params)
        model.fit(Xtr.fillna(0.0).to_numpy(), ytr.to_numpy())
        Xn = expo_now.reindex(columns=self._factor_cols).fillna(0.0).to_numpy()
        score = pd.Series(model.predict(Xn), index=expo_now.index)
        score = _zscore(score)

        out = pd.DataFrame({"code": score.index, "score": score.values})
        out["rank_pct"] = out["score"].rank(pct=True)
        out["trade_date"] = trade_date
        out["version"] = self.version
        out = out.sort_values("score", ascending=False).reset_index(drop=True)
        if persist and not out.empty:
            self.prepo.save_predictions(
                out[["trade_date", "version", "code", "score", "rank_pct"]])
        return out

    def predict_dates(self, reb_dates: list[str]) -> int:
        n = 0
        for d in reb_dates:
            if not self.predict(d).empty:
                n += 1
        logger.info(f"Ranker 预测完成：{n}/{len(reb_dates)} 个调仓日 (version={self.version})")
        return n
