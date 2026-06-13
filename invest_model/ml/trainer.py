"""逐票 XGBoost 多 horizon 训练器。

设计要点：
1. 严格逐票训练：每只标的独立持有 (horizon -> XGBRegressor) 字典
2. 多 horizon 共享特征矩阵，标签独立（不做 multi-output 是为了独立 early stopping）
3. 走 walk-forward CV：用每折 val_idx 做 early stopping，
   最终生产模型用 final_train_range 重新训练
4. 评估指标：IC（rank correlation）、RMSE、命中方向准确率
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
import xgboost as xgb

from invest_model.logger import get_logger
from invest_model.ml.cv import PurgedWalkForwardSplit
from invest_model.ml.labels import LABEL_HORIZONS

logger = get_logger()


# ── 默认超参（500-1000 样本规模下的保守配置）──

DEFAULT_PARAMS: dict = {
    "max_depth": 3,
    "min_child_weight": 20,
    "gamma": 0.05,
    "reg_alpha": 0.5,
    "reg_lambda": 2.0,
    "subsample": 0.7,
    "colsample_bytree": 0.6,
    "colsample_bylevel": 0.8,
    "learning_rate": 0.05,
    "n_estimators": 500,
    "objective": "reg:squarederror",
    "eval_metric": "rmse",
    "tree_method": "hist",
    "random_state": 42,
}

# ETF 专属超参 overlay
# ── 背景 ──
# ETF 的标签波动率显著小于个股（h5d 标准差 ~0.035 vs 个股 ~0.082），叠加 DEFAULT_PARAMS
# 中保守的 min_child_weight=20 / gamma=0.05 / 强 L1+L2 正则，XGBoost 在 ETF 上几乎每棵树
# 都不能分裂 → 全部样本进同一个叶子 → 预测退化为标签均值（≈0）→ spearman IC=0 →
# horizon_weighted_score≈0 → 永远 hold 不交易。
# ── 调整方向 ──
# 1) min_child_weight 20 → 5：允许更小的叶子节点，让模型敢分裂
# 2) gamma 0.05 → 0：取消分裂增益门槛
# 3) reg_alpha / reg_lambda 减半：低标签波动场景下，强正则把所有信号都"压平"
# 4) max_depth 3 → 4：允许更复杂的交互（ETF 特征已比个股精简，深一点不会过拟合）
# 5) colsample_bytree 0.6 → 0.9：ETF 仅 20 维特征，列采样过强会丢有效信号
# 6) learning_rate 0.05 → 0.03：搭配上面的"敢分裂"配置，用更小步长稳定收敛
ETF_PARAMS_OVERLAY: dict = {
    "max_depth": 4,
    "min_child_weight": 5,
    "gamma": 0.0,
    "reg_alpha": 0.2,
    "reg_lambda": 1.0,
    "colsample_bytree": 0.9,
    "learning_rate": 0.03,
}

EARLY_STOPPING_ROUNDS: int = 30


@dataclass
class FoldMetrics:
    """单折评估指标。"""
    fold_id: int
    horizon: int
    n_train: int
    n_val: int
    rmse: float
    ic: float          # Spearman rank correlation
    hit_rate: float    # 方向命中率（pred 与 y 同号比例）
    best_iter: int


@dataclass
class HorizonResult:
    """单 horizon 训练完成后的结果。"""
    horizon: int
    cv_metrics: list[FoldMetrics] = field(default_factory=list)
    final_model: xgb.XGBRegressor | None = None
    final_train_start: str = ""
    final_train_end: str = ""
    n_features: int = 0
    feature_columns: list[str] = field(default_factory=list)
    final_n_samples: int = 0

    @property
    def avg_ic(self) -> float:
        if not self.cv_metrics:
            return 0.0
        return float(np.mean([m.ic for m in self.cv_metrics]))

    @property
    def avg_rmse(self) -> float:
        if not self.cv_metrics:
            return float("nan")
        return float(np.mean([m.rmse for m in self.cv_metrics]))

    @property
    def avg_hit_rate(self) -> float:
        if not self.cv_metrics:
            return 0.0
        return float(np.mean([m.hit_rate for m in self.cv_metrics]))


@dataclass
class StockTrainResult:
    """单票全 horizon 训练结果。"""
    code: str
    horizons: dict[int, HorizonResult] = field(default_factory=dict)


class PerStockMultiHorizonTrainer:
    """逐票多 horizon XGBoost 训练器。"""

    def __init__(
        self,
        horizons: Sequence[int] = LABEL_HORIZONS,
        cv: PurgedWalkForwardSplit | None = None,
        params: dict | None = None,
        early_stopping_rounds: int = EARLY_STOPPING_ROUNDS,
        etf_codes: set[str] | None = None,
        etf_params: dict | None = None,
    ):
        """
        Parameters
        ----------
        etf_codes : set[str] | None
            ETF 标的集合。当某只 code 命中该集合时，训练时会自动用 ``etf_params``
            覆盖默认 ``params``（ETF 标签波动率显著低于个股，需要更激进的分裂配置
            避免模型退化为"预测均值"）。
        etf_params : dict | None
            ETF 专属超参 overlay。为 None 时使用模块级 ``ETF_PARAMS_OVERLAY`` 默认值。
        """
        self.horizons = tuple(horizons)
        self.cv = cv or PurgedWalkForwardSplit(
            n_splits=5,
            train_window=500,
            val_window=60,
            embargo=max(self.horizons),
        )
        self.params = {**DEFAULT_PARAMS, **(params or {})}
        self.early_stopping_rounds = early_stopping_rounds
        self.etf_codes = set(etf_codes) if etf_codes else set()
        self.etf_params = {**ETF_PARAMS_OVERLAY, **(etf_params or {})}

    def _params_for(self, code: str) -> dict:
        """根据 code 类型返回该次训练使用的最终超参（ETF 走 overlay）。"""
        if code in self.etf_codes:
            return {**self.params, **self.etf_params}
        return self.params

    def fit(
        self,
        code: str,
        X: pd.DataFrame,
        y_dict: dict[int, pd.Series],
    ) -> StockTrainResult:
        """对单票训练所有 horizon 模型。

        Parameters
        ----------
        code : str
        X : pd.DataFrame
            特征矩阵，索引为 trade_date（升序）
        y_dict : dict[int, pd.Series]
            {horizon: 标签 Series}，索引必须与 X 对齐

        Returns
        -------
        StockTrainResult
        """
        result = StockTrainResult(code=code)
        # 同一只 code 全 horizon 共用一份超参（ETF/个股 在此处分流）
        params_used = self._params_for(code)
        is_etf = code in self.etf_codes

        for h in self.horizons:
            if h not in y_dict:
                logger.warning(f"[{code}] horizon={h} 无标签，跳过")
                continue

            y = y_dict[h]
            # 对齐 + 去 NaN
            df = X.join(y.rename("y"), how="inner").dropna(subset=["y"])
            if df.shape[0] < self.cv.train_window + self.cv.val_window + self.cv.embargo:
                logger.warning(
                    f"[{code}] horizon={h} 样本不足 ({df.shape[0]} 行)，跳过"
                )
                continue

            X_arr = df.drop(columns=["y"])
            y_arr = df["y"]

            hr = HorizonResult(
                horizon=h,
                n_features=X_arr.shape[1],
                feature_columns=list(X_arr.columns),
            )

            # ── walk-forward CV ──
            best_iters: list[int] = []
            for fold in self.cv.split(len(X_arr)):
                X_tr = X_arr.iloc[fold.train_idx]
                y_tr = y_arr.iloc[fold.train_idx]
                X_val = X_arr.iloc[fold.val_idx]
                y_val = y_arr.iloc[fold.val_idx]

                model = xgb.XGBRegressor(
                    **params_used,
                    early_stopping_rounds=self.early_stopping_rounds,
                )
                model.fit(
                    X_tr, y_tr,
                    eval_set=[(X_val, y_val)],
                    verbose=False,
                )

                pred = model.predict(X_val)
                rmse = float(np.sqrt(np.mean((pred - y_val.values) ** 2)))
                # spearman rank IC
                if len(np.unique(pred)) > 1 and len(np.unique(y_val.values)) > 1:
                    ic, _ = spearmanr(pred, y_val.values)
                    ic = float(ic) if np.isfinite(ic) else 0.0
                else:
                    ic = 0.0
                # 方向命中率（剔除接近 0 的样本，按 |y| > 0.005 过滤噪声）
                mask = np.abs(y_val.values) > 0.005
                if mask.sum() > 0:
                    hit = float(np.mean(np.sign(pred[mask]) == np.sign(y_val.values[mask])))
                else:
                    hit = 0.0

                best_iter = (
                    model.best_iteration if model.best_iteration is not None else self.params["n_estimators"]
                )
                best_iters.append(best_iter)
                hr.cv_metrics.append(FoldMetrics(
                    fold_id=fold.fold_id,
                    horizon=h,
                    n_train=len(fold.train_idx),
                    n_val=len(fold.val_idx),
                    rmse=rmse,
                    ic=ic,
                    hit_rate=hit,
                    best_iter=best_iter,
                ))

            # ── 生产模型：用 final_train_range 重新训练，n_estimators 取 CV 平均 best_iter ──
            start_idx, end_idx = self.cv.final_train_range(len(X_arr))
            X_final = X_arr.iloc[start_idx:end_idx]
            y_final = y_arr.iloc[start_idx:end_idx]
            n_estimators_final = (
                int(np.median(best_iters)) if best_iters else params_used["n_estimators"]
            )
            n_estimators_final = max(50, min(n_estimators_final, params_used["n_estimators"]))

            final_params = {**params_used, "n_estimators": n_estimators_final}
            final_params.pop("early_stopping_rounds", None)
            final_model = xgb.XGBRegressor(**final_params)
            final_model.fit(X_final, y_final, verbose=False)

            hr.final_model = final_model
            hr.final_n_samples = len(X_final)
            if len(X_final) > 0:
                hr.final_train_start = str(X_final.index[0])
                hr.final_train_end = str(X_final.index[-1])

            result.horizons[h] = hr
            params_tag = "ETF" if is_etf else "default"
            logger.info(
                f"[{code}] h={h}d: avg_ic={hr.avg_ic:+.4f} "
                f"avg_rmse={hr.avg_rmse:.4f} hit={hr.avg_hit_rate:.2%} "
                f"final_n={hr.final_n_samples} iter={n_estimators_final} "
                f"params={params_tag}"
            )

        return result
