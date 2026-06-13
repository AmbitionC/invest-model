"""ML 推理 + SHAP 归因。

输入：单票特征向量（pd.Series）
输出：{horizon: pred_return}，以及 SHAP top-k 特征归因列表。

设计：
- 模型由 advisor 在初始化时通过 load_all_artifacts 一次性加载缓存
- 每次推理 O(1) 查询
- SHAP 用 TreeExplainer，对小特征数 (~60) 极快
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import xgboost as xgb

from invest_model.logger import get_logger
from invest_model.ml.persistence import ModelArtifact

logger = get_logger()


@dataclass
class PredictionResult:
    """单票一次推理的全部结果。"""
    code: str
    trade_date: str
    predictions: dict[int, float]              # {horizon: pred_log_return}
    shap_top: list[tuple[str, float]]          # 按 |shap| 降序的 top-k 特征
    feature_snapshot: dict[str, float]         # 用过的特征（便于审计）
    horizon_score: float = 0.0                 # 综合 horizon 加权得分


class MLPredictor:
    """对外推理接口。"""

    def __init__(
        self,
        artifacts: dict[str, dict[int, ModelArtifact]],
        horizon_weights: dict[int, float] | None = None,
        shap_top_k: int = 5,
        primary_horizon: int = 5,
        ic_weighting: bool = True,
        ic_floor: float = 0.05,
    ):
        """
        Parameters
        ----------
        artifacts : dict[code, dict[horizon, ModelArtifact]]
            已经加载好的所有模型
        horizon_weights : dict[int, float]
            综合得分权重，默认 {3: 0.3, 5: 0.5, 10: 0.2}（仅在 ic_weighting=False 时生效）
        shap_top_k : int
            SHAP 归因保留的 top 特征数
        primary_horizon : int
            用于 SHAP 解释的主 horizon（默认用 5d）
        ic_weighting : bool
            True 时按每个模型的 cv_avg_ic 自动归一化 horizon 权重，避免弱信号
            horizon 拉低综合分；若全部 IC <= ic_floor 则退回 horizon_weights
        ic_floor : float
            视作"有效信号"的最低 IC，低于此值的 horizon 自动权重置 0
        """
        self.artifacts = artifacts
        self.horizon_weights = horizon_weights or {3: 0.3, 5: 0.5, 10: 0.2}
        self.shap_top_k = shap_top_k
        self.primary_horizon = primary_horizon
        self.ic_weighting = ic_weighting
        self.ic_floor = ic_floor

    def predict(
        self,
        code: str,
        feature_vec: pd.Series,
        trade_date: str = "",
    ) -> PredictionResult | None:
        """对单票单日特征推理。"""
        models = self.artifacts.get(code)
        if not models:
            return None

        preds: dict[int, float] = {}
        for h, art in models.items():
            x = self._align_features(feature_vec, art.feature_cols)
            if x is None:
                continue
            try:
                p = float(art.model.predict(x.reshape(1, -1))[0])
                preds[h] = p
            except Exception as e:
                logger.warning(f"推理失败 code={code} h={h}: {e}")

        if not preds:
            return None

        eff_weights = self._effective_weights(code, preds.keys())
        total_w = sum(eff_weights.values())
        if total_w > 0:
            score = sum(preds[h] * eff_weights.get(h, 0.0) for h in preds.keys()) / total_w
        else:
            score = float(np.mean(list(preds.values())))

        shap_top = self._explain(code, feature_vec, primary_h=self.primary_horizon)

        return PredictionResult(
            code=code,
            trade_date=trade_date,
            predictions=preds,
            shap_top=shap_top,
            feature_snapshot={k: float(v) for k, v in feature_vec.items() if pd.notna(v)},
            horizon_score=float(score),
        )

    # ── horizon 加权 ───────────────────────────────────

    def _model_quality_score(self, cv_avg_ic: float, cv_hit_rate: float) -> float:
        """质量门控：同时满足 IC 和方向命中率才有效。

        - IC ≤ 0 → 无预测力，返回 0
        - hit_rate < 0.49 且 IC > 0 → 方向系统性反转，返回 0
        - 通过门控 → 返回原始 IC（保持权重量级不变，floor 逻辑在调用处）
        """
        if cv_avg_ic <= 0.0:
            return 0.0
        if cv_hit_rate < 0.49:
            return 0.0
        return cv_avg_ic

    def _effective_weights(
        self, code: str, horizons
    ) -> dict[int, float]:
        """根据 ic_weighting 配置返回最终 horizon 权重 dict。

        ic_weighting=True：用 max(0, quality_score - ic_floor) 作为权重；
          quality_score 为 0 当 IC ≤ 0 或方向命中率 < 49%（反转信号）。
          若全 horizon 权重均为 0，返回零权重（不产生信号），而非回退静态权重。
        ic_weighting=False：使用 self.horizon_weights。
        """
        horizons = list(horizons)
        if not self.ic_weighting:
            return {h: self.horizon_weights.get(h, 0.0) for h in horizons}

        models = self.artifacts.get(code, {})
        ic_w: dict[int, float] = {}
        for h in horizons:
            art = models.get(h)
            ic = float(getattr(art, "cv_avg_ic", 0.0) or 0.0) if art is not None else 0.0
            hit = float(getattr(art, "cv_hit_rate", 0.5) or 0.5) if art is not None else 0.5
            effective_ic = self._model_quality_score(ic, hit)
            ic_w[h] = max(0.0, effective_ic - self.ic_floor)

        if sum(ic_w.values()) < 1e-9:
            return {h: 0.0 for h in horizons}
        return ic_w

    def effective_weights_for(self, code: str) -> dict[int, float]:
        """供外部诊断：返回归一化后的 horizon 权重（sum=1）。"""
        models = self.artifacts.get(code, {})
        raw = self._effective_weights(code, models.keys())
        s = sum(raw.values())
        if s <= 0:
            return raw
        return {h: w / s for h, w in raw.items()}

    # ── helpers ─────────────────────────────────

    def _align_features(
        self, feature_vec: pd.Series, expected_cols: list[str]
    ) -> np.ndarray | None:
        """按训练时的列序对齐，缺失列填 0。"""
        if not expected_cols:
            return None
        arr = np.zeros(len(expected_cols), dtype=float)
        for i, col in enumerate(expected_cols):
            v = feature_vec.get(col)
            if v is None or (isinstance(v, float) and not np.isfinite(v)):
                arr[i] = 0.0
            else:
                try:
                    arr[i] = float(v)
                except (TypeError, ValueError):
                    arr[i] = 0.0
        return arr

    def _explain(
        self, code: str, feature_vec: pd.Series, primary_h: int
    ) -> list[tuple[str, float]]:
        """计算单条样本的 SHAP top-k。"""
        models = self.artifacts.get(code, {})
        art = models.get(primary_h) or next(iter(models.values()), None)
        if art is None:
            return []
        x = self._align_features(feature_vec, art.feature_cols)
        if x is None:
            return []
        try:
            booster: xgb.Booster = art.model.get_booster()
            dmat = xgb.DMatrix(x.reshape(1, -1), feature_names=art.feature_cols)
            shap_vals = booster.predict(dmat, pred_contribs=True)
            # shap_vals 形状 (1, n_features + 1)，最后一列是 bias
            contribs = shap_vals[0][:-1]
            order = np.argsort(np.abs(contribs))[::-1][: self.shap_top_k]
            return [
                (art.feature_cols[i], float(contribs[i]))
                for i in order
                if abs(contribs[i]) > 0
            ]
        except Exception as e:
            logger.warning(f"SHAP 计算失败 code={code}: {e}")
            return []
