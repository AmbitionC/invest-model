"""截面联合训练器（Phase 2）。

使用所有股票的截面数据联合训练 XGBoost 排序模型：
  - 不再对每只股票单独训练，而是按日期将多只股票数据合并
  - 标签：未来 5 日收益在截面内的排名百分位（0-1）
  - 模型目标：``rank:pairwise`` —— 学"谁比谁强"，不学绝对涨幅
  - Walk-forward CV：按日期滚动切分，避免信息泄漏

样本量估算：
  10 只股票 × 1000 交易日 = 10,000 条 / 20 特征 = 500:1（vs 现有 16.7:1）

使用方式：
    trainer = CrossSectionalTrainer(engine)
    result = trainer.fit(stock_data, version='v2_cs')

    # stock_data: dict[code -> {'X': pd.DataFrame, 'y_5d': pd.Series, 'name': str}]
    # X 的列为 RAW_FEATURE_COLUMNS
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.ml.raw_features import RAW_FEATURE_COLUMNS

logger = get_logger()

HORIZON = 5  # 预测 5 日截面排名


@dataclass
class CSTrainResult:
    """截面模型训练结果。"""
    version: str
    n_stocks: int
    n_samples: int
    cv_folds: int
    cv_ic_mean: float
    cv_ic_std: float
    cv_hit_rate: float
    feature_importance: dict[str, float]
    train_start: str
    train_end: str
    model: object = field(repr=False, default=None)
    feature_cols: list[str] = field(default_factory=list)


class CrossSectionalTrainer:
    """截面联合 XGBoost 排序训练器。

    接受多只股票的原始特征矩阵（RAW_FEATURE_COLUMNS），拼成截面数据集，
    以未来5日收益截面排名为标签，训练 rank:pairwise 模型。

    Parameters
    ----------
    engine : Engine
        数据库连接（用于保存结果到 model_registry）。
    n_splits : int
        Walk-forward CV 折数，每折包含 train_window + val_window 交易日。
    train_window : int
        每折训练集交易日数。
    val_window : int
        每折验证集交易日数。
    min_stocks_per_date : int
        每日截面至少需要的股票数量；不满足时跳过该日。
    xgb_params : dict | None
        XGBoost 超参数覆盖。
    """

    DEFAULT_XGB_PARAMS = {
        "objective": "rank:pairwise",
        "eval_metric": "map",
        "max_depth": 4,
        "min_child_weight": 10,
        "learning_rate": 0.05,
        "n_estimators": 200,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "gamma": 0.1,
        "reg_alpha": 0.5,
        "reg_lambda": 2.0,
        "tree_method": "hist",
        "seed": 42,
    }

    def __init__(
        self,
        engine: Engine,
        n_splits: int = 5,
        train_window: int = 800,
        val_window: int = 80,
        min_stocks_per_date: int = 2,
        xgb_params: dict | None = None,
    ):
        self.engine = engine
        self.n_splits = n_splits
        self.train_window = train_window
        self.val_window = val_window
        self.min_stocks_per_date = min_stocks_per_date
        self.xgb_params = {**self.DEFAULT_XGB_PARAMS, **(xgb_params or {})}

    def fit(
        self,
        stock_data: dict[str, dict],
        version: str = "v2_cs",
        save: bool = True,
    ) -> CSTrainResult:
        """训练截面模型。

        Parameters
        ----------
        stock_data : dict[code -> {'X': pd.DataFrame, 'y': dict[horizon->pd.Series], 'name': str}]
            来自 run_cross_sectional_train.py 的特征+标签字典。
            X 的索引为 trade_date，列为 RAW_FEATURE_COLUMNS 或其子集。
            y 的键为 horizon（5），值为对应的前向收益 Series。
        version : str
            模型版本标识。
        save : bool
            是否将模型 artifacts 保存到 model_registry 表。
        """
        import xgboost as xgb
        from scipy.stats import spearmanr

        # 1. 将所有股票数据拼成截面数据集
        panel = self._build_panel(stock_data)
        if panel.empty:
            raise ValueError("截面数据集为空，无法训练")

        dates = sorted(panel["trade_date"].unique())
        n_dates = len(dates)
        n_stocks = panel["code"].nunique()
        logger.info(
            f"[CrossSectionalTrainer] 截面数据集: {n_dates} 个交易日 × {n_stocks} 只股票 "
            f"= {len(panel)} 条样本（{version}）"
        )

        feat_cols = [c for c in RAW_FEATURE_COLUMNS if c in panel.columns]

        # 2. Walk-forward CV
        cv_ics: list[float] = []
        cv_hits: list[float] = []

        fold_size = self.train_window + self.val_window
        step = max(1, (n_dates - fold_size) // max(self.n_splits - 1, 1))

        for fold_idx in range(self.n_splits):
            start_idx = fold_idx * step
            if start_idx + fold_size > n_dates:
                break

            train_dates = dates[start_idx: start_idx + self.train_window]
            val_dates   = dates[start_idx + self.train_window: start_idx + fold_size]

            if not train_dates or not val_dates:
                continue

            X_tr, y_tr, g_tr = self._filter_panel(panel, train_dates, feat_cols)
            X_val, y_val, g_val = self._filter_panel(panel, val_dates, feat_cols)

            if len(X_tr) < 50 or len(X_val) < 10:
                continue

            # 训练
            model = xgb.XGBRanker(**self.xgb_params, verbosity=0)
            model.fit(X_tr, y_tr, group=g_tr)

            # 验证 IC（Spearman）
            y_pred = model.predict(X_val)
            ic, _ = spearmanr(y_pred, y_val.values)
            ic = float(ic) if not math.isnan(ic) else 0.0

            # Hit rate：预测正方向 vs 实际方向（>截面均值算涨）
            med = float(np.median(y_val.values))
            actual_pos = (y_val.values > med).astype(int)
            pred_pos   = (y_pred > np.median(y_pred)).astype(int)
            hit = float((actual_pos == pred_pos).mean())

            cv_ics.append(ic)
            cv_hits.append(hit)
            logger.info(
                f"  fold {fold_idx+1}/{self.n_splits}: "
                f"train={len(X_tr)} val={len(X_val)} IC={ic:+.4f} hit={hit:.2%}"
            )

        cv_ic_mean = float(np.mean(cv_ics)) if cv_ics else 0.0
        cv_ic_std  = float(np.std(cv_ics))  if cv_ics else 0.0
        cv_hit_rate = float(np.mean(cv_hits)) if cv_hits else 0.5

        logger.info(
            f"[CrossSectionalTrainer] CV完成: IC_mean={cv_ic_mean:+.4f} "
            f"IC_std={cv_ic_std:.4f} hit={cv_hit_rate:.2%}"
        )

        # 3. 全量重训（用全部数据训练最终模型）
        X_all, y_all, g_all = self._filter_panel(panel, dates, feat_cols)
        final_model = xgb.XGBRanker(**self.xgb_params, verbosity=0)
        final_model.fit(X_all, y_all, group=g_all)

        # 特征重要度
        imp = final_model.feature_importances_
        feat_imp = {feat_cols[i]: float(imp[i]) for i in range(len(feat_cols))}

        # 排序显示
        top_feats = sorted(feat_imp.items(), key=lambda x: -x[1])[:10]
        logger.info(f"[CrossSectionalTrainer] Top-10特征重要度: {top_feats}")

        result = CSTrainResult(
            version=version,
            n_stocks=n_stocks,
            n_samples=len(panel),
            cv_folds=len(cv_ics),
            cv_ic_mean=cv_ic_mean,
            cv_ic_std=cv_ic_std,
            cv_hit_rate=cv_hit_rate,
            feature_importance=feat_imp,
            train_start=str(dates[0]),
            train_end=str(dates[-1]),
            model=final_model,
            feature_cols=feat_cols,
        )

        if save:
            self._save_model(result)

        return result

    # ── 截面数据构造 ─────────────────────────────────

    def _build_panel(self, stock_data: dict[str, dict]) -> pd.DataFrame:
        """将 stock_data 拼成截面 panel DataFrame。

        列：trade_date, code, [feat_cols...], label_rank (y)
        label_rank = 当日截面内未来5日收益的排名百分位（0=最差，1=最强）。
        """
        frames: list[pd.DataFrame] = []
        for code, d in stock_data.items():
            X: pd.DataFrame = d.get("X")
            y_dict: dict = d.get("y", {})
            if X is None or X.empty:
                continue
            y = y_dict.get(HORIZON)
            if y is None or y.empty:
                continue

            df = X.copy()
            df.index = df.index.astype(str)
            df["code"] = code
            df["trade_date"] = df.index
            df["fwd_ret"] = pd.to_numeric(y, errors="coerce")
            frames.append(df.reset_index(drop=True))

        if not frames:
            return pd.DataFrame()

        panel = pd.concat(frames, ignore_index=True)
        panel = panel.dropna(subset=["fwd_ret"])

        # 截面排名：每日内对 fwd_ret 排名，转为 0-1 百分位
        panel["label_rank"] = (
            panel.groupby("trade_date")["fwd_ret"]
            .rank(method="average", pct=True)
        )

        # 过滤截面内股票数量不足的日期
        date_counts = panel.groupby("trade_date")["code"].count()
        valid_dates = date_counts[date_counts >= self.min_stocks_per_date].index
        panel = panel[panel["trade_date"].isin(valid_dates)]

        return panel.sort_values(["trade_date", "code"]).reset_index(drop=True)

    def _filter_panel(
        self,
        panel: pd.DataFrame,
        dates: list[str],
        feat_cols: list[str],
    ) -> tuple[np.ndarray, pd.Series, list[int]]:
        """从 panel 中提取指定日期集的特征矩阵、标签和 group 大小。"""
        sub = panel[panel["trade_date"].isin(set(dates))].copy()
        sub = sub.sort_values(["trade_date", "code"]).reset_index(drop=True)

        X = sub[feat_cols].fillna(0).values
        y = sub["label_rank"]

        # group = 每个交易日的股票数（XGBoost rank 模式需要）
        groups = sub.groupby("trade_date")["code"].count().values.tolist()

        return X, y, groups

    # ── 模型持久化 ─────────────────────────────────

    def _save_model(self, result: CSTrainResult) -> None:
        """将模型文件 + 元信息行写入 ml_model_registry。

        使用 code='__crosssectional__' 标识截面模型，与 per-stock 模型共享同一张表，
        但以特殊 code 前缀区分，便于 list_registry 过滤。
        """
        try:
            import json
            import pandas as pd
            from invest_model.ml.persistence import ensure_table, get_model_dir
            from invest_model.repositories.base import BaseRepository

            ensure_table(self.engine)

            # 保存模型文件（XGBRanker 也支持 save_model JSON 格式）
            safe_version = result.version.replace("/", "_").replace(".", "_")
            model_file = get_model_dir() / f"xgb___crosssectional___h{HORIZON}_{safe_version}.json"
            result.model.save_model(str(model_file))

            row = {
                "code": "__crosssectional__",
                "horizon": HORIZON,
                "version": result.version,
                "train_start": result.train_start,
                "train_end": result.train_end,
                "n_samples": result.n_samples,
                "n_features": len(result.feature_cols),
                "feature_cols": json.dumps(result.feature_cols, ensure_ascii=False),
                "cv_avg_ic": round(result.cv_ic_mean, 5),
                "cv_avg_rmse": 0.0,
                "cv_hit_rate": round(result.cv_hit_rate, 4),
                "cv_metrics": json.dumps({
                    "cv_ic_std": result.cv_ic_std,
                    "n_stocks": result.n_stocks,
                    "cv_folds": result.cv_folds,
                    "objective": "rank:pairwise",
                }, ensure_ascii=False),
                "model_path": str(model_file),
            }

            df = pd.DataFrame([row])
            repo = BaseRepository(self.engine)
            repo.upsert("ml_model_registry", df, unique_keys=["code", "horizon", "version"])

            logger.info(
                f"[CrossSectionalTrainer] 模型已保存: {model_file.name} "
                f"IC={result.cv_ic_mean:+.4f} hit={result.cv_hit_rate:.2%}"
            )
        except Exception as e:
            logger.error(f"[CrossSectionalTrainer] 模型保存失败: {e}")


class CrossSectionalPredictor:
    """使用截面模型对当日所有股票打分，输出相对强弱排名。

    典型用法（在 daily_pipeline 信号生成后调用）：
        predictor = CrossSectionalPredictor(engine, version='v2_cs')
        scores = predictor.predict_today(raw_features_dict, trade_date)
        # scores: dict[code -> percentile_score (0-1)]
    """

    def __init__(self, engine: Engine, version: str = "v2_cs"):
        self.engine = engine
        self.version = version
        self._model = None
        self._feat_cols: list[str] = []

    def _ensure_model(self) -> bool:
        """惰性加载截面模型（XGBRanker）。返回 True 表示成功。"""
        if self._model is not None:
            return True
        try:
            import json
            import xgboost as xgb
            from invest_model.ml.persistence import ensure_table, get_model_dir
            from invest_model.repositories.base import BaseRepository

            ensure_table(self.engine)
            repo = BaseRepository(self.engine)
            df = repo.read_sql(
                "SELECT model_path, feature_cols FROM ml_model_registry "
                "WHERE code = '__crosssectional__' AND horizon = :h AND version = :v "
                "ORDER BY created_at DESC LIMIT 1",
                {"h": HORIZON, "v": self.version},
            )
            if df.empty:
                logger.warning(f"[CSPredictor] 无截面模型 version={self.version}")
                return False

            model_path = str(df["model_path"].iloc[0])
            feat_raw = df["feature_cols"].iloc[0]
            self._feat_cols = json.loads(feat_raw) if isinstance(feat_raw, str) else (feat_raw or [])

            import os
            if not os.path.exists(model_path):
                logger.error(f"[CSPredictor] 模型文件缺失: {model_path}")
                return False

            # 截面模型使用 XGBRanker（与 XGBRegressor 文件格式相同，均为 XGBoost JSON）
            model = xgb.XGBRanker()
            model.load_model(model_path)
            self._model = model
            logger.info(f"[CSPredictor] 截面模型加载成功: {model_path}")
            return True
        except Exception as e:
            logger.error(f"[CSPredictor] 加载模型失败: {e}")
            return False

    def predict_today(
        self,
        raw_features: dict[str, pd.Series],
        trade_date: str,
    ) -> dict[str, float]:
        """对当日截面打分。

        Parameters
        ----------
        raw_features : dict[code -> pd.Series]
            每只股票的原始特征向量（索引为 RAW_FEATURE_COLUMNS）。
        trade_date : str
            当前交易日（仅用于日志）。

        Returns
        -------
        dict[code -> float]
            每只股票在截面内的预测强弱分（越高越强）。
        """
        import xgboost as xgb

        if not self._ensure_model():
            return {}

        codes = list(raw_features.keys())
        if not codes:
            return {}

        rows = []
        for code in codes:
            feat_vec = raw_features[code]
            row = [feat_vec.get(c, 0.0) for c in self._feat_cols]
            rows.append(row)

        X = np.array(rows, dtype=float)
        X = np.nan_to_num(X, nan=0.0)

        try:
            preds = self._model.predict(X)
            # 转为截面排名百分位
            from scipy.stats import rankdata
            ranks = rankdata(preds, method="average") / len(preds)
            result = {code: float(ranks[i]) for i, code in enumerate(codes)}
            logger.info(
                f"[CSPredictor] {trade_date} 截面打分完成: "
                f"top={max(result, key=result.get)} "
                f"bottom={min(result, key=result.get)}"
            )
            return result
        except Exception as e:
            logger.error(f"[CSPredictor] 推理失败 {trade_date}: {e}")
            return {}
