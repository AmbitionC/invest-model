"""ML 信号模型层（XGBoost 多 horizon 收益预测）。

模块组成：
- features:    特征工程，从信号注册表 + 触发器 + 趋势 + 时序衍生 共约 60 维
- labels:      多 horizon 前瞻收益率标签 (3d/5d/10d)
- cv:          时序 walk-forward 交叉验证，含 embargo 防泄露
- trainer:     逐票 XGBoost 多 horizon 训练
- predictor:   推理 + SHAP 归因
- persistence: 模型文件读写 + ml_model_registry 表

设计目标：替代旧 advisor 模块的 ConfidenceEngine + CompositeScorer 加权 + 阶段判定。
"""

from invest_model.ml.features import (
    FeatureBuilder,
    FEATURE_COLUMNS,
    ETF_FEATURE_COLUMNS,
    SIGNAL_NAME_ORDER,
    TRIGGER_FEATURE_NAMES,
    TREND_FEATURE_NAMES,
    COMPOSITE_TS_NAMES,
    TECH_RAW_NAMES,
    effective_feature_columns,
)
from invest_model.ml.labels import (
    LABEL_HORIZONS,
    make_forward_returns,
)
from invest_model.ml.cv import PurgedWalkForwardSplit
from invest_model.ml.trainer import (
    PerStockMultiHorizonTrainer,
    StockTrainResult,
    HorizonResult,
    DEFAULT_PARAMS,
    ETF_PARAMS_OVERLAY,
)
from invest_model.ml.persistence import (
    save_stock_result,
    save_results,
    load_artifact,
    load_all_artifacts,
    list_registry,
    model_path,
    ModelArtifact,
)
from invest_model.ml.predictor import MLPredictor, PredictionResult

__all__ = [
    "FeatureBuilder",
    "FEATURE_COLUMNS",
    "ETF_FEATURE_COLUMNS",
    "SIGNAL_NAME_ORDER",
    "TRIGGER_FEATURE_NAMES",
    "TREND_FEATURE_NAMES",
    "COMPOSITE_TS_NAMES",
    "TECH_RAW_NAMES",
    "effective_feature_columns",
    "LABEL_HORIZONS",
    "make_forward_returns",
    "PurgedWalkForwardSplit",
    "PerStockMultiHorizonTrainer",
    "StockTrainResult",
    "HorizonResult",
    "DEFAULT_PARAMS",
    "ETF_PARAMS_OVERLAY",
    "save_stock_result",
    "save_results",
    "load_artifact",
    "load_all_artifacts",
    "list_registry",
    "model_path",
    "ModelArtifact",
    "MLPredictor",
    "PredictionResult",
]
