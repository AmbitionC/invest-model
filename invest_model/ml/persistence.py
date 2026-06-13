"""ML 模型持久化。

存储策略：
1. 模型文件：以 JSON 形式落到 {project_root}/models/xgb_{code}_h{horizon}.json
2. 元信息表 ml_model_registry：记录每次训练的 IC / RMSE / 训练区间 / 特征列
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import xgboost as xgb
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.logger import get_logger
from invest_model.ml.trainer import HorizonResult, StockTrainResult
from invest_model.repositories.base import BaseRepository

logger = get_logger()

TABLE = "ml_model_registry"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ml_model_registry (
    code VARCHAR(16) NOT NULL,
    horizon INT NOT NULL,
    version VARCHAR(32) NOT NULL,
    train_start VARCHAR(8) NOT NULL,
    train_end VARCHAR(8) NOT NULL,
    n_samples INT NOT NULL,
    n_features INT NOT NULL,
    feature_cols JSON,
    cv_avg_ic DECIMAL(8,5),
    cv_avg_rmse DECIMAL(8,5),
    cv_hit_rate DECIMAL(6,4),
    cv_metrics JSON,
    model_path VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, horizon, version),
    INDEX idx_version (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ML 模型注册表'
"""


@dataclass
class ModelArtifact:
    """加载到内存的模型 + 元信息。"""
    code: str
    horizon: int
    model: xgb.XGBRegressor
    feature_cols: list[str]
    train_end: str
    cv_avg_ic: float = 0.0
    cv_hit_rate: float = 0.5


# ── 路径 ──────────────────────────────────────────


def get_model_dir() -> Path:
    """返回模型存储根目录（项目根 / models）。"""
    # invest_model/ml/persistence.py → 上推两层是 invest-model 根
    project_root = Path(__file__).resolve().parents[2]
    d = project_root / "models"
    d.mkdir(parents=True, exist_ok=True)
    return d


def model_path(code: str, horizon: int, version: str = "v1") -> Path:
    """返回单个模型文件路径（按 version 隔离，避免不同版本互相覆盖）。"""
    safe_code = code.replace(".", "_")
    safe_version = version.replace("/", "_").replace(".", "_")
    return get_model_dir() / f"xgb_{safe_code}_h{horizon}_{safe_version}.json"


# ── 表 ──────────────────────────────────────────


def _migrate_legacy_schema(engine: Engine) -> None:
    """检测旧 schema（PRIMARY KEY 不含 version），自动 DROP 后重建。

    旧主键 (code, horizon) 会让多 version 训练时彼此覆盖，必须升级。
    本函数 idempotent：新 schema 下不会再触发。
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                """
                SELECT COUNT(*) AS c FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'ml_model_registry'
                  AND INDEX_NAME = 'PRIMARY'
                  AND COLUMN_NAME = 'version'
                """
            )).fetchone()
        if row is None or row[0] != 0:
            return
        logger.warning(
            "ml_model_registry 检测到旧 schema (主键不含 version)，自动 DROP + 重建。"
            "已落库的旧模型记录将被清空，请重新训练。"
        )
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE ml_model_registry"))
    except Exception as e:
        logger.warning(f"_migrate_legacy_schema 异常: {e}")


def ensure_table(engine: Engine) -> None:
    try:
        _migrate_legacy_schema(engine)
        with engine.begin() as conn:
            conn.execute(text(_CREATE_SQL))
    except Exception as e:
        logger.warning(f"ensure ml_model_registry 异常: {e}")


# ── 写入 ──────────────────────────────────────────


def save_stock_result(
    engine: Engine,
    result: StockTrainResult,
    version: str = "v1",
) -> int:
    """落库单票全 horizon 训练结果，并写出模型文件。"""
    if not result.horizons:
        return 0
    ensure_table(engine)

    rows: list[dict] = []
    for h, hr in result.horizons.items():
        if hr.final_model is None:
            continue
        path = model_path(result.code, h, version=version)
        hr.final_model.save_model(str(path))

        cv_summary = [
            {
                "fold_id": m.fold_id,
                "n_train": m.n_train,
                "n_val": m.n_val,
                "rmse": round(m.rmse, 5),
                "ic": round(m.ic, 5),
                "hit_rate": round(m.hit_rate, 4),
                "best_iter": m.best_iter,
            }
            for m in hr.cv_metrics
        ]

        rows.append({
            "code": result.code,
            "horizon": h,
            "version": version,
            "train_start": hr.final_train_start or "",
            "train_end": hr.final_train_end or "",
            "n_samples": hr.final_n_samples,
            "n_features": hr.n_features,
            "feature_cols": json.dumps(hr.feature_columns, ensure_ascii=False),
            "cv_avg_ic": round(hr.avg_ic, 5),
            "cv_avg_rmse": round(hr.avg_rmse, 5) if np.isfinite(hr.avg_rmse) else 0.0,
            "cv_hit_rate": round(hr.avg_hit_rate, 4),
            "cv_metrics": json.dumps(cv_summary, ensure_ascii=False),
            "model_path": str(path),
        })

    if not rows:
        return 0
    df = pd.DataFrame(rows)
    repo = BaseRepository(engine)
    return repo.upsert(TABLE, df, unique_keys=["code", "horizon", "version"])


def save_results(
    engine: Engine,
    results: Iterable[StockTrainResult],
    version: str = "v1",
) -> int:
    total = 0
    for r in results:
        total += save_stock_result(engine, r, version=version)
    return total


# ── 读取 ──────────────────────────────────────────


def list_registry(
    engine: Engine,
    codes: list[str] | None = None,
    version: str | None = None,
) -> pd.DataFrame:
    """读取模型注册表元信息。"""
    ensure_table(engine)
    repo = BaseRepository(engine)
    where = []
    params: dict = {}
    if codes:
        ph = ", ".join([f":c{i}" for i in range(len(codes))])
        where.append(f"code IN ({ph})")
        params.update({f"c{i}": c for i, c in enumerate(codes)})
    if version:
        where.append("version = :v")
        params["v"] = version
    sql = f"SELECT * FROM {TABLE}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY code, horizon"
    try:
        return repo.read_sql(sql, params)
    except Exception:
        return pd.DataFrame()


def load_artifact(
    engine: Engine,
    code: str,
    horizon: int,
    version: str | None = None,
) -> ModelArtifact | None:
    """加载单个 (code, horizon) 模型。"""
    df = list_registry(engine, codes=[code], version=version)
    if df.empty:
        return None
    sub = df[df["horizon"] == horizon]
    if sub.empty:
        return None
    row = sub.iloc[0]
    path = row.get("model_path") or str(model_path(code, horizon, version=version or "v1"))
    if not os.path.exists(path):
        logger.warning(f"模型文件缺失: {path}")
        return None

    model = xgb.XGBRegressor()
    model.load_model(path)
    feat_raw = row.get("feature_cols")
    if isinstance(feat_raw, str):
        feature_cols = json.loads(feat_raw)
    elif isinstance(feat_raw, list):
        feature_cols = feat_raw
    else:
        feature_cols = []

    return ModelArtifact(
        code=code,
        horizon=horizon,
        model=model,
        feature_cols=feature_cols,
        train_end=str(row.get("train_end", "")),
        cv_avg_ic=float(row.get("cv_avg_ic", 0.0) or 0.0),
        cv_hit_rate=float(row.get("cv_hit_rate", 0.5) or 0.5),
    )


def load_all_artifacts(
    engine: Engine,
    codes: list[str],
    horizons: list[int],
    version: str | None = None,
) -> dict[str, dict[int, ModelArtifact]]:
    """加载多票多 horizon 的所有模型，缺失项自动跳过。

    Returns
    -------
    dict[code, dict[horizon, ModelArtifact]]
    """
    out: dict[str, dict[int, ModelArtifact]] = {}
    for code in codes:
        out[code] = {}
        for h in horizons:
            art = load_artifact(engine, code, h, version=version)
            if art is not None:
                out[code][h] = art
    return out
