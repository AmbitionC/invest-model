"""ML 顾问信号持久化。

兼容旧 schema（保留 composite/tech_score 等列），同时通过 ALTER TABLE 增加 ML 字段：
- target_position / current_position / delta_position
- horizon_score / safety_margin / take_profit
- pred_3d / pred_5d / pred_10d
"""

from __future__ import annotations

import json
from typing import Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.advisor.advisor import AdvisorSignal
from invest_model.repositories.base import BaseRepository
from invest_model.logger import get_logger

logger = get_logger()

TABLE = "stock_advisor_signal"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_advisor_signal (
    code VARCHAR(16) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    action VARCHAR(16) NOT NULL,
    confidence INT NOT NULL,
    position_pct DECIMAL(5,4),
    composite DECIMAL(8,5),
    tech_score DECIMAL(8,5),
    fund_score DECIMAL(8,5),
    flow_score DECIMAL(8,5),
    sent_score DECIMAL(8,5),
    triggers JSON,
    attribution TEXT,
    target_position DECIMAL(6,4),
    current_position DECIMAL(6,4),
    delta_position DECIMAL(6,4),
    horizon_score DECIMAL(10,6),
    safety_margin DECIMAL(6,4),
    take_profit TINYINT DEFAULT 0,
    pred_3d DECIMAL(10,6),
    pred_5d DECIMAL(10,6),
    pred_10d DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_confidence (trade_date, confidence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

_ML_COLUMN_ALTERS: list[str] = [
    "ALTER TABLE stock_advisor_signal ADD COLUMN target_position DECIMAL(6,4)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN current_position DECIMAL(6,4)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN delta_position DECIMAL(6,4)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN horizon_score DECIMAL(10,6)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN safety_margin DECIMAL(6,4)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN take_profit TINYINT DEFAULT 0",
    "ALTER TABLE stock_advisor_signal ADD COLUMN pred_3d DECIMAL(10,6)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN pred_5d DECIMAL(10,6)",
    "ALTER TABLE stock_advisor_signal ADD COLUMN pred_10d DECIMAL(10,6)",
]


def _safe_alter(engine: Engine) -> None:
    """逐条 ALTER，忽略已存在错误。"""
    for sql in _ML_COLUMN_ALTERS:
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
        except Exception as e:
            msg = str(e)
            if "Duplicate column name" in msg or "exists" in msg.lower():
                continue
            logger.debug(f"ALTER 跳过: {sql} ({msg[:100]})")


def ensure_table(engine: Engine) -> None:
    """确保 stock_advisor_signal 表存在，并补齐 ML 字段。"""
    try:
        with engine.begin() as conn:
            conn.execute(text(_CREATE_TABLE_SQL))
    except Exception as e:
        logger.warning(f"ensure_table 异常(可能表已存在): {e}")
    _safe_alter(engine)


def _table_exists(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SELECT 1 FROM {TABLE} LIMIT 0"))
        return True
    except Exception:
        return False


def save_advisor_signals(engine: Engine, signals: Sequence[AdvisorSignal]) -> int:
    if not signals:
        return 0
    ensure_table(engine)
    rows = []
    for s in signals:
        rows.append({
            "code": s.code,
            "trade_date": s.trade_date,
            "action": s.action,
            "confidence": s.confidence,
            "position_pct": s.position_pct,
            "composite": s.composite,
            "tech_score": s.sub_scores.get("tech_score", 0),
            "fund_score": s.sub_scores.get("fund_score", 0),
            "flow_score": s.sub_scores.get("flow_score", 0),
            "sent_score": s.sub_scores.get("sent_score", 0),
            "triggers": json.dumps(s.triggers, ensure_ascii=False),
            "attribution": s.attribution,
            "target_position": s.target_position,
            "current_position": s.current_position,
            "delta_position": s.delta_position,
            "horizon_score": s.horizon_score,
            "safety_margin": s.safety_margin,
            "take_profit": int(bool(s.take_profit)),
            "pred_3d": s.sub_scores.get("pred_3d"),
            "pred_5d": s.sub_scores.get("pred_5d"),
            "pred_10d": s.sub_scores.get("pred_10d"),
        })
    df = pd.DataFrame(rows)
    repo = BaseRepository(engine)
    return repo.upsert(TABLE, df, unique_keys=["code", "trade_date"])


def get_advisor_history(
    engine: Engine,
    codes: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    if not _table_exists(engine):
        ensure_table(engine)
        return pd.DataFrame()
    base = BaseRepository(engine)
    placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
    params = {f"c{i}": c for i, c in enumerate(codes)}
    params.update({"s": start_date, "e": end_date})
    try:
        return base.read_sql(
            f"""
            SELECT * FROM {TABLE}
            WHERE code IN ({placeholders}) AND trade_date BETWEEN :s AND :e
            ORDER BY trade_date, code
            """,
            params,
        )
    except Exception as e:
        logger.warning(f"get_advisor_history 查询失败: {e}")
        return pd.DataFrame()


def get_latest_advisor_signals(engine: Engine, trade_date: str) -> pd.DataFrame:
    if not _table_exists(engine):
        ensure_table(engine)
        return pd.DataFrame()
    base = BaseRepository(engine)
    try:
        return base.read_sql(
            f"SELECT * FROM {TABLE} WHERE trade_date = :d ORDER BY confidence DESC",
            {"d": trade_date},
        )
    except Exception as e:
        logger.warning(f"get_latest_advisor_signals 查询失败: {e}")
        return pd.DataFrame()


# 注：旧的 stock_advisor_calibration 表与 CalibrationProfile 已废弃，
# 由 ml_model_registry + xgb_*.json 模型文件取代（见 invest_model/ml/persistence.py）。
