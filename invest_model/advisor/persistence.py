"""顾问信号 + 校准画像持久化。"""

from __future__ import annotations

import json
from typing import Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from invest_model.advisor.advisor import AdvisorSignal
from invest_model.advisor.calibration import CalibrationProfile
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, trade_date),
    INDEX idx_trade_date (trade_date),
    INDEX idx_confidence (trade_date, confidence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ensure_table(engine: Engine) -> None:
    """确保 stock_advisor_signal 表存在，不存在则创建。"""
    try:
        with engine.begin() as conn:
            conn.execute(text(_CREATE_TABLE_SQL))
    except Exception as e:
        logger.warning(f"ensure_table 异常(可能表已存在): {e}")


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


# ── 校准表 ──

_CALIBRATION_TABLE = "stock_advisor_calibration"

_CREATE_CALIBRATION_SQL = """
CREATE TABLE IF NOT EXISTS stock_advisor_calibration (
    code VARCHAR(16) NOT NULL,
    calibrated_at VARCHAR(8) NOT NULL,
    window_days INT,
    composite_mean DECIMAL(8,5),
    composite_std DECIMAL(8,5),
    composite_p75 DECIMAL(8,5),
    composite_p90 DECIMAL(8,5),
    composite_p95 DECIMAL(8,5),
    abs_composite_values JSON,
    action_threshold INT DEFAULT 60,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ensure_calibration_table(engine: Engine) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(_CREATE_CALIBRATION_SQL))
    except Exception:
        pass


def save_calibration_profiles(
    engine: Engine, profiles: dict[str, CalibrationProfile]
) -> int:
    if not profiles:
        return 0
    ensure_calibration_table(engine)
    rows = []
    for code, p in profiles.items():
        rows.append({
            "code": p.code,
            "calibrated_at": p.calibrated_at,
            "window_days": p.window_days,
            "composite_mean": p.composite_mean,
            "composite_std": p.composite_std,
            "composite_p75": p.composite_p75,
            "composite_p90": p.composite_p90,
            "composite_p95": p.composite_p95,
            "abs_composite_values": json.dumps(
                [round(v, 5) for v in p.abs_values]
            ),
            "action_threshold": p.action_threshold,
        })
    df = pd.DataFrame(rows)
    repo = BaseRepository(engine)
    return repo.upsert(_CALIBRATION_TABLE, df, unique_keys=["code"])


def load_calibration_profiles(engine: Engine) -> dict[str, CalibrationProfile]:
    ensure_calibration_table(engine)
    base = BaseRepository(engine)
    try:
        df = base.read_sql(f"SELECT * FROM {_CALIBRATION_TABLE}", {})
    except Exception:
        return {}
    if df.empty:
        return {}

    profiles: dict[str, CalibrationProfile] = {}
    for _, row in df.iterrows():
        abs_vals_raw = row.get("abs_composite_values", "[]")
        if isinstance(abs_vals_raw, str):
            abs_vals = json.loads(abs_vals_raw)
        else:
            abs_vals = []
        profiles[row["code"]] = CalibrationProfile(
            code=row["code"],
            calibrated_at=str(row["calibrated_at"]),
            window_days=int(row.get("window_days", 0)),
            composite_mean=float(row.get("composite_mean", 0)),
            composite_std=float(row.get("composite_std", 0)),
            composite_p75=float(row.get("composite_p75", 0)),
            composite_p90=float(row.get("composite_p90", 0)),
            composite_p95=float(row.get("composite_p95", 0)),
            abs_values=abs_vals,
            action_threshold=int(row.get("action_threshold", 60)),
        )
    return profiles
