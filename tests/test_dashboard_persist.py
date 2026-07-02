"""仪表盘落库层测试：新表建表/读写、盯盘预警幂等、老表补列。"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402

NEW_TABLES = ["watch_alert", "review_report", "fear_daily", "action_plan_account"]


def _engine(tmp_path, name="dash.db"):
    eng = make_engine(f"sqlite:///{tmp_path}/{name}")
    create_schema(eng)
    return eng


def test_new_tables_created(tmp_path):
    eng = _engine(tmp_path)
    repo = BaseRepository(eng)
    for t in NEW_TABLES:
        assert repo.table_exists(t), f"缺表 {t}"


def test_watch_alert_idempotent_upsert(tmp_path):
    from scripts.live_check import _persist_alerts

    eng = _engine(tmp_path)
    now = datetime(2026, 7, 1, 10, 30)
    items = [("H:600000.SH:破MA20，盘后确认清仓", "🔴 持仓 浦发银行 …", "crit"),
             ("W:000001.SZ:⚠️ 到回踩位，企稳放量则买点", "🟢 观察 平安银行 …", "batch"),
             ("ETFSELF:20260701:2", "🔎 ETF实时自检：2/2 取到现价", "batch")]
    _persist_alerts(eng, now, items)
    _persist_alerts(eng, now, items)  # 重跑不重复
    repo = BaseRepository(eng)
    df = repo.read_sql("SELECT * FROM watch_alert ORDER BY dedup_key")
    assert len(df) == 3
    by_key = {r["dedup_key"]: r for _, r in df.iterrows()}
    hold = by_key["H:600000.SH:破MA20，盘后确认清仓"]
    assert hold["kind"] == "hold" and hold["code"] == "600000.SH"
    assert hold["severity"] == "crit" and hold["alert_date"] == "20260701"
    watch = by_key["W:000001.SZ:⚠️ 到回踩位，企稳放量则买点"]
    assert watch["kind"] == "watch" and watch["code"] == "000001.SZ"
    self_check = by_key["ETFSELF:20260701:2"]
    assert self_check["kind"] == "selfcheck" and pd.isna(self_check["code"])


def test_review_report_roundtrip(tmp_path):
    from scripts.review import persist_review

    eng = _engine(tmp_path)
    repo = BaseRepository(eng)
    persist_review(repo, "20260701", "weekly", "# 复盘报告 — 截至 20260701\n\n内容")
    persist_review(repo, "20260701", "weekly", "# 复盘报告 — 截至 20260701\n\n更新后")  # 幂等覆盖
    df = repo.read_sql("SELECT * FROM review_report")
    assert len(df) == 1
    row = df.iloc[0]
    assert row["report_date"] == "20260701" and row["period"] == "weekly"
    assert "更新后" in row["markdown"]
    assert "generated_at" in json.loads(row["meta"])


def test_fear_daily_roundtrip(tmp_path):
    from scripts.fear_gauge import persist_fear

    eng = _engine(tmp_path)
    g = {"date": "20260701", "score": 62.5, "level": "偏恐慌",
         "components": {"动量": 70, "波动率": 55}, "raw": {"idx_chg": -0.012}}
    persist_fear(eng, g)
    df = BaseRepository(eng).read_sql("SELECT * FROM fear_daily")
    assert len(df) == 1
    assert float(df["score"].iloc[0]) == 62.5
    assert json.loads(df["components"].iloc[0])["动量"] == 70


def test_action_plan_account_upsert(tmp_path):
    eng = _engine(tmp_path)
    repo = BaseRepository(eng)
    acct = {"plan_date": "20260701", "equity": 100000.0, "invested_pct": 0.8,
            "cash_pct": 0.2, "n_holdings": 5, "unrealized_pnl_pct": 0.05,
            "gross_target": 0.9, "risk_off": 0, "model_ic_mean": 0.03,
            "model_ic_ir": 0.5, "model_hit": 0.55, "model_conf_label": "中"}
    repo.upsert("action_plan_account", pd.DataFrame([acct]), ["plan_date"])
    repo.upsert("action_plan_account",
                pd.DataFrame([{**acct, "n_holdings": 6}]), ["plan_date"])
    df = repo.read_sql("SELECT * FROM action_plan_account")
    assert len(df) == 1 and int(df["n_holdings"].iloc[0]) == 6


def test_action_plan_column_patch_on_old_table(tmp_path):
    """老库已有 11 列版 action_plan：create_schema 应通过 _COLUMN_PATCHES 补 3 列。"""
    eng = make_engine(f"sqlite:///{tmp_path}/old.db")
    with eng.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE action_plan ("
            "plan_date VARCHAR(8), code VARCHAR(16), name VARCHAR(32), "
            "action VARCHAR(12), cur_weight NUMERIC, tgt_weight NUMERIC, "
            "shares_delta NUMERIC, reason VARCHAR(64), stop_price NUMERIC, "
            "ref_price NUMERIC, grade VARCHAR(2), "
            "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, "
            "PRIMARY KEY (plan_date, code))")
    create_schema(eng)
    repo = BaseRepository(eng)
    row = {"plan_date": "20260701", "code": "600000.SH", "name": "浦发银行",
           "action": "watch", "cur_weight": 0.0, "tgt_weight": 0.0,
           "shares_delta": 0.0, "reason": "观察", "stop_price": None,
           "ref_price": 12.3, "grade": "A", "trigger_hint": "回踩≈12.0 / 突破>13.1",
           "model_rank": 0.92, "model_view": "看好 前8% ★★★"}
    repo.upsert("action_plan", pd.DataFrame([row]), ["plan_date", "code"])
    df = repo.read_sql("SELECT trigger_hint, model_rank, model_view FROM action_plan")
    assert df["trigger_hint"].iloc[0].startswith("回踩")
    assert float(df["model_rank"].iloc[0]) == 0.92
    assert "★" in df["model_view"].iloc[0]
