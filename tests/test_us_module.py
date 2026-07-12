"""美股模块单元测试——全离线：sqlite + 合成数据，不打网、不依赖 yfinance。"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.us import config as C  # noqa: E402
from invest_model.us import fundamentals as F  # noqa: E402
from invest_model.us import options as O  # noqa: E402
from invest_model.us import signals as S  # noqa: E402


# ── 信号 ────────────────────────────────────────────────────

def test_ma_trend_above_below():
    up = pd.Series(np.linspace(100, 200, 260))
    down = pd.Series(np.linspace(200, 100, 260))
    assert S.ma_trend(up) == "above"
    assert S.ma_trend(down) == "below"
    assert S.ma_trend(pd.Series([1.0] * 10)) == "above"   # 数据不足不惩罚


def test_vix_regime_and_dip_window():
    assert S.vix_regime(15) == "calm"
    assert S.vix_regime(25) == "alert"
    assert S.vix_regime(35) == "panic"
    assert S.vix_regime(None) == "alert"                  # 缺数据保守
    closes = pd.Series([100.0] * 200 + [80.0])            # 回撤 20%
    dd = S.drawdown_from_high(closes)
    assert abs(dd - 0.2) < 1e-9
    assert S.dip_window(35, dd) is True                   # 恐慌+深回撤
    assert S.dip_window(25, dd) is False                  # VIX 不够
    assert S.dip_window(35, 0.05) is False                # 回撤不够


def test_selling_puts_gate():
    assert S.selling_puts_allowed(18, "above") is True
    assert S.selling_puts_allowed(35, "above") is False   # 恐慌停卖
    assert S.selling_puts_allowed(18, "below") is False   # 破线停卖


# ── 基本面 ───────────────────────────────────────────────────

def _q(ni_seq, fcf=None, nd=None, gm=None):
    n = len(ni_seq)
    return pd.DataFrame({
        "quarter_end": [f"202{4 + i // 4}{(i % 4) * 3 + 1:02d}30" for i in range(n)],
        "net_income": ni_seq,
        "revenue": [x * 10 for x in ni_seq],
        "fcf": fcf if fcf is not None else ni_seq,
        "net_debt": nd if nd is not None else [0] * n,
        "gross_margin": gm if gm is not None else [0.5] * n,
        "ni_yoy": [None] * n,
    })


def test_growth_accel_detects_deceleration():
    # 8 季：前 4 季基数 100，同比 +80%×3 季后最后一季 +50%（减速 -30pp）
    q = _q([100, 100, 100, 100, 180, 180, 180, 150])
    accel = F.growth_accel(q)
    assert accel is not None and accel < 0
    assert F.growth_accel(_q([100, 100, 100, 100, 100])) is None  # 不足 6 季


def test_mine_probes():
    ni = [100] * 8
    assert F.mine_probes(_q(ni, fcf=[20] * 8)) != []              # FCF 背离
    assert "净债务连续两季上升" in F.mine_probes(_q(ni, nd=[1, 2, 3, 4, 5, 6, 7, 8]))
    assert "毛利率连续两季下滑" in F.mine_probes(
        _q(ni, gm=[0.5, 0.5, 0.5, 0.5, 0.5, 0.48, 0.45, 0.40]))
    assert F.mine_probes(_q(ni)) == []                            # 干净公司零红旗


def test_certainty_grade_rules():
    g, why = F.certainty_grade(0.05, [], 0.30)
    assert g == "A" and "US-F3" in why
    g, _ = F.certainty_grade(-0.02, [], 0.30)
    assert g == "B"
    g, why = F.certainty_grade(-0.30, [], 0.30)                   # 失速
    assert g == "C" and "US-F1" in why
    g, why = F.certainty_grade(0.05, ["FCF与净利背离"], 0.30)       # 红旗一票否决
    assert g == "C" and "US-F2" in why
    g, _ = F.certainty_grade(None, [], None)                      # 完全无数据
    assert g == "C"
    # 深度不足（yfinance仅5-6季）：同比正+零红旗 → B（仅挡A不挡B）
    g, why = F.certainty_grade(None, [], 0.20)
    assert g == "B" and "深度不足" in why


# ── 期权打分 ─────────────────────────────────────────────────

def _chain(close=50.0):
    rows = []
    for strike in (40, 45, 47.5, 50, 55):
        rows.append({"code": "KO", "strategy_side": "put", "expiry": "20260814",
                     "dte": 30, "strike": strike, "bid": 0.6, "ask": 0.8,
                     "iv": 0.25, "open_interest": 500})
    for strike in (52.5, 55, 60):
        rows.append({"code": "KO", "strategy_side": "call", "expiry": "20260814",
                     "dte": 30, "strike": strike, "bid": 0.5, "ask": 0.7,
                     "iv": 0.22, "open_interest": 400})
    return pd.DataFrame(rows)


def test_score_csp_filters_and_ranks():
    got = O.score_csp(_chain(), close=50.0, budget=6000.0)
    assert not got.empty
    # 全额担保 ≤ 预算、安全边际 ≥5%、绝无高于现价的行权价
    assert (got["collateral"] <= 6000).all()
    assert (got["safety_margin"] >= C.OPT_MIN_SAFETY).all()
    assert (got["strike"] < 50).all()
    # 排序：安全边际优先（确定性>收益率）
    assert got.iloc[0]["strike"] == got["strike"].min()
    assert "US-O1" in got.iloc[0]["reason"]


def test_score_csp_quality_gate_and_budget():
    assert O.score_csp(_chain(), 50.0, 6000.0, quality_ok=False).empty  # C级不卖
    assert O.score_csp(_chain(), 50.0, 3000.0).empty                    # 担保买不起


def test_score_cc_never_locks_loss():
    got = O.score_cc(_chain(), close=50.0, cost_price=54.0, shares=100)
    assert not got.empty
    assert (got["strike"] >= 54.0).all()      # 行权价 ≥ 成本，不锁死亏损
    assert O.score_cc(_chain(), 50.0, 54.0, shares=50).empty   # 不足100股


# ── 计划端到端（sqlite 合成库，注入合成期权链）────────────────────

@pytest.fixture()
def us_db(tmp_path, monkeypatch):
    db = f"sqlite:///{tmp_path}/us.db"
    from invest_model.data import create_schema, make_engine
    engine = make_engine(db)
    create_schema(engine)
    repo = BaseRepository(engine)
    dates = pd.bdate_range("2025-07-01", periods=260).strftime("%Y%m%d")
    rows = []
    for code, base, slope in (("SPY", 500, 0.3), ("QQQ", 480, 0.3),
                              ("KO", 50, 0.0), ("^VIX", 16, 0.0)):
        for i, d in enumerate(dates):
            px = base + slope * i
            rows.append({"code": code, "trade_date": d, "open": px, "high": px,
                         "low": px, "close": px, "volume": 1000})
    repo.upsert("us_stock_daily", pd.DataFrame(rows), ["code", "trade_date"])
    repo.upsert("us_stock_info", pd.DataFrame([
        {"code": "SPY", "name": "SPDR S&P 500", "kind": "etf", "sector": "", "sleeve_hint": "core"},
        {"code": "QQQ", "name": "Invesco QQQ", "kind": "etf", "sector": "", "sleeve_hint": "core"},
        {"code": "KO", "name": "Coca-Cola", "kind": "stock", "sector": "Staples",
         "sleeve_hint": "satellite"},
    ]), ["code"])
    # KO 8 季干净增长（B 级：同比正、未加速）
    q = _q([100, 100, 100, 100, 130, 130, 130, 130])
    q["code"] = "KO"
    q["ni_yoy"] = [None] * 4 + [0.3] * 4
    q["revenue_yoy"] = q["ni_yoy"]
    repo.upsert("us_fundamental_q", q, ["code", "quarter_end"])
    repo.upsert("us_account_snapshot", pd.DataFrame([{
        "snapshot_date": str(dates[-1]), "cash": 20000.0,
        "market_value": 0.0, "total_asset": 20000.0}]), ["snapshot_date"])
    monkeypatch.setenv("INVEST_DB_URL", db)
    return engine


def test_build_plan_end_to_end(us_db):
    from invest_model.us.plan import build_plan
    res = build_plan(us_db, fetch_chain=lambda code: _chain())
    assert res["plan"].startswith("ok:")
    repo = BaseRepository(us_db)
    plan = repo.read_sql("SELECT * FROM us_action_plan")
    assert not plan.empty
    core = plan[plan["sleeve"] == "core"]
    assert len(core) == 1 and core.iloc[0]["code"] == C.CORE_ETF
    assert float(core.iloc[0]["target_value"]) == pytest.approx(20000 * 0.5, rel=0.01)
    sat = plan[(plan["sleeve"] == "satellite") & (plan["code"] == "KO")]
    assert len(sat) == 1 and sat.iloc[0]["grade"] == "B"
    assert float(sat.iloc[0]["target_value"]) == pytest.approx(20000 * 0.15 / 2, rel=0.01)
    # 造血：B 级 + $50 股担保 $4750 ≤ $6000 预算 → 应有 CSP 候选
    opts = repo.read_sql("SELECT * FROM us_option_candidate")
    assert not opts.empty and (opts["strategy"] == "csp").any()
    acct = repo.read_sql("SELECT * FROM us_plan_account")
    assert acct.iloc[0]["spy_trend"] == "above"
    assert acct.iloc[0]["vix_regime"] == "calm"
    md = res["markdown"]
    assert "核心锚" in md and "期权造血" in md and "US-C1" in md


def test_build_plan_panic_pauses_puts(us_db):
    repo = BaseRepository(us_db)
    last = repo.read_sql("SELECT MAX(trade_date) d FROM us_stock_daily")["d"].iloc[0]
    repo.upsert("us_stock_daily", pd.DataFrame([{
        "code": "^VIX", "trade_date": str(last), "open": 38, "high": 38,
        "low": 38, "close": 38, "volume": 0}]), ["code", "trade_date"])
    from invest_model.us.plan import build_plan
    res = build_plan(us_db, fetch_chain=lambda code: _chain())
    repo2 = BaseRepository(us_db)
    income = repo2.read_sql(
        "SELECT * FROM us_action_plan WHERE sleeve='income'")
    assert any("US-O4" in str(r) for r in income["reason"]), "恐慌应暂停新卖put"
    assert res["options"] == 0


def test_schema_contains_us_tables(us_db):
    repo = BaseRepository(us_db)
    for t in ("us_stock_daily", "us_stock_info", "us_fundamental_q",
              "us_option_candidate", "us_action_plan", "us_plan_account",
              "us_account_snapshot", "us_current_holding"):
        assert repo.table_exists(t), t
