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
from invest_model.us import valuation as V  # noqa: E402


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


def test_selling_puts_mode():
    assert S.selling_puts_mode(18, "above") == "normal"
    assert S.selling_puts_mode(35, "above") == "strict"   # 恐慌切strict（不停卖）
    assert S.selling_puts_mode(18, "below") == "strict"   # 破线切strict


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


# ── 估值锚（V2，全哥"买股票=买公司=算回本周期"）──────────────────

def test_payback_years_and_verdict():
    # 市值1000、净现金100、FCF 60、净利90 → EV=900、保守取60 → 15年=cheap边界
    pb = V.payback_years(1000, 100, 60, 90)
    assert pb == 15.0 and V.verdict(pb) == "cheap"
    assert V.verdict(20.0) == "fair"
    assert V.verdict(30.0) == "expensive"
    assert V.payback_years(1000, 0, -5, 10) == float("inf")   # 保守取负FCF→不赚真钱
    assert V.verdict(float("inf")) == "expensive"
    assert V.verdict(None) == "unknown"
    assert V.payback_years(1000, 1200, 10, 10) == 0.0         # 市值<净现金：白送


def test_anchor_price():
    assert V.anchor_price(100.0, 30.0) == 50.0    # 回本30年→接盘价打对折
    assert V.anchor_price(100.0, 12.0) == 100.0   # 已cheap→锚=现价
    assert V.anchor_price(100.0, float("inf")) is None  # 不赚真钱没有心甘情愿价


def test_chase_high_flag():
    rally = pd.Series([100.0] * 100 + list(np.linspace(100, 190, 152)))  # 距低点+90%
    assert V.chase_high_flag(rally, "fair") is True
    assert V.chase_high_flag(rally, "cheap") is False   # cheap 豁免（周期股底部翻倍）
    flat = pd.Series([100.0] * 252)
    assert V.chase_high_flag(flat, "expensive") is False


def test_mine_probes_v2_capex_and_ocf():
    ni = [100] * 8
    q = _q(ni)
    q["capex"] = [150] * 8                      # capex=1.5×净利 连续
    assert any("capex黑洞" in f for f in F.mine_probes(q))
    q2 = _q(ni)
    q2["ocf"] = [50] * 6 + [-10, -20]           # 经营现金流连续两季为负
    assert any("经营现金流" in f for f in F.mine_probes(q2))


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


def test_score_csp_anchor_caps_strike():
    """V2：行权价必须 ≤ 心甘情愿接盘价（估值锚），strict 再打九折。"""
    got = O.score_csp(_chain(), 50.0, 6000.0, anchor=45.0)
    assert not got.empty and (got["strike"] <= 45.0).all()   # 47.5 被锚排除
    strict = O.score_csp(_chain(), 50.0, 6000.0, anchor=45.0, mode="strict")
    assert strict.empty or (strict["strike"] <= 40.5).all()  # 45×0.9
    assert O.score_csp(_chain(), 50.0, 6000.0, anchor=None, mode="strict").empty  # 无锚恐慌不卖


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
    repo.upsert("us_valuation", pd.DataFrame([{
        "code": "KO", "asof": str(dates[-1]), "market_cap": 200_000_000_000,
        "net_cash": 0, "fcf_ttm": 16_700_000_000, "ni_ttm": 17_000_000_000,
        "payback_years": 12.0, "verdict": "cheap", "anchor_price": 50.0,
        "chase_high": 0}]), ["code", "asof"])
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


def test_build_plan_panic_strict_mode(us_db):
    """V2：恐慌不停卖——切 strict（只允许 cheap 档+接盘锚九折以下行权价）。
    KO 是 cheap 档、锚 50 → strict cap=45 → 40/45 两档 put 仍可入选。"""
    repo = BaseRepository(us_db)
    last = repo.read_sql("SELECT MAX(trade_date) d FROM us_stock_daily")["d"].iloc[0]
    repo.upsert("us_stock_daily", pd.DataFrame([{
        "code": "^VIX", "trade_date": str(last), "open": 38, "high": 38,
        "low": 38, "close": 38, "volume": 0}]), ["code", "trade_date"])
    from invest_model.us.plan import build_plan
    res = build_plan(us_db, fetch_chain=lambda code: _chain())
    repo2 = BaseRepository(us_db)
    income = repo2.read_sql("SELECT * FROM us_action_plan WHERE sleeve='income'")
    assert any("strict" in str(r) for r in income["reason"]), "恐慌应切strict模式"
    opts = repo2.read_sql(
        "SELECT * FROM us_option_candidate WHERE strategy='csp'")
    assert not opts.empty, "strict下cheap档仍应有候选（情绪化最重=期权最好时机）"
    assert (opts["strike"] <= 45.0 + 1e-9).all(), "strict行权价必须≤锚×0.9"


def test_build_plan_expensive_blocks_buy(us_db):
    """V2 估值闸：expensive 档即使 B 级也不 buy（高于估值绝不碰）。"""
    repo = BaseRepository(us_db)
    last = repo.read_sql("SELECT MAX(asof) d FROM us_valuation")["d"].iloc[0]
    repo.upsert("us_valuation", pd.DataFrame([{
        "code": "KO", "asof": str(last), "market_cap": 200_000_000_000,
        "net_cash": 0, "fcf_ttm": 5_000_000_000, "ni_ttm": 6_000_000_000,
        "payback_years": 40.0, "verdict": "expensive", "anchor_price": 18.75,
        "chase_high": 0}]), ["code", "asof"])
    from invest_model.us.plan import build_plan
    build_plan(us_db, fetch_chain=lambda code: _chain())
    repo2 = BaseRepository(us_db)
    sat = repo2.read_sql(
        "SELECT * FROM us_action_plan WHERE sleeve='satellite' AND code='KO'")
    assert sat.iloc[0]["action"] == "watch"
    assert "US-V1" in str(sat.iloc[0]["reason"])


def test_schema_contains_us_tables(us_db):
    repo = BaseRepository(us_db)
    for t in ("us_stock_daily", "us_stock_info", "us_fundamental_q",
              "us_option_candidate", "us_action_plan", "us_plan_account",
              "us_account_snapshot", "us_current_holding", "us_valuation"):
        assert repo.table_exists(t), t
