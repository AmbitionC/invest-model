"""人生导师·股票投资专题内化测试（提案 P7/P8/P9 + 可解释性）。

覆盖：排雷 7 规则打分器、4 个候选影子因子、下跌二分法闸、收益三来源标签、
因子归因分解。出处与验证链接见 docs/rulebook.md。
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine
from invest_model.factors.library import CANDIDATE_FACTORS, compute_factors
from invest_model.model.combiner import ICCombiner
from invest_model.repositories.base import BaseRepository
from invest_model.signals.buypoint import BuyPointConfig, _earnings_deteriorated
from invest_model.universe.quality_screen import (QualityConfig, build_quality_flags,
                                                  screen_cross_section)


@pytest.fixture()
def eng(tmp_path):
    e = make_engine(f"sqlite:///{tmp_path}/t.db")
    create_schema(e)
    return e


# ── 排雷打分器（对齐 verification/profit-vs-cash-and-fraud-screen 的干净/问题对照）──

def _fina_frame() -> pd.DataFrame:
    """干净公司 vs 问题公司（乐视/獐子岛型）+ 行业中位参照组。"""
    rows = {
        # 5 只同业干净公司撑起行业中位（毛利率 ~30%）
        **{f"CLEAN{i}.SZ": dict(industry="制造", gross_margin=28 + i, accounts_receiv=15,
                                revenue=100, goodwill=5, eq_exc_min=80, n_income=12,
                                n_income_attr_p=11, n_cashflow_act=11,
                                fee_ratio=0.20, fee_ratio_prev=0.20) for i in range(5)},
        # 净利润为正但归母 ≫ 净利润（2 倍 > 1.5 上限）、经营现金流为负 → R5/R6 双触发
        "DIRTY0.SZ": dict(industry="农林牧渔", gross_margin=55, accounts_receiv=65,
                          revenue=100, goodwill=40, eq_exc_min=70, n_income=3,
                          n_income_attr_p=6, n_cashflow_act=-4,
                          fee_ratio=0.12, fee_ratio_prev=0.20),
    }
    return pd.DataFrame(rows).T


def test_fraud_screen_clean_vs_dirty():
    res = screen_cross_section(_fina_frame())
    assert res.loc["CLEAN0.SZ", "n_flags"] == 0, res.loc["CLEAN0.SZ", "flags"]
    # 问题公司：应收/商誉/归母异号/现金背离/三费骤降 5 条 + 免税行业放大器 ≥6
    assert res.loc["DIRTY0.SZ", "n_flags"] >= 5, res.loc["DIRTY0.SZ", "flags"]
    joined = "；".join(res.loc["DIRTY0.SZ", "flags"])
    for kw in ("应收", "商誉", "归母", "现金", "三费", "免税"):
        assert kw in joined, f"缺红旗关键词 {kw}: {joined}"


def test_fraud_screen_industry_calibrated_margin():
    """毛利率红旗按行业内比较：行业整体高毛利不误伤，个股显著超行业中位才旗。"""
    rows = {f"BJ{i}.SH": dict(industry="白酒", gross_margin=85 + (30 if i == 0 else 0),
                              revenue=100, accounts_receiv=10, goodwill=0, eq_exc_min=100,
                              n_income=30, n_income_attr_p=29, n_cashflow_act=30,
                              fee_ratio=0.2, fee_ratio_prev=0.2) for i in range(6)}
    res = screen_cross_section(pd.DataFrame(rows).T)
    assert res.loc["BJ1.SH", "n_flags"] == 0          # 行业性高毛利：不旗
    assert res.loc["BJ0.SH", "n_flags"] == 1          # 显著超行业中位：旗


def test_fraud_screen_missing_columns_skip():
    """缺列（无 stock_fina_ext 数据）时对应规则整体跳过，不误旗。"""
    fina = pd.DataFrame({"A.SZ": dict(industry="电子", gross_margin=30)}).T
    res = screen_cross_section(fina)
    assert res.loc["A.SZ", "n_flags"] == 0


def test_build_quality_flags_persists(eng):
    repo = BaseRepository(eng)
    repo.upsert("stock_info", pd.DataFrame([
        {"ts_code": "000001.SZ", "name": "干净", "industry": "制造"},
        {"ts_code": "000002.SZ", "name": "问题", "industry": "软件"},
    ]), ["ts_code"])
    repo.upsert("stock_fina_ext", pd.DataFrame([
        dict(code="000001.SZ", report_date="20250331", ann_date="20250425",
             goodwill=5, minority_int=1, eq_exc_min=80, accounts_receiv=15, revenue=100,
             n_income=12, n_income_attr_p=11, sell_exp=8, admin_exp=8, fin_exp=4,
             n_cashflow_act=11),
        dict(code="000002.SZ", report_date="20250331", ann_date="20250425",
             goodwill=40, minority_int=1, eq_exc_min=70, accounts_receiv=65, revenue=100,
             n_income=-2, n_income_attr_p=6, sell_exp=5, admin_exp=5, fin_exp=2,
             n_cashflow_act=-4),
    ]), ["code", "report_date"])
    res = build_quality_flags(eng, "20250601", ["000001.SZ", "000002.SZ"])
    assert res.loc["000002.SZ", "n_flags"] >= 4
    assert res.loc["000001.SZ", "n_flags"] == 0
    saved = repo.read_sql("SELECT * FROM quality_flag WHERE trade_date='20250601'")
    assert len(saved) == 2 and saved["n_flags"].max() >= 4


# ── 候选影子因子（P8）──

def test_compute_factors_new_candidates_present():
    for f in ("growth_accel", "bp_ex_goodwill", "dividend_yield", "insider_conviction"):
        assert f in CANDIDATE_FACTORS


def test_compute_factors_new_candidate_values():
    raw = pd.DataFrame({
        "pe_ttm": [10.0, 20.0], "pb": [1.0, 2.0], "ps_ttm": [1.0, 2.0],
        "roe": [15.0, 8.0], "roa": [8.0, 4.0], "gross_margin": [30.0, 20.0],
        "revenue_yoy": [10.0, 5.0], "profit_yoy": [20.0, -10.0],
        "mom_60": [0.1, -0.1], "mom_120": [0.2, -0.2], "ret_5": [0.01, -0.01],
        "vol_20": [0.02, 0.03], "circ_mv": [1e6, 2e6], "turnover_rate": [1.0, 2.0],
        "industry": ["电子", "医药"],
        # 新候选原料
        "q_profit_accel": [12.0, -30.0],
        "eq_exc_min": [8e9, 7e9], "goodwill": [1e9, 4e9], "total_mv": [1e6, 1e6],  # 万元
        "dv_ttm": [3.0, 0.5],
        "insider_conviction": [4.0, np.nan],
    }, index=["A.SZ", "B.SZ"])
    out = compute_factors(raw)
    assert out.loc["A.SZ", "growth_accel"] == 12.0
    # 扣商誉 BP：(80亿−10亿)/(1e6万元×1e4=100亿) = 0.7
    assert abs(out.loc["A.SZ", "bp_ex_goodwill"] - 0.7) < 1e-9
    assert abs(out.loc["B.SZ", "bp_ex_goodwill"] - 0.3) < 1e-9
    assert out.loc["A.SZ", "dividend_yield"] == 3.0
    assert out.loc["A.SZ", "insider_conviction"] == 4.0
    assert np.isnan(out.loc["B.SZ", "insider_conviction"])


# ── 下跌二分法（P9）──

def test_earnings_deteriorated_gate(eng):
    repo = BaseRepository(eng)
    repo.upsert("stock_fina_indicator", pd.DataFrame([
        # 双负：业绩驱动下跌 → 禁抄
        dict(code="BAD.SZ", report_date="20250331", ann_date="20250425",
             profit_yoy=-25.0, q_profit_yoy=-40.0),
        # 累计正、单季负：不确认（单期波动）
        dict(code="MIX.SZ", report_date="20250331", ann_date="20250425",
             profit_yoy=15.0, q_profit_yoy=-5.0),
        # 双正：健康
        dict(code="OK.SZ", report_date="20250331", ann_date="20250425",
             profit_yoy=30.0, q_profit_yoy=25.0),
    ]), ["code", "report_date"])
    bad = _earnings_deteriorated(repo, "20250601", ["BAD.SZ", "MIX.SZ", "OK.SZ", "NA.SZ"])
    assert bad == {"BAD.SZ"}


def test_buypoint_config_defaults():
    cfg = BuyPointConfig()
    assert cfg.fear_fundamental_gate is True     # P9 默认开启（只收紧 fear 特例分支）


# ── 可解释性：因子归因分解 + 收益三来源 ──

def test_combiner_contributions_format(eng):
    comb = ICCombiner(eng)
    expo = pd.DataFrame({"ep": [1.0, -1.0], "mom_60": [0.5, 2.0], "roe": [0.0, 0.1]},
                        index=["A.SZ", "B.SZ"])
    w = pd.Series({"ep": 0.8, "mom_60": 0.4, "roe": 0.2})
    attr = comb.contributions(expo, w)
    assert attr["A.SZ"].startswith("ep+0.80")
    assert "mom_60+0.80" in attr["B.SZ"] and "ep-0.80" in attr["B.SZ"]


def test_return_sources_labels():
    from invest_model.orchestration.action_plan import _return_sources
    expo = pd.DataFrame({
        "profit_yoy": [1.2, -0.5, -0.2, 0.0],
        "ep": [0.0, 1.0, 0.2, 0.1],
        "bp": [0.0, 0.4, 0.1, 0.0],
        "dividend_yield": [np.nan, np.nan, 1.5, np.nan],
    }, index=["G.SZ", "V.SZ", "D.SZ", "N.SZ"])
    src = _return_sources(expo)
    assert src["G.SZ"] == "成长"
    assert src["V.SZ"] == "修复"
    assert src["D.SZ"] == "红利"
    assert "N.SZ" not in src                      # 无鲜明定位不硬贴标签


def test_fmt_attr_compact():
    from invest_model.orchestration.action_plan import _fmt_attr
    # 因子代码 → 中文展示（用户可读；未知代码原样保留兜底）
    assert _fmt_attr("ep+0.82|mom_60+1.15|roe-0.31") == "低PE↑、中期动量↑、净资产收益↓"
    assert _fmt_attr("unknown_f+0.5") == "unknown_f↑"
    assert _fmt_attr(None) == ""
