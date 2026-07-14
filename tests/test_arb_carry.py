"""套利 carry 纯函数单测 + 红利筛选可交易性硬闸（B股/ST 剔除）。"""

import pandas as pd
from sqlalchemy import create_engine

from invest_model.arb.carry import (
    build_carry_signals,
    dividend_carry_net,
    double_low,
    reverse_repo_carry,
)
from invest_model.arb.config import ArbConfig


def test_reverse_repo_carry_thursday_three_days():
    c = reverse_repo_carry(2.0, 3)      # 2% 年化、周四计息3天
    assert c["interest_days"] == 3
    assert abs(c["effective_yield"] - 0.02 * 3 / 365) < 1e-12


def test_reverse_repo_carry_defaults_min_one_day():
    c = reverse_repo_carry(None, 0)
    assert c["interest_days"] == 1
    assert c["effective_yield"] == 0.0


def test_dividend_carry_long_hold_tax_free():
    cfg = ArbConfig()
    r = dividend_carry_net(5.0, 400, cfg)     # >1年
    assert r["tax"] == cfg.tax_long
    assert abs(r["expected_net"] - 0.05) < 1e-12


def test_dividend_carry_short_hold_taxed():
    cfg = ArbConfig()
    r = dividend_carry_net(5.0, 10, cfg)      # <1月
    assert r["tax"] == cfg.tax_short
    assert abs(r["expected_net"] - 0.05 * (1 - 0.20)) < 1e-12


def test_double_low_score_and_premium():
    # 转股价10、正股12 → 转股价值120；转债价格115 → 折价
    r = double_low(115.0, 10.0, 12.0)
    assert abs(r["conv_value"] - 120.0) < 1e-9
    assert r["conv_premium"] < 0                      # 折价
    assert abs(r["score"] - (115.0 + r["conv_premium"] * 100)) < 1e-9


def test_double_low_lower_is_better():
    cheap = double_low(105.0, 10.0, 10.0)   # 平价、低价
    rich = double_low(140.0, 10.0, 10.0)    # 平价、高价
    assert cheap["score"] < rich["score"]


def test_double_low_invalid_inputs():
    r = double_low(0, 0, 0)
    assert r["score"] != r["score"]         # nan


# ── 红利筛选可交易性硬闸（E14 判据②前置：B股/ST 不得入防守底盘）──────────

def _div_db():
    """构造含 B股/ST/正常股 的最小库：stock_fundamental(dv_ratio) + stock_info。"""
    from invest_model.data import create_schema
    from invest_model.repositories.base import BaseRepository
    e = create_engine("sqlite:///:memory:")
    create_schema(e)
    repo = BaseRepository(e)
    rows = [
        # code, name, dv
        ("200521.SZ", "深XXB", 9.0),     # 深B：应剔除
        ("900905.SH", "沪XXB", 8.5),     # 沪B：应剔除
        ("600188.SH", "ST兖矿", 7.0),    # ST：应剔除
        ("601088.SH", "中国神华", 6.0),  # 正常：应入选
        ("600028.SH", "中国石化", 5.0),  # 正常：应入选
        ("601006.SH", "大秦铁路", 2.0),  # 低于 dv 下限 3.0：应剔除
    ]
    repo.upsert("stock_info", pd.DataFrame(
        [{"ts_code": c, "name": n} for c, n, _ in rows]), ["ts_code"])
    repo.upsert("stock_fundamental", pd.DataFrame(
        [{"code": c, "trade_date": "20260714", "dv_ratio": dv} for c, _, dv in rows]),
        ["code", "trade_date"])
    return e


def test_dividend_screen_excludes_b_shares_and_st():
    e = _div_db()
    out = build_carry_signals(e, "20260714", ArbConfig(), persist=False)
    assert "dividend_carry" in out
    picked = set(out["dividend_carry"]["code"])
    assert picked == {"601088.SH", "600028.SH"}        # 只剩正常高息股
    for bad in ("200521.SZ", "900905.SH", "600188.SH", "601006.SH"):
        assert bad not in picked
