"""宏观数据层（P54）测试：长表 melt / 读数还原 / 基数校正 / 通胀计。

不连 tushare、不连生产库——用合成帧与临时 sqlite。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402
from invest_model.signals.macro import (  # noqa: E402
    chen_readings,
    inflation_gauge,
    load_panel,
    pick,
    seasonality,
    yoy_from_level,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from ingest_macro import _q_to_month, melt_frame  # noqa: E402


@pytest.fixture()
def repo(tmp_path):
    e = make_engine(f"sqlite:///{tmp_path}/m.db")
    create_schema(e)
    return BaseRepository(e)


# ── melt：不写死列名 ────────────────────────────────────────

def test_melt_is_column_name_agnostic():
    """接口返回什么数值列就落什么，文本列自动排除——这是长表设计的全部意义。"""
    df = pd.DataFrame({"month": ["202401", "202402"], "m1_yoy": [5.9, 1.2],
                       "m2_yoy": [8.7, 8.3], "ts_code": ["X", "X"]})
    out = melt_frame(df, "cn_m", "M")
    assert set(out.series) == {"cn_m.m1_yoy", "cn_m.m2_yoy"}     # 文本列 ts_code 不入库
    assert out.freq.unique().tolist() == ["M"]
    assert float(out[(out.period == "202401") & (out.series == "cn_m.m1_yoy")].value.iloc[0]) == 5.9

    # 换个从没见过的列名，照样落库、无需改代码或改表
    df2 = pd.DataFrame({"month": ["202401"], "brand_new_metric": [42.0]})
    assert melt_frame(df2, "cn_m", "M").series.tolist() == ["cn_m.brand_new_metric"]


def test_melt_handles_missing_period_key_and_dates():
    assert melt_frame(pd.DataFrame({"a": [1]}), "x", "M").empty      # 无时间键 → 跳过
    d = pd.DataFrame({"trade_date": ["2024-01-02"], "1y": [3.45]})
    assert melt_frame(d, "shibor_lpr", "D").period.iloc[0] == "20240102"


def test_quarter_key_maps_to_quarter_end_month():
    assert _q_to_month("2024Q1") == "202403"
    assert _q_to_month("2024Q4") == "202412"


# ── 读数还原 ────────────────────────────────────────────────

def _seed(repo, rows):
    repo.upsert("macro_series", pd.DataFrame(rows), ["period", "series"])


def test_panel_and_alias_lookup(repo):
    _seed(repo, [{"period": "202401", "series": "cn_m.m1_yoy", "value": 5.9,
                  "freq": "M", "source": "cn_m"},
                 {"period": "202402", "series": "cn_m.m1_yoy", "value": 1.2,
                  "freq": "M", "source": "cn_m"}])
    panel = load_panel(repo)
    assert list(panel.index) == ["202401", "202402"]
    assert float(pick(panel, "m1_yoy").iloc[-1]) == 1.2
    assert pick(panel, "m2_yoy") is None            # 缺就是 None，不猜不填充


def test_readings_report_unavailable_when_empty(repo):
    assert chen_readings(repo)["available"] is False


def test_scissors_and_credit_growth(repo):
    rows = []
    for i, (m1, m2) in enumerate([(5.9, 8.7), (1.2, 8.3), (-1.0, 7.0)]):
        p = f"20240{i + 1}"
        rows += [{"period": p, "series": "cn_m.m1_yoy", "value": m1, "freq": "M", "source": "cn_m"},
                 {"period": p, "series": "cn_m.m2_yoy", "value": m2, "freq": "M", "source": "cn_m"}]
    # 社融存量：13 个月，年化增速 10%
    for i in range(13):
        p = f"{2023 + i // 12}{(i % 12) + 1:02d}"
        rows.append({"period": p, "series": "cn_sf.stk_endperiod",
                     "value": 100.0 * (1.10 ** (i / 12)), "freq": "M", "source": "cn_sf"})
    _seed(repo, rows)
    r = chen_readings(repo)
    assert r["available"] is True
    assert r["m1_m2_scissors"]["value"] == pytest.approx(-8.0)      # −1.0 − 7.0
    assert r["sf_stock_yoy"]["value"] == pytest.approx(10.0, abs=0.1)


def test_yoy_from_level_needs_enough_history():
    s = pd.Series([1.0] * 5, index=[f"20240{i}" for i in range(1, 6)])
    assert yoy_from_level(s, periods=12) is None


# ── 基数校正（他判「假开门红」的那一步）───────────────────────

def test_seasonality_detects_january_m1_drop():
    """构造"过去十年 1 月 M1 环比均减少"，看季节性函数是否读得出来。"""
    idx, vals, v = [], [], 100.0
    for y in range(2014, 2025):
        for m in range(1, 13):
            v += -5.0 if m == 1 else 1.0
            idx.append(f"{y}{m:02d}")
            vals.append(v)
    s = pd.Series(vals, index=idx)
    jan = seasonality(s, 1)
    assert jan["up_rate"] == 0.0 and jan["median_mom"] < 0      # 1 月历史上必减
    assert seasonality(s, 6)["up_rate"] == 1.0                  # 6 月必增
    assert seasonality(pd.Series([1.0, 2.0]), 1) is None        # 样本不足 → None


# ── 通胀计（名义 vs 实际 GDP 金叉）────────────────────────────

def _gdp_panel(nominal_growth: float, real_yoy: float) -> pd.DataFrame:
    rows = []
    lvl = 100.0
    for i in range(10):
        p = f"{2023 + i // 4}{((i % 4) + 1) * 3:02d}"
        rows += [{"period": p, "series": "cn_gdp.gdp", "value": lvl},
                 {"period": p, "series": "cn_gdp.gdp_yoy", "value": real_yoy}]
        lvl *= (1 + nominal_growth) ** 0.25
    return pd.DataFrame(rows).pivot_table(index="period", columns="series",
                                          values="value", aggfunc="last").sort_index()


def test_inflation_gauge_cross():
    hot = inflation_gauge(_gdp_panel(0.08, 5.0))     # 名义 8% > 实际 5% ⟹ 金叉
    assert hot["cross"] is True and hot["gap"] == pytest.approx(3.0, abs=0.2)
    cold = inflation_gauge(_gdp_panel(0.03, 5.0))    # 名义 3% < 实际 5% ⟹ 平减指数为负
    assert cold["cross"] is False and cold["gap"] < 0
    assert inflation_gauge(pd.DataFrame()) is None


def test_inflation_gauge_is_a_reading_not_a_signal():
    """守住边界：读数层不得输出任何仓位/方向字段。"""
    r = inflation_gauge(_gdp_panel(0.08, 5.0))
    assert not (set(r) & {"action", "position", "signal", "weight", "direction", "buy", "sell"})
