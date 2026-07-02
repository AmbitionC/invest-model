"""数据层增量测试：PIT ST 过滤（namechange）、北向候选因子影子机制。"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.data import create_schema, make_engine
from invest_model.factors.library import CANDIDATE_FACTORS, FACTORS
from invest_model.factors.loader import FactorDataLoader
from invest_model.factors.processor import process_factors
from invest_model.model.combiner import ICCombiner
from invest_model.repositories.base import BaseRepository
from invest_model.universe import UniverseBuilder, UniverseConfig


@pytest.fixture()
def eng(tmp_path):
    e = make_engine(f"sqlite:///{tmp_path}/t.db")
    create_schema(e)
    return e


def _seed_universe_day(repo, dt: str, codes: list[str]):
    repo.upsert("stock_daily", pd.DataFrame(
        [{"code": c, "trade_date": dt, "close": 10.0, "pct_chg": 0.0,
          "volume": 1000.0, "amount": 5000.0} for c in codes]),
        ["code", "trade_date"])
    repo.upsert("stock_fundamental", pd.DataFrame(
        [{"code": c, "trade_date": dt, "circ_mv": 500000.0} for c in codes]),
        ["code", "trade_date"])


def test_pit_st_filter_uses_historical_name(eng):
    """namechange 存在时，ST 过滤按 trade_date 当时的名称判定。

    000001.SZ 现名正常，但 2020-2021 期间名为 *ST平银：
    2021 年截面应剔除、2023 年截面（已摘帽）应保留。
    """
    repo = BaseRepository(eng)
    repo.upsert("stock_info", pd.DataFrame([
        {"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行", "list_date": "20100101"},
        {"ts_code": "000002.SZ", "name": "万科A", "industry": "地产", "list_date": "20100101"},
    ]), ["ts_code"])
    repo.upsert("stock_namechange", pd.DataFrame([
        {"ts_code": "000001.SZ", "name": "平安银行", "start_date": "20100101",
         "end_date": "20191231", "change_reason": "上市"},
        {"ts_code": "000001.SZ", "name": "*ST平银", "start_date": "20200101",
         "end_date": "20220101", "change_reason": "实施ST"},
        {"ts_code": "000001.SZ", "name": "平安银行", "start_date": "20220102",
         "end_date": None, "change_reason": "摘帽"},
    ]), ["ts_code", "start_date"])
    for dt in ("20210601", "20230601"):
        _seed_universe_day(repo, dt, ["000001.SZ", "000002.SZ"])

    ub = UniverseBuilder(eng, UniverseConfig(min_list_days=30))
    assert "000001.SZ" not in ub.build("20210601", persist=False), "当时是 *ST 应剔除"
    assert "000001.SZ" in ub.build("20230601", persist=False), "已摘帽应保留"
    assert "000002.SZ" in ub.build("20210601", persist=False)


def test_candidate_factor_shadow_not_scored(eng):
    """候选因子：processor 正常处理其暴露，但 combiner 打分不使用。"""
    df = pd.DataFrame({
        "mom_60": [0.1, 0.2, 0.3, 0.4],
        "nb_ratio_chg_20": [1.0, -1.0, 2.0, -2.0],
        "industry": ["A", "A", "B", "B"],
        "ln_circ_mv": [10.0, 11.0, 12.0, 13.0],
    }, index=["s1", "s2", "s3", "s4"])
    processed = process_factors(df, neutralize=False)
    assert "nb_ratio_chg_20" in processed.columns, "候选因子应被处理落库"

    comb = ICCombiner(eng)
    w = comb.weights("20240101")            # 无 IC 历史 → 等权先验（仅 FACTORS）
    assert set(w.index) == set(FACTORS)
    assert not set(CANDIDATE_FACTORS) & set(w.index), "候选因子不应有打分权重"
    s_with = comb.score(processed, w)
    s_without = comb.score(processed.drop(columns=["nb_ratio_chg_20"]), w)
    pd.testing.assert_series_equal(s_with, s_without)


def test_northbound_change_factor(eng):
    """nb_ratio_chg_20 = 窗口内北向持股占比首末差（百分点）。"""
    repo = BaseRepository(eng)
    dates = [d.strftime("%Y%m%d") for d in pd.bdate_range("2024-05-01", periods=16)]
    rows = [{"code": "600000.SH", "trade_date": d, "vol": 1e6,
             "ratio": 3.0 + 0.1 * i} for i, d in enumerate(dates)]
    repo.upsert("stock_hk_hold", pd.DataFrame(rows), ["code", "trade_date"])

    nb = FactorDataLoader(eng)._northbound(dates[-1], {"600000.SH"})
    assert not nb.empty
    assert nb.loc["600000.SH", "nb_ratio_chg_20"] == pytest.approx(1.5)


def test_northbound_absent_table_is_silent(tmp_path):
    """无 hk_hold 数据（本地合成库）→ 候选因子静默缺席，不影响任何下游。"""
    e = make_engine(f"sqlite:///{tmp_path}/bare.db")
    create_schema(e)
    nb = FactorDataLoader(e)._northbound("20240601", {"600000.SH"})
    assert nb.empty
