"""P4 组合层测试：缓冲区换手抑制（hold_buffer）+ 波动倒数加权（inv_vol）。"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.portfolio.constructor import PortfolioConfig, build_targets


def _scores(n: int = 40) -> pd.DataFrame:
    codes = [f"c{i:02d}" for i in range(1, n + 1)]
    return pd.DataFrame({"code": codes,
                         "score": [float(n - i) for i in range(n)]})


def test_hold_buffer_keeps_incumbents_in_buffer_zone():
    """已持有且排名在 top_n×1.5 内的票保留；掉出缓冲区的换出；名额按排名递补。"""
    cfg = PortfolioConfig(top_n=5, hold_buffer=1.5, industry_cap=None, max_weight=0.5)
    w = build_targets(_scores(), cfg, current_codes={"c06", "c20"})
    assert set(w) == {"c01", "c02", "c03", "c04", "c06"}, \
        "c06(排名6≤7.5缓冲)应保留、c20(排名20)应换出、前4名递补"


def test_hold_buffer_off_is_plain_topn():
    cfg = PortfolioConfig(top_n=5, hold_buffer=0.0, industry_cap=None, max_weight=0.5)
    w = build_targets(_scores(), cfg, current_codes={"c06", "c20"})
    assert set(w) == {"c01", "c02", "c03", "c04", "c05"}, "缓冲关闭时行为不变"


def test_hold_buffer_reduces_turnover():
    """排名小幅波动（第5名↔第6名互换）时，缓冲版不换仓、朴素版换仓。"""
    s1 = _scores(10)
    s2 = s1.copy()
    # 第5、6名互换分数
    s2.loc[s2["code"] == "c05", "score"] = 4.5
    s2.loc[s2["code"] == "c06", "score"] = 5.5
    cfg_buf = PortfolioConfig(top_n=5, hold_buffer=1.5, industry_cap=None, max_weight=0.5)
    cfg_plain = PortfolioConfig(top_n=5, hold_buffer=0.0, industry_cap=None, max_weight=0.5)
    held = set(build_targets(s1, cfg_buf))
    assert set(build_targets(s2, cfg_buf, current_codes=held)) == held, \
        "c05 掉到第6名仍在缓冲区内，不应换出"
    assert set(build_targets(s2, cfg_plain, current_codes=held)) != held, \
        "朴素 top-N 会因排名互换而换仓"


def test_inv_vol_tilts_weight_to_low_vol():
    cfg = PortfolioConfig(top_n=2, scheme="inv_vol", industry_cap=None, max_weight=0.9)
    s = _scores(2)
    w = build_targets(s, cfg, vol_map={"c01": 0.04, "c02": 0.01})
    assert w["c02"] > w["c01"], "低波票应获得更高权重（即便排名靠后）"
    assert sum(w.values()) == pytest.approx(1.0)


def test_inv_vol_without_volmap_equals_rank_weight():
    s = _scores(5)
    cfg_iv = PortfolioConfig(top_n=5, scheme="inv_vol", industry_cap=None, max_weight=0.5)
    cfg_rw = PortfolioConfig(top_n=5, scheme="rank_weight", industry_cap=None, max_weight=0.5)
    assert build_targets(s, cfg_iv) == build_targets(s, cfg_rw), \
        "无波动数据时 inv_vol 退化为 rank_weight"
