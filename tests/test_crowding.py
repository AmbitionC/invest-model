"""E7 拥挤度度量单测：HHI/Top-3、主题归一化、操作计划预警文案。"""

from __future__ import annotations

import math

import pandas as pd

from invest_model.portfolio.crowding import (concentration, crowding_hints, hhi,
                                             theme_of)


def test_hhi_extremes():
    # 全压一处 → HHI=1；均分 4 处 → 0.25
    assert hhi(pd.Series({"a": 1.0})) == 1.0
    assert math.isclose(hhi(pd.Series({"a": 1, "b": 1, "c": 1, "d": 1})), 0.25)
    assert math.isnan(hhi(pd.Series(dtype=float)))
    assert math.isnan(hhi(pd.Series({"a": 0.0, "b": 0.0})))


def test_concentration_flags_crowded():
    # Top-3 份额 100% 且 HHI 高 → crowded
    info = concentration(pd.Series({"半导体": 6, "算力/AI": 3, "机器人": 1}))
    assert info["n"] == 3          # n = 分组数（非总条数）
    assert info["crowded"] is True
    assert math.isclose(info["top3_share"], 1.0)
    assert info["top3_detail"][0][0] == "半导体"

    # 十个行业各 10% → 分散，不预警
    even = concentration(pd.Series({f"ind{i}": 1 for i in range(10)}))
    assert even["crowded"] is False
    assert math.isclose(even["top3_share"], 0.3)


def test_theme_of_keywords():
    assert theme_of("HBM/存储链前道设备") == "半导体"
    assert theme_of("800G光模块+液冷放量") == "算力/AI"
    assert theme_of("券商超跌反弹") == "券商/保险"
    assert theme_of("完全无关的文本") == "其他"


def test_crowding_hints_holdings_by_weight():
    # 三只票同属两个行业、权重集中 → 触发持仓行业拥挤
    weights = {"600000.SH": 0.4, "600001.SH": 0.35, "600002.SH": 0.2}
    imap = {"600000.SH": "半导体", "600001.SH": "半导体", "600002.SH": "算力"}
    hints = crowding_hints(weights, imap, advisor_catalysts=None)
    assert any("持仓行业拥挤" in h for h in hints)


def test_crowding_hints_advisor_theme_pileup():
    # 投顾 long 信号全扎堆半导体+算力 → 触发信号扎堆预警
    cats = ["存储链HBM", "芯片封测", "光模块算力", "液冷服务器", "先进制程晶圆"]
    hints = crowding_hints(held_weights={}, industry_map={}, advisor_catalysts=cats)
    assert any("投顾信号扎堆" in h for h in hints)


def test_crowding_hints_diversified_silent():
    # 分散持仓 + 分散主题 → 无预警
    weights = {f"60000{i}.SH": 0.1 for i in range(8)}
    imap = {f"60000{i}.SH": f"行业{i}" for i in range(8)}
    cats = ["黄金有色", "创新药医药", "军工国防", "锂电光伏", "白酒消费"]
    assert crowding_hints(weights, imap, cats) == []
