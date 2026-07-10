"""P12 回归：指数 MA60 事件闸（默认关=现状逐字一致）+ E9 验证脚本冒烟。"""
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from invest_model.portfolio.market_timing import MarketTiming, ma60_gate_active


def _series(last: float, base: float = 100.0, n: int = 80) -> pd.Series:
    vals = [base] * (n - 1) + [last]
    idx = [f"2026{i:04d}" for i in range(1, n + 1)]
    return pd.Series(vals, index=idx)


def test_gate_pure_function():
    # 恒 100、末日 98：MA60≈99.97，98 < 99.97×0.99=98.97 → 有效跌破
    assert ma60_gate_active(_series(98.0), break_pct=0.01) is True
    # 末日 99.5：在迟滞区（<MA60 但未破 1%）→ 保守不触发
    assert ma60_gate_active(_series(99.5), break_pct=0.01) is False
    # 末日 101：线上 → 不触发
    assert ma60_gate_active(_series(101.0), break_pct=0.01) is False
    # 样本不足 60 根 → 不触发（宁松勿误伤）
    assert ma60_gate_active(_series(90.0, n=30), break_pct=0.01) is False


_DATES = [d.strftime("%Y%m%d") for d in pd.bdate_range("2026-01-01", periods=80)]
_LAST, _PREV = _DATES[-1], _DATES[-2]


@pytest.fixture()
def timing_db(tmp_path):
    db = f"sqlite:///{tmp_path}/t.db"
    from invest_model.data import create_schema, make_engine
    create_schema(make_engine(db))
    eng = create_engine(db)
    # 指数：79 天恒 100 + 末日 95（有效跌破）；趋势分量低但线性 gross 仍 > floor
    rows = [("000300.SH", d, 100.0) for d in _DATES[:-1]]
    rows.append(("000300.SH", _LAST, 95.0))
    with eng.begin() as conn:
        conn.execute(text(
            "INSERT INTO index_daily (code, trade_date, close) VALUES " +
            ",".join(f"('{c}','{d}',{p})" for c, d, p in rows)))
    return db


def test_gate_off_by_default_unchanged(timing_db):
    from invest_model.data import make_engine
    eng = make_engine(timing_db)
    off = MarketTiming(eng, floor=0.5).gross_exposure(_LAST)
    on = MarketTiming(eng, floor=0.5, ma60_gate=True).gross_exposure(_LAST)
    # 默认关：线性口径（跌破日 gross 仍由信号线性决定，> floor——恒价低波抬升信号）
    assert off > 0.5
    # 开启：事件降到位 = floor；且不破 floor（不连乘）
    assert on == 0.5


def test_gate_no_effect_when_above_ma60(timing_db):
    from invest_model.data import make_engine
    eng = make_engine(timing_db)
    off = MarketTiming(eng, floor=0.5).gross_exposure(_PREV)   # 末日前一天：恒价在线上
    on = MarketTiming(eng, floor=0.5, ma60_gate=True).gross_exposure(_PREV)
    assert on == off                     # 未跌破 → 开关无影响


def test_e9_smoke_on_synthetic(timing_db):
    """E9 冒烟：合成正弦指数（有涨跌穿越 MA60），脚本能跑通并输出裁决段。"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts" / "validation"))
    import e9_index_ma60 as e9
    from invest_model.data import make_engine
    from invest_model.repositories.base import BaseRepository
    eng = create_engine(timing_db)
    # 造 600 天正弦波动的两指数（保证跌破/回踩事件都出现）
    dates = [d.strftime("%Y%m%d") for d in pd.bdate_range("2023-01-01", periods=600)]
    for code in ("000300.SH", "399006.SZ"):
        rows = []
        for i, dt in enumerate(dates):
            px = 100 + 15 * np.sin(i / 40) + (i % 7) * 0.3
            rows.append(f"('{code}','{dt}',{px:.2f})")
        with eng.begin() as conn:
            conn.execute(text("DELETE FROM index_daily WHERE code=:c"), {"c": code})
            conn.execute(text(
                "INSERT INTO index_daily (code, trade_date, close) VALUES " + ",".join(rows)))
    md = e9.run(BaseRepository(make_engine(timing_db)))
    assert "E9" in md and "裁决" in md and "P12 裁决" in md
    assert "000300.SH" in md and "399006.SZ" in md
