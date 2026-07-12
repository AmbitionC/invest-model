"""E10（防御底仓混合）/ E11（因子分族）验证脚本的回归测试。

覆盖：
  - E10 纯函数：mix_nav 月度再平衡数学、max_drawdown、annualized、underwater_days
  - E10 冒烟：合成 index_daily（进攻高波动、底仓低波动）→ 30% 底仓应改善回撤
  - E11 冒烟：合成 factor_ic_log（交易类强 IC、会计类零 IC）→ 族对比方向正确
  - 数据缺失降级：两脚本在空库上"不判定"而非抛异常
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts" / "validation"))

import e10_defensive_sleeve as e10  # noqa: E402
import e11_factor_families as e11  # noqa: E402
from common import get_repo  # noqa: E402

from invest_model.data import create_schema, make_engine  # noqa: E402
from invest_model.repositories.base import BaseRepository  # noqa: E402


# ── E10 纯函数 ─────────────────────────────────────────────
def _dates(n: int) -> list[str]:
    return [d.strftime("%Y%m%d") for d in pd.bdate_range("2019-01-01", periods=n)]


def test_mix_nav_weights_zero_and_one():
    ds = _dates(60)
    off = pd.Series(np.linspace(100, 130, 60), index=ds)   # 单调涨
    dfn = pd.Series(np.full(60, 50.0), index=ds)           # 恒定
    nav0 = e10.mix_nav(off, dfn, 0.0)
    assert nav0.iloc[-1] == pytest.approx(130 / 100, rel=1e-9)   # 全进攻=进攻收益
    nav1 = e10.mix_nav(off, dfn, 1.0)
    assert nav1.iloc[-1] == pytest.approx(1.0, rel=1e-9)         # 全底仓=零收益


def test_mix_nav_monthly_rebalance_between():
    ds = _dates(120)
    rng = np.random.default_rng(7)
    off = pd.Series(100 * np.cumprod(1 + rng.normal(0, 0.02, 120)), index=ds)
    dfn = pd.Series(100 * np.cumprod(1 + rng.normal(0.0005, 0.005, 120)), index=ds)
    nav = e10.mix_nav(off, dfn, 0.3)
    # 混合净值应介于两个单腿净值的最小/最大之间（月度再平衡不产生杠杆）
    lo = np.minimum(off / off.iloc[0], dfn / dfn.iloc[0])
    hi = np.maximum(off / off.iloc[0], dfn / dfn.iloc[0])
    assert (nav >= lo * 0.999).all() and (nav <= hi * 1.001).all()


def test_max_drawdown_and_underwater():
    nav = pd.Series([1.0, 1.2, 0.9, 1.0, 1.3], index=_dates(5))
    assert e10.max_drawdown(nav) == pytest.approx(0.9 / 1.2 - 1)
    assert e10.underwater_days(nav) == 2      # 0.9、1.0 两天在 1.2 峰值下方


def test_annualized_two_years_double():
    n = 488  # ≈2 年
    nav = pd.Series(np.linspace(1, 2, n), index=_dates(n))
    assert e10.annualized(nav) == pytest.approx(2 ** (244 / n) - 1, rel=1e-6)


# ── 冒烟：合成库 ────────────────────────────────────────────
@pytest.fixture()
def repo(tmp_path):
    eng = make_engine(f"sqlite:///{tmp_path}/t.db")
    create_schema(eng)
    return BaseRepository(eng)


def _seed_index(repo, code: str, closes: np.ndarray, dates: list[str]):
    df = pd.DataFrame({"code": code, "trade_date": dates, "close": closes,
                       "open": closes, "high": closes, "low": closes,
                       "pct_chg": 0.0, "volume": 1.0, "amount": 1.0})
    repo.upsert("index_daily", df, ["code", "trade_date"])


def test_e10_smoke_defensive_improves_dd(repo):
    n = 900
    ds = _dates(n)
    rng = np.random.default_rng(3)
    # 进攻：高波动带一段深跌；底仓：低波动缓涨
    off = 100 * np.cumprod(1 + rng.normal(0.0002, 0.025, n))
    off[300:400] *= np.linspace(1, 0.6, 100)               # 人造深回撤
    dfn = 100 * np.cumprod(1 + rng.normal(0.0003, 0.006, n))
    for oc in e10.OFFENSE_CODES:
        _seed_index(repo, oc, off, ds)
    _seed_index(repo, e10.DEFENSIVE_CODES[0], dfn, ds)
    md = e10.run(repo)
    assert "E10" in md and "裁决" in md
    assert "数据不足" not in md.split("###")[1]            # 至少第一个代理有数据
    # 30% 底仓的回撤数值应严格小于纯进攻（人造场景下必然成立）
    assert "MaxDD 改善 +" in md


def test_e10_no_data_degrades(repo):
    md = e10.run(repo)
    assert "不判定" in md


def test_e11_family_contrast(repo):
    dates = _dates(300)
    rows = []
    rng = np.random.default_rng(5)
    for d in dates:
        for f in e11.FAMILIES["交易类"]:
            rows.append({"trade_date": d, "factor_name": f, "horizon": 20,
                         "ic": 0.06, "rank_ic": 0.06 + rng.normal(0, 0.01)})
        for f in e11.FAMILIES["会计类"]:
            rows.append({"trade_date": d, "factor_name": f, "horizon": 20,
                         "ic": 0.0, "rank_ic": rng.normal(0, 0.01)})
        rows.append({"trade_date": d, "factor_name": e11.SIZE_FACTOR, "horizon": 20,
                     "ic": 0.0, "rank_ic": 0.05 if d < dates[150] else -0.05})
    repo.upsert("factor_ic_log", pd.DataFrame(rows), ["trade_date", "factor_name", "horizon"])
    md = e11.run(repo)
    assert "E11" in md and "裁决" in md
    assert "✅ 触发「会计类降权」提案" in md               # 人造强对比必触发
    assert "small_size" in md


def test_e11_no_data_degrades(repo):
    md = e11.run(repo)
    assert "不判定" in md
