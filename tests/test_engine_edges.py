"""回测引擎边界单元测试：无隐性杠杆、退市清算、板块涨跌停、
forward_returns 退市覆盖、ICCombiner 权重截断。

用手工构造的小 SQLite 数据直接驱动 CSBacktestEngine / 纯函数，
不依赖完整闭环，验证 review 中修复的各边界行为。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from invest_model.backtest.cs_engine import CSBacktestConfig, CSBacktestEngine
from invest_model.data import create_schema, make_engine
from invest_model.model.combiner import ICCombiner
from invest_model.model.dataset import forward_returns
from invest_model.repositories.base import BaseRepository
from invest_model.repositories.factor_repo import FactorRepository


def _mk_engine(tmp_path, name: str):
    eng = make_engine(f"sqlite:///{tmp_path}/{name}.db")
    create_schema(eng)
    return eng


def _insert_daily(eng, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["volume"] = df.get("volume", 1000.0)
    df["amount"] = df.get("amount", 1000.0)
    BaseRepository(eng).upsert("stock_daily", df, ["code", "trade_date"])


def _dates(start: str, n: int) -> list[str]:
    return [d.strftime("%Y%m%d") for d in pd.bdate_range(start, periods=n)]


def test_no_implicit_leverage_when_sell_blocked(tmp_path):
    """旧持仓跌停卖不掉时，新买入必须按可用现金缩放，Σ权重不得超过 1。"""
    eng = _mk_engine(tmp_path, "lev")
    dates = _dates("2021-06-01", 12)
    rows = []
    for i, d in enumerate(dates):
        # A：第 7 天（首个调仓信号的执行次日之后的调仓执行日）跌停
        pct_a = -10.0 if i == 6 else 0.0
        rows.append({"code": "000001.SZ", "trade_date": d, "close": 10.0, "pct_chg": pct_a})
        rows.append({"code": "000002.SZ", "trade_date": d, "close": 20.0, "pct_chg": 0.0})
        rows.append({"code": "000003.SZ", "trade_date": d, "close": 30.0, "pct_chg": 0.0})
    _insert_daily(eng, rows)

    reb = [dates[0], dates[5]]  # 信号日：d0 建仓 A+B；d5 换仓到 C（d6 执行，A 跌停卖不掉）
    targets_by_date = {
        dates[0]: {"000001.SZ": 0.5, "000002.SZ": 0.5},
        dates[5]: {"000003.SZ": 1.0},
    }
    cfg = CSBacktestConfig(start_date=dates[0], end_date=dates[-1])
    res = CSBacktestEngine(eng, cfg, lambda dt, cur: targets_by_date.get(dt, {}), reb).run()

    assert res.nav_df["invested"].max() <= 1.0 + 1e-6, \
        f"权重和出现隐性杠杆：{res.nav_df['invested'].max()}"
    # A 跌停卖不掉仍在仓位里，C 的买入被缩到 ≤ 可用现金
    last = res.nav_df.iloc[-1]
    assert last["invested"] <= 1.0 + 1e-6


def test_full_exit_bypasses_min_trade_band(tmp_path):
    """权重低于换手带的出局票（tgt=0）必须能卖掉，不得成为僵尸小仓位。"""
    eng = _mk_engine(tmp_path, "zombie")
    dates = _dates("2021-06-01", 12)
    rows = []
    for d in dates:
        rows.append({"code": "000001.SZ", "trade_date": d, "close": 10.0, "pct_chg": 0.0})
        rows.append({"code": "000002.SZ", "trade_date": d, "close": 20.0, "pct_chg": 0.0})
    _insert_daily(eng, rows)

    # d0 建仓 A=2%+B=90%；d3 把 A 减到 0.5%（低于 1% 换手带）；d7 A 出局(tgt=0)
    targets_by_date = {
        dates[0]: {"000001.SZ": 0.02, "000002.SZ": 0.90},
        dates[3]: {"000001.SZ": 0.005, "000002.SZ": 0.90},
        dates[7]: {"000002.SZ": 0.90},
    }
    reb = [dates[0], dates[3], dates[7]]
    cfg = CSBacktestConfig(start_date=dates[0], end_date=dates[-1])
    res = CSBacktestEngine(eng, cfg, lambda dt, cur: targets_by_date.get(dt, {}), reb).run()

    assert res.nav_df["position_count"].iloc[-1] == 1, \
        "0.5% 的 A 在 tgt=0 时应被清仓，而非因低于换手带永远卡在组合里"
    sells_a = [t for t in res.trades if t["code"] == "000001.SZ" and t["action"] == "sell"]
    assert sells_a and sells_a[-1]["weight"] == 0.0


def test_delisted_holding_is_liquidated(tmp_path):
    """行情消失超过 delist_after_days 的持仓应被强制清算，不能永久冻结。"""
    eng = _mk_engine(tmp_path, "delist")
    dates = _dates("2021-06-01", 15)
    rows = []
    for i, d in enumerate(dates):
        # D 第 3 天后退市（无行情）；E 全程正常
        if i <= 2:
            rows.append({"code": "000004.SZ", "trade_date": d, "close": 10.0, "pct_chg": 0.0})
        rows.append({"code": "000005.SZ", "trade_date": d, "close": 20.0, "pct_chg": 0.0})
    _insert_daily(eng, rows)

    cfg = CSBacktestConfig(start_date=dates[0], end_date=dates[-1],
                           delist_after_days=5, delist_recovery=0.5)
    targets = {dates[0]: {"000004.SZ": 0.6, "000005.SZ": 0.4}}
    res = CSBacktestEngine(eng, cfg, lambda dt, cur: targets.get(dt, {}), [dates[0]]).run()

    delist_trades = [t for t in res.trades if t["action"] == "delist"]
    assert len(delist_trades) == 1 and delist_trades[0]["code"] == "000004.SZ"
    # 清算后仓位只剩 E；recovery=0.5 → 退市损失已落地（nav 下降）
    assert res.nav_df["position_count"].iloc[-1] == 1
    assert res.nav_df["nav"].iloc[-1] < 1.0


def test_board_specific_limit_thresholds(tmp_path):
    """创业板 +15% 不算涨停可买；主板 +10% 涨停不可买。"""
    eng = _mk_engine(tmp_path, "limits")
    dates = _dates("2021-06-01", 4)
    rows = []
    for i, d in enumerate(dates):
        pct = 15.0 if i == 1 else 0.0   # 执行日（信号次日）大涨
        rows.append({"code": "300001.SZ", "trade_date": d, "close": 10.0, "pct_chg": pct})
        pct_m = 10.0 if i == 1 else 0.0
        rows.append({"code": "000001.SZ", "trade_date": d, "close": 10.0, "pct_chg": pct_m})
    _insert_daily(eng, rows)

    cfg = CSBacktestConfig(start_date=dates[0], end_date=dates[-1])
    targets = {dates[0]: {"300001.SZ": 0.5, "000001.SZ": 0.5}}
    res = CSBacktestEngine(eng, cfg, lambda dt, cur: targets.get(dt, {}), [dates[0]]).run()

    bought = {t["code"] for t in res.trades if t["action"] == "buy"}
    assert "300001.SZ" in bought, "创业板 2020-08 后 ±20%，+15% 应可买"
    assert "000001.SZ" not in bought, "主板 +10% 涨停应不可买"


def test_forward_returns_covers_delisted(tmp_path):
    """区间内退市的票用最后成交价计算收益，而非被剔除。"""
    eng = _mk_engine(tmp_path, "fwd")
    dates = _dates("2021-06-01", 6)
    rows = []
    for i, d in enumerate(dates):
        rows.append({"code": "000006.SZ", "trade_date": d, "close": 10.0 + i, "pct_chg": 0.0})
        if i <= 1:  # X 第 2 天后停牌/退市，最后价 5.0（较期初 10.0 腰斩）
            rows.append({"code": "000007.SZ", "trade_date": d,
                         "close": 10.0 if i == 0 else 5.0, "pct_chg": 0.0})
    _insert_daily(eng, rows)

    ret = forward_returns(eng, dates[0], dates[-1], ["000006.SZ", "000007.SZ"])
    assert "000007.SZ" in ret.index, "退市票不应从前瞻收益中剔除"
    assert ret["000007.SZ"] == pytest.approx(-0.5)
    assert ret["000006.SZ"] == pytest.approx(15.0 / 10.0 - 1.0)


def test_combiner_weight_clipped(tmp_path):
    """IC 序列 std 极小导致 ICIR 爆表时，合成权重应被截断在 ±3。"""
    eng = _mk_engine(tmp_path, "clip")
    frepo = FactorRepository(eng)
    ic_dates = [d.strftime("%Y%m%d") for d in pd.bdate_range("2021-01-01", periods=6, freq="21B")]
    rows = []
    for i, d in enumerate(ic_dates):
        # mom_60：均值 0.1、std≈1e-6 → ICIR ≈ 1e5，必须被 clip
        rows.append({"trade_date": d, "factor_name": "mom_60", "horizon": 21,
                     "ic": 0.1, "rank_ic": 0.1 + i * 1e-7})
        rows.append({"trade_date": d, "factor_name": "roe", "horizon": 21,
                     "ic": 0.05, "rank_ic": 0.05 * (-1) ** i})
    frepo.save_ic_log(pd.DataFrame(rows))

    w = ICCombiner(eng, window=12, mode="icir").weights("20220101")
    assert w.abs().max() <= 3.0 + 1e-9, f"权重未截断：{w.abs().max()}"
    assert w["mom_60"] == pytest.approx(3.0)
