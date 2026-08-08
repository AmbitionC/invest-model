"""宽基四腿状态机单测（handoff §2.1.1/2.1.2/2.1.3，2026-08-08）。

三条修复的回归位：
  A. 科创50 阶梯谓词不再恒假——距峰回撤触及第一档（−50%）买入窗开，常量取唯一真源；
  B. 卖出闸优先于恐慌——fear≥75 时价格已过卖出闸必须显示 sell，不得显示恐慌抢买窗；
  C. 锚买腿谓词走 broad_gates.BUY_MUL（判定与展示同源），行为与原字面量逐值一致。

不连库不读 CSV：monkeypatch 取数与恐慌分，喂合成序列直跑生产函数。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import invest_model.orchestration.action_plan as ap  # noqa: E402
from invest_model.broad_gates import BUY_MUL, LADDER_RUNG, SELL_MUL  # noqa: E402


def _series(vals):
    # 索引只需单调可比的字符串日期；末位即 data_date
    idx = [f"{20240000 + i}" for i in range(1, len(vals) + 1)]
    return pd.Series([float(v) for v in vals], index=idx)


def _states(monkeypatch, series_by_csv, fear):
    monkeypatch.setattr(ap, "_index_hist_by",
                        lambda loop, dt, csvn, code, col=None: series_by_csv.get(csvn))
    monkeypatch.setattr(ap, "_fear_score", lambda loop, dt: fear)
    return {s["name"]: s for s in ap._broad_leg_states(None, "20260808")}


def test_ladder_opens_at_first_rung(monkeypatch):
    # 峰 1400 → 现价 690：距峰 −50.7% ≤ −50% ⟹ 买入窗开；第一档线 = 峰×0.50 = 700
    vals = list(range(1000, 1401, 4)) + [690]
    st = _states(monkeypatch, {"index_dump_000688_SH.csv": _series(vals)}, fear=None)
    s = st["科创50"]
    assert s["state"] == "buy"
    assert s["dd"] <= -LADDER_RUNG[0]
    assert abs(s["ladder_line"] - 1400 * (1 - LADDER_RUNG[0])) < 1e-9


def test_ladder_closed_above_first_rung(monkeypatch):
    # 距峰 −25%：窗口关（修复前谓词恒假、修复后也不该误开）
    vals = list(range(1000, 1401, 4)) + [1050]
    st = _states(monkeypatch, {"index_dump_000688_SH.csv": _series(vals)}, fear=None)
    assert st["科创50"]["state"] == "hold"
    assert st["科创50"]["ladder_line"] is not None   # 阶梯线常年可见，供图表/落库


def test_fear_alone_never_shows_panic(monkeypatch):
    # 历史不足 1250 天 ⟹ r1250 无值＝价格闸结构性关闭：fear=80 也不显示 panic
    # （XV-2 引擎对齐；修复前科创50 在 r1250 存在前照样显示抢买窗）。
    # 现价 140 已过卖出闸 130 ⟹ sell；105 在闸间 ⟹ hold（不再是 panic）。
    hi = [100.0] * 299 + [140.0]
    mid = [100.0] * 299 + [105.0]
    st = _states(monkeypatch, {"index_dump_000300_SH.csv": _series(hi)}, fear=80.0)
    assert st["沪深300"]["state"] == "sell"
    st = _states(monkeypatch, {"index_dump_000300_SH.csv": _series(mid)}, fear=80.0)
    assert st["沪深300"]["state"] == "hold"


def test_panic_requires_price_below_r1250(monkeypatch):
    # 早期低价拖低 expanding 中位（=50），近5年高位（r1250=300）：
    # 现价 150 ＝ 引擎 B2 腿真买区（fear≥75 且 150<300），即便已过 expanding 卖出闸
    # （150>65）也显示 panic——红利 2018-10/2024-01 真底场景，卖出优先会把它藏掉。
    vals = [50.0] * 1300 + [300.0] * 1250 + [150.0]
    st = _states(monkeypatch, {"index_dump_000300_SH.csv": _series(vals)}, fear=80.0)
    assert st["沪深300"]["state"] == "panic"
    # 同一序列 fear<75 ⟹ 回到卖出区（价格闸单独不够）
    st = _states(monkeypatch, {"index_dump_000300_SH.csv": _series(vals)}, fear=40.0)
    assert st["沪深300"]["state"] == "sell"


def test_no_flying_knife_above_r1250(monkeypatch):
    # 现价 350 在近5年中位线（300）上方：fear=80 也不显示抢买窗——2022-04 创业板
    # 在 5 年中位线上方 15~24% 的高位恐慌日＝E36 判「接飞刀」要挡的场景（XV-2 主修）。
    vals = [50.0] * 1300 + [300.0] * 1250 + [350.0]
    st = _states(monkeypatch, {"index_dump_000300_SH.csv": _series(vals)}, fear=80.0)
    assert st["沪深300"]["state"] == "sell"   # 350 > expanding中位50×1.30



def test_broad_legs_hint_renders_ladder(monkeypatch):
    # 提示行冒烟：阶梯腿显示「距峰…L50档<价>」而非「距锚」，且陈旧价格出告警
    vals = list(range(1000, 1401, 4)) + [690]
    monkeypatch.setattr(ap, "_index_hist_by",
                        lambda loop, dt, csvn, code, col=None:
                        {"index_dump_000688_SH.csv": _series(vals)}.get(csvn))
    monkeypatch.setattr(ap, "_fear_score", lambda loop, dt: None)
    txt = ap._broad_legs_hint(None, "20260808")
    assert txt and "距峰" in txt and "L50档" in txt and "数据陈旧告警" in txt


def test_anchor_buy_uses_true_source(monkeypatch):
    # 创业板买入闸 = 中位线×BUY_MUL（0.90）：0.89×中位 → buy；0.91×中位 → 非 buy
    assert BUY_MUL["创业板"] == 0.90 and SELL_MUL["创业板"] == 1.43   # 真源值本身的回归位
    lo = [100.0] * 299 + [89.0]
    hi = [100.0] * 299 + [91.0]
    st = _states(monkeypatch, {"spread_full_history.csv": _series(lo)}, fear=None)
    assert st["创业板"]["state"] == "buy"
    st = _states(monkeypatch, {"spread_full_history.csv": _series(hi)}, fear=None)
    assert st["创业板"]["state"] == "hold"
