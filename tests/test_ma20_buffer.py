"""P10 回归：evaluate_holding 的“未盈利新仓破MA20缓冲”（只测纯函数，无 DB）。

构造：前 20 日恒为 100，末日为 x。则 MA20(末) = (19*100 + x)/20，
close=x < MA20 ⇔ x < 100。取 x=95 → close=95 < MA20=99.75（破 MA20）。
trail_full 默认 False（仅看 MA20）。

P10.1 补充回归（修实盘路径失效 + 转盈即清）：
  - replay_hold_tier：实盘重建 prev_tier 须与回测逐日携带 new_tier 的决策序列逐字一致；
  - 新仓窗口不足 20 行时不得每日重复减半（原 replay_tier 切片内 MA20=NaN 记不上档）；
  - 缓冲仓在 MA20 下方涨回成本（非新鲜破位）→ 持有；站回 MA20 后再破位才清。
"""
import numpy as np
import pandas as pd

from invest_model.portfolio.risk import (RiskConfig, evaluate_holding,
                                         replay_hold_tier)


def _hist(last: float = 95.0) -> pd.Series:
    vals = [100.0] * 20 + [last]
    idx = [f"202601{i:02d}" for i in range(1, len(vals) + 1)]
    return pd.Series(vals, index=idx)


def test_unprofit_break_ma20_trims_not_exit():
    # 成本 100、现价 95（浮亏 -5%，未盈利）破 MA20 → 减半，不清仓
    cfg = RiskConfig(enabled=True)  # ma20_unprofit_trim 默认 True
    dec = evaluate_holding(_hist(95.0), cost=100.0, cfg=cfg, prev_tier=0)
    assert dec.action == "trim"
    assert abs(dec.keep_frac - 0.5) < 1e-9
    assert dec.new_tier == 1
    assert "减半" in dec.reason


def test_profit_break_ma20_exits():
    # 成本 90、现价 95（浮盈 +5.6%，已盈利）破 MA20 → 正常清仓止盈
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_hist(95.0), cost=90.0, cfg=cfg, prev_tier=0)
    assert dec.action == "exit"
    assert "破MA20清仓" in dec.reason


def test_hard_stop_takes_precedence():
    # 成本 100、现价 91（-9% ≤ -8%）→ 硬止损优先，不走 MA20 缓冲分支
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_hist(91.0), cost=100.0, cfg=cfg, prev_tier=0)
    assert dec.action == "exit"
    assert "硬止损" in dec.reason


def test_switch_off_restores_original_clear():
    # 关闭开关 → 未盈利破 MA20 也清仓（逐字恢复原行为）
    cfg = RiskConfig(enabled=True, ma20_unprofit_trim=False)
    dec = evaluate_holding(_hist(95.0), cost=100.0, cfg=cfg, prev_tier=0)
    assert dec.action == "exit"
    assert "破MA20清仓" in dec.reason


def test_already_trimmed_then_holds():
    # 已减半过(prev_tier=1)、仍未盈利再破 MA20 → 持有（不重复减半，靠硬止损兜底）
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_hist(95.0), cost=100.0, cfg=cfg, prev_tier=1)
    assert dec.action == "hold"
    assert dec.new_tier == 1


def test_profit_gate_configurable():
    # gate 提到 +8%：浮盈 +5.6%(未达 gate) 仍视为“未盈利” → 减半
    cfg = RiskConfig(enabled=True, ma20_profit_gate=0.08)
    dec = evaluate_holding(_hist(95.0), cost=90.0, cfg=cfg, prev_tier=0)
    assert dec.action == "trim"


# ── P10.1 ──────────────────────────────────────────────────────────────


def _series(vals: list[float], prefix: str = "D") -> pd.Series:
    return pd.Series(vals, index=[f"{prefix}{i:03d}" for i in range(len(vals))])


def _run_live(hist_full: pd.Series, entry_i: int, cost: float, cfg: RiskConfig):
    """模拟实盘 action_plan 逐日调用：prev 用 replay_hold_tier 重建。返回决策列表。"""
    out = []
    for t in range(entry_i + 1, len(hist_full)):
        hist = hist_full.iloc[: t + 1]
        prev = replay_hold_tier(hist.iloc[:-1], cost, cfg,
                                replay_from=str(hist_full.index[entry_i]))
        out.append(evaluate_holding(hist, cost, cfg, prev_tier=prev))
    return out


def _run_backtest(hist_full: pd.Series, entry_i: int, cost: float, cfg: RiskConfig):
    """模拟回测 cs_engine 逐日调用：prev 逐日携带 new_tier。返回决策列表。"""
    out, tier = [], 0
    for t in range(entry_i + 1, len(hist_full)):
        dec = evaluate_holding(hist_full.iloc[: t + 1], cost, cfg, prev_tier=tier)
        tier = dec.new_tier
        out.append(dec)
    return out


def test_new_position_no_repeat_trim_live_path():
    # 新仓建仓 4 日连续贴 MA20 下方：首破减半一次，之后持有——不得每日重复减半
    vals = list(np.linspace(90, 100, 60)) + [99.0, 98.5, 98.2, 98.0, 97.8, 97.6]
    decs = _run_live(_series(vals), 60, 99.0, RiskConfig(enabled=True))
    assert [d.action for d in decs] == ["trim", "hold", "hold", "hold", "hold"]
    assert decs[0].keep_frac == 0.5


def test_buffered_turn_profitable_below_ma20_holds():
    # 缓冲仓在 MA20 下方涨回成本上方（非新鲜破位）→ 持有，不因“转盈”被清
    vals = [100.0] * 60 + [96.0, 95.5, 96.0, 98.0, 98.5]
    decs = _run_live(_series(vals, "E"), 35, 98.0, RiskConfig(enabled=True))
    assert [d.action for d in decs[-5:]] == ["trim", "hold", "hold", "hold", "hold"]


def test_reclaim_then_fresh_break_exits():
    # 缓冲→收复 MA20 →转盈→再次新鲜破位 → 此时才清仓止盈
    vals = ([100.0] * 60 + [96.0, 95.5, 96.0, 98.0, 99.5, 100.5, 101.0, 101.5, 99.0])
    decs = _run_live(_series(vals, "F"), 35, 98.0, RiskConfig(enabled=True))
    assert decs[-1].action == "exit" and "破MA20清仓" in decs[-1].reason
    assert all(d.action != "exit" for d in decs[:-1])


def test_profitable_trend_fresh_break_still_exits():
    # 盈利趋势仓首次跌破 MA20（新鲜破位）→ 原止盈行为不变
    vals = list(np.linspace(90, 120, 60)) + [112.0]
    decs = _run_live(_series(vals, "G"), 20, 95.0, RiskConfig(enabled=True))
    assert decs[-1].action == "exit" and "破MA20清仓" in decs[-1].reason


def test_live_matches_backtest_paths():
    # 实盘(replay_hold_tier 重建) 与 回测(new_tier 携带) 的决策序列逐字一致
    cfg = RiskConfig(enabled=True)
    cases = [
        (list(np.linspace(90, 100, 60)) + [99.0, 98.5, 98.2, 98.0, 97.8], 60, 99.0),
        ([100.0] * 60 + [96.0, 95.5, 96.0, 98.0, 99.5, 100.5, 101.5, 99.0], 35, 98.0),
        ([100.0] * 40 + [97.0, 95.0, 93.0, 91.5, 90.0], 20, 100.0),   # 硬止损兜底
        (list(np.linspace(90, 120, 60)) + [112.0, 111.0], 20, 95.0),  # 盈利新鲜破位
    ]
    for vals, ei, cost in cases:
        live = _run_live(_series(vals, "H"), ei, cost, cfg)
        back = _run_backtest(_series(vals, "H"), ei, cost, cfg)
        for dl, db in zip(live, back):
            assert (dl.action, dl.reason) == (db.action, db.reason)
            if db.action == "exit":       # 回测清仓后仓位消失，之后不再可比
                break


def test_switch_off_replay_restores_original():
    # 开关关：replay_hold_tier 迁移退化为 step_tier 原样（首破即档3）
    cfg = RiskConfig(enabled=True, ma20_unprofit_trim=False)
    vals = [100.0] * 30 + [95.0, 96.0]
    tier = replay_hold_tier(_series(vals, "I"), 98.0, cfg, replay_from="I010")
    assert tier == 3
