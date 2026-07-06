"""P10 回归：evaluate_holding 的“未盈利新仓破MA20缓冲”（只测纯函数，无 DB）。

构造：前 20 日恒为 100，末日为 x。则 MA20(末) = (19*100 + x)/20，
close=x < MA20 ⇔ x < 100。取 x=95 → close=95 < MA20=99.75（破 MA20）。
trail_full 默认 False（仅看 MA20）。
"""
import pandas as pd

from invest_model.portfolio.risk import RiskConfig, evaluate_holding


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
