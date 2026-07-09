"""R1 回归：trailing_only 白名单的硬止损豁免（只测纯函数，无 DB）。

背景：白名单语义=「只按均线移动止盈管、豁免 -8% 硬止损」，live_check 盘中一直如此；
盘后 build_action_plan 此前没把豁免传给 evaluate_holding，导致对白名单票（实例：
卫星化学 -8.47%）盘后连发「硬止损清仓」、盘中却显示「持有」。修复=evaluate_holding
新增 exempt_hard_stop 入参，仅掐硬止损分支：逻辑证伪与均线移动止盈不受影响，
默认 False 时行为逐字不变（回测 cs_engine 不传参 → 基线零变化）。

序列构造同 test_ma20_buffer：前 20 日恒为 100、末日为 x，x<100 即破 MA20。
"""
import pandas as pd

from invest_model.portfolio.risk import RiskConfig, evaluate_holding


def _hist(last: float) -> pd.Series:
    vals = [100.0] * 20 + [last]
    idx = [f"202601{i:02d}" for i in range(1, len(vals) + 1)]
    return pd.Series(vals, index=idx)


def _flat_hist(level: float = 90.0) -> pd.Series:
    # 恒定价：close == MA20，不构成破位，只可能触发硬止损
    vals = [level] * 21
    idx = [f"202601{i:02d}" for i in range(1, len(vals) + 1)]
    return pd.Series(vals, index=idx)


def test_default_hard_stop_unchanged():
    # 默认不豁免：-10% ≤ -8% → 硬止损清仓（原行为逐字保持）
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_flat_hist(90.0), cost=100.0, cfg=cfg)
    assert dec.action == "exit"
    assert "硬止损" in dec.reason


def test_exempt_skips_hard_stop_only():
    # 豁免：同样 -10%，但价格在 MA20 上（恒定价）→ 持有；stop_price 仍照算供展示
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_flat_hist(90.0), cost=100.0, cfg=cfg, exempt_hard_stop=True)
    assert dec.action == "hold"
    assert abs(dec.stop_price - 92.0) < 1e-9


def test_exempt_still_runs_ma20_branch():
    # 豁免票破 MA20（未盈利，-5%）→ 仍走 P10 减半缓冲：豁免只掐硬止损
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_hist(95.0), cost=100.0, cfg=cfg, exempt_hard_stop=True)
    assert dec.action == "trim"
    assert "破MA20" in dec.reason


def test_exempt_deep_loss_falls_through_to_ma20():
    # 豁免票 -12%（越过硬止损线）且破 MA20 → 由 MA20 分支接管而非硬止损
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_hist(88.0), cost=100.0, cfg=cfg, exempt_hard_stop=True)
    assert dec.action in ("trim", "exit")
    assert "硬止损" not in dec.reason


def test_exempt_does_not_shield_exit_codes():
    # 逻辑证伪不受豁免影响 → 仍无条件清仓
    cfg = RiskConfig(enabled=True)
    dec = evaluate_holding(_flat_hist(90.0), cost=100.0, cfg=cfg,
                           in_exit_codes=True, exempt_hard_stop=True)
    assert dec.action == "exit"
    assert "逻辑证伪" in dec.reason
