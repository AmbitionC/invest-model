"""套利模块风控：纯函数 + 单调，回测与实盘同源调用（对齐 risk.py 约定）。

覆盖文档红线：① 全程自有资金、零杠杆（ledger_invariant）；② 逻辑止损而非价格
止损——水流破了就走（carry_logic_stop）；③ 盲区 α 小仓位、单笔亏得起
（alpha_position_cap）。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from invest_model.portfolio.risk import ExitDecision


@dataclass
class ArbRiskConfig:
    carry_logic_reverse_thresh: float = 5.0    # 水表 composite 较建仓回落此值→逻辑止损
    alpha_sleeve_cap: float = 0.15             # α sleeve 上限
    alpha_name_cap: float = 0.05               # α 单票上限
    alpha_single_loss_tol: float = 0.02        # α 单笔可容忍的账户级最大亏损
    cb_call_exit: bool = True                  # 可转债强赎→退出
    cb_premium_stop: float = 0.30              # 转股溢价率超此值→退出（双低破坏）


def carry_logic_stop(flow_now: float, flow_entry: float,
                     cfg: ArbRiskConfig | None = None) -> ExitDecision:
    """水表反转逻辑止损（非价格）。单调：一旦水流反转即清仓，价格波动不动摇。"""
    cfg = cfg or ArbRiskConfig()
    if flow_now is None or flow_entry is None or not np.isfinite(flow_now) or not np.isfinite(flow_entry):
        return ExitDecision("hold", 1.0, "水表数据缺失，维持")
    if flow_now < 0 or flow_now <= flow_entry - cfg.carry_logic_reverse_thresh:
        return ExitDecision("exit", 0.0, "水表反转·逻辑止损（跟水不跟价）", new_tier=3)
    return ExitDecision("hold", 1.0, "水流未破，持有")


def alpha_position_cap(equity: float, name_stop_loss_pct: float,
                       cfg: ArbRiskConfig | None = None) -> float:
    """α 单票权重上限：同时满足单票上限 与「单笔亏得起」。

    name_stop_loss_pct：该票的逻辑/硬止损幅度（如 0.15）。单笔最大账户亏损
    = weight * stop_loss_pct ≤ alpha_single_loss_tol → weight ≤ tol/stop_loss_pct。
    """
    cfg = cfg or ArbRiskConfig()
    cap = cfg.alpha_name_cap
    if name_stop_loss_pct and name_stop_loss_pct > 0:
        cap = min(cap, cfg.alpha_single_loss_tol / name_stop_loss_pct)
    return max(0.0, cap)


def ledger_invariant(sleeve_weights: dict[str, float],
                     tol: float = 1e-9) -> tuple[bool, float, str | None]:
    """零杠杆红线：Σ 风险 sleeve 权重 ≤ 1。

    返回 (ok, scale, violation)。越界时 scale<1 用于把风险 sleeve 收缩向现金
    （绝不反向放大）；ok=True 时 scale=1.0。
    """
    total = sum(max(0.0, float(w or 0.0)) for w in sleeve_weights.values())
    if total <= 1.0 + tol:
        return True, 1.0, None
    scale = 1.0 / total
    return False, scale, f"套利账本 Σ={total:.2%}>100%，按零杠杆红线收缩 scale={scale:.3f}"
