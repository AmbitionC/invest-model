"""套利风控纯函数单测：零杠杆红线 / 逻辑止损 / α 单笔亏得起。"""

from invest_model.arb.config import ArbConfig
from invest_model.arb.ledger import allocate_sleeves
from invest_model.portfolio.arb_risk import (
    ArbRiskConfig,
    alpha_position_cap,
    carry_logic_stop,
    ledger_invariant,
)


def test_ledger_invariant_ok():
    ok, scale, viol = ledger_invariant({"defense_A": 0.55, "offense_B": 0.35, "alpha": 0.10})
    assert ok and scale == 1.0 and viol is None


def test_ledger_invariant_violation_shrinks():
    ok, scale, viol = ledger_invariant({"defense_A": 0.7, "offense_B": 0.5, "alpha": 0.2})
    assert not ok and scale < 1.0 and viol is not None
    # 收缩后 Σ==1
    total = (0.7 + 0.5 + 0.2) * scale
    assert abs(total - 1.0) < 1e-9


def test_carry_logic_stop_water_reversed():
    d = carry_logic_stop(flow_now=-1.0, flow_entry=20.0)
    assert d.action == "exit" and "逻辑止损" in d.reason


def test_carry_logic_stop_holds_when_water_intact():
    d = carry_logic_stop(flow_now=18.0, flow_entry=20.0)
    assert d.action == "hold"


def test_alpha_position_cap_loss_tolerable():
    cfg = ArbRiskConfig(alpha_name_cap=0.05, alpha_single_loss_tol=0.02)
    # 止损 15% → weight ≤ 0.02/0.15 ≈ 0.133，但受 name_cap 0.05 约束
    assert alpha_position_cap(1.0, 0.15, cfg) == 0.05
    # 止损 60% → weight ≤ 0.0333 < name_cap
    assert abs(alpha_position_cap(1.0, 0.60, cfg) - 0.02 / 0.60) < 1e-9


def test_allocate_sleeves_zero_leverage():
    cfg = ArbConfig()
    a = allocate_sleeves(cfg, fear_score=50)
    assert abs(a["defense_A"] + a["offense_B"] + a["alpha"] + a["cash"] - 1.0) < 1e-9
    assert not a["_panic"]


def test_allocate_sleeves_panic_ammo():
    cfg = ArbConfig()
    a = allocate_sleeves(cfg, fear_score=80)     # ≥75 触发恐慌弹药
    assert a["_panic"]
    # 恐慌弹药：进攻顶到上限、防守压到下限，Σ 仍守零杠杆
    assert a["offense_B"] == cfg.offense_max
    assert a["defense_A"] == cfg.defense_min
    assert a["alpha"] <= cfg.alpha_max
    assert abs(a["defense_A"] + a["offense_B"] + a["alpha"] + a["cash"] - 1.0) < 1e-9
    assert a["cash"] >= 0
