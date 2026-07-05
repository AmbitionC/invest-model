"""套利模块配置：config-as-strategy（对齐 LoopConfig 约定，不引入策略类注册表）。

主开关默认关（观察态）。环境变量覆盖：
  ARB_ENABLED=1        启用套利资金账本（默认 0=观察态，计划回退纯引擎 B）
  ARB_WATERTILT=1      启用三水表倾斜引擎 B（默认 0=影子，倾斜乘子=1.0）
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# 版本族（回测/派生行按此命名空间隔离，与 cs_ic_v1/pf_v2 同族）。
VERSION_REPO = "arb_repo_v1"
VERSION_DIV = "arb_div_v1"
VERSION_CB = "arb_cb_v1"
VERSION_ALPHA = "arb_alpha_v1"
VERSION_FLOW = "arb_flow_v1"
VERSION_LEDGER = "arb_ledger_v1"


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() not in ("0", "false", "no", "off")


@dataclass
class ArbConfig:
    # ── 主开关（一键回退：ARB_ENABLED=0 → 计划等同今天的纯引擎 B）──
    enabled: bool = False
    watermeter_tilt: bool = False          # 三水表倾斜引擎 B（影子默认关）

    # ── 三 sleeve 上下限（单一资金池，Σ≤100% 零杠杆）──
    defense_min: float = 0.50
    defense_max: float = 0.60
    offense_min: float = 0.30
    offense_max: float = 0.40
    alpha_min: float = 0.05
    alpha_max: float = 0.15

    # ── 逐策略开关（数据缺失时对应策略自动降级，预算入现金）──
    reverse_repo: bool = True
    dividend: bool = True
    convertible: bool = True
    panic_ammo: bool = True                # 恐慌弹药：极端恐慌把现金→进攻/α

    # ── 模型参数 ──
    tilt_boost: float = 1.15               # 命中高 flow_score 行业的 conviction 乘子
    flow_threshold: float = 0.5            # composite z 超此值视为水表共振
    double_low_top_n: int = 20
    dividend_top_n: int = 15
    dividend_min_dv: float = 3.0           # 股息率(%)下限
    alpha_name_cap: float = 0.05           # α 单票上限
    fear_ammo_trigger: float = 75.0        # fear_gauge≥此值触发恐慌弹药
    # A 股红利差别税：持有期→税率
    tax_short: float = 0.20                # <1 月
    tax_mid: float = 0.10                  # 1 月-1 年
    tax_long: float = 0.0                  # >1 年（防守取长持免税桶）

    # ── 治理 ──
    watermeter_promote_periods: int = 12   # 水表影子 IC 攒够 N 期方可晋升
    version: str = VERSION_LEDGER

    @classmethod
    def from_env(cls, **overrides) -> "ArbConfig":
        cfg = cls(**overrides)
        cfg.enabled = _env_bool("ARB_ENABLED", cfg.enabled)
        cfg.watermeter_tilt = _env_bool("ARB_WATERTILT", cfg.watermeter_tilt)
        return cfg

    def sleeve_bounds(self) -> dict[str, tuple[float, float]]:
        return {
            "defense_A": (self.defense_min, self.defense_max),
            "offense_B": (self.offense_min, self.offense_max),
            "alpha": (self.alpha_min, self.alpha_max),
        }
